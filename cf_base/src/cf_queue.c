/*
 *  Citrusleaf Foundation
 *  src/queue.c - queue framework
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/cf_base_types.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_errno.h"
#ifdef EXTERNAL_LOCKS
#include "citrusleaf/cf_hooks.h"
#endif
#include "citrusleaf/cf_log_internal.h"

#include "citrusleaf/cf_queue.h"


// #define DEBUG 1

#ifdef EXTERNAL_LOCKS
#define QUEUE_LOCK(_q) if (_q->threadsafe) \
		cf_hooked_mutex_lock(_q->LOCK);
#else
#define QUEUE_LOCK(_q) if (_q->threadsafe) \
		pthread_mutex_lock(&_q->LOCK);
#endif 

#ifdef EXTERNAL_LOCKS
#define QUEUE_UNLOCK(_q) if (_q->threadsafe) \
		cf_hooked_mutex_unlock(_q->LOCK);
#else
#define QUEUE_UNLOCK(_q) if (_q->threadsafe) \
		pthread_mutex_unlock(&_q->LOCK);
#endif 

/* SYNOPSIS
 * Queue
 */


/* cf_queue_create
 * Initialize a queue */
cf_queue *
cf_queue_create(size_t elementsz, bool threadsafe)
{
	cf_queue *q = NULL;

	q = (cf_queue*)malloc( sizeof(cf_queue));
	/* FIXME error msg */
	if (!q)
		return(NULL);
	q->allocsz = CF_QUEUE_ALLOCSZ;
	q->write_offset = q->read_offset = 0;
	q->elementsz = elementsz;
	q->threadsafe = threadsafe;

	q->queue = (uint8_t*)malloc(CF_QUEUE_ALLOCSZ * elementsz);
	if (! q->queue) {
		free(q);
		return(NULL);
	}

	if (!q->threadsafe)
		return(q);

#ifdef EXTERNAL_LOCKS
	if (!g_mutex_hooks)
		return(q);

	if ((q->LOCK = cf_hooked_mutex_alloc()) == NULL) {
		/* FIXME error msg */
		free(q->queue);
		free(q);
		return(NULL);
	}

#else
	if (0 != pthread_mutex_init(&q->LOCK, NULL)) {
		/* FIXME error msg */
		free(q->queue);
		free(q);
		return(NULL);
	}

	if (0 != pthread_cond_init(&q->CV, NULL)) {
		pthread_mutex_destroy(&q->LOCK);
		free(q->queue);
		free(q);
		return(NULL);
	}
#endif // EXTERNAL_LOCKS
	return(q);
}

// TODO: probably need to wait until whoever is inserting or removing is finished
// Sort of. Anyone in a race with the destructor, who has a pointer to the queue,
// is in jepardy anyway.

void
cf_queue_destroy(cf_queue *q)
{
	if (q->threadsafe) {
#ifdef EXTERNAL_LOCKS
		cf_hooked_mutex_free(q->LOCK);
#else
		pthread_cond_destroy(&q->CV);
		pthread_mutex_destroy(&q->LOCK);
#endif // EXTERNAL_LOCKS
	}
	memset((void*)q->queue, 0, q->allocsz * q->elementsz);
	free(q->queue);
	memset((void*)q, 0, sizeof(cf_queue) );
	free(q);
}

int
cf_queue_sz(cf_queue *q)
{
	int rv;

	QUEUE_LOCK(q);
	rv = CF_Q_SZ(q);
	QUEUE_UNLOCK(q);

	return(rv);
	
}

//
// Internal function. Call with new size with lock held.
// *** THIS ONLY WORKS ON FULL QUEUES ***
//
int
cf_queue_resize(cf_queue *q, uint32_t new_sz)
{
	// check - a lot of the code explodes badly if queue is not full
	if (CF_Q_SZ(q) != q->allocsz) {
//		cf_debug(CF_QUEUE,"cf_queue: internal error: resize on non-full queue");
		return(-1);
	}
	
	// the rare case where the queue is not fragmented, and realloc makes sense
	// and none of the offsets need to move
	if (0 == q->read_offset % q->allocsz) {
		q->queue = (uint8_t*)realloc(q->queue, new_sz * q->elementsz);
		if (!q->queue) {
//			cf_debug(CF_QUEUE," pfft! out of memory! crash!");
			return(-1);
		}
		q->read_offset = 0;
		q->write_offset = q->allocsz;
	}
	else {
		
		uint8_t *newq = (uint8_t*)malloc(new_sz * q->elementsz);
		if (!newq) {
//			cf_debug(CF_QUEUE," pffth! out of memory! crash!");
			return(-1);
		}
		// endsz is used uint8_ts in the old queue from the insert point to the end
		uint32_t endsz = (uint32_t)((q->allocsz - (q->read_offset % q->allocsz)) * q->elementsz);
		memcpy(&newq[0], CF_Q_ELEM_PTR(q, q->read_offset), endsz);
		memcpy(&newq[endsz], &q->queue[0], (q->allocsz * q->elementsz) - endsz); 
		
		free(q->queue);
		q->queue = newq;

		q->write_offset = q->allocsz;
		q->read_offset = 0;
	}

	q->allocsz = new_sz;
	return(0);	
}

// we have to guard against wraparound, call this occasionally
// I really expect this will never get called....
// HOWEVER it can be a symptom of a queue getting really, really deep
//
void
cf_queue_unwrap(cf_queue *q)
{
	int sz = CF_Q_SZ(q);
	q->read_offset %= q->allocsz;
	q->write_offset = q->read_offset + sz;
}


/* cf_queue_push
 * Push goes to the front, which currently means memcpying the entire queue contents
 * */
int
cf_queue_push(cf_queue *q, void *ptr)
{
	/* FIXME arg check - and how do you do that, boyo? Magic numbers? */

	/* FIXME error */

	QUEUE_LOCK(q);

	/* Check queue length */
	if (CF_Q_SZ(q) == q->allocsz) {
		/* resize is a pain for circular buffers */
		if (0 != cf_queue_resize(q, q->allocsz + CF_QUEUE_ALLOCSZ)) {
			QUEUE_UNLOCK(q);
			return(-1);
		}
	}

	// todo: if queues are power of 2, this can be a shift
	memcpy(CF_Q_ELEM_PTR(q,q->write_offset), ptr, q->elementsz);
	q->write_offset++;
	// we're at risk of overflow if the write offset is that high
	if (q->write_offset & 0xC0000000) cf_queue_unwrap(q);

#ifndef EXTERNAL_LOCKS	
	if (q->threadsafe)
		pthread_cond_signal(&q->CV);
#endif 

	QUEUE_UNLOCK(q);

	return(0);
}

/* cf_queue_push_limit
 * Push element on the queue only if size < limit.
 * */
bool
cf_queue_push_limit(cf_queue *q, void *ptr, uint32_t limit)
{
	QUEUE_LOCK(q);
	uint32_t size = CF_Q_SZ(q);

	if (size >= limit) {
		QUEUE_UNLOCK(q);
		return false;
	}

	/* Check queue length */
	if (size == q->allocsz) {
		/* resize is a pain for circular buffers */
		if (0 != cf_queue_resize(q, q->allocsz + CF_QUEUE_ALLOCSZ)) {
			QUEUE_UNLOCK(q);
			return false;
		}
	}

	memcpy(CF_Q_ELEM_PTR(q,q->write_offset), ptr, q->elementsz);
	q->write_offset++;
	// we're at risk of overflow if the write offset is that high
	if (q->write_offset & 0xC0000000) cf_queue_unwrap(q);

#ifndef EXTERNAL_LOCKS
	if (q->threadsafe)
		pthread_cond_signal(&q->CV);
#endif

	QUEUE_UNLOCK(q);
	return true;
}

/* cf_queue_pop
 * if ms_wait < 0, wait forever
 * if ms_wait = 0, don't wait at all
 * if ms_wait > 0, wait that number of ms
 * */
int
cf_queue_pop(cf_queue *q, void *buf, int ms_wait)
{
	if (NULL == q) {
		cf_error("cf_queue_pop: try passing in a queue");
		return(-1);
	}

#ifdef EXTERNAL_LOCKS 
	if (ms_wait != CF_QUEUE_NOWAIT) {   // this implementation won't wait
		cf_error("cf_queue_pop: only nowait supported");
		return(-1);
	}
#endif // EXTERNAL_LOCKS

	QUEUE_LOCK(q);

	struct timespec tp;
	if (ms_wait > 0) {
#ifdef OSX
		uint64_t curms = cf_getms(); // using the cl generic functions defined in cf_clock.h. It is going to have slightly less resolution than the pure linux version
		tp.tv_sec = (curms + ms_wait)/1000;
		tp.tv_nsec = (ms_wait %1000) * 1000000;
#else // linux
		clock_gettime( CLOCK_REALTIME, &tp); 
		tp.tv_sec += ms_wait / 1000;
		tp.tv_nsec += (ms_wait % 1000) * 1000000;
		if (tp.tv_nsec > 1000000000) {
			tp.tv_nsec -= 1000000000;
			tp.tv_sec++;
		}
#endif
	}

	/* FIXME error checking */
	/* Note that we apparently have to use a while() loop.  Careful reading
	 * of the pthread_cond_signal() documentation says that AT LEAST ONE
	 * waiting thread will be awakened... */
	if (q->threadsafe) {
#ifdef EXTERNAL_LOCKS
		if (CF_Q_EMPTY(q)) {
			QUEUE_UNLOCK(q);
			return(CF_QUEUE_EMPTY);
		}
#else
		while (CF_Q_EMPTY(q)) {
			if (CF_QUEUE_FOREVER == ms_wait) {
				pthread_cond_wait(&q->CV, &q->LOCK);
			}
			else if (CF_QUEUE_NOWAIT == ms_wait) {
				pthread_mutex_unlock(&q->LOCK);
				return(CF_QUEUE_EMPTY);
			}
			else {
				pthread_cond_timedwait(&q->CV, &q->LOCK, &tp);
				if (CF_Q_EMPTY(q)) {
					pthread_mutex_unlock(&q->LOCK);
					return(CF_QUEUE_EMPTY);
				}
			}
		}
#endif // EXTERNAL_LOCKS
	} else if (CF_Q_EMPTY(q))
		return(CF_QUEUE_EMPTY);

	memcpy(buf, CF_Q_ELEM_PTR(q,q->read_offset), q->elementsz);
	q->read_offset++;
	
	// interesting idea - this probably keeps the cache fresher
	// because the queue is fully empty just make it all zero
	if (q->read_offset == q->write_offset) {
		q->read_offset = q->write_offset = 0;
	}

	QUEUE_UNLOCK(q);

	return(0);
}


void
cf_queue_delete_offset(cf_queue *q, uint32_t index)
{
	index %= q->allocsz;
	uint32_t r_index = q->read_offset % q->allocsz;
	uint32_t w_index = q->write_offset % q->allocsz;
	
	// assumes index is validated!
	
	// if we're deleting the one at the head, just increase the readoffset
	if (index == r_index) {
		q->read_offset++;
		return;
	}
	// if we're deleting the tail just decrease the write offset
	if (w_index && (index == w_index - 1)) {
		q->write_offset--;
		return;
	}
	// and the memory copy is overlapping, so must use memmove
	if (index > r_index) {
		memmove( &q->queue[ (r_index + 1) * q->elementsz ],
			     &q->queue[ r_index * q->elementsz ],
				 (index - r_index) * q->elementsz );
		q->read_offset++;
		return;
	}
	
	if (index < w_index) {
		memmove( &q->queue[ index * q->elementsz ],
			     &q->queue[ (index + 1) * q->elementsz ],
			     (w_index - index - 1) * q->elementsz);
		q->write_offset--;
		return;
	}
	
//	cf_debug(CF_QUEUE,"QUEUE_DELETE_OFFSET: FAIL FAIL FAIL FAIL");

}
	
	

// Iterate over all queue members calling the callback

int 
cf_queue_reduce(cf_queue *q,  cf_queue_reduce_fn cb, void *udata)
{
	if (NULL == q)
		return(-1);

	QUEUE_LOCK(q);	

	if (CF_Q_SZ(q)) {
		
		// it would be faster to have a local variable to hold the index,
		// and do it in uint8_ts or something, but a delete
		// will change the read and write offset, so this is simpler for now
		// can optimize if necessary later....
		
		for (uint32_t i = q->read_offset ;
			 i < q->write_offset ;
			 i++)
		{
			
			int rv = cb(CF_Q_ELEM_PTR(q, i), udata);
			
			// rv == 0 i snormal case, just increment to next point
			if (rv == -1) {
				break; // found what it was looking for
			}
			else if (rv == -2) { // delete!
				cf_queue_delete_offset(q, i);
				goto Found;
			}
		};
	}
	
Found:	
	QUEUE_UNLOCK(q);

	return(0);
	
}

// Special case: delete elements from the queue
// pass 'true' as the 'only_one' parameter if you know there can be only one element
// with this value on the queue

int
cf_queue_delete(cf_queue *q, void *buf, bool only_one)
{
	if (NULL == q)
		return(CF_QUEUE_ERR);

	QUEUE_LOCK(q);
	
	bool found = false;
	
	if (CF_Q_SZ(q)) {

		for (uint32_t i = q->read_offset ;
			 i < q->write_offset ;
			 i++)
		{
			
			int rv = memcmp(CF_Q_ELEM_PTR(q,i), buf, q->elementsz);
			
			if (rv == 0) { // delete!
				cf_queue_delete_offset(q, i);
				found = true;
				if (only_one == true)	goto Done;
			}
		};
	}

Done:
	QUEUE_UNLOCK(q);

	if (found == false)
		return(CF_QUEUE_EMPTY);
	else
		return(CF_QUEUE_OK);
}


//
// Priority queue implementation
//

cf_queue_priority *
cf_queue_priority_create(size_t elementsz, bool threadsafe)
{
	cf_queue_priority *q = (cf_queue_priority*)malloc(sizeof(cf_queue_priority));
	if (!q)	return(0);
	
	q->threadsafe = threadsafe;
	q->low_q = cf_queue_create(elementsz, false);
	if (!q->low_q) 		goto Fail1;
	q->medium_q = cf_queue_create(elementsz, false);
	if (!q->medium_q)	goto Fail2;
	q->high_q = cf_queue_create(elementsz, false);
	if (!q->high_q)		goto Fail3;
	
	if (threadsafe == false)
		return(q);
#ifdef EXTERNAL_LOCKS
	q->LOCK = cf_hooked_mutex_alloc();
	if (!q->LOCK ) goto Fail5;
#else	
	if (0 != pthread_mutex_init(&q->LOCK, NULL))
		goto Fail4;

	if (0 != pthread_cond_init(&q->CV, NULL))
		goto Fail5;
#endif // EXTERNAL_LOCKS
	
	return(q);
	
Fail5:	
#ifdef EXTERNAL_LOCKS
	cf_hooked_mutex_free(q->LOCK);
#else
	pthread_mutex_destroy(&q->LOCK);
Fail4:	
#endif // EXTERNAL_LOCKS
	cf_queue_destroy(q->high_q);
Fail3:	
	cf_queue_destroy(q->medium_q);
Fail2:	
	cf_queue_destroy(q->low_q);
Fail1:	
	free(q);
	return(0);
}

void 
cf_queue_priority_destroy(cf_queue_priority *q)
{
	cf_queue_destroy(q->high_q);
	cf_queue_destroy(q->medium_q);
	cf_queue_destroy(q->low_q);
	if (q->threadsafe) {
#ifdef EXTERNAL_LOCKS
		cf_hooked_mutex_free(q->LOCK);
#else
		pthread_mutex_destroy(&q->LOCK);
		pthread_cond_destroy(&q->CV);
#endif // EXTERNAL_LOCKS
	}
	free(q);
}

int 
cf_queue_priority_push(cf_queue_priority *q, void *ptr, int pri)
{
	
	QUEUE_LOCK(q);
	
	int rv;
	if (pri == CF_QUEUE_PRIORITY_HIGH)
		rv = cf_queue_push(q->high_q, ptr);
	else if (pri == CF_QUEUE_PRIORITY_MEDIUM)
		rv = cf_queue_push(q->medium_q, ptr);
	else if (pri == CF_QUEUE_PRIORITY_LOW)
		rv = cf_queue_push(q->low_q, ptr);
	else {
		rv = -1;
	}

#ifndef EXTERNAL_LOCKS
	if (rv == 0 && q->threadsafe)
		pthread_cond_signal(&q->CV);
#endif 

	QUEUE_UNLOCK(q);
		
	return(rv);	
}

int 
cf_queue_priority_pop(cf_queue_priority *q, void *buf, int ms_wait)
{
	QUEUE_LOCK(q);

	struct timespec tp;
	if (ms_wait > 0) {
#ifdef OSX
		uint64_t curms = cf_getms(); // using the cl generic functions defined in cf_clock.h. It is going to have slightly less resolution than the pure linux version
		tp.tv_sec = (curms + ms_wait)/1000;
		tp.tv_nsec = (ms_wait %1000) * 1000000;
#else // linux
		clock_gettime( CLOCK_REALTIME, &tp); 
		tp.tv_sec += ms_wait / 1000;
		tp.tv_nsec += (ms_wait % 1000) * 1000000;
		if (tp.tv_nsec > 1000000000) {
			tp.tv_nsec -= 1000000000;
			tp.tv_sec++;
		}
#endif
	}

	if (q->threadsafe) {
#ifdef EXTERNAL_LOCKS
		if (CF_Q_PRI_EMPTY(q)) {
			QUEUE_UNLOCK(q);
			return -1;
		}
#else
		while (CF_Q_PRI_EMPTY(q)) {
			if (CF_QUEUE_FOREVER == ms_wait) {
				pthread_cond_wait(&q->CV, &q->LOCK);
			}
			else if (CF_QUEUE_NOWAIT == ms_wait) {
				pthread_mutex_unlock(&q->LOCK);
				return(CF_QUEUE_EMPTY);
			}
			else {
				pthread_cond_timedwait(&q->CV, &q->LOCK, &tp);
				if (CF_Q_PRI_EMPTY(q)) {
					pthread_mutex_unlock(&q->LOCK);
					return(CF_QUEUE_EMPTY);
				}
			}
		}
#endif //EXERNAL_LOCKS
	}
	
	int rv;
	if (CF_Q_SZ(q->high_q))
		rv = cf_queue_pop(q->high_q, buf, 0);
	else if (CF_Q_SZ(q->medium_q))
		rv = cf_queue_pop(q->medium_q, buf, 0);
	else if (CF_Q_SZ(q->low_q))
		rv = cf_queue_pop(q->low_q, buf, 0);
	else rv = CF_QUEUE_EMPTY;
		
	QUEUE_UNLOCK(q);

		
	return(rv);
}

int
cf_queue_priority_sz(cf_queue_priority *q)
{
	int rv = 0;
	QUEUE_LOCK(q);
	rv += cf_queue_sz(q->high_q);
	rv += cf_queue_sz(q->medium_q);
	rv += cf_queue_sz(q->low_q);
	QUEUE_UNLOCK(q);
	return(rv);
}



//
// Test code
// it's always really annoying to have a queue that malfunctions in some small way
// so best to have a pretty serious torture test suite
//
#if 0

#define TEST1_SZ 400
#define TEST1_INTERVAL 10

void *
cf_queue_test_1_write(void *arg)
{
	cf_queue *q = (cf_queue *) arg;
	
	for (int i=0; i<TEST1_SZ;i++) {
		
		usleep(TEST1_INTERVAL * 1000);
		
		int rv;
		rv = cf_queue_push(q, &i);
		if (0 != rv) {
			cf_error("queue push failed: error %d",rv);
			return((void *)-1);
		}
	}
	
	return((void *)0);
	
}	

void *
cf_queue_test_1_read(void *arg)
{
	cf_queue *q = (cf_queue *) arg;
	
	for (int i=0;i<TEST1_SZ;i++) {
		
		// sleep twice as long as the inserter, to test overflow
		usleep(TEST1_INTERVAL * 1000 * 2);
		
		int  v = -1;
		int rv = cf_queue_pop(q, &v, CF_QUEUE_FOREVER);
		if (rv != CF_QUEUE_OK) {
			cf_error("cf_queue_test1: pop error %d",rv);
			return((void *) -1);
		}
		if (v != i) {
			cf_error("cf_queue_test1: pop value error: %d should be %d",v,i);
			return((void *) -1);
		}
		
	}
	return((void *) 0);	
}


int
cf_queue_test_1()
{
	pthread_t 	write_th;
	pthread_t     read_th;
	cf_queue    *q;
	
	q = cf_queue_create(sizeof(int), true);
	
	pthread_create( & write_th, 0, cf_queue_test_1_write, q);
	
	pthread_create( & read_th, 0, cf_queue_test_1_read, q);
	
	void *th_return;
	
	if (0 != pthread_join(write_th, &th_return)) {
		cf_error("queue test 1: could not join1 %d",errno);
		return(-1);
	}
	
	if (0 != th_return) {
		cf_error("queue test 1: returned error %p",th_return);
		return(-1);
	}
	
	if (0 != pthread_join(read_th, &th_return)) {
		cf_error("queue test 1: could not join2 %d",errno);
		return(-1);
	}
	
	if (0 != th_return) {
		cf_error("queue test 1: returned error 2 %p",th_return);
		return(-1);
	}
	
	cf_queue_destroy(q);
	
	return(0);
}

int
cf_queue_test()
{
	if (0 != cf_queue_test_1()) {
		cf_error("CF QUEUE TEST ONE FAILED");
		return(-1);
	}
	
	return(0);
}

#endif // 0
