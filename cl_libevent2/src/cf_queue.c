/*
 *  Citrusleaf Foundation
 *  src/queue.c - queue framework
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>
#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"

#include "citrusleaf_event2/cf_queue.h"

// #define DEBUG 1

/* SYNOPSIS
 * Queue
 */


/* cf_queue_create
 * Initialize a queue */
cf_queue *
cf_queue_create(size_t elementsz)
{
	cf_queue *q = NULL;

	q = malloc( sizeof(cf_queue));
	/* FIXME error msg */
	if (!q)
		return(NULL);
	q->allocsz = CF_QUEUE_ALLOCSZ;
	q->write_offset = q->read_offset = 0;
	q->elementsz = elementsz;
	q->LOCK = NULL;

	q->queue = malloc(CF_QUEUE_ALLOCSZ * elementsz);
	if (! q->queue) {
		free(q);
		return(NULL);
	}

	if (!g_lock_cb)
		return(q);

	if ((q->LOCK = g_lock_cb->alloc()) == NULL) {
		/* FIXME error msg */
		free(q->queue);
		free(q);
		return(NULL);
	}

	return(q);
}

// TODO: probably need to wait until whoever is inserting or removing is finished
// Sort of. Anyone in a race with the destructor, who has a pointer to the queue,
// is in jepardy anyway.

void
cf_queue_destroy(cf_queue *q)
{
	MUTEX_FREE(q->LOCK);

	memset(q->queue, 0, q->allocsz * q->elementsz);
	free(q->queue);
	memset(q, 0, sizeof(cf_queue) );
	free(q);
}

int
cf_queue_sz(cf_queue *q)
{
	int rv;

	MUTEX_LOCK(q->LOCK);
	rv = CF_Q_SZ(q);
	MUTEX_UNLOCK(q->LOCK);

	return(rv);
	
}

//
// Internal function. Call with new size with lock held.
// *** THIS ONLY WORKS ON FULL QUEUES ***
//
int
cf_queue_resize(cf_queue *q, uint new_sz)
{
	// check - a lot of the code explodes badly if queue is not full
	if (CF_Q_SZ(q) != q->allocsz) {
//		cf_debug(CF_QUEUE,"cf_queue: internal error: resize on non-full queue");
		return(-1);
	}
	
	// the rare case where the queue is not fragmented, and realloc makes sense
	// and none of the offsets need to move
	if (0 == q->read_offset % q->allocsz) {
		q->queue = realloc(q->queue, new_sz * q->elementsz);
		if (!q->queue) {
//			cf_debug(CF_QUEUE," pfft! out of memory! crash!");
			return(-1);
		}
		q->read_offset = 0;
		q->write_offset = q->allocsz;
	}
	else {
		
		uint8_t *newq = malloc(new_sz * q->elementsz);
		if (!newq) {
//			cf_debug(CF_QUEUE," pffth! out of memory! crash!");
			return(-1);
		}
		// endsz is used uint8_ts in the old queue from the insert point to the end
		uint endsz = (q->allocsz - (q->read_offset % q->allocsz)) * q->elementsz;
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
 * 
 * */
int
cf_queue_push(cf_queue *q, void *ptr)
{
	/* FIXME arg check - and how do you do that, boyo? Magic numbers? */

	MUTEX_LOCK(q->LOCK);

	/* Check queue length */
	if (CF_Q_SZ(q) == q->allocsz) {
		/* resize is a pain for circular buffers */
		if (0 != cf_queue_resize(q, q->allocsz + CF_QUEUE_ALLOCSZ)) {
			MUTEX_UNLOCK(q->LOCK);
			return(-1);
		}
	}

	// todo: if queues are power of 2, this can be a shift
	memcpy(CF_Q_ELEM_PTR(q,q->write_offset), ptr, q->elementsz);
	q->write_offset++;
	// we're at risk of overflow if the write offset is that high
	if (q->write_offset & 0x80000000) cf_queue_unwrap(q);

	MUTEX_UNLOCK(q->LOCK);

	return(0);
}


/* cf_queue_pop
 * if ms_wait < 0, wait forever
 * if ms_wait = 0, don't wait at all
 * if ms_wait > 0, wait that number of ms
 * */
int
cf_queue_pop(cf_queue *q, void *buf, int ms_wait)
{
	if (NULL == q)
		return(-1);

	if (ms_wait != CF_QUEUE_NOWAIT) {   // this implementation won't wait
		fprintf(stderr, "cf_queue_pop: only nowait supported\n");
		return(-1);
	}

	MUTEX_LOCK(q->LOCK);

	if (CF_Q_EMPTY(q)) {
		MUTEX_UNLOCK(q->LOCK);
		return(CF_QUEUE_EMPTY);
	}

	memcpy(buf, CF_Q_ELEM_PTR(q,q->read_offset), q->elementsz);
	q->read_offset++;
	
	// interesting idea - this probably keeps the cache fresher
	// because the queue is fully empty just make it all zero
	if (q->read_offset == q->write_offset) {
		q->read_offset = q->write_offset = 0;
	}

	MUTEX_UNLOCK(q->LOCK);

	return(0);
}


void
cf_queue_delete_offset(cf_queue *q, uint index)
{
	index %= q->allocsz;
	uint r_index = q->read_offset % q->allocsz;
	uint w_index = q->write_offset % q->allocsz;
	
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

	MUTEX_LOCK(q->LOCK);

	if (CF_Q_SZ(q)) {
		
		// it would be faster to have a local variable to hold the index,
		// and do it in uint8_ts or something, but a delete
		// will change the read and write offset, so this is simpler for now
		// can optimize if necessary later....
		
		for (uint i = q->read_offset ; 
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
				break;
			}
		};
	}

	MUTEX_UNLOCK(q->LOCK);

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

	MUTEX_LOCK(q->LOCK);

	bool found = false;
	
	if (CF_Q_SZ(q)) {

		for (uint i = q->read_offset ; 
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
	MUTEX_UNLOCK(q->LOCK);

	if (found == false)
		return(CF_QUEUE_EMPTY);
	else
		return(CF_QUEUE_OK);
}


