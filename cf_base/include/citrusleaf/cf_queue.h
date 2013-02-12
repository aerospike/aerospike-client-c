/*
 *  Citrusleaf Foundation
 *  include/queue.h - queue structures
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_base_types.h"


/* SYNOPSIS
 * Queue
 */
 
#ifdef __cplusplus
extern "C" {
#endif


/* cf_queue
 * A queue */
#define CF_QUEUE_ALLOCSZ 64
typedef struct cf_queue_s {
	bool threadsafe;  // sometimes it's good to live dangerously
	unsigned int allocsz;      // number of queue elements currently allocated
	unsigned int write_offset; // 0 offset is first queue element.
						   // write is always greater than or equal to read
	unsigned int read_offset; // 
	size_t elementsz;     // number of bytes in an element
#ifdef EXTERNAL_LOCKS
	void *LOCK; // the lock object
#else
	pthread_mutex_t LOCK;  // the mutex lock
	pthread_cond_t CV;    // hte condvar
#endif // EXTERNAL_LOCKS
	uint8_t *queue;         // the actual bytes that make up the queue
} cf_queue;

#define CF_Q_SZ(__q) (__q->write_offset - __q->read_offset)

#define CF_Q_EMPTY(__q) (__q->write_offset == __q->read_offset)

// todo: maybe it's faster to keep the read and write offsets in bytes,
// to avoid the extra multiply?
#define CF_Q_ELEM_PTR(__q, __i) (&__q->queue[ (__i % __q->allocsz) * __q->elementsz ] )


/* External functions */
extern cf_queue *cf_queue_create(size_t elementsz, bool threadsafe);

extern void cf_queue_destroy(cf_queue *q);

// Always pushes to the end of the queue
extern int cf_queue_push(cf_queue *q, void *ptr);

// Push element on the queue only if size < limit.
extern bool cf_queue_push_limit(cf_queue *q, void *ptr, uint32_t limit);

// Get the number of elements currently in the queue
extern int cf_queue_sz(cf_queue *q);




// POP pops from the end of the queue, which is the most efficient
// But understand this makes it LIFO, the least fair of queues
// Elements added at the very beginning might not make it out

#define CF_QUEUE_EMPTY -2
#define CF_QUEUE_ERR -1
#define CF_QUEUE_OK 0

// mswait < 0 wait forever
// mswait == 0 wait not at all
// mswait > 0 wait that number of ms
#define CF_QUEUE_FOREVER -1
#define CF_QUEUE_NOWAIT 0
extern int cf_queue_pop(cf_queue *q, void *buf, int mswait);

// Queue Reduce
// Run the entire queue, calling the callback, with the lock held
// You can return values in the callback to cause deletes
// Great for purging dying stuff out of a queue synchronously
//
// Return -2 from the callback to trigger a delete
// return -1 stop iterating the queue
// return 0 for success
typedef int (*cf_queue_reduce_fn) (void *buf, void *udata);

extern int cf_queue_reduce(cf_queue *q, cf_queue_reduce_fn cb, void *udata);

//
// The most common reason to want to 'reduce' is delete - so provide
// a simple delete function
extern int cf_queue_delete(cf_queue *q, void *buf, bool only_one);


//
// A simple priority queue implementation, which is simply a set of queues
// underneath
// This currently doesn't support 'delete' and 'reduce' functionality
//

typedef struct cf_queue_priority_s {
	bool threadsafe;
	cf_queue	*low_q;
	cf_queue	*medium_q;
	cf_queue	*high_q;
#ifdef EXTERNAL_LOCKS
	void *		LOCK;
#else	
	pthread_mutex_t		LOCK;
	pthread_cond_t	 	CV;
#endif
} cf_queue_priority;

#define CF_QUEUE_PRIORITY_HIGH 1
#define CF_QUEUE_PRIORITY_MEDIUM 2
#define CF_QUEUE_PRIORITY_LOW 3

#define CF_Q_PRI_EMPTY(__q) (CF_Q_EMPTY(__q->low_q) && CF_Q_EMPTY(__q->medium_q) && CF_Q_EMPTY(__q->high_q))

extern cf_queue_priority *cf_queue_priority_create(size_t elementsz, bool threadsafe);
extern void cf_queue_priority_destroy(cf_queue_priority *q);
extern int cf_queue_priority_push(cf_queue_priority *q, void *ptr, int pri);
extern int cf_queue_priority_pop(cf_queue_priority *q, void *buf, int mswait);
extern int cf_queue_priority_sz(cf_queue_priority *q);



//
// Call this function to do a set of internal validation unit tests
// 0 means success, and it blocks until complete

extern int cf_queue_test();

#ifdef __cplusplus
} // end extern "C"
#endif

