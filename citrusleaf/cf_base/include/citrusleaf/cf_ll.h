/*
 *  Citrusleaf Foundation
 *  include/ll.h - linked list functionality
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#pragma once

#include <pthread.h>
#include <stdint.h>

#include "citrusleaf/cf_base_types.h"

/* SYNOPSIS
 * LinkedList
 * Sometimes the answer is a doubly linked list. It's not that frequent, but
 * all the corner cases in a double linked list can be annoying.
 *
 * the current use pattern is the caller creates a structure that starts with a 'cf_ll_element',
 * ie, can be cast to a cf_ll_element. The caller allocates and frees the memory.
 * (There are far cooler ways to do this, so if you want to improve this, go ahead!
 *
 */

#ifdef __cplusplus
extern "C" {
#endif
 
//
// Callbacks in use!
// These function expose functional programming paradigms across linked
// lists
 
 
#define CF_LL_REDUCE_DELETE (1)
#define CF_LL_REDUCE_INSERT (2)

struct cf_ll_element_s;
typedef int (*cf_ll_reduce_fn) (struct cf_ll_element_s *e, void *udata);
typedef void (*cf_ll_destructor) (struct cf_ll_element_s *e);
 
 
/* cf_ll_element
 * The element that must be included in structures
 * This element should be the FIRST element in the structure where it is being included
 */

typedef struct cf_ll_element_s {
	struct cf_ll_element_s *next;
	struct cf_ll_element_s *prev;
} cf_ll_element;

/* cf_ll
** the linked list container
*/



typedef struct cf_ll_s {
	struct cf_ll_element_s *head;
	struct cf_ll_element_s *tail;
	cf_ll_destructor		destroy_fn;
	uint32_t			sz;
	bool				uselock;
#ifdef EXTERNAL_LOCKS
	void               *LOCK;
#else
	pthread_mutex_t		LOCK;
#endif //EXTERNAL_LOCKS
} cf_ll;

// Insert to head
extern void cf_ll_prepend(cf_ll *ll, cf_ll_element *e );
// Insert to tail
extern void cf_ll_append(cf_ll *ll, cf_ll_element *e );

// Insert after element !! warning! consider threadsafety before using this call!
extern void cf_ll_insert_after(cf_ll *ll, cf_ll_element *cur, cf_ll_element *ins);
// Insert before element !! warning! consider threadsafey before using this call!
extern void cf_ll_insert_before(cf_ll *ll, cf_ll_element *cur, cf_ll_element *ins);

static inline cf_ll_element *cf_ll_get_head(cf_ll *ll)
{
	return(ll->head);
}

static inline cf_ll_element *cf_ll_get_tail(cf_ll *ll)
{
	return(ll->tail);
}

static inline cf_ll_element *cf_ll_get_next(cf_ll_element *e)
{
	return(e->next);
}

static inline cf_ll_element *cf_ll_get_prev(cf_ll_element *e)
{
	return(e->prev);
}

// delete element - the real joy of a doubly linked list
// If a destructor function has been set, call it as well
extern void cf_ll_delete(cf_ll *ll, cf_ll_element *e );

extern uint32_t cf_ll_size(cf_ll *ll);

//
// The way these reduces work:
// ** If you're reducing and you want to delete this element, return CF_LL_REDUCE_DELETE
// and it'll be removed from the list - but iteration will not halt
// ** If you return a negative value, the reduction will terminate immediatly and that
// return value will be passed to the reducer
// ** The 'forward' parameter specifies whether you want to traverse from front to back,
// pass in 'false' to go tail-to-head


extern int cf_ll_reduce( cf_ll *ll,  bool forward, cf_ll_reduce_fn fn, void *udata);

//
// Insert-before
// Sometimes you want to iterate a list, and insert before a certain element.
// Common when you're trying to keep a sorted list and you have some knowledge
// that either the list is short, or you're doing inserts of a particular pattern
// so that a sorted-table is not the right answer.
//
// Similar to the reduce function: if you want to bail out of the insert, return a negative
// If you want to insert "here", return the special code
// At the end of the list, you will be passed a null element (thus meaning you'll always
// be called at least once)


extern int cf_ll_insert_reduce(cf_ll *ll, cf_ll_element *e, bool forward, cf_ll_reduce_fn fn, void *udata);


/* Call this function on a head structure to initialize it to empty
** Call with whether you want a locked version of a lockfree version
** if you're handling your own locks
*/

/*
** If you're using a standard delete methodology, then don't need a destructor function,
** and can leave it blank. But if you're using the reduce / delete pattern, then
** there's not an easy way for the application-level delete to occur, because you can't
** free the structure first then call delete. (you could insert the element on a queue,
** but it would have to carefully be a reference counted object, and then you'd still
** need the linked-list-reduceor to decrement the linked list....)
** In that case, you need to have a destructor
** function that gets fired every time a removal from the list occurs. even on explicit
** deletes it's called, just to be fancy.
**
** Note that when the destructor is called, the lock for the linked list is held
** (if you've allocated the linked list with a lock)
*/


extern int cf_ll_init(cf_ll *ll, cf_ll_destructor destroy_fn, bool uselock);

#ifdef __cplusplus
} // end extern "C"
#endif


