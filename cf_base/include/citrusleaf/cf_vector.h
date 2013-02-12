/*
 * A general purpose vector
 * Uses locks, so only moderately fast
 * If you need to deal with sparse data, really sparse data,
 * use a hash table. This assumes that packed data is a good idea.
 * Does the fairly trivial realloc thing for extension,
 * so 
 * And you can keep adding cool things to it
 * Copywrite 2008 Brian Bulkowski
 * All rights reserved
 */

#pragma once
 
#include <pthread.h>
#include <stdint.h>

#include "citrusleaf/cf_base_types.h"


#ifdef __cplusplus
extern "C" {
#endif

typedef struct cf_vector_s {
	uint32_t value_len;
	unsigned int flags;
	unsigned int alloc_len; // number of elements currently allocated
	unsigned int len;       // number of elements in table, largest element set
	uint8_t *vector;
	bool	stack_struct;
	bool	stack_vector;
#ifdef EXTERNAL_LOCKS
	void    *LOCK;
#else
	pthread_mutex_t		LOCK;
#endif // EXTERNAL_LOCKS
} cf_vector;


#define VECTOR_ELEM_SZ(_v) ( _h->value_len )

#define VECTOR_FLAG_BIGLOCK 0x01 // support multithreaded access with a single big lock
#define VECTOR_FLAG_INITZERO 0x02 // internally init the vector objects to 0
#define VECTOR_FLAG_BIGRESIZE 0x04 // appends will be common - speculatively allocate extra memory
#define VECTOR_REDUCE_DELETE (1) // indicate that a delete should be done during the reduction


/*
 * Create a vector with malloc for handing around
 */

cf_vector *
cf_vector_create(uint32_t value_len, uint32_t init_sz, unsigned int flags);

/*
** create a stack vector, but with an allocated internal-vector-bit
*/

int
cf_vector_init(cf_vector *v, uint32_t value_len, uint32_t init_sz, unsigned int flags);

void
cf_vector_init_smalloc(cf_vector *v, uint32_t value_len, uint8_t *sbuf, int sbuf_sz, unsigned int flags);

#define cf_vector_define(__x, __value_len, __flags) \
	uint8_t cf_vector##__x[1024]; cf_vector __x; cf_vector_init_smalloc(&__x, __value_len, cf_vector##__x, sizeof(cf_vector##__x), __flags);

void
cf_vector_clone_stack(cf_vector *source, cf_vector *target, uint8_t *target_buf);

#define cf_vector_define_clone(__source, __target) \
	uint8_t cf_vector##__target[__source.len]; cf_vector __target; cf_vector_clone_stack(&__source, &__target, cf_vector##__target);

/* Place a value into the hash
 * Value will be copied into the hash
 */
extern int cf_vector_set(cf_vector *v, uint32_t index, void *value);
extern int cf_vector_get(cf_vector *v, uint32_t index, void *value);
// this is very dangerous if it's a multithreaded vector. Use _vlock if multithrad.
extern void * cf_vector_getp(cf_vector *v, uint32_t index);
extern void * cf_vector_getp_vlock(cf_vector *v, uint32_t index, pthread_mutex_t **vlock);
extern int cf_vector_append(cf_vector *v, void *value);

#define cf_vector_reset( __v ) (__v)->len = 0; if ( (__v)->flags & VECTOR_FLAG_INITZERO) memset( (__v)->vector, 0, (__v)->alloc_len * (__v)->value_len);

// Adds a an element to the end, only if it doesn't exist already
// uses a bit-by-bit compare, thus is O(N) against the current length
// of the vector
extern int cf_vector_append_unique(cf_vector *v, void *value);

// Deletes an element by moving all the remaining elements down by one
extern int cf_vector_delete(cf_vector *v, uint32_t index);
// Delete a range in the vector. Inclusive. Thus:
//   a vector with len 5, you could delete start=0, end=3, leaving one element at the beginning (slot 0)
//   don't set start and end the same, that's a single element delete, use vector_delete instead
//   (or change the code to support that!)
//   returns -1 on bad ranges
extern int cf_vector_delete_range(cf_vector *v, uint32_t start_index, uint32_t end_index);

// There may be more allocated than you need. Fix that.
//
extern void cf_vector_compact(cf_vector *v);


/*
** Get the number of elements currently in the vector
*/
static inline unsigned int cf_vector_size(cf_vector *v)
{
	return(v->len);	
}


/*
 * Destroy the entire hash - all memory will be freed
 */
extern void cf_vector_destroy(cf_vector *v);

/*
** nice wrapper functions
** very common vector types are pointers, and integers
*/

static inline cf_vector *cf_vector_pointer_create(uint32_t init_sz, uint32_t flags)
{
	return(cf_vector_create(sizeof(void *), init_sz, flags));
}

static inline int cf_vector_pointer_init(cf_vector *v, uint32_t init_sz, uint32_t flags)
{
	return(cf_vector_init(v, sizeof(void *), init_sz, flags));
}

static inline int cf_vector_pointer_set(cf_vector *v, uint32_t index, void *p)
{
	return(cf_vector_set(v, index, &p));
}

static inline void * cf_vector_pointer_get(cf_vector *v, uint32_t index) {
	void *p;
	cf_vector_get(v, index, &p);
	return(p);
}

static inline int cf_vector_pointer_append(cf_vector *v, void *p)
{
	return(cf_vector_append(v, &p));
}

/*
** integer vectors!
*/

static inline cf_vector *cf_vector_integer_create(uint32_t init_sz, uint32_t flags)
{
	return(cf_vector_create(sizeof(int), init_sz, flags));
}

static inline int cf_vector_integer_init(cf_vector *v, uint32_t init_sz, uint32_t flags)
{
	return(cf_vector_init(v, sizeof(int), init_sz, flags));
}

static inline int cf_vector_integer_set(cf_vector *v, uint32_t index, int i)
{
	return(cf_vector_set(v, index, &i));
}

static inline int cf_vector_integer_get(cf_vector *v, uint32_t index) {
	int i;
	cf_vector_get(v, index, &i);
	return(i);
}

static inline int cf_vector_integer_append(cf_vector *v, int i)
{
	return(cf_vector_append(v, &i));
}

#ifdef __cplusplus
} // end extern "C"
#endif


