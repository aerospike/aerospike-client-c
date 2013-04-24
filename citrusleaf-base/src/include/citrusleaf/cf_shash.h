/*
 * A general purpose hashtable implementation
 * Uses locks, so only moderately fast
 * Just, hopefully, the last hash table you'll ever need
 * And you can keep adding cool things to it
 * Copywrite 2008 Brian Bulkowski
 * All rights reserved
 */

#pragma once
 
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/cf_base_types.h"

// #define CITRUSLEAF 1

#define SHASH_ERR_FOUND -4
#define SHASH_ERR_NOTFOUND -3
#define SHASH_ERR_BUFSZ -2
#define SHASH_ERR -1
#define SHASH_OK 0

#ifdef CITRUSLEAF
#include "cf.h"
#else
typedef uint8_t byte;
#endif

/* cf_hash_fnv
 * The 64-bit Fowler-Noll-Voll hash function (FNV-1a) */
//
// This algorithm is PUBLIC DOMAIN:
//
// FNV hash algorithms and source code have been released into the public domain. The authors of the
// FNV algorithmm look deliberate steps to disclose the algorhtm in a public forum soon after it was
// invented. More than a year passed after this public disclosure and the authors deliberatly took no
// steps to patent the FNV algorithm. Therefore it is safe to say that the FNV authors have no patent
// claims on the FNV algorithm as published.
// Source: http://www.isthe.com/chongo/tech/comp/fnv/index.html
//
// FNV on Wikipedia: http://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
//
static inline uint64_t
cf_hash_fnv(void *buf, size_t bufsz)
{
	uint64_t hash = 0xcbf29ce484222325ULL;
	uint8_t *bufp = (uint8_t *)buf;
	uint8_t *bufe = bufp + bufsz;

	while (bufp < bufe) {
		/* XOR the current byte into the bottom of the hash */
		hash ^= (uint64_t)*bufp++;

		/* Multiply by the 64-bit FNV magic prime */
		hash *= 0x100000001b3ULL;
	}

	return(hash);
}

/*
 * A generic call for hash functions the user can create
 */
typedef uint32_t (*shash_hash_fn) (void *key);

/*
** Typedef for a "reduce" fuction that is called on every node
** (Note about return value: some kinds of reduces can manipulate the hash table,
**  allowing deletion. See the particulars of the reduce call.)
*/
typedef int (*shash_reduce_fn) (void *key, void *data, void *udata);

// Simple (and slow) element is when
// everything is variable (although a very nicely packed structure for 32 or 64
typedef struct shash_elem_s {
	struct shash_elem_s *next;
	bool	in_use;
	uint8_t	data[];   // key_len bytes of key, value_len bytes of value
} shash_elem;


#define SHASH_ELEM_KEY_PTR(_h, _e) 	( (void *) _e->data )

#define SHASH_ELEM_VALUE_PTR(_h, _e) ( (void *) (_e->data + _h->key_len) )

typedef struct shash_s {
	unsigned int elements; // INVALID in manylocks case - see notes under get_size
	uint32_t key_len;
	uint32_t value_len;
	unsigned int flags;
	shash_hash_fn	h_fn;
	
	unsigned int table_len; // number of elements currently in the table
	void *table;
	pthread_mutex_t		biglock;
	pthread_mutex_t		*lock_table;
} shash;

#define SHASH_ELEM_SZ(_h) ( sizeof(shash_elem) + (_h->key_len) + (_h->value_len) )

#define SHASH_CR_RESIZE 0x01   // support resizes (will sometimes hang for long periods)
#define SHASH_CR_GRAB   0x02   // support 'grab' call (requires more memory)
#define SHASH_CR_MT_BIGLOCK 0x04 // support multithreaded access with a single big lock
#define SHASH_CR_MT_MANYLOCK 0x08 // support multithreaded access with a pool of object loccks

#define SHASH_REDUCE_DELETE (1) // indicate that a delete should be done during the reduction

/*
 * Create a hash table
 * Pass in the hash function (required)
 * the key length if static (if not static pass 0
 * the value length if static (if not static pass 0
 * The initial table size
 * a set of flags
 */

int
shash_create(shash **h, shash_hash_fn h_fn, uint32_t key_len, uint32_t value_len, uint32_t sz, unsigned int flags);

/* Place a value into the hash
 * Value will be copied into the hash
 */
int
shash_put(shash *h, void *key, void *value);
int
shash_put_unique(shash *h, void *key, void *value);

/* call with the buffer you want filled; if you just want to check for
 * existence, call with value set to NULL
 */
int
shash_get(shash *h, void *key, void *value);

/* Returns the pointer to the internal item, and a locked-lock
 * which allows the touching of internal state. If non-lock hash table,
 * vlock param will be ignored
 *
 * Note that the vlock is passed back only when the return code is BB_OK.
 * In the case where nothing is found, no lock is held.
 * It might be better to do it the other way, but you can change it later if you want
 */
int
shash_get_vlock(shash *h, void *key, void **value,pthread_mutex_t **vlock);

/* Does a get and delete at the same time so you can make sure only one person
 * gets what was inserted
 */
int
shash_get_and_delete(shash *h, void *key, void *value);


/*
** Got a key you want removed - this is the function to call
*/
int
shash_delete(shash *h, void *key);

/*
** Special function you can call when you already have the lock - such as
** a vlock get
*/
int
shash_delete_lockfree(shash *h, void *key);


/*
** Get the number of elements currently in the hash
*/
uint32_t
shash_get_size(shash *h);

/*
 * An interesting idea: readv / writev for these functions?
 */

/* Find / get a value from the hash
 * But take the reference count on the object; must be returned with the
 * return call
 */
int
shash_grab(shash *h, void *key, uint32_t key_len, void **value, uint32_t *value_len);

/* Return a value that has been gotten
 */
int
shash_return(shash *h, void *value);

/*
** Map/Reduce pattern - call the callback on every element in the hash
** Warning: the entire traversal can hold the lock in the 'biglock' case,
** so make the reduce_fn lightweight! Consider queuing or soemthing if you
** want to do something fancy
*/
int
shash_reduce(shash *h, shash_reduce_fn reduce_fn, void *udata);

/*
** Map/Reduce pattern - call the callback on every element in the hash
** This instance allows deletion of hash elements during the reduce:
** return -1 to cause the deletion of the element visisted
*/

int
shash_reduce_delete(shash *h, shash_reduce_fn reduce_fn, void *udata);


/*
 * Destroy the entire hash - all memory will be freed
 */
void
shash_destroy(shash *h);

