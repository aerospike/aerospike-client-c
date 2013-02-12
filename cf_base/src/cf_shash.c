/*
 * A general purpose hashtable implementation
 * Good at multithreading
 * Just, hopefully, the last reasonable hash table you'll ever need
 * Copywrite 2008 Brian Bulkowski
 * All rights reserved
 */

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/cf_shash.h"


int
shash_create(shash **h_r, shash_hash_fn h_fn, uint32_t key_len, uint32_t value_len, uint32_t sz, unsigned int flags)
{
	shash *h;

	h = (shash*)malloc(sizeof(shash));
	if (!h)	return(SHASH_ERR);

	h->elements = 0;
	h->table_len = sz;
	h->key_len = key_len;
	h->value_len = value_len;
	h->flags = flags;
	h->h_fn = h_fn;

	if ((flags & SHASH_CR_MT_BIGLOCK) && (flags & SHASH_CR_MT_MANYLOCK)) {
		*h_r = 0;
		return(SHASH_ERR);
	}
	
	h->table = malloc(sz * SHASH_ELEM_SZ(h));
	if (!h->table) {
		free(h);
		*h_r = 0;
		return(SHASH_ERR);
	}
	
	shash_elem *table = (shash_elem*)h->table;
	for (unsigned int i=0;i<sz;i++) {
		table->in_use = false;
		table->next = 0;
		// next element in head table
		table = (shash_elem *) (((uint8_t *)table) + SHASH_ELEM_SZ(h));
	}
	
	if (flags & SHASH_CR_MT_BIGLOCK) {
		if (0 != pthread_mutex_init ( &h->biglock, 0) ) {
			free(h->table); free(h);
			return(SHASH_ERR);
		}
	}
	else
		memset( (void*)&h->biglock, 0, sizeof( h->biglock ) );
	
	if (flags & SHASH_CR_MT_MANYLOCK) {
		h->lock_table = (pthread_mutex_t*)malloc( sizeof(pthread_mutex_t) * sz);
		if (! h->lock_table) {
			free(h);
			*h_r = 0;
			return(SHASH_ERR);
		}
		for (unsigned int i=0;i<sz;i++) {
			pthread_mutex_init( &(h->lock_table[i]), 0 );
		}
	}
	else
		h->lock_table = 0;
	

	*h_r = h;

	return(SHASH_OK);
}

//
// If MANYLOCK, then the elements counter will be wrong because there's no single
// lock to use to protect. We've got a choice to use an atomic to make sure
// the get_size call is fast in that case (thus slowing down every access), or
// running the list and make get_size slow.
// Choose making get_size slow in the case, but retaining parallelism


uint32_t
shash_get_size(shash *h)
{

    uint32_t elements = 0;
	
	if (h->flags & SHASH_CR_MT_MANYLOCK) {


        
        for (unsigned int i=0; i<h->table_len ; i++) {
            
            pthread_mutex_t *l = &(h->lock_table[i]);
            pthread_mutex_lock( l );
            
            shash_elem *list_he = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * i));
            while (list_he) {
                
                // not in use is common at head pointer - unused bucket
                if (list_he->in_use == false) 
                    break;
                
                elements++;
                
                list_he = list_he->next;
            };
    
            pthread_mutex_unlock(l);
    
        }

    }
    
    else if (h->flags & SHASH_CR_MT_BIGLOCK) {
    	pthread_mutex_lock(&h->biglock);
    	elements = h->elements;
    	pthread_mutex_unlock(&h->biglock);
    }
    else {
    	elements = h->elements;
    }
    
	return(elements);
}

int
shash_put(shash *h, void *key, void *value)
{
	// Calculate hash
	unsigned int hash = h->h_fn(key);
	hash %= h->table_len;

	pthread_mutex_t		*l = 0;
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		l = &h->biglock;
	}
	else if (h->flags & SHASH_CR_MT_MANYLOCK) {
		l = & (h->lock_table[hash]);
	}
	if (l)     pthread_mutex_lock( l );
		
	shash_elem *e = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * hash));	

	// most common case should be insert into empty bucket, special case
	if ( e->in_use == false )
		goto Copy;

	shash_elem *e_head = e;

	// This loop might be skippable if you know the key is not already in the hash
	// (like, you just searched and it's single-threaded)	
	while (e) {
		if ( memcmp( SHASH_ELEM_KEY_PTR(h, e) , key, h->key_len) == 0) {
			memcpy( SHASH_ELEM_VALUE_PTR(h, e), value, h->value_len);
			if (h->flags & SHASH_CR_MT_BIGLOCK)
				pthread_mutex_unlock(&h->biglock);
			return(SHASH_OK);
		}
		e = e->next;
	}

	e = (shash_elem *) malloc( SHASH_ELEM_SZ(h) );
	if (!e)  return (SHASH_ERR);
	e->next = e_head->next;
	e_head->next = e;
	
Copy:
	memcpy(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len);
	memcpy(SHASH_ELEM_VALUE_PTR(h, e), value, h->value_len);
	e->in_use = true;
	h->elements++;
	if (l)	pthread_mutex_unlock( l );
	return(SHASH_OK);	
}

// Fail if there's already a value there

int
shash_put_unique(shash *h, void *key, void *value)
{
	// Calculate hash
	unsigned int hash = h->h_fn(key);
	hash %= h->table_len;

	pthread_mutex_t		*l = 0;
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		l = &h->biglock;
	}
	else if (h->flags & SHASH_CR_MT_MANYLOCK) {
		l = & (h->lock_table[hash]);
	}
	if (l)     pthread_mutex_lock( l );
		
	shash_elem *e = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * hash));	

	// most common case should be insert into empty bucket, special case
	if ( e->in_use == false ) {
		goto Copy;
	}

	shash_elem *e_head = e;

	while (e) {
		if ( memcmp(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len) == 0) {
			if (l){
//			if (h->flags & SHASH_CR_MT_BIGLOCK)
//				pthread_mutex_unlock(&h->biglock);
				pthread_mutex_unlock(l);
			}
			return(SHASH_ERR_FOUND);
		}
		e = e->next;
	}

	e = (shash_elem *) malloc( SHASH_ELEM_SZ(h) );
	if (!e) return (SHASH_ERR);
	e->next = e_head->next;
	e_head->next = e;
	
Copy:
	memcpy(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len);
	memcpy(SHASH_ELEM_VALUE_PTR(h, e), value, h->value_len);
	e->in_use = true;
	h->elements++;
	if (l)	pthread_mutex_unlock( l );
	return(SHASH_OK);	

}



int
shash_get(shash *h, void *key, void *value)
{
	int rv = SHASH_ERR;
	
	unsigned int hash = h->h_fn(key);
	hash %= h->table_len;

	pthread_mutex_t		*l = 0;
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		l = &h->biglock;
	}
	else if (h->flags & SHASH_CR_MT_MANYLOCK) {
		l = & (h->lock_table[hash]);
	}
	if (l)     pthread_mutex_lock( l );
	
	shash_elem *e = (shash_elem *) ( ((byte *)h->table) + (SHASH_ELEM_SZ(h) * hash));	

	if (e->in_use == false) {
		rv = SHASH_ERR_NOTFOUND;
		goto Out;
	}
	
	do {
		if ( memcmp(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len) == 0) {
            if (NULL != value)
			    memcpy(value, SHASH_ELEM_VALUE_PTR(h, e), h->value_len);
			rv = SHASH_OK; 
			goto Out;
		}
		e = e->next;
	} while (e);
	rv = SHASH_ERR_NOTFOUND;
	
Out:
	if (l) pthread_mutex_unlock(l);

	return(rv);
					
}

//
// Note that the vlock is passed back only when the return code is SHASH_OK.
// In the case where nothing is found, no lock is held.
// It might be better to do it the other way, but you can change it later if you want

int
shash_get_vlock(shash *h, void *key, void **value, pthread_mutex_t **vlock)
{
	int rv = SHASH_ERR;
	
	unsigned int hash = h->h_fn(key);
	hash %= h->table_len;

	pthread_mutex_t		*l = 0;
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		l = &h->biglock;
	}
	else if (h->flags & SHASH_CR_MT_MANYLOCK) {
		l = & (h->lock_table[hash]);
	}
	if (l)     pthread_mutex_lock( l );
	
	shash_elem *e = (shash_elem *) ( ((byte *)h->table) + (SHASH_ELEM_SZ(h) * hash));	

	if (e->in_use == false) {
		rv = SHASH_ERR_NOTFOUND;
		goto Out;
	}
	
	do {
		if ( memcmp(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len) == 0) {
			*value = SHASH_ELEM_VALUE_PTR(h, e);
			rv = SHASH_OK; 
			goto Out;
		}
		e = e->next;
	} while (e);
	
	rv = SHASH_ERR_NOTFOUND;
	
Out:
	if (l) {
		if (rv == SHASH_OK) {
			*vlock = l;
		}
		else {
			pthread_mutex_unlock( l );
			*vlock = 0;
		}
	}
	else {
		// for safety sake
		if (vlock) *vlock = 0;
	}

	return(rv);
					
}


int
shash_delete(shash *h, void *key)
{

	// Calculate hash
	unsigned int hash = h->h_fn(key);
	hash %= h->table_len;
	int rv = SHASH_ERR;

	pthread_mutex_t		*l = 0;
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		l = &h->biglock;
	}
	else if (h->flags & SHASH_CR_MT_MANYLOCK) {
		l = & (h->lock_table[hash]);
	}
	if (l)     pthread_mutex_lock( l );
		
	shash_elem *e = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * hash));	

	// If bucket empty, def can't delete
	if ( e->in_use == false ) {
		rv = SHASH_ERR_NOTFOUND;
		goto Out;
	}

	shash_elem *e_prev = 0;

	// Look for teh element and destroy if found
	while (e) {
		if ( memcmp(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len) == 0) {
			// Found it
			// patchup pointers & free element if not head
			if (e_prev) {
				e_prev->next = e->next;
				free (e);
			}
			// am at head - more complicated
			else {
				// at head with no next - easy peasy!
				if (0 == e->next) {
					e->in_use = false;
				}
				// at head with a next - more complicated
				else {
					shash_elem *_t = e->next;
					memcpy(e, e->next, SHASH_ELEM_SZ(h) );
					free(_t);
				}
			}
			h->elements--;
			rv = SHASH_OK;
			goto Out;

		}
		e_prev = e;
		e = e->next;
	}
	rv = SHASH_ERR_NOTFOUND;

Out:
	if (l) 
		pthread_mutex_unlock(l);
	return(rv);	
	

}

//
// I kind of feel bad about this cut n paste, but
// it's nice in the lock state to slim down the area under which the lock is taken
// doing multiple helper functions may be better...

int
shash_delete_lockfree(shash *h, void *key)
{
	// Calculate hash
	unsigned int hash = h->h_fn(key);
	hash %= h->table_len;

	shash_elem *e = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * hash));	

	// If bucket empty, def can't delete
	if ( e->in_use == false )
		return( SHASH_ERR_NOTFOUND );

	shash_elem *e_prev = 0;

	// Look for teh element and destroy if found
	while (e) {
		if ( memcmp(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len) == 0) {
			// Found it
			// patchup pointers & free element if not head
			if (e_prev) {
				e_prev->next = e->next;
				free (e);
			}
			// am at head - more complicated
			else {
				// at head with no next - easy peasy!
				if (0 == e->next) {
					e->in_use = false;
				}
				// at head with a next - more complicated
				else {
					shash_elem *_t = e->next;
					memcpy(e, e->next, SHASH_ELEM_SZ(h) );
					free(_t);
				}
			}
			h->elements--;
			return( SHASH_OK );
		}
		e_prev = e;
		e = e->next;
	}
	
	return ( SHASH_ERR_NOTFOUND );

	
}

int
shash_get_and_delete(shash *h, void *key, void *value)
{
	// Calculate hash
	unsigned int hash = h->h_fn(key);
	hash %= h->table_len;
	int rv = SHASH_ERR;

	pthread_mutex_t		*l = 0;
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		l = &h->biglock;
	}
	else if (h->flags & SHASH_CR_MT_MANYLOCK) {
		l = & (h->lock_table[hash]);
	}
	if (l)     pthread_mutex_lock( l );
		
	shash_elem *e = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * hash));	

	// If bucket empty, def can't delete
	if ( e->in_use == false ) {
		rv = SHASH_ERR_NOTFOUND;
		goto Out;
	}

	shash_elem *e_prev = 0;

	// Look for teh element and destroy if found
	while (e) {
		if ( memcmp(SHASH_ELEM_KEY_PTR(h, e), key, h->key_len) == 0) {
		
			// Found it - check length of buffer and copy to destination
			memcpy(value, SHASH_ELEM_VALUE_PTR(h, e), h->value_len);

			
			// patchup pointers & free element if not head
			if (e_prev) {
				e_prev->next = e->next;
				free (e);
			}
			// am at head - more complicated
			else {
				// at head with no next - easy peasy!
				if (0 == e->next) {
					e->in_use = false;
				}
				// at head with a next - more complicated
				else {
					shash_elem *_t = e->next;
					memcpy(e, e->next, SHASH_ELEM_SZ(h) );
					free(_t);
				}
			}
			h->elements--;
			rv = SHASH_OK;
			goto Out;

		}
		e_prev = e;
		e = e->next;
	}
	rv = SHASH_ERR_NOTFOUND;

Out:
	if (l)
		pthread_mutex_unlock( l );
		
	return(rv);	
	

}



// Call the function over every node in the tree
// Can be lock-expensive at the moment, until we improve the lockfree code
// the return value is the non-zero return value of any of the reduce calls,
// if there was one that didn't return zero.
int
shash_reduce(shash *h, shash_reduce_fn reduce_fn, void *udata)
{
	int rv = 0;
	
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		pthread_mutex_lock( &h->biglock);
	}

	for (unsigned int i=0; i<h->table_len ; i++) {
		
		pthread_mutex_t *l = 0;
		if (h->flags & SHASH_CR_MT_MANYLOCK) {
			l = &(h->lock_table[i]);
			pthread_mutex_lock( l );
		}
		
		shash_elem *list_he = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * i));
		while (list_he) {
			
			// not in use is common at head pointer - unused bucket
			if (list_he->in_use == false)
				break;
			
			rv = reduce_fn(	SHASH_ELEM_KEY_PTR(h, list_he), SHASH_ELEM_VALUE_PTR(h, list_he), udata);
			if (0 != rv) {
                if (l) pthread_mutex_unlock(l);
				goto Out;
			}
			
			list_he = list_he->next;
		};

		if (l)	pthread_mutex_unlock(l);

	}
Out:
	if (h->flags & SHASH_CR_MT_BIGLOCK)
		pthread_mutex_unlock(&h->biglock);

	return(rv);
}

// A special version of 'reduce' that supports deletion
// In this case, if you return '1' from the reduce fn, that node will be
// deleted. All other return values will terminate the reduce and pass
// that value back to the caller. We're using '1' because in this codebase,
// negative numbers are errors
int
shash_reduce_delete(shash *h, shash_reduce_fn reduce_fn, void *udata)
{
	int rv = 0;
	
	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		pthread_mutex_lock( &h->biglock);
	}
	
	for (unsigned int i=0; i<h->table_len ; i++) {

		pthread_mutex_t *l = 0;
		if (h->flags & SHASH_CR_MT_MANYLOCK) {
			l = &(h->lock_table[i]);
			pthread_mutex_lock( l );
		}
		
		shash_elem *list_he = (shash_elem *) ( ((uint8_t *)h->table) + (SHASH_ELEM_SZ(h) * i));
		shash_elem *prev_he = 0;
		
		while (list_he) {
			// This kind of structure might have the head as an empty element,
			// that's a signal to move along
			if (list_he->in_use == false)
				break;
			
			rv = reduce_fn( SHASH_ELEM_KEY_PTR(h, list_he), SHASH_ELEM_VALUE_PTR(h, list_he), udata);
			
			// Delete is requested
			// Leave the pointers in a "next" state
			if (rv == SHASH_REDUCE_DELETE) {

                h->elements--;
                
				// patchup pointers & free element if not head
				if (prev_he) {
					prev_he->next = list_he->next;
					free (list_he);
					list_he = prev_he->next;
				}
				// am at head - more complicated
				else {
					// at head with no next - easy peasy!
					if (0 == list_he->next) {
						list_he->in_use = false;
						list_he = 0;
					}
					// at head with a next - more complicated -
					// copy next into current and free next
					// (the old trick of how to delete from a singly
					// linked list without a prev pointer)
					// Somewhat confusingly, prev_he stays 0
					// and list_he stays where it is
					else {
						shash_elem *_t = list_he->next;
						memcpy(list_he, list_he->next, SHASH_ELEM_SZ(h) );
						free(_t);
					}
				}
				rv = 0;
			}
			else if (0 != rv) {
				if (l)	pthread_mutex_unlock(l);
				goto Out;
			}
			else { // don't delete, just forward everything
				prev_he = list_he;
				list_he = list_he->next;
			}	
		};
		
		if (l) pthread_mutex_unlock(l);
	}

Out:

	if (h->flags & SHASH_CR_MT_BIGLOCK)
		pthread_mutex_unlock( &h->biglock);
	
	return(rv);
}


void
shash_destroy(shash *h)
{
	shash_elem *e_table = (shash_elem*)h->table;
	for (unsigned int i=0;i<h->table_len;i++) {
		if (e_table->next) {
			shash_elem *e = e_table->next;
			shash_elem *t;
			while (e) {
				t = e->next;
				free(e);
				e = t;
			}
		}
		e_table = (shash_elem *) (((uint8_t *)e_table) + SHASH_ELEM_SZ(h));
	}

	if (h->flags & SHASH_CR_MT_BIGLOCK) {
		pthread_mutex_destroy(&h->biglock);
	}
	if (h->flags & SHASH_CR_MT_MANYLOCK) {
		for (unsigned int i=0;i<h->table_len;i++) {
			pthread_mutex_destroy(&(h->lock_table[i]));
		}
		free(h->lock_table);
	}

	free(h->table);
	free(h);	
}	
