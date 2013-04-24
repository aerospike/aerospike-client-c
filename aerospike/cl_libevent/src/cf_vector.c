/*
 * A general purpose hashtable implementation
 * Good at multithreading
 * Just, hopefully, the last reasonable hash table you'll ever need
 * Copywrite 2008 Brian Bulkowski
 * All rights reserved
 */

#include <string.h>
#include <stdlib.h>


#include "citrusleaf_event/cf_vector.h"



cf_vector *
cf_vector_create( uint32_t value_len, uint32_t init_sz, uint flags)
{
	cf_vector *v;

	v = malloc(sizeof(cf_vector));
	if (!v)	return(0);

	v->value_len = value_len;
	v->flags = flags;
	v->alloc_len = init_sz;
	v->len = 0;
	v->stack_struct = false;
	v->stack_vector = false;
	if (init_sz) {
		v->vector = malloc(init_sz * value_len);
		if (!v->vector)	return(0);
	}
	else
		v->vector = 0;
	if (flags & VECTOR_FLAG_INITZERO)
		memset(v->vector, 0, init_sz * value_len);
	return(v);
}

int
cf_vector_init(cf_vector *v, uint32_t value_len, uint32_t init_sz, uint flags)
{
	v->value_len = value_len;
	v->flags = flags;
	v->alloc_len = init_sz;
	v->len = 0;
	v->stack_struct = true;
	v->stack_vector = false;
	if (init_sz) {
		v->vector = malloc(init_sz * value_len);
		if (!v->vector)	return(-1);
	}
	else
		v->vector = 0;
	if (flags & VECTOR_FLAG_INITZERO)
		memset(v->vector, 0, init_sz * value_len);
	return(0);
}

void
cf_vector_init_smalloc(cf_vector *v, uint32_t value_len, uint8_t *sbuf, int sbuf_sz, uint flags)
{
	v->value_len = value_len;
	v->flags = flags;
	v->alloc_len = sbuf_sz / value_len;
	v->len = 0;
	v->stack_struct = true;
	v->stack_vector = true;
	v->vector = sbuf;
	if (flags & VECTOR_FLAG_INITZERO)
		memset(v->vector, 0, sbuf_sz);
}


void
cf_vector_destroy(cf_vector *v)
{
	if (v->vector && (v->stack_vector == false))	free(v->vector);
	if (v->stack_struct == false) free(v);
}

static int
cf_vector_resize(cf_vector *v, uint32_t new_sz)
{
	if (v->flags & VECTOR_FLAG_BIGRESIZE) {
		if (new_sz < 50)	new_sz = 50;
		else if (new_sz < v->alloc_len * 2)
			new_sz = v->alloc_len * 2;
	}
	uint8_t *_t;
	if (v->vector == 0 || v->stack_vector) {
		_t = malloc(new_sz * v->value_len);
		if (v->stack_vector) {
			memcpy(_t, v->vector, v->alloc_len * v->value_len); 
			v->stack_vector = false;
		}

	}
	else
		_t = realloc(v->vector, (new_sz) * v->value_len);
	if (!_t)	return(-1);
	v->vector = _t;
	if (v->flags & VECTOR_FLAG_INITZERO)
		memset(v->vector + (v->alloc_len * v->value_len), 0, (new_sz + 2) - v->alloc_len);
	v->alloc_len = new_sz;
	return(0);
}


int
cf_vector_set(cf_vector *v, uint32_t index, void *value)
{
	if (index >= v->alloc_len)
		if (0 != cf_vector_resize(v, index+1))	return(-1);
	memcpy(v->vector + (index * v->value_len), value, v->value_len);
	if (index > v->len)	v->len = index;
	return(0);
}

int
cf_vector_append_lockfree(cf_vector *v, void *value)
{
	if (v->len + 1 >= v->alloc_len)
		if (0 != cf_vector_resize(v, v->len + 2))	return(-1);
	memcpy(v->vector + (v->len * v->value_len), value, v->value_len);
	v->len ++;
	return(0);
	
}



int
cf_vector_append(cf_vector *v, void *value)
{
	int rv;
	rv = cf_vector_append_lockfree(v, value);
	return(rv);
}

int
cf_vector_append_unique(cf_vector *v, void *value)
{
	int rv=0;
	uint8_t	*_b = v->vector;
	uint32_t	_l = v->value_len;
	for (uint i=0;i<v->len;i++) {
		if (0 == memcmp(value, _b, _l)) {
			goto Found;
		}
		_b += _l;
	}
	rv = cf_vector_append_lockfree(v, value);
Found:	
	return(rv);
}

// Copy the vector element into the pointer I give

int
cf_vector_get(cf_vector *v, uint32_t index, void *value_p)
{
	if (index >= v->alloc_len)
		return(-1);
	memcpy(value_p, v->vector + (index * v->value_len), v->value_len);
	return(0);
}

void *
cf_vector_getp(cf_vector *v, uint32_t index)
{
	if (index >= v->alloc_len)
		return(0);
	void *r = v->vector + (index * v->value_len);
	return( r );
}

int
cf_vector_delete(cf_vector *v, uint32_t index)
{
	// check bounds
	if (index >= v->len)
		return (-1);
	// check for last - no copy
	if (index != v->len - 1) {
		memmove(v->vector + (index * v->value_len), 
				v->vector + ((index+1) * v->value_len),
				(v->len - (index+1)) * v->value_len );
	}
	v->len --;
	
	return(0);
}

int
cf_vector_delete_range(cf_vector *v, uint32_t idx_start, uint32_t idx_end)
{
	// check bounds
	if (idx_start >= idx_end)
		return (-1);
	if (idx_start >= v->len)
		return(-1);
	if (idx_end >= v->len)
		return(-1);
	
	// Copy down if not at end
	if (idx_end != v->len - 1) {
		memmove( v->vector + (idx_start * v->value_len),
				v->vector + ((idx_end+1) * v->value_len),
			    (v->len - (idx_end+1)) * v->value_len );
	
	}
	v->len -= (idx_end - idx_start) + 1;
	
	return(0);
}

void
cf_vector_compact(cf_vector *v)
{
	if (v->alloc_len && (v->len != v->alloc_len)) {
		v->vector = realloc(v->vector, v->len * v->alloc_len);
		v->alloc_len = v->len;
	}
	return;
}

