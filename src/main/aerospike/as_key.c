/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>

#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cl_object.h>

#include <stdbool.h>
#include <stdint.h>

#include "_shim.h"

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

extern inline as_key * as_key_init_str(as_key * key, const as_namespace ns, const as_set set, const char * value);
extern inline as_key * as_key_init_raw(as_key * key, const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size);
extern inline as_key * as_key_new_str(const as_namespace ns, const as_set set, const char * value);
extern inline as_key * as_key_new_raw(const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size);

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_key * as_key_cons(as_key * key, bool free, const as_namespace ns, const char * set, const as_key_value * valuep, const as_digest_value digest)
{
	if ( ! set ) {
		set = "";
	}

	if ( ! (ns && *ns != '\0' && strlen(ns) < AS_NAMESPACE_MAX_SIZE && strlen(set) < AS_SET_MAX_SIZE) ) {
		return NULL;
	}

	key->_free = free;
	strcpy(key->ns, ns);
	strcpy(key->set, set);
	key->valuep = (as_key_value *) valuep;
	
	if ( digest == NULL ) {
		key->digest.init = false;
		memset(key->digest.value, 0, AS_DIGEST_VALUE_SIZE);	
	}
	else {
		key->digest.init = true;
		memcpy(key->digest.value, digest, AS_DIGEST_VALUE_SIZE);
	}
	return key;
}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_init(as_key * key, const as_namespace ns, const as_set set, const char * value)
{
	return as_key_init_str(key, ns, set, value);
}

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 */
as_key * as_key_init_int64(as_key * key, const as_namespace ns, const as_set set, int64_t value)
{
	if ( !key ) return key;

	as_integer_init((as_integer *) &key->value, value);
	return as_key_cons(key, false, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_init_strp(as_key * key, const as_namespace ns, const as_set set, const char * value, bool free)
{
	if ( !key ) return key;
	
	as_string_init((as_string *) &key->value, (char *) value, free);
	return as_key_cons(key, false, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 */
as_key * as_key_init_rawp(as_key * key, const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size, bool free)
{
	if ( !key ) return key;
	
	as_bytes_init_wrap((as_bytes *) &key->value, (uint8_t *) value, size, free);
	return as_key_cons(key, false, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated as_key with a digest.
 */
as_key * as_key_init_digest(as_key * key, const as_namespace ns, const as_set set, const as_digest_value digest)
{
	if ( !key ) return key;

	return as_key_cons(key, false, ns, set, NULL, digest);
}

/**
 *	Initialize a stack allocated `as_key` with a digest value.
 */
as_key * as_key_init_value(as_key * key, const as_namespace ns, const as_set set, const as_key_value * value)
{
	if ( !key ) return key;

	return as_key_cons(key, false, ns, set, value, NULL);
}

/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_new(const as_namespace ns, const as_set set, const char * value)
{
	return as_key_new_str(ns, set, value);
}

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 */
as_key * as_key_new_int64(const as_namespace ns, const as_set set, int64_t value)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;

	as_integer_init((as_integer *) &key->value, value);
	return as_key_cons(key, true, ns, set, &key->value, NULL);
}


/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_new_strp(const as_namespace ns, const as_set set, const char * value, bool free)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;
	
	as_string_init((as_string *) &key->value, (char *) value, free);
	return as_key_cons(key, true, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 */
as_key * as_key_new_rawp(const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size, bool free)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;
	
	as_bytes_init_wrap((as_bytes *) &key->value, (uint8_t *) value, size, free);
	return as_key_cons(key, true, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated as_key with a digest value.
 */
as_key * as_key_new_digest(const as_namespace ns, const as_set set, const as_digest_value digest)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;

	return as_key_cons(key, true, ns, set, NULL, digest);
}

/**
 *	Initialize a stack allocated `as_key` to a an `as_key_value`.
 */
as_key * as_key_new_value(const as_namespace ns, const as_set set, const as_key_value * value)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;

	return as_key_cons(key, true, ns, set, value, NULL);
}

/**
 *	Destory the `as_key`, releasing resources.
 */
void as_key_destroy(as_key * key)
{
	if ( !key ) return;
	if ( !key->valuep ) return;

	as_val_destroy((as_val *) key->valuep);

	if ( key->_free ) {
		free(key);
	}
}

/**
 *	Compute the digest value.
 */
as_digest * as_key_digest(as_key * key)
{
	if ( !key ) return NULL;
	if ( key->digest.init ) return &key->digest;

	char * set = key->set;
	as_val * val = (as_val *) key->valuep;
	as_digest * digest = &key->digest;
	
	if ( val ) {
		cl_object obj;
		asval_to_clobject(val, &obj);
		citrusleaf_calculate_digest(set, &obj, (cf_digest *) digest->value);
		key->digest.init = true;
		return &key->digest;
	}

	return NULL;
}
