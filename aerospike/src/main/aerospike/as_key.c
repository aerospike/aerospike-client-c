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
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_key * as_key_defaults(as_key * key, bool free, const as_namespace ns, const char * set, const as_key_value * valuep)
{
	if ( ! set ) set = "";
	if ( ! (ns && *ns != '\0' && strlen(ns) < AS_NAMESPACE_MAX_SIZE && strlen(set) < AS_SET_MAX_SIZE) ) {
		return NULL;
	}
	key->_free = free;
	strcpy(key->ns, ns);
	strcpy(key->set, set);
	key->valuep = (as_key_value *) valuep;
	key->digest.init = false;
	memset(key->digest.value,0,AS_DIGEST_VALUE_SIZE);
	return key;
}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *	    as_key_init("ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *	
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_init(as_key * key, const as_namespace ns, const as_set set, const char * value)
{
	return as_key_init_str(key, ns, set, value);
}

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *	    as_key_init_int64(&key, "ns", "set", 123);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key`.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_init_int64(as_key * key, const as_namespace ns, const as_set set, int64_t value)
{
	if ( !key ) return key;

	as_integer_init((as_integer *) &key->value, value);
	return as_key_defaults(key, false, ns, set, &key->value);
}

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *	    as_key_init_str(&key, "ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key`.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_init_str(as_key * key, const as_namespace ns, const as_set set, const char * value)
{
	if ( !key ) return key;
	
	as_string_init((as_string *) &key->value, (char *) value, false);
	return as_key_defaults(key, false, ns, set, &key->value);
}

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 *
 *	~~~~~~~~~~{.c}
 *		uint8_t rgb[3] = {254,254,120};
 *
 *		as_key key;
 *	    as_key_init_raw(&key, "ns", "set", rgb, 3);
 *	~~~~~~~~~~
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_init_raw(as_key * key, const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size)
{
	if ( !key ) return key;
	
	as_bytes_init_wrap((as_bytes *) &key->value, (uint8_t *) value, size, false);
	return as_key_defaults(key, false, ns, set, &key->value);
}

/**
 *	Initialize a stack allocated `as_key` to a an `as_key_value`.
 *
 *	~~~~~~~~~~{.c}
 *		as_string str;
 *		as_string_init(&str, "abc", false);
 *
 *		as_key key;
 *	    as_key_init_value(&key, "ns", "set", (as_key_value *) str);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key`.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_init_value(as_key * key, const as_namespace ns, const as_set set, const as_key_value * value)
{
	if ( !key ) return key;
	return as_key_defaults(key, false, ns, set, value);
}


/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *	    as_key * key = as_key_new("ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_new(const as_namespace ns, const as_set set, const char * value)
{
	return as_key_new_str(ns, set, value);
}

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 *
 *	~~~~~~~~~~{.c}
 *	    as_key * key = as_key_new_int64("ns", "set", 123);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_new_int64(const as_namespace ns, const as_set set, int64_t value)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;

	as_integer_init((as_integer *) &key->value, value);
	return as_key_defaults(key, true, ns, set, &key->value);
}


/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *	    as_key * key = as_key_new_str("ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_new_str(const as_namespace ns, const as_set set, const char * value)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;
	
	as_string_init((as_string *) &key->value, (char *) value, false);
	return as_key_defaults(key, true, ns, set, &key->value);
}

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 *
 *	~~~~~~~~~~{.c}
 *		uint8_t rgb[3] = {254,254,120};
 *
 *	    as_key * key = as_key_new_raw("ns", "set", rgb, 3);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_new_raw(const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;
	
	as_bytes_init_wrap((as_bytes *) &key->value, (uint8_t *) value, size, false);
	return as_key_defaults(key, true, ns, set, &key->value);
}

/**
 *	Initialize a stack allocated `as_key` to a an `as_key_value`.
 *
 *	~~~~~~~~~~{.c}
 *		as_string str;
 *		as_string_init(&keystr, "abc", false);
 *
 *		as_key * key = as_key_new_value("ns", "set", (as_key_value *) str);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 */
as_key * as_key_new_value(const as_namespace ns, const as_set set, const as_key_value * value)
{
	as_key * key = (as_key *) malloc(sizeof(as_key));
	if ( !key ) return key;
	return as_key_defaults(key, true, ns, set, value);
}

/**
 *	Destory the `as_key`, releasing resources.
 *
 *	@param key The `as_key` to destroy.
 */
void as_key_destroy(as_key * key)
{
	if ( !key ) return;
	if ( !key->valuep ) return;
	as_val_destroy((as_val *) key->valuep);
}

/**
 *	Compute the digest value
 *
 *	@param digest The digest to compute the as_digest.value for.
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
