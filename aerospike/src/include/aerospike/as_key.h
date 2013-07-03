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

/**
 *	@defgroup as_key_t Keys
 *	@copydoc as_key
 */

#pragma once 

#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	The size of as_digest.value
 */
#define AS_DIGEST_VALUE_SIZE 20

/**
 *	The maxium size of as_namespace.
 */
#define AS_NAMESPACE_MAX_SIZE 32

/**
 *	The maxium size of as_set.
 */
#define AS_SET_MAX_SIZE 64

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Namespace Name
 */
typedef char as_namespace[AS_NAMESPACE_MAX_SIZE];

/**
 *	Set Name
 */
typedef char as_set[AS_SET_MAX_SIZE];

/**
 *	The digest is the value used to locate a record based on the
 *	set and digest of the record. The digest is calculated using RIPEMD-160.
 *	Keys for digests can be either a string or integer.
 *
 *	To initialize a stack allocated instance as an string digest:
 *	
 *	~~~~~~~~~~{.c}
 *	as_digest digest;
 *	as_digest_init(&digest, "set", as_string_new("abc",false));
 *	~~~~~~~~~~
 *
 *	Alternatively, you can use an integer digest:
 *
 *	~~~~~~~~~~{.c}
 *	as_digest_init2(&digest, "set", 123);
 *	~~~~~~~~~~
 *
 *	You can also heap allocate and initialize an instance:
 *
 *	~~~~~~~~~~{.c}
 *	as_digest * digest = as_digest_new("set", "digest");
 *	~~~~~~~~~~
 *	
 *	When you are finished using the digest, you should always destroy it:
 *
 *	~~~~~~~~~~{.c}
 *	as_digest_destroy(digest);
 *	~~~~~~~~~~
 *	
 */
typedef struct as_digest_s {

	/**
	 *	Indicates whether the digest was calculated.
	 */
	bool init;

	/**
	 *	The digest value.
	 */
	uint8_t value[AS_DIGEST_VALUE_SIZE];

} as_digest;

/**
 *	Key value
 */
typedef union as_key_value_u {

	/**
	 *	Integer value.
	 */
	as_integer integer;

	/**
	 * String value.
	 */
	as_string string;

	/**
	 * Raw value.
	 */
	as_bytes bytes;

} as_key_value;


/** 
 *	Key used for accessing and modifying records in a cluster.
 *
 *	The key can either be stack or heap allocated. 
 *	
 *	For stack allocated as_key, you should you the following functions to
 *	initialize the value:
 *	- as_key_init()
 *	- as_key_init_int64()
 *	- as_key_init_str()
 *	- as_key_init_raw()
 *	- as_key_init_value()
 *
 *	For heap allocated as_key, you should use the following functions
 *	to allocate and initialize the value:
 *	- as_key_new()
 *	- as_key_new_int64()
 *	- as_key_new_str()
 *	- as_key_new_raw()
 *	- as_key_new_value()
 *
 *	An example of using a stack allocated as_key:
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *	~~~~~~~~~~
 *	
 *	An example of using a heap allocated as_key:
 *
 *	~~~~~~~~~~{.c}
 *	as_key * key = as_key_new("ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	With a key, you can use the following APIs to access or modify records:
 *	- aerospike_key_get()
 *	- aerospike_key_select()
 *	- aerospike_key_exists()
 *	- aerospike_key_put()
 *	- aerospike_key_operate()
 *	- aerospike_key_remove()
 *	- aerospike_key_apply()
 *
 *	Each of the operations internally use an as_digest to calculate the location
 *	for the given key. The digest is a hash value of the key and set. It is 
 *	calculated once, and is reused as often as the key is reused. To get the digest
 *	value of a key, use:
 *	- as_key_digest()
 *
 *	@ingroup as_key_t
 */
typedef struct as_key_s {

	/**
	 *	@private
	 *	If true, then `as_key_destroy()` will free this instance.
	 */
	bool _free;

	/**
	 *	The namespace the key belongs to.
	 */
	as_namespace ns;

	/**
	 *	The set the key belongs to.
	 */
	as_set set;

	/**
	 *	The key value.
	 */
	as_key_value value;

	/**
	 *	The key value pointer.
	 *	If NULL, then there is no value.
	 *	It can point to as_key.value or a different value.
	 */
	as_key_value * valuep;

	/**
	 * Digest for the key.
	 */
	as_digest digest;

} as_key;

/******************************************************************************
 *	as_key FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *	
 *	@param key		The key to initialize.
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_init(as_key * key, const as_namespace ns, const as_set set, const char * value);

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init_int64(&key, "ns", "set", 123);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key`.
 *
 *	@param key		The key to initialize.
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_init_int64(as_key * key, const as_namespace ns, const as_set set, int64_t value);

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init_str(&key, "ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key`.
 *
 *	@param key		The key to initialize.
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_init_str(as_key * key, const as_namespace ns, const as_set set, const char * value);

/**
 *	Initialize a stack allocated `as_key` to bytes array.
 *
 *	~~~~~~~~~~{.c}
 *	uint8_t rgb[3] = {254,254,120};
 *	
 *	as_key key;
 *	as_key_init_raw(&key, "ns", "set", rgb, 3);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key`.
 *
 *	@param key		The key to initialize.
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *	@param size		The number of bytes in value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_init_raw(as_key * key, const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size);

/**
 *	Initialize a stack allocated `as_key` to an `as_key_value`.
 *
 *	~~~~~~~~~~{.c}
 *	as_string str;
 *	as_string_init(&str, "abc", false);
 *	
 *	as_key key;
 *	as_key_init_value(&key, "ns", "set", (as_key_value *) str);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key`.
 *
 *	@param key 		The key to initialize.
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return The initialized `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_init_value(as_key * key, const as_namespace ns, const as_set set, const as_key_value * value);


/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *	as_key * key = as_key_new("ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return A new `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_new(const as_namespace ns, const as_set set, const char * value);

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 *
 *	~~~~~~~~~~{.c}
 *	as_key * key = as_key_new_int64("ns", "set", 123);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return A new `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_new_int64(const as_namespace ns, const as_set set, int64_t value);

/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *	as_key * key = as_key_new_str("ns", "set", "key");
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return A new `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_new_str(const as_namespace ns, const as_set set, const char * value);

/**
 *	Initialize a stack allocated `as_key` to a byte array.
 *
 *	~~~~~~~~~~{.c}
 *	uint8_t rgb[3] = {254,254,120};
 *	
 *	as_key * key = as_key_new_raw("ns", "set", rgb, 3);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *	@param size		The number of bytes in the value.
 *
 *	@return A new `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_new_raw(const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size);

/**
 *	Initialize a stack allocated `as_key` to a an `as_key_value`.
 *
 *	~~~~~~~~~~{.c}
 *	as_string str;
 *	as_string_init(&str, "abc", false);
 *	
 *	as_key * key = as_key_new_value("ns", "set", (as_key_value *) str);
 *	~~~~~~~~~~
 *
 *	Use `as_key_destroy()` to release resources allocated to `as_key` via
 *	this function.
 *
 *	@param ns 		The namespace for the key.
 *	@param set		The set for the key.
 *	@param value	The key's value.
 *
 *	@return A new `as_key` on success. Otherwise NULL.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_key * as_key_new_value(const as_namespace ns, const as_set set, const as_key_value * value);

/**
 *	Destory the `as_key`, releasing resources.
 *
 *	~~~~~~~~~~{.c}
 *	as_key_destroy(key);
 *	~~~~~~~~~~
 *
 *	@param key The `as_key` to destroy.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
void as_key_destroy(as_key * key);

/**
 *	Get the digest for the given key. 
 *
 *	The digest is computed the first time function is called. Subsequent calls
 *	will return the previously calculated value.
 *
 *	~~~~~~~~~~{.c}
 *	as_digest * digest = as_key_digest(key);
 *	~~~~~~~~~~
 *
 *	@param key The key to get the digest for.
 *
 *	@return The digest for the key.
 *
 *	@relates as_key
 *	@ingroup as_key_t
 */
as_digest * as_key_digest(as_key * key);
