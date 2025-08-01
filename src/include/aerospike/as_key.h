/*
 * Copyright 2008-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once 

#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_error.h>
#include <aerospike/as_string.h>
#include <aerospike/as_status.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

/**
 * The size of as_digest.value
 */
#define AS_DIGEST_VALUE_SIZE 20

/**
 * The maxium size of as_namespace.
 */
#define AS_NAMESPACE_MAX_SIZE 32

/**
 * The maxium size of as_set.
 */
#define AS_SET_MAX_SIZE 64

//---------------------------------
// Types
//---------------------------------

/**
 * Namespace Name
 */
typedef char as_namespace[AS_NAMESPACE_MAX_SIZE];

/**
 * Set Name
 */
typedef char as_set[AS_SET_MAX_SIZE];

/**
 * Digest value
 */
typedef uint8_t as_digest_value[AS_DIGEST_VALUE_SIZE];

/**
 * The digest is the value used to locate a record based on the
 * set and digest of the record. The digest is calculated using RIPEMD-160.
 * Keys for digests can be either a string or integer.
 */
typedef struct as_digest_s {

	/**
	 * Indicates whether the digest was calculated.
	 */
	bool init;

	/**
	 * The digest value.
	 */
	as_digest_value value;

} as_digest;

/**
 * Key value
 */
typedef union as_key_value_u {

	/**
	 * Integer value.
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
 * A key is used for locating records in the database.
 *
 * ## Initialization
 *
 * A key can either be stack or heap allocated. Use one of the following 
 * functions to properly initialize an as_key.
 * 
 * Each function requires a namespace, set and key value. The set can be 
 * and empty string.
 * 
 * For stack allocated as_key, you should you the following functions to
 * initialize the value:
 *
 * - as_key_init()	- Initialize the key with a string value.
 * - as_key_init_int64() - Initialize the key with an int64_t value.
 * - as_key_init_str() - Same as as_key_init(). 
 * - as_key_init_raw() - Initialize the key with byte array.
 * - as_key_init_value() - Initialize the key with an as_key_value.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 * @endcode
 * 
 * For heap allocated as_key, you should use the following functions
 * to allocate and initialize the value on the heap.
 *
 * - as_key_new() 	- Initialize the key with a string value.
 * - as_key_new_int64() - Initialize the key with an int64_t value.
 * - as_key_new_str() - Same as as_key_new(). 
 * - as_key_new_raw() - Initialize the key with byte array.
 * - as_key_new_value() - Initialize the key with an as_key_value.
 *
 * @code
 * as_key* key = as_key_new("ns", "set", "key");
 * @endcode
 *
 * ## Destruction
 *
 * When you no longer require an instance of as_key, you should release the
 * key and associated resources via as_key_destroy().
 *
 * @code
 * as_key_destroy(key);
 * @endcode
 *
 * This function should be used on both stack and heap allocated keys.
 *
 * ## Operations
 *
 * The following are operations which require a key.
 *
 * - aerospike_key_get()
 * - aerospike_key_select()
 * - aerospike_key_exists()
 * - aerospike_key_put()
 * - aerospike_key_operate()
 * - aerospike_key_remove()
 * - aerospike_key_apply()
 *
 * ## Digest
 *
 * Each operation that requires a key,  internally generates a digest for the 
 * key. The digest is a hash value used to locate a record in the cluster. Once
 * calculated, the digest will be reused.
 *
 * To get the digest value of a key, use as_key_digest().
 *
 * @ingroup client_objects
 */
typedef struct as_key_s {

	/**
	 * @private
	 * If true, then as_key_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * The namespace the key belongs to.
	 */
	as_namespace ns;

	/**
	 * The set the key belongs to.
	 */
	as_set set;

	/**
	 * The key value.
	 */
	as_key_value value;

	/**
	 * The key value pointer.
	 * If NULL, then there is no value.
	 * It can point to as_key.value or a different value.
	 */
	as_key_value* valuep;

	/**
	 * Digest for the key.
	 */
	as_digest digest;

} as_key;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize a stack allocated as_key to a NULL-terminated string value.
 *
 * @code
 * as_key key;
 * as_key_init(&key, "ns", "set", "key");
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 * 
 * @param key		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_init(as_key* key, const char* ns, const char* set, const char* value);

/**
 * Initialize a stack allocated as_key to a int64_t value.
 *
 * @code
 * as_key key;
 * as_key_init_int64(&key, "ns", "set", 123);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key.
 *
 * @param key		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_init_int64(as_key* key, const char* ns, const char* set, int64_t value);

/**
 * Initialize a stack allocated as_key to a NULL-terminated string value.
 *
 * @code
 * as_key key;
 * as_key_init_strp(&key, "ns", "set", stdup("key"), true);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key.
 *
 * @param key		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 * @param free		If true, then the key's value can be freed when the key is destroyed.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_init_strp(as_key* key, const char* ns, const char* set, const char* value, bool free);

/**
 * Initialize a stack allocated as_key to a NULL-terminated string value.
 *
 * @code
 * as_key key;
 * as_key_init_str(&key, "ns", "set", "key");
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key.
 *
 * @param key		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value. Must last for the lifetime of the key.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
static inline as_key*
as_key_init_str(as_key* key, const char* ns, const char* set, const char* value)
{
	return as_key_init_strp(key, ns, set, value, false);
}

/**
 * Initialize a stack allocated as_key to bytes array.
 *
 * @code
 * uint8_t * rgb = (uint8_t *) malloc(3);
 * rgb[0] = 255;
 * rgb[1] = 255;
 * rgb[3] = 255;
 * 
 * as_key key;
 * as_key_init_rawp(&key, "ns", "set", rgb, 3, true);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key.
 *
 * @param key		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 * @param size		The number of bytes in value.
 * @param free		If true, then the key's value can be freed when the key is destroyed.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_init_rawp(as_key* key, const char* ns, const char* set, const uint8_t* value, uint32_t size, bool free);

/**
 * Initialize a stack allocated as_key to bytes array.
 *
 * @code
 * uint8_t rgb[3] = {254,254,120};
 * 
 * as_key key;
 * as_key_init_raw(&key, "ns", "set", rgb, 3);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key.
 *
 * @param key		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 * @param size		The number of bytes in value. Must last for the lifetime of the key.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
static inline as_key*
as_key_init_raw(as_key* key, const char* ns, const char* set, const uint8_t* value, uint32_t size)
{
	return as_key_init_rawp(key, ns, set, value, size, false);
}

/**
 * Initialize a stack allocated as_key with a digest.
 *
 * @code
 * as_digest_value digest = {0};
 * 
 * as_key key;
 * as_key_init_digest(&key, "ns", "set", digest);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key.
 * 
 * @param key 		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param digest	The digest for the key.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_init_digest(as_key* key, const char* ns, const char* set, const as_digest_value digest);

/**
 * Initialize a stack allocated as_key to an as_key_value.
 *
 * @code
 * as_string str;
 * as_string_init(&str, "abc", false);
 * 
 * as_key key;
 * as_key_init_value(&key, "ns", "set", (as_key_value *) str);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key.
 *
 * @param key 		The key to initialize.
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 *
 * @return The initialized as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_init_value(as_key* key, const char* ns, const char* set, const as_key_value* value);

/**
 * Creates and initializes a heap allocated as_key to a NULL-terminated string value.
 *
 * @code
 * as_key* key = as_key_new("ns", "set", "key");
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_new(const char* ns, const char* set, const char* value);

/**
 * Creates and initializes a heap allocated as_key to a int64_t value.
 *
 * @code
 * as_key* key = as_key_new_int64("ns", "set", 123);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_new_int64(const char* ns, const char* set, int64_t value);

/**
 * Creates and initializes a heap allocated as_key to a NULL-terminated string value.
 *
 * @code
 * as_key* key = as_key_new_strp("ns", "set", strdup("key"), true);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 * @param free		If true, then the key's value can be freed when the key is destroyed.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_new_strp(const char* ns, const char* set, const char* value, bool free);

/**
 * Creates and initializes a heap allocated as_key to a NULL-terminated string value.
 *
 * @code
 * as_key* key = as_key_new_str("ns", "set", "key");
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value	The key's value. Must last for the lifetime of the key.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
static inline as_key*
as_key_new_str(const char* ns, const char* set, const char* value)
{
	return as_key_new_strp(ns, set, value, false);
}

/**
 * Creates and initializes a heap allocated as_key to a byte array.
 *
 * @code
 * uint8_t * rgb = (uint8_t *) malloc(3);
 * rgb[0] = 255;
 * rgb[1] = 255;
 * rgb[3] = 255;
 * 
 * as_key* key = as_key_new_rawp("ns", "set", rgb, 3, true);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 * @param size		The number of bytes in the value.
 * @param free		If true, then the key's value can be freed when the key is destroyed.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_new_rawp(const char* ns, const char* set, const uint8_t* value, uint32_t size, bool free);

/**
 * Creates and initializes a heap allocated as_key to a byte array.
 *
 * @code
 * uint8_t rgb[3] = {254,254,120};
 * 
 * as_key* key = as_key_new_raw("ns", "set", rgb, 3);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value. Must last for the lifetime of the key.
 * @param size		The number of bytes in the value.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
static inline as_key*
as_key_new_raw(const char* ns, const char* set, const uint8_t* value, uint32_t size)
{
	return as_key_new_rawp(ns, set, value, size, false);
}

/**
 * Creates and initializes a heap allocated as_key with a digest.
 *
 * @code
 * as_digest_value digest = {0};
 * 
 * as_key* key = as_key_new_digest("ns", "set", digest);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param digest	The key's digest.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_new_digest(const char* ns, const char* set, const as_digest_value digest);

/**
 * Creates and initializes a heap allocated as_key to a an as_key_value.
 *
 * @code
 * as_string str;
 * as_string_init(&str, "abc", false);
 * 
 * as_key* key = as_key_new_value("ns", "set", (as_key_value *) str);
 * @endcode
 *
 * Use as_key_destroy() to release resources allocated to as_key via
 * this function.
 *
 * @param ns 		The namespace for the key.
 * @param set		The set for the key.
 * @param value		The key's value.
 *
 * @return A new as_key on success. Otherwise NULL.
 *
 * @relates as_key
 */
AS_EXTERN as_key*
as_key_new_value(const char* ns, const char* set, const as_key_value* value);

/**
 * Destory the as_key, releasing resources.
 *
 * @code
 * as_key_destroy(key);
 * @endcode
 *
 * @param key The as_key to destroy.
 *
 * @relates as_key
 */
AS_EXTERN void
as_key_destroy(as_key* key);

/**
 * Get the digest for the given key. 
 *
 * The digest is computed the first time function is called. Subsequent calls
 * will return the previously calculated value.
 *
 * @code
 * as_digest * digest = as_key_digest(key);
 * @endcode
 *
 * @param key The key to get the digest for.
 *
 * @return The digest for the key.
 *
 * @relates as_key
 */
AS_EXTERN as_digest*
as_key_digest(as_key* key);

/**
 * Set the digest value in the key structure.  Keys must be integer, string or blob.
 * Otherwise, an error is returned.
 *
 * @param err Error message that is populated on error.
 * @param key The key to get the digest for.
 *
 * @return Status code.
 *
 * @relates as_key
 */
AS_EXTERN as_status
as_key_set_digest(as_error* err, as_key* key);

#ifdef __cplusplus
} // end extern "C"
#endif
