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
 *	@defgroup digest Digest API
 *	@{
 */

#pragma once 

#include <aerospike/as_bin.h>
#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	The length of as_digest.value[]. 
 */
#define AS_DIGEST_VALUE_LEN 20

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Key value
 */
typedef struct as_key_s {

	/**
	 *	The type of key.
	 */
	as_type type;

	/**
	 *	The key used to generate the digest.
	 */
	union {

		/**
		 *	The int64_t value of the key.
		 */
		int64_t int64;

		/**
		 *	The NULL terminated string value of the key.
		 */
		char * str;

	} value;

} as_key;

/**
 *	The digest is the value used to locate a record based on the
 *	set and key of the record. The digest is calculated using RIPEMD-160.
 *	Keys for digests can be either a string or integer.
 *
 *	To initialize a stack allocated instance as an string key:
 *	
 *	~~~~~~~~~~{.c}
 *	    as_digest digest;
 *	    as_digest_init(&digest, "set", "key");
 *	~~~~~~~~~~
 *
 *	Alternatively, you can use an integer key:
 *
 *	~~~~~~~~~~{.c}
 *	    as_digest_init2(&digest, "set", 123);
 *	~~~~~~~~~~
 *
 *	You can also heap allocate and initialize an instance:
 *
 *	~~~~~~~~~~{.c}
 *	    as_digest * digest = as_digest_new("set", "key");
 *	~~~~~~~~~~
 *	
 *	When you are finished using the digest, you should always destroy it:
 *
 *	~~~~~~~~~~{.c}
 *	    as_digest_destroy(digest);
 *	~~~~~~~~~~
 *	
 */
typedef struct as_digest_s {

	/**
	 *	@private
	 *	If true, then as_digest_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	The set used to generate the digest.
	 */
	char * set;

	/**
	 *	The key used to generate the digest.
	 */
	as_key key;

	/**
	 *	The digest value.
	 */
	uint8_t value[AS_DIGEST_VALUE_LEN];

} as_digest;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initializes a stack allocated digest.
 *
 *	@param digest 		The digest to initialize.
 *	@param set 			The set for the digest.
 *	@param key 			The key for the digest. A NULL-terminated string.
 *
 *	@return The initialized digest on success. Otherwise NULL;
 */
as_digest * as_digest_init(as_digest * digest, const char * set, const char * key);

/**
 *	Initializes a stack allocated digest containing an int64_t key.
 *
 *	@param digest 		The digest to initialize.
 *	@param set 			The set for the digest.
 *	@param key 			The key for the digest.
 *
 *	@return The initialized digest on success. Otherwise NULL;
 */
as_digest * as_digest_init2(as_digest * digest, const char * set, int64_t key);

/**
 *	Creates a new heap allocated digest.
 *
 *	@param set 			The set for the digest.
 *	@param key 			The key for the digest.
 *
 *	@return The new digest on success. Otherwise NULL;
 */
as_digest * as_digest_new(const char * set, const char * key);

/**
 *	Creates a new heap allocated digest containing an int64_t key.
 *
 *	@param set 			The set for the digest.
 *	@param key 			The key for the digest.
 *
 *	@return The new digest on success. Otherwise NULL;
 */
as_digest * as_digest_new2(const char * set, int64_t key);

/**
 *	Destory the digest, releasing resources.
 *
 *	@param digest The digest to destroy.
 */
void as_digest_destroy(as_digest * digest);

/**
 *	Compute the digest value.
 *
 *	@param digest The digest to recompute.
 */
void as_digest_compute(as_digest * digest);

/**
 *	@}
 */