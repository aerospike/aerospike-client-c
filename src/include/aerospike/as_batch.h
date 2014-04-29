/******************************************************************************
 *      Copyright 2008-2013 by Aerospike.
 *
 *      Permission is hereby granted, free of charge, to any person obtaining a copy 
 *      of this software and associated documentation files (the "Software"), to 
 *      deal in the Software without restriction, including without limitation the 
 *      rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *      sell copies of the Software, and to permit persons to whom the Software is 
 *      furnished to do so, subject to the following conditions:
 *      
 *      The above copyright notice and this permission notice shall be included in 
 *      all copies or substantial portions of the Software.
 *      
 *      THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *      IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *      FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *      AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *      LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *      FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *      IN THE SOFTWARE.
 *****************************************************************************/

#pragma once 
#pragma GCC diagnostic ignored "-Waddress"

#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <stdint.h>
#include <stdbool.h>

/*****************************************************************************
 *	STRUCTURES
 *****************************************************************************/

/**
 *	A collection of keys to be batch processed.
 */
typedef struct as_batch_s {

	/**
	 *	If true, then this structure will be freed when as_batch_destroy() 
	 *	is called.
	 */
	bool _free;
	
	/**
	 *	Sequence of keys in the batch.
	 */
	struct {

		/**
		 *	If true, then this structure will be freed when as_batch_destroy()
		 *	is called.
		 */
		bool _free;

		/**
		 *	The number of keys this structure contains.
		 */
		uint32_t size;

		/**
		 *	The keys contained by this batch.
		 */
		as_key * entries;

	} keys;

} as_batch;

/**
 *	The (key, result, record) for an entry in a batch read.
 *	The result is AEROSPIKE_OK if the record is found,
 *	AEROSPIKE_ERR_RECORD_NOT_FOUND if the transaction succeeds but the record is
 *	not found, or another error code if the transaction fails.
 *	The record is NULL if either the transaction failed or the record does not
 *	exist. For aerospike_batch_exists() calls the record will never contain bins
 *	but will contain metadata (generation and expiration).
 */
typedef struct as_batch_read_s {

	/**
	 *	The key requested.
	 */
	const as_key * key;

	/**
	 *	The result of the transaction to read this key.
	 */
	as_status result;

	/**
	 *	The record for the key requested, NULL if the key was not found.
	 */
	as_record record;

} as_batch_read;


/*********************************************************************************
 *	INSTANCE MACROS
 *********************************************************************************/


/** 
 *	Initializes `as_batch` with specified capacity using alloca().
 *
 *	For heap allocation, use `as_batch_new()`.
 *
 *	~~~~~~~~~~{.c}
 *	as_batch batch;
 *	as_batch_inita(&batch, 2);
 *	as_key_init(as_batch_get(&batch, 0), "ns", "set", "key1");
 *	as_key_init(as_batch_get(&batch, 1), "ns", "set", "key2");
 *	~~~~~~~~~~
 *
 *	When the batch is no longer needed, then use as_batch_destroy() to
 *	release the batch and associated resources.
 *	
 *	@param __batch		The query to initialize.
 *	@param __capacity	The number of keys to allocate.
 *
 *	@relates as_batch
 *	@ingroup batch_object
 */
#define as_batch_inita(__batch, __size) \
	if ( (__batch) != NULL ) {\
		(__batch)->_free = false;\
		(__batch)->keys.entries = (as_key *) alloca(sizeof(as_key) * __size);\
		if ( (__batch)->keys.entries ) { \
			(__batch)->keys._free = false;\
			(__batch)->keys.size = __size;\
		}\
 	}

/*********************************************************************************
 *	INSTANCE FUNCTIONS
 *********************************************************************************/

/**
 *	Create and initialize a heap allocated as_batch capable of storing
 *	`capacity` keys.
 *
 *	~~~~~~~~~~{.c}
 *	as_batch * batch = as_batch_new(2);
 *	as_key_init(as_batch_get(batch, 0), "ns", "set", "key1");
 *	as_key_init(as_batch_get(batch, 1), "ns", "set", "key2");
 *	~~~~~~~~~~
 *
 *	When the batch is no longer needed, then use as_batch_destroy() to
 *	release the batch and associated resources.
 *	
 *	@param capacity		The number of keys to allocate.
 *
 *	@relates as_batch
 *	@ingroup batch_object
 */
as_batch * as_batch_new(uint32_t size);

/**
 *	Initialize a stack allocated as_batch capable of storing `capacity` keys.
 *
 *	~~~~~~~~~~{.c}
 *	as_batch batch;
 *	as_batch_init(&batch, 2);
 *	as_key_init(as_batch_get(&batch, 0), "ns", "set", "key1");
 *	as_key_init(as_batch_get(&batch, 1), "ns", "set", "key2");
 *	~~~~~~~~~~
 *
 *	When the batch is no longer needed, then use as_batch_destroy() to
 *	release the batch and associated resources.
 *	
 *	@param batch		The batch to initialize.
 *	@param capacity		The number of keys to allocate.
 *	
 *	@relates as_batch
 *	@ingroup batch_object
 */
as_batch * as_batch_init(as_batch * batch, uint32_t size);

/**
 *	Destroy the batch of keys.
 *
 *	~~~~~~~~~~{.c}
 *	as_batch_destroy(batch);
 *	~~~~~~~~~~
 *
 *	@param batch 	The batch to release.
 *
 *	@relates as_batch
 *	@ingroup batch_object
 */
void as_batch_destroy(as_batch * batch);

/**
 *	Get the key at given position of the batch. If the position is not
 *	within the allocated capacity for the batchm then NULL is returned.
 *
 *	@param batch 	The batch to get the key from.
 *	@param i		The position of the key.
 *
 *	@return On success, the key at specified position. If position is invalid, then NULL.
 *
 *	@relates as_batch
 *	@ingroup batch_object
 */
static inline as_key * as_batch_keyat(const as_batch * batch, uint32_t i)
{
	return (batch != NULL && batch->keys.entries != NULL && batch->keys.size > i) ? &batch->keys.entries[i] : NULL;
}
