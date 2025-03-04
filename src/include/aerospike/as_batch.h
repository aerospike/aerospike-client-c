/*
 * Copyright 2008-2024 Aerospike, Inc.
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

#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * A collection of keys to be batch processed.
 */
typedef struct as_batch_s {

	/**
	 * Sequence of keys in the batch.
	 */
	struct {

		/**
		 * The keys contained by this batch.
		 */
		as_key* entries;

		/**
		 * The number of keys this structure contains.
		 */
		uint32_t size;

		/**
		 * If true, then this structure will be freed when as_batch_destroy()
		 * is called.
		 */
		bool _free;

	} keys;

	/**
	 * If true, then this structure will be freed when as_batch_destroy()
	 * is called.
	 */
	bool _free;

} as_batch;

/**
 * Batch key and record result.
 */
typedef struct as_batch_result_s {
	/**
	 * The requested key.
	 */
	const as_key* key;

	/**
	 * Record result after batch command has completed.  Will be null if record was not found
	 * or an error occurred.
	 */
	as_record record;

	/**
	 * Result code for this returned record. If not AEROSPIKE_OK, the record will be null.
	 */
	as_status result;

	/**
	 * Is it possible that the write command may have completed even though an error
	 * occurred for this record. This may be the case when a client error occurs (like timeout)
	 * after the command was sent to the server.
	 */
	bool in_doubt;
} as_batch_result;

/**
 * The (key, result, record) for an entry in a batch read.
 * @deprecated Use as_batch_result instead.
 */
typedef as_batch_result as_batch_read;

//---------------------------------
// Macros
//---------------------------------

/** 
 * Initializes `as_batch` with specified capacity using alloca().
 *
 * For heap allocation, use `as_batch_new()`.
 *
 * ~~~~~~~~~~{.c}
 * as_batch batch;
 * as_batch_inita(&batch, 2);
 * as_key_init(as_batch_get(&batch, 0), "ns", "set", "key1");
 * as_key_init(as_batch_get(&batch, 1), "ns", "set", "key2");
 * ~~~~~~~~~~
 *
 * When the batch is no longer needed, then use as_batch_destroy() to
 * release the batch and associated resources.
 * 
 * @param __batch		The query to initialize.
 * @param __size		The number of keys to allocate.
 *
 * @relates as_batch
 * @ingroup batch_operations
 */
#define as_batch_inita(__batch, __size) \
	do {\
		(__batch)->keys.entries = (as_key*) alloca(sizeof(as_key) * (__size));\
		if ((__batch)->keys.entries) { \
			(__batch)->keys.size = (__size);\
			(__batch)->keys._free = false;\
		}\
		(__batch)->_free = false;\
	} while(0)

//---------------------------------
// Functions
//---------------------------------

/**
 * Create and initialize a heap allocated as_batch capable of storing
 * `capacity` keys.
 *
 * ~~~~~~~~~~{.c}
 * as_batch* batch = as_batch_new(2);
 * as_key_init(as_batch_get(batch, 0), "ns", "set", "key1");
 * as_key_init(as_batch_get(batch, 1), "ns", "set", "key2");
 * ~~~~~~~~~~
 *
 * When the batch is no longer needed, then use as_batch_destroy() to
 * release the batch and associated resources.
 * 
 * @param size			The number of keys to allocate.
 *
 * @relates as_batch
 * @ingroup batch_operations
 */
AS_EXTERN as_batch*
as_batch_new(uint32_t size);

/**
 * Initialize a stack allocated as_batch capable of storing `capacity` keys.
 *
 * ~~~~~~~~~~{.c}
 * as_batch batch;
 * as_batch_init(&batch, 2);
 * as_key_init(as_batch_get(&batch, 0), "ns", "set", "key1");
 * as_key_init(as_batch_get(&batch, 1), "ns", "set", "key2");
 * ~~~~~~~~~~
 *
 * When the batch is no longer needed, then use as_batch_destroy() to
 * release the batch and associated resources.
 * 
 * @param batch			The batch to initialize.
 * @param size			The number of keys to allocate.
 * 
 * @relates as_batch
 * @ingroup batch_operations
 */
AS_EXTERN as_batch*
as_batch_init(as_batch* batch, uint32_t size);

/**
 * Destroy the batch of keys.
 *
 * ~~~~~~~~~~{.c}
 * as_batch_destroy(batch);
 * ~~~~~~~~~~
 *
 * @param batch 	The batch to release.
 *
 * @relates as_batch
 * @ingroup batch_operations
 */
AS_EXTERN void
as_batch_destroy(as_batch* batch);

/**
 * Get the key at given position of the batch. If the position is not
 * within the allocated capacity for the batchm then NULL is returned.
 *
 * @param batch 		The batch to get the key from.
 * @param i			The position of the key.
 *
 * @return On success, the key at specified position. If position is invalid, then NULL.
 *
 * @relates as_batch
 * @ingroup batch_operations
 */
static inline as_key*
as_batch_keyat(const as_batch* batch, uint32_t i)
{
	return (batch != NULL && batch->keys.entries != NULL && batch->keys.size > i) ? &batch->keys.entries[i] : NULL;
}

#ifdef __cplusplus
} // end extern "C"
#endif
