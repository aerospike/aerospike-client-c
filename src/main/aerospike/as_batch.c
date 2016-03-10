/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_batch.h>

#include <citrusleaf/alloc.h>

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

/**
 *	Get the key at given position of the batch. If the position is not
 *	within the allocated capacity for the batchm then NULL is returned.
 */
extern inline as_key * as_batch_keyat(const as_batch * batch, uint32_t i);


/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Create and initialize a heap allocated as_batch capable of storing
 *	`capacity` keys.
 */
as_batch * as_batch_new(uint32_t size)
{
	as_batch * batch = (as_batch *) cf_malloc(sizeof(as_batch) + sizeof(as_key) * size);
	if ( !batch ) return NULL;

	batch->_free = true;
	batch->keys._free = false;
	batch->keys.size = size;
	batch->keys.entries = (as_key *) (batch + 1);
	return batch;
}

/**
 *	Initialize a stack allocated as_batch capable of storing `capacity` keys.
 */
as_batch * as_batch_init(as_batch * batch, uint32_t size)
{
	if ( !batch ) return batch;

	as_key * entries = NULL;
	if ( size > 0 ) {
		entries = (as_key *) cf_malloc(sizeof(as_key) * size);
		if ( !entries ) return batch;
	}

	batch->_free = false;
	batch->keys._free = true;
	batch->keys.size = size;
	batch->keys.entries = entries;
	return batch;
}

/**
 *	Destroy the batch of keys.
 */
void as_batch_destroy(as_batch * batch)
{
	if ( !batch ) return;

	if ( batch->keys.entries ) {

		for (int i = 0; i < batch->keys.size; i++ ) {
			as_key_destroy(&batch->keys.entries[i]);
		}

		if ( batch->keys._free ) {
			cf_free(batch->keys.entries);
		}

		batch->keys._free = false;
		batch->keys.size = 0;
		batch->keys.entries = NULL;
	}

	if ( batch->_free ) {
		cf_free(batch);
	}
}
