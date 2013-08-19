/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <aerospike/as_batch.h>
#include <citrusleaf/cl_batch.h>
#include <citrusleaf/cf_random.h>

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

/**
 *	Get the key at given position of the batch. If the position is not
 *	within the allocated capacity for the batchm then NULL is returned.
 */
extern inline as_key * as_batch_get(as_batch * batch, uint32_t i);

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Create and initialize a heap allocated as_batch capable of storing
 *	`capacity` keys.
 */
as_batch * as_batch_new(uint32_t capacity)
{
	as_batch * batch = (as_batch *) malloc(sizeof(as_batch) + sizeof(as_key) * capacity);
	if ( !batch ) return NULL;

	batch->_free = true;
	batch->keys._free = false;
	batch->keys.size = 0;
	batch->keys.capacity = capacity;
	batch->keys.entries = (as_key *) (batch + sizeof(batch));
	return batch;
}

/**
 *	Initialize a stack allocated as_batch capable of storing `capacity` keys.
 */
as_batch * as_batch_init(as_batch * batch, uint32_t capacity)
{
	if ( !batch ) return batch;

	as_key * entries = NULL;
	if ( capacity > 0 ) {
		entries = (as_key *) malloc(sizeof(as_key) * capacity);
		if ( !entries ) return batch;
	}

	batch->_free = false;
	batch->keys._free = true;
	batch->keys.size = 0;
	batch->keys.capacity = capacity;
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
			free(batch->keys.entries);
		}

		batch->keys._free = false;
		batch->keys.size = 0;
		batch->keys.capacity = 0;
		batch->keys.entries = NULL;
	}

	if ( batch->_free ) {
		free(batch);
	}
}