/*
 * Copyright 2008-2014 Aerospike, Inc.
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

/**
 *	@defgroup batch_operations Batch Operations
 *	@ingroup client_operations
 *
 *	Aerospike provides a batch API to access data in the cluster. 
 *
 *	The Batch API is a collection of APIs that use as_keyset as for looking up
 *	records for accessing in the cluster. 
 *	
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	This callback will be called with the results of aerospike_batch_get(),
 *	or aerospike_batch_exists() functions.
 *
 * 	The `results` argument will be an array of `n` as_batch_read entries. The
 * 	`results` argument is on the stack and is only available within the context
 * 	of the callback. To use the data outside of the callback, copy the data.
 *
 *	~~~~~~~~~~{.c}
 *	bool my_callback(const as_batch_read * results, uint32_t n, void * udata) {
 *		return true;
 *	}
 *	~~~~~~~~~~
 *
 *	@param results 		The results from the batch request.
 *	@param n			The number of results from the batch request.
 *	@param udata 		User-data provided to the calling function.
 *	
 *	@return `true` on success. Otherwise, an error occurred.
 *
 *	@ingroup batch_operations
 */
typedef bool (* aerospike_batch_read_callback)(const as_batch_read * results, uint32_t n, void * udata);

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Look up multiple records by key, then return all bins.
 *
 *	~~~~~~~~~~{.c}
 *	as_batch batch;
 *	as_batch_inita(&batch, 3);
 *	
 *	as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 *	as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 *	as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 *	
 *	if ( aerospike_batch_get(&as, &err, NULL, &batch, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	as_batch_destroy(&batch);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param batch		The batch of keys to read.
 *	@param callback 	The callback to invoke for each record read.
 *	@param udata		The user-data for the callback.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup batch_operations
 */
as_status aerospike_batch_get(
	aerospike * as, as_error * err, const as_policy_batch * policy, 
	const as_batch * batch, 
	aerospike_batch_read_callback callback, void * udata
	);

/**
 *	Test whether multiple records exist in the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	as_batch batch;
 *	as_batch_inita(&batch, 3);
 *	
 *	as_key_init(as_batch_keyat(&batch,0), "ns", "set", "key1");
 *	as_key_init(as_batch_keyat(&batch,1), "ns", "set", "key2");
 *	as_key_init(as_batch_keyat(&batch,2), "ns", "set", "key3");
 *	
 *	if ( aerospike_batch_exists(&as, &err, NULL, &batch, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	as_batch_destroy(&batch);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param batch		The batch of keys to read.
 *	@param callback 	The callback to invoke for each record read.
 *	@param udata		The user-data for the callback.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup batch_operations
 */
as_status aerospike_batch_exists(
	aerospike * as, as_error * err, const as_policy_batch * policy, 
	const as_batch * batch, 
	aerospike_batch_read_callback callback, void * udata
	);
