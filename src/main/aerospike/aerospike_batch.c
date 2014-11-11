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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "_shim.h"

#include <citrusleaf/cl_batch.h>
#include "../citrusleaf/internal.h"

/************************************************************************
 * 	TYPES
 ************************************************************************/

typedef struct batch_bridge_s {

	// Needed for logging only.
	aerospike * as;

	// Array of results.
	as_batch_read * results;

	// Number of array elements.
	uint32_t n;

} batch_bridge;

/**************************************************************************
 * 	STATIC FUNCTIONS
 **************************************************************************/

static int
cl_batch_cb(char *ns, cf_digest *keyd, char *set, cl_object *key, int result,
		uint32_t generation, uint32_t ttl, cl_bin *bins, uint16_t n_bins,
		void *udata)
{
	batch_bridge * p_bridge = (batch_bridge *) udata;
	as_batch_read * p_r = NULL;

	// Find the digest.
	for (uint32_t i = 0; i < p_bridge->n; i++) {
		p_r = &p_bridge->results[i];

		if (memcmp(keyd, p_r->key->digest.value, AS_DIGEST_VALUE_SIZE) == 0) {
			// Not bothering to check set, which is not always filled.
			break;
		}

		p_r = NULL;
	}

	if (! p_r) {
		as_log_error("Couldn't find digest");
		return -1; // not that this is even checked...
	}

	// Fill out this result slot.
	as_error err;
	p_r->result = as_error_fromrc(&err, result);

	// If the result wasn't success, we won't have any record data or metadata.
	if (result != 0) {
		return 0;
	}

	as_record_init(&p_r->record, n_bins); // works even if n_bins is 0

	// There should be record metadata.
	p_r->record.gen = (uint16_t)generation;
	p_r->record.ttl = ttl;

	// There may be bin data.
	if (n_bins != 0) {
		clbins_to_asrecord(bins, (uint32_t)n_bins, &p_r->record);
	}

	return 0;
}

static as_status batch_read(
		aerospike * as, as_error * err, const as_policy_batch * policy,
		const as_batch * batch,
		aerospike_batch_read_callback callback, void * udata,
		bool get_bin_data
		)
{
	as_error_reset(err);

	// Lazily initialize batch machinery:
	cl_cluster_batch_init(as->cluster);

	uint32_t n = batch->keys.size;
	as_batch_read* results = (as_batch_read*)alloca(sizeof(as_batch_read) * n);

	if (! results) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"failed results array allocation");
	}

	cf_digest* digests = (cf_digest*)alloca(sizeof(cf_digest) * n);

	if (! digests) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"failed digests array allocation");
	}

	// Because we're wrapping the old functionality, we only support a batch
	// with all keys in the same namespace.
	char* ns = batch->keys.entries[0].ns;

	for (uint32_t i = 0; i < n; i++) {
		if (strcmp(ns, batch->keys.entries[i].ns) != 0) {
			// Don't need to destroy results' records since they won't have any
			// associated allocations yet.
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
					"batch keys must all be in the same namespace");
		}

		as_batch_read * p_r = &results[i];

		p_r->result = -1; // TODO - make an 'undefined' error
		as_record_init(&p_r->record, 0);
		p_r->key = (const as_key*)as_batch_keyat(batch, i);

		memcpy(&digests[i], as_key_digest((as_key*)p_r->key)->value,
				AS_DIGEST_VALUE_SIZE);
	}

	batch_bridge bridge;
	bridge.as = as;
	bridge.results = results;
	bridge.n = n;

	cl_rv rc = citrusleaf_batch_read(as->cluster, ns, digests, n, NULL, 0,
			get_bin_data, cl_batch_cb, &bridge);

	callback(results, n, udata);

	for (uint32_t i = 0; i < n; i++) {
		as_record_destroy(&results[i].record);
	}

	return as_error_fromrc(err, rc);
}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Look up multiple records by key, then return all bins.
 */
as_status aerospike_batch_get(
	aerospike * as, as_error * err, const as_policy_batch * policy, 
	const as_batch * batch, 
	aerospike_batch_read_callback callback, void * udata
	)
{
	return batch_read(as, err, policy, batch, callback, udata, true);
}

/**
 *	Test whether multiple records exist in the cluster.
 */
as_status aerospike_batch_exists(
	aerospike * as, as_error * err, const as_policy_batch * policy, 
	const as_batch * batch, 
	aerospike_batch_read_callback callback, void * udata
	)
{
	return batch_read(as, err, policy, batch, callback, udata, false);
}
