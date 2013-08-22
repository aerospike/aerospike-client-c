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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "_log.h"
#include "_shim.h"

#include <citrusleaf/cl_batch.h>

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
cl_batch_cb(char *ns, cf_digest *keyd, char *set, int result,
		uint32_t generation, uint32_t ttl, cl_bin *bins, uint16_t n_bins,
		void *udata)
{
	batch_bridge * p_bridge = (batch_bridge *) udata;
	aerospike * as = p_bridge->as;
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
		as_err(LOGGER, "couldn't find digest");
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
		aerospike * as, as_error * err, const as_policy_read * policy,
		const as_batch * batch,
		aerospike_batch_read_callback callback, void * udata,
		bool get_bin_data
		)
{
	as_error_reset(err);

	// This is not very nice:
	citrusleaf_batch_init();

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

	for (uint32_t i = 0; i < n; i++) {
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

	cl_rv rc = citrusleaf_batch_read(as->cluster, batch->keys.entries[0].ns,
			digests, n, NULL, 0, get_bin_data, cl_batch_cb, &bridge);

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
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_batch * batch, 
	aerospike_batch_read_callback callback, void * udata
	)
{
	return batch_read(as, err, policy, batch, callback, udata, true);
}

/**
 *	Look up multiple records by key, then return selected bins.
 */
as_status aerospike_batch_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_batch * batch, const char * bins[], 
	aerospike_batch_read_callback callback, void * udata
	)
{
	return AEROSPIKE_OK;
}

/**
 *	Test whether multiple records exist in the cluster.
 */
as_status aerospike_batch_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_batch * batch, 
	aerospike_batch_read_callback callback, void * udata
	)
{
	return batch_read(as, err, policy, batch, callback, udata, false);
}
