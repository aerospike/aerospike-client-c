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

#include <citrusleaf/cl_batch.h>
/************************************************************************
 * 	STRUCTURES
 ************************************************************************/

typedef struct batch_bridge_s{
	
	//user-provided data
	void *udata;

	//user-provided callback for batch read
	aerospike_batch_read_callback read_cb;
	
	//user-provided callback for batch exist
	aerospike_batch_exists_callback exist_cb;

	//to distinguish between read and exists callbacks
	bool is_read;
} batch_bridge;

/**************************************************************************
 * 	STATIC FUNCTIONS
 **************************************************************************/

static int simplebatch_cb(
	char *ns, cf_digest *keyd, char *set, uint32_t generation,
	uint32_t record_void_time, cl_bin *bins, int n_bins, bool is_last, void *udata)
{
	batch_bridge * bridge = (batch_bridge * ) udata;
	
	// Fill the bin data
	as_record _rec, * rec = &_rec;
    as_record_inita(rec, n_bins);
    clbins_to_asrecord(bins, n_bins, rec); 	

	// Fill the metadata
	 as_key_init_value(&rec->key, ns, set, NULL);
     memcpy(rec->key.digest.value, keyd, sizeof(cf_digest));
     rec->key.digest.init = true;
     rec->gen = generation;
     rec->ttl = record_void_time;
	
	// Call the callback that user wanted to callback
	if( bridge->is_read == true) {
		 bridge->read_cb(&rec->key, rec, bridge->udata);
	}
	// The responsibility to free the bins is on the called callback function
	citrusleaf_bins_free(bins, n_bins);

	// release the record
	as_record_destroy(rec);

    return 0;
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
	as_status rc = AEROSPIKE_OK;

	batch_bridge bridge_udata; 
	bridge_udata.udata = udata;
	bridge_udata.read_cb = callback;

	int n_digests = batch->keys.size;
	cf_digest * digests = NULL;

	if (n_digests > 0) {
		digests = (cf_digest *) malloc (sizeof(cf_digest) * n_digests);
		for ( int i=0; i<n_digests; i++){
			memcpy(digests[i].digest, batch->keys.entries[i].digest.value, sizeof(cf_digest));
		}
	}		
	
	
	cl_rv rv = citrusleaf_get_many_digest(as->cluster, batch->keys.entries[0].ns, digests, n_digests, NULL, 0, 
					batch->get_key, simplebatch_cb, &bridge_udata);
		
	rc = as_error_fromrc(err, rv);

	return rc;
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
	aerospike_batch_exists_callback callback, void * udata
	)
{
	return AEROSPIKE_OK;
}
