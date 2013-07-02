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

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_key.h>

#include <citrusleaf/as_scan.h>
#include <citrusleaf/cl_scan.h>
#include <citrusleaf/cf_random.h>

#include "_log.h"
#include "_policy.h"
#include "_shim.h"

/******************************************************************************
 * LOCAL STRUCTURES
 *****************************************************************************/

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct scan_bridge_s
{
	void							* user_udata; 	// udata actually passed by the user
	aerospike_scan_foreach_callback	user_cb;		// The callback the user wanted to call
} scan_bridge;

// TODO: not sure what this is needed for
//typedef int (* citrusleaf_udf_scan_callback)(as_val *, void *);

/******************************************************************************
 * FUNCTION DECLS
 *****************************************************************************/

as_status aerospike_scan_init(aerospike * as, as_error * err);

as_status aerospike_scan_destroy(aerospike * as, as_error * err);

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static void as_scan_toclscan(const as_scan * scan, const as_policy_scan * policy, cl_scan * clscan, bool background, uint64_t * job_id) 
{
	clscan->job_id = 0;
	clscan->ns = scan->namespace;
	clscan->setname = scan->set;
	clscan->params.fail_on_cluster_change = policy->fail_on_cluster_change;
	clscan->params.priority = scan->priority;
	clscan->params.pct = scan->percent;

	if ( background ) {
		if ( job_id != NULL ) {
			clscan->job_id = *job_id;
		}

		if ( clscan->job_id == 0 ) {
			clscan->job_id = (cf_get_rand64())/2;
			if ( job_id != NULL ) {
				*job_id = clscan->job_id;
			}
		}
	}

	if ( scan->foreach.module && scan->foreach.function ) {
		if ( background ) {
			clscan->udf.type = CL_SCAN_UDF_BACKGROUND;
		}
		else {
			clscan->udf.type = CL_SCAN_UDF_CLIENT_RECORD;
		}
		clscan->udf.filename = scan->foreach.module;
		clscan->udf.function = scan->foreach.function;
		clscan->udf.arglist = scan->foreach.arglist;
	}
	else {
		clscan->udf.type = CL_SCAN_UDF_NONE;
		clscan->udf.filename = NULL;
		clscan->udf.function = NULL;
		clscan->udf.arglist = NULL;
	}
}

static as_status process_node_response(cf_vector *v, as_error *err)
{
	as_status rc = AEROSPIKE_OK;
	
	// This returns a vector of return values, the size of which is the size of the cluster
	int sz = cf_vector_size(v);
	cl_node_response resp;
	for(int i=0; i < sz; i++) {

		cf_vector_get(v, i, &resp);
		// Even if one of the node responded with an error, set the overall status as error
		if (resp.node_response != CITRUSLEAF_OK) {
			rc = as_error_fromrc(err, resp.node_response);
		}

		// Set the resp back to zero
		memset(&resp, 0, sizeof(cl_node_response));
	}

	// Free the result vector
	cf_vector_destroy(v);

	return rc;
}

/**
 * The is the proxy callback function. This will be passed to the old simple-scan
 * mechanism. When this gets called, it will create an as_val structure out of the
 * record and will call the callback that user supplied (folded into the udata structure)
 */
static int simplescan_cb(char *ns, cf_digest *keyd, char *set, uint32_t generation,
		uint32_t record_void_time, cl_bin *bins, int n_bins, bool is_last, void *udata)
{
		scan_bridge *bridge_udata = (scan_bridge *)udata;

		// Fill the bin data
		as_record r;
		as_record_inita(&r, n_bins);
		clbins_to_asrecord(bins, n_bins, &r);

		// Fill the metadata
		as_key_init_value(&r.key, ns, set, NULL);
		memcpy(r.key.digest.value, keyd, sizeof(cf_digest));
		r.key.digest.init = true;
		r.gen = generation;
		r.ttl = record_void_time;

		// Call the callback that user wanted to callback
		(*bridge_udata->user_cb)((as_val *)&r, bridge_udata->user_udata);
		
		return 0;
}

/**
 * This is the main driver function which can cater to different types of
 * scan interfaces exposed to the outside world. This functions should not be
 * exposed to the outside world. This generic function exists because we dont
 * want to duplicate too much of code.
 * 
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param node      - the name of the node to perform the scan on.
 * @param scan      - the scan to perform
 * @param callback  - the function to be called for each record scanned.
 * @param udata     - user-data to be passed to the callback
 *
 * @return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
static as_status aerospike_scan_generic(
	aerospike * as, as_error * err, const as_policy_scan * policy,
	const char * node, const as_scan * scan,
	aerospike_scan_foreach_callback callback, void * udata)
{

	cl_rv clrv;
	as_status rc = AEROSPIKE_OK;

	cl_scan clscan;
	as_scan_toclscan(scan, policy, &clscan, false, NULL);

	if ( clscan.udf.type == CL_SCAN_UDF_NONE ) {

		scan_bridge bridge_udata = {
			.user_udata = udata,
			.user_cb = callback
		};

		struct cl_scan_parameters_s params = {
			.fail_on_cluster_change = clscan.params.fail_on_cluster_change,
			.priority = clscan.params.priority,
			.concurrent_nodes = false,
			.threads_per_node = 0
		};

		// If the user want to execute only on a single node...
		if (node) {
			clrv = citrusleaf_scan_node(as->cluster, (char *) node, scan->namespace, scan->set, NULL, 0, 
						scan->no_bins, scan->percent, simplescan_cb, &bridge_udata, &params);
			rc = as_error_fromrc(err, clrv);
		} else {

			// We are not using the very old citrusleaf_scan() call here. First of all, its
			// very inefficient. It makes a single node on the cluster coordinate the job
			// of scan. Moreover, it does not accept params like priority etc.
			cf_vector *v = citrusleaf_scan_all_nodes(as->cluster, scan->namespace, scan->set, NULL, 0, 
						scan->no_bins, scan->percent, simplescan_cb, &bridge_udata, &params);
			rc = process_node_response(v, err);
		}

	} else {
		// If the user want to execute only on a single node...
		if (node) {
			clrv = citrusleaf_udf_scan_node(as->cluster, &clscan, (char *)node, (aerospike_scan_foreach_callback) callback, udata);
			rc = as_error_fromrc(err, clrv);
		} 
		else {

			cf_vector *v = citrusleaf_udf_scan_all_nodes(as->cluster, &clscan, (aerospike_scan_foreach_callback) callback, udata);
			rc = process_node_response(v, err);
		}
	}

	return rc;
}


/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Scan the records in the specified namespace and set in the cluster.
 *
 * Scan will be run in the background by a thread on client side.
 * 
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param scan      - the scan to perform
 * @param scan_id   - the id for the scan job, which can be used for querying the status of the scan.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, uint64_t * scan_id
	)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_scan p;
	as_policy_scan_resolve(&p, &as->config.policies, policy);
	
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	cl_scan clscan;
	as_scan_toclscan(scan, &p, &clscan, true, scan_id);

	cf_vector *v = citrusleaf_udf_scan_background(as->cluster, &clscan);
	as_status rc = process_node_response(v, err);
	return rc;
}

/**
 * Scan the records in the specified namespace and set in a specified node.
 *
 * Scan will be run in the background by a thread on client side.
 *
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param scan      - the scan to perform
 * @param scan_id   - the id for the scan job, which can be used for querying the status of the scan.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_node_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const char * node, const as_scan * scan, uint64_t * scan_id) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_scan p;
	as_policy_scan_resolve(&p, &as->config.policies, policy);
	
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	cl_scan clscan;
	as_scan_toclscan(scan, &p, &clscan, true, scan_id);

	cl_rv clrv = citrusleaf_udf_scan_node_background(as->cluster, &clscan, (char *) node);
	as_status rc = as_error_fromrc(err, clrv);
	return rc;
}

/**
 * Scan the records in the specified namespace and set in the cluster.
 * Call the callback function for each record scanned. When all records have 
 * been scanned, then callback will be called with a NULL value for the record.
 * 
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param scan      - the scan to perform
 * @param udata     - user-data to be passed to the callback
 * @param callback  - the function to be called for each record scanned.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_foreach(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, 
	aerospike_scan_foreach_callback callback, void * udata) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_scan p;
	as_policy_scan_resolve(&p, &as->config.policies, policy);
	
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	return aerospike_scan_generic(as, err, &p, NULL, scan, callback, udata);
}

/**
 * Scan the records in the specified namespace and set on a single node in the cluster.
 * Call the callback function for each record scanned. When all records have 
 * been scanned, then callback will be called with a NULL value for the record.
 * 
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param node      - the name of the node to perform the scan on.
 * @param scan      - the scan to perform
 * @param udata     - user-data to be passed to the callback
 * @param callback  - the function to be called for each record scanned.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_node_foreach(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const char * node, const as_scan * scan, 
	aerospike_scan_foreach_callback callback, void * udata) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_scan p;
	as_policy_scan_resolve(&p, &as->config.policies, policy);
	
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	return aerospike_scan_generic(as, err, &p, node, scan, callback, udata);
}

/**
 * Initialize scan environment
 */
as_status aerospike_scan_init(aerospike * as, as_error * err) 
{
	extern cf_atomic32 scan_initialized;
	if ( scan_initialized > 0 ) {
		return AEROSPIKE_OK;
	}
	citrusleaf_scan_init();
	return AEROSPIKE_OK;
}

/**
 * Destroy scan environment
 */
as_status aerospike_scan_destroy(aerospike * as, as_error * err) 
{
	extern cf_atomic32 scan_initialized;
	if ( scan_initialized == 0 ) {
		return AEROSPIKE_OK;
	}
	citrusleaf_scan_shutdown();
	return AEROSPIKE_OK;
}
