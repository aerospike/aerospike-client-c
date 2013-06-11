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
#include <citrusleaf/as_scan.h>
#include <citrusleaf/cl_scan.h>
#include <citrusleaf/cf_random.h>
#include "shim.h"

/******************************************************************************
 * LOCAL STRUCTURES
 *****************************************************************************/
typedef struct scan_bridge_s
{
	void							*user_udata; // udata actually passed by the user
	aerospike_scan_foreach_callback	*user_cb;	// The callback the user wanted to call
} scan_bridge;

/******************************************************************************
 * TYPES
 *****************************************************************************/
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

static void as_scan_toclscan(const as_scan * scan, const as_policy_scan * policy, cl_scan * clscan) 
{
	clscan->ns = scan->namespace;
	clscan->setname = scan->set;
	clscan->params.fail_on_cluster_change = policy->fail_on_cluster_change;
	clscan->params.priority = scan->priority;
	clscan->params.pct = scan->percent;
	clscan->udf.filename = scan->foreach.module;
	clscan->udf.function = scan->foreach.function;
	clscan->udf.arglist = scan->foreach.arglist;

	switch (scan->type) {
		case AS_SCAN_TYPE_NORMAL:
			clscan->udf.type = AS_SCAN_UDF_NONE;
			break;
		case AS_SCAN_TYPE_UDF_RECORD:
			clscan->udf.type = AS_SCAN_UDF_CLIENT_RECORD;
			break;
		case AS_SCAN_TYPE_UDF_BACKGROUND:
			clscan->udf.type = AS_SCAN_UDF_BACKGROUND;
			break;
		default:
			fprintf(stderr, "Encountered an unknown type of scan\n");
	}
}

static as_status process_node_response(cf_vector *v, as_error *err)
{
	as_status rc = AEROSPIKE_OK;
	
	// This returns a vector of return values, the size of which is the size of the cluster
	int sz = cf_vector_size(v);
	cl_node_response resp;
	for(int i=0; i <= sz; i++) {

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
		as_record *r = as_record_new(n_bins);
		as_record_frombins(r, bins, n_bins);

		// Fill the metadata
		r->digest.set = strdup(set);
		memcpy(r->digest.value, keyd, sizeof(cf_digest));
		r->gen = generation;
		r->ttl = record_void_time;

		// Call the callback that user wanted to callback
		(*bridge_udata->user_cb)((as_val *)r, bridge_udata->user_udata);

		// It is our job to destroy the record
		as_record_destroy(r);

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
	
	as_policy_scan * p = policy ? (as_policy_scan *) policy : &as->config.policies.scan;


	// If the user want to execute only on a single node...
	if (scan->type == AS_SCAN_TYPE_NORMAL) {

		// Transform into form understandeable by the old simple scan mechanism
		cl_scan_parameters scan_params;
		scan_params.fail_on_cluster_change = policy->fail_on_cluster_change;
		scan_params.priority = scan->priority;

		scan_bridge bridge_udata;
		bridge_udata.user_udata = udata;
		bridge_udata.user_cb = callback;

		if (node) {
			clrv = citrusleaf_scan_node(as->cluster, node, scan->namespace, scan->set, NULL, 0, 
						scan->nobindata, scan->percent, simplescan_cb, &bridge_udata, &scan_params);
			rc = as_error_fromrc(err, clrv);
		} else {

			cf_vector *v = citrusleaf_scan_all_nodes(as->cluster, scan->namespace, scan->set, NULL, 0, 
						scan->nobindata, scan->percent, simplescan_cb, &bridge_udata, &scan_params);
			rc = process_node_response(v, err);
		}

	} else {

		// Transform into form understandeable by the old scan-udf mechanism
		cl_scan clscan;
		as_scan_toclscan(scan, p, &clscan);

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
 * Scan will be run in the background by a thread on client side.
 * No callback will be called in this case
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
	const as_scan * scan
	)
{
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	as_policy_scan * p = policy ? (as_policy_scan *) policy : &as->config.policies.scan;

	cl_scan clscan;
	as_scan_toclscan(scan, p, &clscan);

	citrusleaf_udf_scan_background(as->cluster, &clscan);

	return AEROSPIKE_OK;

}

/**
 * Scan the records in the specified namespace and set in a specified node.
 * Scan will be run in the background by a thread on client side.
 * No callback will be called in this case
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
	const char *node, const as_scan * scan) 
{
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	as_policy_scan * p = policy ? (as_policy_scan *) policy : &as->config.policies.scan;

	cl_scan clscan;
	as_scan_toclscan(scan, p, &clscan);

	citrusleaf_udf_scan_node_background(as->cluster, &clscan, (char *) node);

	return AEROSPIKE_OK;
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
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	return aerospike_scan_generic(as, err, policy, NULL, scan, callback, NULL);
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
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	return aerospike_scan_generic(as, err, policy, node, scan, callback, NULL);
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



/******************************************************************************
 * FUNCTIONS defined in as_scan.h
 *****************************************************************************/
// Defining them here because there is no as_scan.c file in aerospike folder
// Need to check if it is ok to create a new file as_scan.c. Need to double 
// check that it will not cause conflict with as_scan.c in main folder
// If it is ok, we need to move them in a file named as_scan.c in this folder
// TODO: Check

/**
 * Create and initializes a new scan on the heap.
 */
as_scan * as_scan_new(const char * ns, const char * set, uint64_t *job_id)
{
	as_scan *scan = (as_scan *) malloc(sizeof(as_scan));
	if (scan == NULL) return NULL;

	memset(scan, 0, sizeof(as_scan));
	return as_scan_init(scan, ns, set, job_id);
}

/**
 * Initializes a scan.
 */
as_scan * as_scan_init(as_scan * scan, const char * ns, const char * set, uint64_t *job_id)
{
	if (scan == NULL) return scan;

	scan->type = AS_SCAN_TYPE_NORMAL;
	scan->priority = AS_SCAN_PRIORITY_LOW;
	scan->job_id = (cf_get_rand64())/2;
	*job_id = scan->job_id;
	scan->percent = 100;
	scan->nobindata = false;
	scan->set = set == NULL ? NULL : strdup(set);
	scan->namespace = ns == NULL ? NULL : strdup(ns);
	as_udf_call_init(&scan->foreach, NULL, NULL, NULL);

	return scan;
}

/**
 * Releases all resources allocated to the scan.
 */
void as_scan_destroy(as_scan * scan)
{
	if ( scan == NULL ) return;

	if (scan->namespace) {
		free(scan->namespace);
		scan->namespace = NULL;
	}
	if (scan->set) {
		free(scan->set);
		scan->set = NULL;
	}
	as_udf_call_destroy(&scan->foreach);

	// If the whole structure should be freed
	if (scan->_free) {
		free(scan);
	}
}

/**
 * Apply a UDF to each record scanned on the server.
 */
void as_scan_foreach(as_scan * scan, as_scan_type type, const char * module, const char * function, as_list * arglist)
{
	scan->type = type;
	as_udf_call_init(&scan->foreach, module, function, arglist);
}
