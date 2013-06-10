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
#include "shim.h"

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef int (* citrusleaf_udf_scan_callback)(as_val *, void *);

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
	clscan->udf.type = scan->foreach.module && scan->foreach.function ? AS_SCAN_UDF_CLIENT_RECORD : AS_SCAN_UDF_NONE;
	clscan->udf.filename = scan->foreach.module;
	clscan->udf.function = scan->foreach.function;
	clscan->udf.arglist = scan->foreach.arglist;
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
	as_status rc = AEROSPIKE_OK;
	
	as_policy_scan * p = policy ? (as_policy_scan *) policy : &as->config.policies.scan;

	cl_scan clscan;
	as_scan_toclscan(scan, p, &clscan);

	// If the user want to execute only on a single node...
	if (node) {
		cl_rv rv = citrusleaf_udf_scan_node(as->cluster, &clscan, (char *)node, (citrusleaf_udf_scan_callback) callback, udata);
		rc = as_error_fromrc(err, rv);
	} 
	else {

		cf_vector *v = citrusleaf_udf_scan_all_nodes(as->cluster, &clscan, (citrusleaf_udf_scan_callback) callback, udata);

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

	return aerospike_scan_generic(as, err, policy, NULL, scan, NULL, NULL);
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

	return aerospike_scan_generic(as, err, policy, node, scan, NULL, NULL);
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
