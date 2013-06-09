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
#include "shim.h"

/******************************************************************************
 * FUNCTION DECLS
 *****************************************************************************/

as_status aerospike_scan_init(aerospike * as, as_error * err);
as_status aerospike_scan_destroy(aerospike * as, as_error * err);
as_status aerospike_scan_generic(
	aerospike * as, as_error * err, const as_policy_scan * policy,
	const char * node, const as_scan * scan,
	aerospike_scan_foreach_callback callback, void * udata);

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

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
as_status aerospike_scan_generic(
	aerospike * as, as_error * err, const as_policy_scan * policy,
	const char * node, const as_scan * scan,
	aerospike_scan_foreach_callback callback, void * udata)
{
	as_status retstat;
	
	retstat = AEROSPIKE_OK;

	// If the user want to execute only on a single node...
	if (node) {

		cl_rv rv = citrusleaf_udf_scan_node(as->cluster, (as_scan *)scan, (char *)node, callback, udata);
		retstat = as_error_fromrc(err, rv);

	} else {

		cf_vector *v = citrusleaf_udf_scan_all_nodes(as->cluster, (as_scan *)scan, callback, udata);

		// This returns a vector of return values, the size of which is the size of the cluster
		int sz = cf_vector_size(v);
		as_node_response resp;
		for(int i=0; i <= sz; i++) {

			cf_vector_get(v, i, &resp);
			// Even if one of the node responded with an error, set the overall status as error
			if (resp.node_response != CITRUSLEAF_OK) {
				retstat = as_error_fromrc(err, resp.node_response);
			}

			// Set the resp back to zero
			memset(&resp, 0, sizeof(as_node_response));
		}

		// Free the result vector
		cf_vector_destroy(v);
	}

	return AEROSPIKE_OK;
}

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
as_status aerospike_scan_all_nodes_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan
	)
{
	if ( aerospike_scan_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	citrusleaf_udf_scan_background(as->cluster, (as_scan *)scan);

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

	citrusleaf_udf_scan_node_background(as->cluster, (as_scan *)scan, (char *)node);

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
as_status aerospike_scan_all_nodes(
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
as_status aerospike_scan_node(
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
