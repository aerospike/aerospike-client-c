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

#pragma once

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

// The current state of this file is not usable as it is missing many definitions
// If we include both they will cause conflicts as they have the same name
// For now we should be using all the definitions from citrusleaf/as_scan.h
// #include <aerospike/as_scan.h>
#include <citrusleaf/as_scan.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Callback for the `aerospike_scan_*` function.
 */
typedef int (* aerospike_scan_foreach_callback)(const as_val *, void *);

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
as_status aerospike_scan_all_nodes_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan
	);

/**
 * Scan the records in the specified namespace and set in a specified node.
 * Scan will be run in the background by a thread on client side.
 * No callback will be called in this case
 * 
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param node      - the name of the node to perform the scan on.
 * @param scan      - the scan to perform
 * @param scan_id   - the id for the scan job, which can be used for querying the status of the scan.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_node_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const char *node, const as_scan * scan
	);

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
	aerospike_scan_foreach_callback callback, void * udata
	);

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
	aerospike_scan_foreach_callback callback, void * udata
	);
