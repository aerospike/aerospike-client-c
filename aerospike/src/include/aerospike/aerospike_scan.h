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

/** 
 *	@defgroup scan_api Scan API
 *	@ingroup client_api
 *
 *	Aerospike provides several modes of scanning data in a cluster.
 *
 *	A scan can be performed on the entire cluster or a single node. When a scan
 *	is executed, a scan job is sent from the client to the cluster (or node). 
 *	The client can their wait for results to return or let the scan run 
 *	independently.
 * 
 *	Scans operations:
 *	- aerospike_scan_background()
 *	- aerospike_scan_foreach()
 *
 *	@{
 */

#pragma once

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	This callback will be called for each value or record returned from 
 *	a scan.
 *
 *	The following functions accept the callback:
 *	-	aerospike_scan_foreach()
 *	-	aerospike_scan_node_foreach()
 *	
 *	~~~~~~~~~~{.c}
 *	bool my_callback(const as_val * val, void * udata) {
 *		return true;
 *	}
 *	~~~~~~~~~~
 *
 *	@param val 			The value received from the query.
 *	@param udata 		User-data provided to the calling function.
 *
 *	@return `true` to continue to the next value. Otherwise, iteration will end.
 */
typedef bool (* aerospike_scan_foreach_callback)(const as_val * val, void * udata);

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Scan the records in the specified namespace and set in the cluster.
 *
 *	Scan will be run in the background by a thread on client side.
 *	No callback will be called in this case.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *	
 *	uint64_t scanid = 0;
 *	
 *	if ( aerospike_scan_background(&as, &err, NULL, &scan, &scanid) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("Running background scan job: %ll", scanid);
 *	}
 *
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 *	
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan 			The scan to execute against the cluster.
 *	@param scan_id		The id for the scan job, which can be used for querying the status of the scan.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, uint64_t * scan_id
	);

/**
 *	Check the status of a scan running on the server.
 *	
 *	~~~~~~~~~~{.c}
 *	uint64_t scan_id = 1234;
 *	as_scan_status scan_status = AS_SCAN_STATUS_UNDEF;
 *	
 *	if ( aerospike_scan_status(&as, &err, NULL, &scan, scan_id, &scan_status) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("Scan id=%ll, status=%s", scan_id, scan_status);
 *	}
 *	~~~~~~~~~~
 *	
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan_id		The id for the scan job to check the status of.
 *	@param status		The status of the scan, to be populated by this operation.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_status(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	uint64_t scan_id, as_scan_status * status
	);

/**
 *	Scan the records in the specified namespace and set in the cluster.
 *
 *	Call the callback function for each record scanned. When all records have 
 *	been scanned, then callback will be called with a NULL value for the record.
 *
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *	
 *	if ( aerospike_scan_foreach(&as, &err, NULL, &scan, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 *	
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan			The scan to execute against the cluster.
 *	@param callback		The function to be called for each record scanned.
 *	@param udata			User-data to be passed to the callback.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_foreach(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, 
	aerospike_scan_foreach_callback callback, void * udata
	);

/**
 *	@}
 */
