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
 *	@defgroup scan_operations Scan Operations
 *	@ingroup client_operations
 *
 *	Aerospike Scan Operations provide the ability to scan all record of a 
 *	namespace and set in an Aerospike database. 
 *
 *	## Usage
 *
 *	Before you can execute a scan, you first need to define a scan using 
 *	as_scan. See as_scan for details on defining scans.
 *
 *	Once you have a scan defined, then you can execute the scan
 *	using either:
 *
 *	- aerospike_scan_foreach() — Execute a scan on the database, then process 
 *		the results.
 *	- aerospike_scan_background() — Send a scan to the database, and not wait 
 *		for completed. The scan is given an id, which can be used to query the
 *		scan status.
 *
 *	When aerospike_scan_foreach() is executed, it will process the results
 *	and create records on the stack. Because the records are on the stack, 
 *	they will only be available within the context of the callback function.
 *
 *	When aerospike_scan_background() is executed, the client will not wait for 
 *	results from the database. Instead, the client will be given a scan_id, 
 *	which can be used to query the scan status on the database via 
 *	aerospike_scan_info().
 *
 *	## Walk-through
 *	
 *	First, we build a scan using as_scan. The scan will be on the "test"
 *	namespace and "demo" set. We will select only bins "a" and "b" to be returned 
 *	for each record.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *
 *	as_scan_select_inita(&scan, 2);
 *	as_scan_select(&scan, "a");
 *	as_scan_select(&scan, "B");
 *	~~~~~~~~~~
 *	
 *	Now that we have a scan defined, we want to execute it using 
 *	aerospike_scan_foreach().
 *	
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_scan_foreach(&as, &err, NULL, &scan, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *	
 *	The callback provided to the function above is implemented as:
 *	
 *	~~~~~~~~~~{.c}
 *	bool callback(const as_val * val, void * udata) {
 *		as_record * rec = as_record_fromval(val);
 *		if ( !rec ) return false;
 *		fprintf("record contains %d bins", as_record_numbins(rec));
 *		return true;
 *	}
 *	~~~~~~~~~~
 *	
 *	An as_scan is simply a scan definition, so it does not contain any state,
 *	allowing it to be reused for multiple scan operations. 
 *	
 *	When you are finished with the scan, you should destroy the resources 
 *	allocated to it:
 *
 *	~~~~~~~~~~{.c}
 *	as_scan_destroy(&scan);
 *	~~~~~~~~~~
 */

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
 *
 *	@ingroup scan_operations
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
 *	The scanid can be used to query the status of the scan running in the 
 *	database via aerospike_scan_info().
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan 		The scan to execute against the cluster.
 *	@param scan_id		The id for the scan job, which can be used for querying the status of the scan.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 *	@ingroup scan_operations
 */
as_status aerospike_scan_background(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, uint64_t * scan_id
	);

/**
 *	Wait for a background scan to be completed by servers.
 *
 *	~~~~~~~~~~{.c}
 *	uint64_t scan_id = 1234;
 *	aerospike_scan_wait(&as, &err, NULL, scan_id, 0);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan_id		The id for the scan job.
 *	@param interval_ms	The polling interval in milliseconds. If zero, 1000 ms is used.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 */
as_status aerospike_scan_wait(
	aerospike * as, as_error * err, const as_policy_info * policy,
	uint64_t scan_id, uint32_t interval_ms
	);

/**
 *	Check the progress of a background scan running on the database. The status
 *	of the scan running on the datatabse will be populated into an as_scan_info.
 *	
 *	~~~~~~~~~~{.c}
 *	uint64_t scan_id = 1234;
 *	as_scan_info scan_info;
 *	
 *	if ( aerospike_scan_info(&as, &err, NULL, &scan, scan_id, &scan_info) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("Scan id=%ll, status=%d percent=%d", scan_id, scan_info.status, scan_info.progress_pct);
 *	}
 *	~~~~~~~~~~
 *	
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param scan_id		The id for the scan job to check the status of.
 *	@param info			Information about this scan, to be populated by this operation.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 *	@ingroup scan_operations
 */
as_status aerospike_scan_info(
	aerospike * as, as_error * err, const as_policy_info * policy,
	uint64_t scan_id, as_scan_info * info
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
 *	@param udata		User-data to be passed to the callback.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 *	@ingroup scan_operations
 */
as_status aerospike_scan_foreach(
	aerospike * as, as_error * err, const as_policy_scan * policy, 
	const as_scan * scan, 
	aerospike_scan_foreach_callback callback, void * udata
	);
