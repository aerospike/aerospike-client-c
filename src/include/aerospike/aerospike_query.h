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
 *	@defgroup query_operations Query Operations (3.0 only)
 *	@ingroup client_operations
 *
 *	The Aerospike Query Operations provide the ability to query data in the 
 *	Aerospike database. The queries can only be performed on secondary indexes, 
 *	which have been created in the database. To scan all the records in the
 *	database, then you must use the @ref scan_operations.
 *	
 *	## Usage
 *
 *	Before you can execute a query, you first need to build a query using 
 *	as_query. See as_query for details on building queries.
 *
 *	Once you have a query defined, then you can execute the query :
 *
 *	-	aerospike_query_foreach() -	Executes a query and invokes a callback
 *		function for each result returned.
 *	
 *	When aerospike_query_foreach() is executed, it will process the results
 *	and create records on the stack. Because the records are on the stack, 
 *	they will only be available within the context of the callback function.
 *
 *
 *	## Walk-through
 *	
 *	First, we define a query using as_query. The query will be for the "test"
 *	namespace and "demo" set. We will add a where predicate on "bin2", on which
 *	we have already created a secondary index. 
 *	
 *	~~~~~~~~~~{.c}
 *	as_query query;
 *	as_query_init(&query, "test", "demo");
 *
 *	as_query_where_init(&query, 1);
 *	as_query_where(&query, "bin2", integer_equals(100));
 *	~~~~~~~~~~
 *
 *	Now that we have a query defined, we want to execute it using 
 *	aerospike_query_foreach().
 *	
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_query_foreach(&as, &err, NULL, &query, callback, NULL) != AEROSPIKE_OK ) {
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
 *	An as_query is simply a query definition, so it does not contain any state,
 *	allowing it to be reused for multiple query operations. 
 *	
 *	When you are finished with the query, you should destroy the resources 
 *	allocated to it:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_destroy(&query);
 *	~~~~~~~~~~
 *
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stream.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	This callback will be called for each value or record returned from 
 *	a query.
 *
 *	The aerospike_query_foreach() function accepts this callback.
 *
 *	~~~~~~~~~~{.c}
 *	bool my_callback(as_val * val, void * udata) {
 *		return true;
 *	}
 *	~~~~~~~~~~
 *
 *	@param val 			The value received from the query.
 *	@param udata 		User-data provided to the calling function.
 *
 *	@return `true` to continue to the next value. Otherwise, iteration will end.
 *
 *	@ingroup query_operations
 */
typedef bool (* aerospike_query_foreach_callback)(const as_val * val, void * udata);

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Execute a query and call the callback function for each result item.
 *
 *	~~~~~~~~~~{.c}
 *	as_query query;
 *	as_query_init(&query, "test", "demo");
 *	as_query_select(&query, "bin1");
 *	as_query_where(&query, "bin2", integer_equals(100));
 *	
 *	if ( aerospike_query_foreach(&as, &err, NULL, &query, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	
 *	as_query_destroy(&query);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param query			The query to execute against the cluster.
 *	@param callback		The callback function to call for each result value.
 *	@param udata			User-data to be passed to the callback.
 *
 *	@return AEROSPIKE_OK on success, otherwise an error.
 *
 *	@ingroup query_operations
 */
as_status aerospike_query_foreach(
	aerospike * as, as_error * err, const as_policy_query * policy, 
	const as_query * query, 
	aerospike_query_foreach_callback callback, void * udata
	);
