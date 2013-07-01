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
 *	The Aerospike Query API provides the ability to query data in the 
 *	Aerospike cluster. The queries can only be performed on secondary indexes, 
 *	which have been created in the cluster. 
 *
 *	The API provides two functions for execution queries:
 *	-	aerospike_query_foreach() -	Executes a query and invokes a callback
 *		function for each result returned.
 *	-	aerospike_query_stream() - 	Executes a query and writes the results
 *		to the stream provided. 
 *
 *	In order to execute a query, you first need to create and populate an
 *	as_query object. The as_query documentation provides instructions on 
 *	populating an as_query object.
 *
 *	## Walk-through
 *	
 *	To begin, you should first create an as_query object. We want to query data
 *	in the "test" namespace and "demo" set. So, we will use a stack allocated
 *	query and initialize it with the namespace and set:
 *
 *	~~~~~~~~~~{.c}
 *	as_query query;
 *	as_query_init(&query, "test", "demo");
 *	~~~~~~~~~~
 *
 *	You could have used a heap allocated as_query via the as_query_new() 
 *	function.
 *
 *	Queries require a secondary index lookup, which is defined as a predicate.
 *	To add predicates, you should use the as_query_where() function. 
 *
 *	~~~~~~~~~~{.c}
 *	as_query_where_init(&query, 1);
 *	as_query_where(&query, "bin2", integer_equals(100));
 *	~~~~~~~~~~
 *
 *	The above specifies that we want to find all records that contain "bin2",
 *	with an integer value of `100`.
 *	
 *	There are other optional query modifiers, such as:
 *	-	as_query_select() -		Select specified bins. Functions like an SQL 
 *								select.
 *	-	as_query_limit() -		Limit the number results.
 *	-	as_query_orderby() -	Order the results by a bin and the direction.
 *
 *	Once you have built your query, you will want to execute it. For this we
 *	will use the aerospike_query_foreach() function:
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_query_foreach(&as, &err, NULL, &query, callback, NULL) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *	
 *	When you are finished with the query, you should destroy the resources 
 *	allocated to it:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_destroy(&query);
 *	~~~~~~~~~~
 *
 *	You can reuse an as_query for multiple query executions.
 *	
 *	@addtogroup query_api
 *	@{
 */

#pragma once

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
 */
typedef bool (* aerospike_query_foreach_callback)(as_val * val, void * udata);

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
 */
as_status aerospike_query_foreach(
	aerospike * as, as_error * err, const as_policy_query * policy, 
	const as_query * query, 
	aerospike_query_foreach_callback callback, void * udata
	);

/**
 *	Execute a query and send the results to a writable stream.
 *
 *	~~~~~~~~~~{.c}
 *		as_query query;
 *		as_query_init(&query, "test", "demo");
 *		as_query_select(&query, "bin1");
 *		as_query_where(&query, "bin2", integer_equals(100));
 *
 *		if ( aerospike_query_stream(&as, &err, NULL, &query, &stream) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *
 *		as_query_destroy(&query);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param query		The query to execute against the cluster.
 *	@param stream 		The writable stream to write results to.
 *
 *	@return AEROSPIKE_OK on success, otherwise an error.
 */
as_status aerospike_query_stream(
	aerospike * as, as_error * err, const as_policy_query * policy, 
	const as_query * query, as_stream * stream
);

/**
 *	@}
 */
