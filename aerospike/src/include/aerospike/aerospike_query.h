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

/** 
 * Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer commodo tincidunt magna vitae vulputate. Nullam mattis tortor tortor, at pretium massa viverra ut. Pellentesque vitae eros et mi aliquam rhoncus. Quisque varius at lorem ut faucibus. Duis scelerisque, dui eu elementum aliquet, purus lectus tempor purus, nec dapibus quam felis id sapien. Fusce non pulvinar nulla. Curabitur in mauris lobortis, congue nisl vitae, pharetra felis. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Integer tincidunt gravida consectetur. Ut nisi ipsum, sagittis nec urna nec, gravida consequat lectus. Fusce sed neque pulvinar, bibendum metus in, lobortis dui. Proin a justo luctus, feugiat libero sed, tincidunt nisi. Aenean pulvinar, justo non rutrum vehicula, odio tellus blandit lorem, in faucibus dolor tellus ut mi.
 *
 * Vivamus nec porta lacus, ut laoreet augue. Phasellus eleifend ultricies tempus. Fusce auctor nisl at lacinia rhoncus. Curabitur vel adipiscing libero. Morbi vel mi ac lorem vulputate condimentum ac a odio. Praesent hendrerit ligula leo, sit amet laoreet odio condimentum quis. Pellentesque gravida volutpat interdum. Nam tristique enim sagittis est sagittis condimentum. Aenean et tellus a lacus fringilla volutpat sit amet dapibus ligula. Nullam consectetur velit ac felis sagittis molestie. Nunc a ante ac ipsum volutpat lobortis vel ut nibh.
 *
 * @defgroup query Query API
 * @{
 */

#pragma once

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stream.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Callback for aerospike_query_foreach(). This call back will be called for 
 * each value or record returned from the query.
 *
 * @param val 			The value received from the query.
 * @param udata 		User-data provided to the calling function.
 *
 * @return `true` to continue to the next value. Otherwise, iteration will end.
 */
typedef bool (* aerospike_query_foreach_callback)(as_val * val, void * udata);

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Execute a query and call the callback function for each result item.
 *
 *     as_query query;
 *     as_query_init(&query, "test", "demo");
 *     as_query_select(&query, "bin1");
 *     as_query_where(&query, "bin2", integer_equals(100));
 *     
 *     if ( aerospike_query_foreach(&as, &err, NULL, &query, callback, NULL) != AEROSPIKE_OK ) {
 *         fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *     }
 *     
 *     as_query_destroy(&query);
 *
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param query			The query to execute against the cluster.
 * @param callback		The callback function to call for each result value.
 * @param udata			User-data to be passed to the callback.
 *
 * @return AEROSPIKE_OK on success, otherwise an error.
 */
as_status aerospike_query_foreach(
	aerospike * as, as_error * err, const as_policy_query * policy, 
	const as_query * query, 
	aerospike_query_foreach_callback callback, void * udata
	);

/**
 * Execute a query and send the results to a writable stream.
 *
 *     as_query query;
 *     as_query_init(&query, "test", "demo");
 *     as_query_select(&query, "bin1");
 *     as_query_where(&query, "bin2", integer_equals(100));
 *
 *     if ( aerospike_query_stream(&as, &err, NULL, &query, &stream) != AEROSPIKE_OK ) {
 *         fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *     }
 *
 *     as_query_destroy(&query);
 *
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param query			The query to execute against the cluster.
 * @param stream 		The writable stream to write results to.
 *
 * @return AEROSPIKE_OK on success, otherwise an error.
 */
as_status aerospike_query_stream(
	aerospike * as, as_error * err, const as_policy_query * policy, 
	const as_query * query, as_stream * stream
);

/**
 * @}
 */