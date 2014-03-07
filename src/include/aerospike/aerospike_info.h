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
 *	@defgroup info_operations Info Operations
 *	@ingroup client_operations
 *
 *	The Info API provides the ability to query an Aerospike cluster for 
 *	information. 
 *
 *	The following API are provided:
 *	- aerospike_info_host() - Query a single host in the cluster.
 *	- aerospike_info_foreach() - Query every host in the cluster.
 *
 */

#pragma once 

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Callback for aerospike_info_foreach()
 *	
 *	@param err			The status and possible error information for the info request.
 *	@param node			The node which provided the response.
 *	@param res			The response to the info request. The response must be freed by the caller.
 *	@param udata			The udata provided to the aerospike_info_foreach()
 *
 *	@return TRUE to continue to the next info response. FALSE to stop processing.
 *
 *	@ingroup info_operations
 */
typedef bool (* aerospike_info_foreach_callback)(const as_error * err, const as_node * node, const char * req, char * res, void * udata);

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Send an info request to a specific host. The response must be freed by the caller.
 *	
 *	~~~~~~~~~~{.c}
 *	char * res = NULL;
 *	if ( aerospike_info_host(&as, &err, NULL, "127.0.0.1", 3000, "info", &res) != AEROSPIKE_OK ) {
 *		// handle error
 *	}
 *	else {
 *		// handle response
 *		free(res);
 *		res = NULL;
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param addr			The IP address or hostname to send the request to.
 *	@param port			The port to send the request to.
 *	@param req			The info request to send.
 *	@param res			The response from the node. The response will be a NULL terminated string, allocated by the function, and must be freed by the caller.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error.
 *
 *	@ingroup info_operations
 */
as_status aerospike_info_host(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * addr, uint16_t port, const char * req, 
	char ** res
	);

/**
 *	Send an info request to the entire cluster.
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_info_foreach(&as, &err, NULL, "info", callback, NULL) != AEROSPIKE_OK ) {
 *		// handle error
 *	}
 *	~~~~~~~~~~
 *
 *	The callback takes a response string, which the caller is reponsible for freeing.
 *
 *	~~~~~~~~~~{.c}
 *	bool callback(const as_error * err, const char * node, char * res, void * udata) {
 *		// handle response
 *		free(res);
 *		res = NULL;
 *	}
 *	~~~~~~~~~~
 *	     
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param req			The info request to send.
 *	@param callback		The function to call when a response is received.
 *	@param udata			User-data to send to the callback.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error.
 *
 *	@ingroup info_operations
 */
as_status aerospike_info_foreach(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * req, 
    aerospike_info_foreach_callback callback, void * udata
	);
