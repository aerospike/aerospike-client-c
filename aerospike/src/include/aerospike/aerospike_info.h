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
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Callback for aerospike_info_foreach()
 * 
 * @param err       - The status and possible error information for the info request.
 * @param node      - The node which provided the response.
 * @param res       - The response to the info request. The response must be freed by the caller.
 * @param udata     - The udata provided to the aerospike_info_foreach()
 *
 * @return TRUE to continue to the next info response. FALSE to stop processing.
 */
typedef bool (* aerospike_info_foreach_callback)(const as_error * err, const char * node, const char * res, void * udata);

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Send an info request to a single node. The response must be freed by the caller.
 * 
 *      char * res = NULL;
 *      if ( aerospike_info_node(&as, &err, NULL, "node1", "info", &res) != AEROSPIKE_OK ) {
 *          // handle error
 *      }
 *      else {
 *          // handle response
 *          free(res);
 *          res = NULL;
 *      }
 *
 * @param as        - the cluster to send the request to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param node      - the specific node to send the request to.
 * @param req       - the info request to send.
 * @param res       - the response from the node. The response will be a NULL terminated string, allocated by the function, and must be freed by the caller.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error.
 */
as_status aerospike_info_node(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * node, const char * req, 
	char ** res
	);

/**
 * Send an info request to the entire cluster.
 *
 *      bool callback(const as_error * err, const char * node, char * res, void * udata) {
 *          // handle response
 *          free(res);
 *          res = NULL;
 *      }
 *      
 *      if ( aerospike_info_foreach(&as, &err, NULL, "info", callback) != AEROSPIKE_OK ) {
 *          // handle error
 *      }
 *
 * @param as        - the cluster to send the request to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param req       - the info request to send.
 * @param udata     - user-data to send to the callback.
 * @param callback  - the function to call when a response is received.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error.
 */
as_status aerospike_info_foreach(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * req, 
    aerospike_info_foreach_callback callback, void * udata
	);
