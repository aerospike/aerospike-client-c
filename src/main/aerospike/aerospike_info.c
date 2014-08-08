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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_error.h>
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>


#include <citrusleaf/citrusleaf.h>
#include <aerospike/as_cluster.h>
#include <citrusleaf/cl_info.h>

#include <netinet/in.h>
#include <sys/socket.h>

#include "_log.h"
#include "_policy.h"
#include "_shim.h"

/******************************************************************************
 * TYPES
 *****************************************************************************/

struct citrusleaf_info_cluster_foreach_data_s {
	aerospike_info_foreach_callback callback;
	void * udata;
};

typedef struct citrusleaf_info_cluster_foreach_data_s citrusleaf_info_cluster_foreach_data;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

bool citrusleaf_info_cluster_foreach_callback(const as_node * node, const char * req, char * res, void * udata)
{
	if ( ! node ) {
		return FALSE;
	}
	
	as_error err;
    as_error_reset(&err);

    citrusleaf_info_cluster_foreach_data * data = (citrusleaf_info_cluster_foreach_data *) udata;
	
	bool result = (data->callback)(&err, node, req, res, data->udata);

	return result;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 *	Send an info request to a specific host. The response must be freed by the caller on success.
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
	char ** res) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_info p;
	as_policy_info_resolve(&p, &as->config.policies, policy);

	if (! as) {
		return AEROSPIKE_ERR;
	}

	cl_rv rc = citrusleaf_info_auth(as->cluster, (char *) addr, port, (char *) req, res, p.timeout);

	if (rc) {
		as_strncpy(err->message, *res, sizeof(err->message));
		free(*res);
		return as_error_fromrc(err, rc);
	}
		
	return AEROSPIKE_OK;
}

/**
 *	Send an info request to the entire cluster.
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_info_foreach(&as, &err, NULL, "info", callback, NULL) != AEROSPIKE_OK ) {
 *		// handle error
 *	}
 *	~~~~~~~~~~
 *
 *	The callback takes a response string. The caller should not free this string.
 *
 *	~~~~~~~~~~{.c}
 *	bool callback(const as_error * err, const char * node, char * res, void * udata) {
 *		// handle response
 *	}
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param req			The info request to send.
 *	@param callback		The function to call when a response is received.
 *	@param udata		User-data to send to the callback.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error.
 *
 *	@ingroup info_operations
 */
as_status aerospike_info_foreach(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * req, 
	aerospike_info_foreach_callback callback, void * udata)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_info p;
	as_policy_info_resolve(&p, &as->config.policies, policy);
	
	if ( !as ) {
		return AEROSPIKE_ERR;
	}

	citrusleaf_info_cluster_foreach_data data = {
		.callback = callback,
		.udata = udata
	};
	
	char* error = 0;

	int rc = citrusleaf_info_cluster_foreach(
		as->cluster, req, p.send_as_is, p.check_bounds, p.timeout, (void *) &data, &error,
		citrusleaf_info_cluster_foreach_callback);

	if (rc) {
		as_strncpy(err->message, error, sizeof(err->message));
		free(error);
		return as_error_fromrc(err, rc);
	}
	return AEROSPIKE_OK;
}
