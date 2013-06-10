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
#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/cl_info.h>

#include <netinet/in.h>
#include <sys/socket.h>

#include "shim.h"
#include "log.h"

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

bool citrusleaf_info_cluster_foreach_callback(const cl_cluster_node * clnode, const struct sockaddr_in * sa_in, const char * req, char * res, void * udata)
{
	if ( ! clnode ) {
		return FALSE;
	}

	as_error err;
    as_error_reset(&err);

    citrusleaf_info_cluster_foreach_data * data = (citrusleaf_info_cluster_foreach_data *) udata;

    as_node node;
    memcpy(node.name, clnode->name, AS_NODE_NAME_LEN);

	bool result = (data->callback)(&err, &node, req, res, data->udata);

	return result;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Send an info request to a specific host. The response must be freed by the caller.
 * 
 *      char * res = NULL;
 *      if ( aerospike_info_host(&as, &err, NULL, "127.0.0.1", 3000, "info", &res) != AEROSPIKE_OK ) {
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
 * @param addr      - the IP address or hostname to send the request to.
 * @param port      - the port to send the request to.
 * @param req       - the info request to send.
 * @param res       - the response from the node. The response will be a NULL terminated string, allocated by the function, and must be freed by the caller.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error.
 */
as_status aerospike_info_host(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * addr, uint16_t port, const char * req, 
	char ** res) 
{
	// if policy is NULL, then get default policy
	as_policy_info * p = policy ? (as_policy_info *) policy : &as->config.policies.info;
	
	if (! as) {
		return AEROSPIKE_ERR;
	}

	cl_rv rc = citrusleaf_info((char *) addr, port, (char *) req, res, p->timeout);

	return as_error_fromrc(err, rc);
}

/**
 * Send an info request to a specific node. The response must be freed by the caller.
 * 
 *      char * res = NULL;
 *      if ( aerospike_info_host(&as, &err, NULL, "127.0.0.1", 3000, "info", &res) != AEROSPIKE_OK ) {
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
 * @param node      - the name of the node to send the request to.
 * @param req       - the info request to send.
 * @param res       - the response from the node. The response will be a NULL terminated string, allocated by the function, and must be freed by the caller.
 *
 * @return AEROSPIKE_OK on success. Otherwise an error.
 */
as_status aerospike_info_node(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * node, const char * req, 
	char ** res
	)
{
	/**
	 * NOTE: We do not have the equivalent in the OLD API. 
	 * TODO: Evaluate whether we need this.
	 */
	return AEROSPIKE_ERR;
}

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
	aerospike_info_foreach_callback callback, void * udata)
{
	// if policy is NULL, then get default policy
	as_policy_info * p = policy ? (as_policy_info *) policy : &as->config.policies.info;
	
	if ( !as ) {
		return AEROSPIKE_ERR;
	}

	citrusleaf_info_cluster_foreach_data data = {
		.callback = callback,
		.udata = udata
	};

	cl_rv rc = citrusleaf_info_cluster_foreach(
		as->cluster, req, p->send_as_is, p->check_bounds, p->timeout,
		(void *) &data, citrusleaf_info_cluster_foreach_callback);

	return as_error_fromrc(err, rc);
}
