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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log.h>
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_info.h>

#include <netinet/in.h>
#include <sys/socket.h>

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
	
	if (! policy) {
		policy = &as->config.policies.info;
	}

	if (! as) {
		return AEROSPIKE_ERR_CLIENT;
	}

	cl_rv rc = citrusleaf_info_auth(as->cluster, (char *) addr, port, (char *) req, res, policy->timeout);

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
 *	bool callback(const as_error * err, const as_node * node, const char * req, char * res, void * udata) {
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
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
	
	if ( !as ) {
		return AEROSPIKE_ERR_CLIENT;
	}

	citrusleaf_info_cluster_foreach_data data = {
		.callback = callback,
		.udata = udata
	};
	
	char* error = 0;

	int rc = citrusleaf_info_cluster_foreach(
		as->cluster, req, policy->send_as_is, policy->check_bounds, policy->timeout, (void *) &data, &error,
		citrusleaf_info_cluster_foreach_callback);

	if (rc) {
		as_strncpy(err->message, error, sizeof(err->message));
		free(error);
		return as_error_fromrc(err, rc);
	}
	return AEROSPIKE_OK;
}
