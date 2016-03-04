/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/aerospike_info.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_info.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_lookup.h>
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>

#include <citrusleaf/alloc.h>

#include <netinet/in.h>
#include <sys/socket.h>

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
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
	
	as_vector sockaddr_in_v;
	as_vector_inita(&sockaddr_in_v, sizeof(struct sockaddr_in), 5);
	
	as_status status = as_lookup(err, (char*)addr, port, &sockaddr_in_v);
	
	if (status) {
		as_vector_destroy(&sockaddr_in_v);
		return status;
	}
	
	uint64_t deadline = as_socket_deadline(policy->timeout);
	as_cluster* cluster = as->cluster;
	status = AEROSPIKE_ERR_CLUSTER;
	bool loop = true;
	
	for (uint32_t i = 0; i < sockaddr_in_v.size && loop; i++) {
		struct sockaddr_in* sa_in = as_vector_get(&sockaddr_in_v, i);
		status = as_info_command_host(cluster, err, sa_in, (char*)req, policy->send_as_is, deadline, res);

		switch (status) {
			case AEROSPIKE_OK:
			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_INDEX_FOUND:
			case AEROSPIKE_ERR_INDEX_NOT_FOUND:
				loop = false;
				break;
				
			default:
				break;
		}
	}
	as_vector_destroy(&sockaddr_in_v);
	return status;
}

/**
 *	Send an info request to a specific socket address. The response must be freed by the caller on success.
 *
 *	~~~~~~~~~~{.c}
 *	char * res = NULL;
 *	if ( aerospike_info_socket_address(&as, &err, NULL, &socket_addr, "info", &res) != AEROSPIKE_OK ) {
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
 *	@param sa_in		The IP address and port to send the request to.
 *	@param req			The info request to send.
 *	@param res			The response from the node. The response will be a NULL terminated string, allocated by the function, and must be freed by the caller.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error.
 *
 *	@ingroup info_operations
 */
as_status aerospike_info_socket_address(
	aerospike * as, as_error * err, const as_policy_info * policy,
	struct sockaddr_in* sa_in, const char * req,
	char ** res)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
	
	uint64_t deadline = as_socket_deadline(policy->timeout);
	return as_info_command_host(as->cluster, err, sa_in, (char*)req, policy->send_as_is, deadline, res);
}

/**
 *	Send an info request to a node in the cluster.  If node request fails, send request to the next
 *	node in the cluster.  Repeat until the node request succeeds. The response must be freed by 
 *	the caller on success.
 *
 *	~~~~~~~~~~{.c}
 *	char * res = NULL;
 *	if ( aerospike_info_any(&as, &err, NULL, "info", &res) != AEROSPIKE_OK ) {
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
 *	@param req			The info request to send.
 *	@param res			The response from the node. The response will be a NULL terminated string, allocated by the function, and must be freed by the caller.
 *
 *	@return AEROSPIKE_OK on success. Otherwise an error.
 *
 *	@ingroup info_operations
 */
as_status aerospike_info_any(
	aerospike * as, as_error * err, const as_policy_info * policy,
	const char * req, char ** res)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
	
	as_status status = AEROSPIKE_ERR_CLUSTER;
	uint64_t deadline = as_socket_deadline(policy->timeout);
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	bool loop = true;
	
	for (uint32_t i = 0; i < nodes->size && loop; i++) {
		as_node* node = nodes->array[i];
		status = as_info_command_node(err, node, (char*)req, policy->send_as_is, deadline, res);
		
		switch (status) {
			case AEROSPIKE_OK:
			case AEROSPIKE_ERR_TIMEOUT:
			case AEROSPIKE_ERR_INDEX_FOUND:
			case AEROSPIKE_ERR_INDEX_NOT_FOUND:
				loop = false;
				break;
				
			default:
				break;
		}
	}
	as_nodes_release(nodes);
	return status;
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
	aerospike * as, as_error * err, const as_policy_info * policy, const char * req,
	aerospike_info_foreach_callback callback, void * udata)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
	
	as_status status = AEROSPIKE_ERR_CLUSTER;
	uint64_t deadline = as_socket_deadline(policy->timeout);
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		char* response = 0;
	
		status = as_info_command_node(err, node, (char*)req, policy->send_as_is, deadline, &response);
		
		if (status == AEROSPIKE_OK) {
			bool result = callback(err, node, req, response, udata);
			cf_free(response);
			
			if (! result) {
				status = AEROSPIKE_ERR_QUERY_ABORTED;
				break;
			}
		}
		else {
			if (status != AEROSPIKE_ERR_CLUSTER) {
				break;
			}
		}
	}
	as_nodes_release(nodes);
	return status;
}
