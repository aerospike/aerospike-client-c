/*
 * Copyright 2008-2018 Aerospike, Inc.
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

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
aerospike_info_node(
	aerospike* as, as_error* err, const as_policy_info* policy, as_node* node,
	const char* req, char** res
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}

	uint64_t deadline = as_socket_deadline(policy->timeout);
	return as_info_command_node(err, node, (char*)req, policy->send_as_is, deadline, res);
}

as_status
aerospike_info_host(
	aerospike* as, as_error* err, const as_policy_info* policy, const char* hostname, uint16_t port,
	const char* req, char** res
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
		
	as_address_iterator iter;
	as_status status = as_lookup_host(&iter, err, hostname, port);
	
	if (status) {
		return status;
	}
	
	uint64_t deadline = as_socket_deadline(policy->timeout);
	as_cluster* cluster = as->cluster;
	struct sockaddr* addr;
	status = AEROSPIKE_ERR_CLUSTER;
	bool loop = true;
	
	while (loop && as_lookup_next(&iter, &addr)) {
		status = as_info_command_host(cluster, err, addr, (char*)req, policy->send_as_is, deadline, res, hostname);
		
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
	as_lookup_end(&iter);
	return status;
}

as_status
aerospike_info_socket_address(
	aerospike* as, as_error* err, const as_policy_info* policy, struct sockaddr_in* sa_in,
	const char* req, char** res
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.info;
	}
	
	uint64_t deadline = as_socket_deadline(policy->timeout);
	return as_info_command_host(as->cluster, err, (struct sockaddr*)sa_in, (char*)req, policy->send_as_is, deadline, res, NULL);
}

as_status
aerospike_info_any(
	aerospike* as, as_error* err, const as_policy_info* policy, const char* req, char** res
	)
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

as_status
aerospike_info_foreach(
	aerospike* as, as_error* err, const as_policy_info* policy, const char* req,
	aerospike_info_foreach_callback callback, void* udata
	)
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
			break;
		}
	}
	as_nodes_release(nodes);
	return status;
}
