/*
 * Copyright 2008-2017 Aerospike, Inc.
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
#include <aerospike/as_lookup.h>
#include <aerospike/as_address.h>
#include <aerospike/as_info.h>

as_status
as_lookup_host(as_address_iterator* iter, as_error* err, const char* hostname, in_port_t port)
{
	iter->hostname_is_alias = true;

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	// Check if hostname is really an IPv4 address.
	struct in_addr ipv4;
	
	if (inet_pton(AF_INET, hostname, &ipv4) == 1) {
		hints.ai_family = AF_INET;
		hints.ai_flags = AI_NUMERICHOST;
		iter->hostname_is_alias = false;
	}
	else {
		// Check if hostname is really an IPv6 address.
		struct in6_addr ipv6;
		
		if (inet_pton(AF_INET6, hostname, &ipv6) == 1) {
			hints.ai_family = AF_INET6;
			hints.ai_flags = AI_NUMERICHOST;
			iter->hostname_is_alias = false;
		}
	}
	
	int ret = getaddrinfo(hostname, NULL, &hints, &iter->addresses);
	
	if (ret) {
		return as_error_update(err, AEROSPIKE_ERR_INVALID_HOST, "Invalid hostname %s: %s", hostname, gai_strerror(ret));
	}
	
	iter->current = iter->addresses;
	iter->port_be = cf_swap_to_be16(port);
	return AEROSPIKE_OK;
}

as_status
as_lookup_node(as_cluster* cluster, as_error* err, const char* tls_name, struct sockaddr* addr, as_node_info* node_info)
{
	uint64_t deadline = as_socket_deadline(cluster->conn_timeout_ms);
	
	as_socket* sock = &node_info->socket;
	as_status status = as_info_create_socket(cluster, err, addr, deadline, tls_name, sock);
	
	if (status) {
		return status;
	}
	
	char* command;
	int args;
	
	if (cluster->cluster_name) {
		command = "node\nfeatures\ncluster-name\n";
		args = 3;
	}
	else {
		command = "node\nfeatures\n";
		args = 2;
	}
	
	char* response = 0;
	status = as_info_command(err, sock, command, true, deadline, 0, &response);
	
	if (status) {
		as_socket_close(sock);
		return status;
	}
	
	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), args);
	
	as_info_parse_multi_response(response, &values);
	
	if (values.size != args) {
		// Vector was probably resized on heap. Destroy vector.
		as_vector_destroy(&values);
		goto Error;
	}
	
	as_name_value* nv = as_vector_get(&values, 0);
	char* node_name = nv->value;
	
	if (node_name == 0 || *node_name == 0) {
		goto Error;
	}
	as_strncpy(node_info->name, node_name, AS_NODE_NAME_SIZE);
	
	if (cluster->cluster_name) {
		nv = as_vector_get(&values, 2);
		
		if (strcmp(cluster->cluster_name, nv->value) != 0) {
			char addr_name[AS_IP_ADDRESS_SIZE];
			as_address_name(addr, addr_name, sizeof(addr_name));
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Invalid node %s %s Expected cluster name '%s' Received '%s'",
					node_info->name, addr_name, cluster->cluster_name, nv->value);
			cf_free(response);
			as_socket_close(sock);
			return AEROSPIKE_ERR_CLIENT;
		}
	}
	
	nv = as_vector_get(&values, 1);
	char* begin = nv->value;
	
	if (begin == 0) {
		goto Error;
	}
	
	char* end = begin;
	uint32_t features = 0;
	
	while (*begin) {
		while (*end) {
			if (*end == ';') {
				*end++ = 0;
				break;
			}
			end++;
		}
		
		if (strcmp(begin, "geo") == 0) {
			features |= AS_FEATURES_GEO;
		}
		else if (strcmp(begin, "float") == 0) {
			features |= AS_FEATURES_DOUBLE;
		}
		else if (strcmp(begin, "batch-index") == 0) {
			features |= AS_FEATURES_BATCH_INDEX;
		}
		else if (strcmp(begin, "replicas-all") == 0) {
			features |= AS_FEATURES_REPLICAS_ALL;
		}
		else if (strcmp(begin, "pipelining") == 0) {
			features |= AS_FEATURES_PIPELINING;
		}
		else if (strcmp(begin, "peers") == 0) {
			features |= AS_FEATURES_PEERS;
		}
		begin = end;
	}
	node_info->features = features;
	cf_free(response);
	return AEROSPIKE_OK;
	
Error: {
	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_name(addr, addr_name, sizeof(addr_name));
	as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid node info response from %s: %s", addr_name, response);
	cf_free(response);
	as_socket_close(sock);
	return AEROSPIKE_ERR_CLIENT;
	}
}
