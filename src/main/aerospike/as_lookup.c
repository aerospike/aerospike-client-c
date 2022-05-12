/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_info.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_string_builder.h>

/******************************************************************************
 * Declarations
 *****************************************************************************/

const char*
as_cluster_get_alternate_host(as_cluster* cluster, const char* hostname);

/******************************************************************************
 * Static Functions
 *****************************************************************************/

static as_status
as_switch_to_clear_socket(as_cluster* cluster, as_error* err, as_node_info* node_info, uint64_t deadline)
{
	// Obtain non-TLS addresses.
	char* command = cluster->use_services_alternate ? "service-clear-alt\n" : "service-clear-std\n";
	char* response = NULL;
	as_status status = as_info_command(err, &node_info->socket, NULL, command, true, deadline, 0, &response);
	as_socket_close(&node_info->socket);

	if (status) {
		return status;
	}

	as_vector hosts;
	as_vector_inita(&hosts, sizeof(as_host), 4);

	char* value = NULL;
	status = as_info_parse_single_response(response, &value);

	if (status) {
		goto SWITCH_ERROR;
	}

	if (! as_host_parse_addresses(value, &hosts)) {
		goto SWITCH_ERROR;
	}

	as_error error_local;
	as_address_iterator iter;
	as_host* host;
	const char* hostname;
	struct sockaddr* addr;

	// Find first valid non-TLS address.
	for (uint32_t i = 0; i < hosts.size; i++) {
		host = as_vector_get(&hosts, i);
		hostname = as_cluster_get_alternate_host(cluster, host->name);
		status = as_lookup_host(&iter, &error_local, hostname, host->port);

		if (status) {
			continue;
		}

		while (as_lookup_next(&iter, &addr)) {
			status = as_socket_create_and_connect(&node_info->socket, &error_local, addr, NULL, NULL, deadline);

			if (status == AEROSPIKE_OK) {
				if (node_info->session) {
					status = as_authenticate(cluster, &error_local, &node_info->socket, NULL,
											 node_info->session, 0, deadline);
				}

				if (status == AEROSPIKE_OK) {
					node_info->host.name = (char*)hostname;
					node_info->host.tls_name = NULL;
					node_info->host.port = host->port;
					as_address_copy_storage(addr, &node_info->addr);
					as_lookup_end(&iter);
					goto SWITCH_SUCCESS;
				}

				// Close and try next address.
				as_socket_close(&node_info->socket);
			}
		}
		as_lookup_end(&iter);
	}

SWITCH_ERROR:
	// Failed to find non-TLS socket.
	status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid service hosts string: '%s'", response);

SWITCH_SUCCESS:
	as_vector_destroy(&hosts);
	cf_free(response);
	return status;
}

static as_status
as_set_node_address(as_cluster* cluster, as_error* err, char* response, char* tls_name, as_node_info* node_info)
{
	if (! (*response)) {
		// Server does not support service level call (service-clear-std, ...).
		// Load balancer detection is not possible.
		return AEROSPIKE_OK;
	}

	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_short_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));

	as_vector hosts;
	as_vector_inita(&hosts, sizeof(as_host), 4);

	if (! as_host_parse_addresses(response, &hosts)) {
		as_vector_destroy(&hosts);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid service hosts string: '%s'", response);
	}

	// Search real hosts for seed.
	as_host* host;
	const char* hostname;

	for (uint32_t i = 0; i < hosts.size; i++) {
		host = as_vector_get(&hosts, i);
		hostname = as_cluster_get_alternate_host(cluster, host->name);

		if (strcmp(hostname, addr_name) == 0) {
			// Found seed which is not a load balancer.
			as_vector_destroy(&hosts);
			return AEROSPIKE_OK;
		}
	}

	// Seed not found, so seed is probably a load balancer.
	// Find first valid real host.
	as_socket sock;
	as_address_iterator iter;
	struct sockaddr* addr;
	as_error error_local;
	as_status status;

	for (uint32_t i = 0; i < hosts.size; i++) {
		host = as_vector_get(&hosts, i);
		hostname = as_cluster_get_alternate_host(cluster, host->name);
		status = as_lookup_host(&iter, &error_local, hostname, host->port);

		if (status != AEROSPIKE_OK) {
			continue;
		}

		while (as_lookup_next(&iter, &addr)) {
			uint64_t deadline = as_socket_deadline(cluster->conn_timeout_ms);
			status = as_socket_create_and_connect(&sock, err, addr, cluster->tls_ctx,
												  tls_name, deadline);

			if (status == AEROSPIKE_OK) {
				if (node_info->session) {
					status = as_authenticate(cluster, &error_local, &sock, NULL,
											 node_info->session, 0, deadline);
				}

				if (status == AEROSPIKE_OK) {
					// Socket is non-blocking, so must use socket to really tell if connect succeeded.
					char* response2 = 0;
					status = as_info_command(err, &sock, NULL, "node", true, deadline, 0, &response2);

					if (status == AEROSPIKE_OK) {
						// Verify node name.
						char* node_name = 0;
						status = as_info_parse_single_response(response2, &node_name);

						if (status == AEROSPIKE_OK && strcmp(node_name, node_info->name) == 0) {
							// Switch address.
							char new_addr_name[AS_IP_ADDRESS_SIZE];
							as_address_short_name(addr, new_addr_name, sizeof(new_addr_name));
							as_log_info("Switch node address from %s to node's access-address %s", addr_name, new_addr_name);

							as_socket_close(&node_info->socket);
							memcpy(&node_info->socket, &sock, sizeof(as_socket));
							node_info->host.name = (char*)hostname;
							node_info->host.tls_name = tls_name;
							node_info->host.port = host->port;
							as_address_copy_storage(addr, &node_info->addr);
							as_lookup_end(&iter);
							as_vector_destroy(&hosts);
							cf_free(response2);
							return AEROSPIKE_OK;
						}
						cf_free(response2);
					}
				}
				// Close and try next address.
				as_socket_close(&sock);
			}
		}
		as_lookup_end(&iter);
	}

	// Failed to find a valid address. IP Address is probably internal on the cloud
	// because the server access-address is not configured.  Log warning and continue
	// with original seed.
	as_log_info("Invalid address %s. access-address is probably not configured on server.", response);
	as_vector_destroy(&hosts);
	return AEROSPIKE_OK;
}

/******************************************************************************
 * Functions
 *****************************************************************************/

as_status
as_lookup_host(as_address_iterator* iter, as_error* err, const char* hostname, uint16_t port)
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
		return as_error_update(err, AEROSPIKE_ERR_INVALID_HOST, "Invalid hostname %s: %s",
							   hostname, gai_strerror(ret));
	}
	
	iter->current = iter->addresses;
	iter->port_be = cf_swap_to_be16(port);
	return AEROSPIKE_OK;
}

as_status
as_lookup_node(
	as_cluster* cluster, as_error* err, as_host* host, struct sockaddr* addr,
	bool detect_load_balancer, as_node_info* node_info
	)
{
	uint64_t deadline = as_socket_deadline(cluster->conn_timeout_ms);
	
	as_status status = as_socket_create_and_connect(&node_info->socket, err, addr, cluster->tls_ctx,
													host->tls_name, deadline);

	if (status) {
		return status;
	}

	node_info->host = *host;
	as_address_copy_storage(addr, &node_info->addr);
	node_info->session = NULL;

	if (cluster->auth_enabled) {
		deadline = as_socket_deadline(cluster->login_timeout_ms);
		status = as_cluster_login(cluster, err, &node_info->socket, deadline, node_info);

		if (status) {
			char str[512];
			snprintf(str, sizeof(str), " from %s:%d", host->name, host->port);
			as_error_append(err, str);
			as_socket_close(&node_info->socket);
			return status;
		}

		if (cluster->tls_ctx && cluster->tls_ctx->for_login_only) {
			// Switch to using non-TLS socket.
			status = as_switch_to_clear_socket(cluster, err, node_info, deadline);

			if (status) {
				cf_free(node_info->session);
				return status;
			}

			// Disable load balancer detection since non-TLS address has already
			// been retrieved via service info command.
			detect_load_balancer = false;
		}
	}

	as_string_builder sb;
	as_string_builder_inita(&sb, 256, false);
	as_string_builder_append(&sb, "node\npartition-generation\nfeatures\n");
	uint32_t args = 3;

	if (cluster->cluster_name) {
		as_string_builder_append(&sb, "cluster-name\n");
		args++;
	}

	if (detect_load_balancer) {
		if (as_address_is_local(addr)) {
			// localhost is never a load balancer.
			detect_load_balancer = false;
		}
		else {
			// Seed may be load balancer with changing address. Determine real address.
			const char* address_command = (cluster->tls_ctx) ?
				cluster->use_services_alternate ? "service-tls-alt" : "service-tls-std" :
				cluster->use_services_alternate ? "service-clear-alt" : "service-clear-std";

			as_string_builder_append(&sb, address_command);
			as_string_builder_append_char(&sb, '\n');
			args++;
		}
	}

	char* response = 0;
	status = as_info_command(err, &node_info->socket, NULL, sb.data, true, deadline, 0, &response);
	
	if (status) {
		as_socket_error_append(err, (struct sockaddr*)&node_info->addr);
		as_node_info_destroy(node_info);
		return status;
	}
	
	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), args);
	
	as_info_parse_multi_response(response, &values);
	
	if ((detect_load_balancer && values.size < args - 1) || (!detect_load_balancer && values.size != args)) {
		// Vector was probably resized on heap. Destroy vector.
		as_vector_destroy(&values);
		goto Error;
	}

	args = 0;

	// Process node name.
	as_name_value* nv = as_vector_get(&values, args++);
	char* node_name = nv->value;
	
	if (node_name == 0 || *node_name == 0) {
		goto Error;
	}
	as_strncpy(node_info->name, node_name, AS_NODE_NAME_SIZE);

	// Process partition generation.
	nv = as_vector_get(&values, args++);
	uint32_t gen = (uint32_t)strtoul(nv->value, NULL, 10);

	if (gen == (uint32_t)-1) {
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node %s %s is not yet fully initialized",
						node_info->name, addr_name);
		cf_free(response);
		as_node_info_destroy(node_info);
		return AEROSPIKE_ERR_CLIENT;
	}

	// Process features.
	nv = as_vector_get(&values, args++);
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

		if (strcmp(begin, "pscans") == 0) {
			features |= AS_FEATURES_PARTITION_SCAN;
		}
		else if (strcmp(begin, "query-show") == 0) {
			features |= AS_FEATURES_QUERY_SHOW;
		}
		else if (strcmp(begin, "batch-any") == 0) {
			features |= AS_FEATURES_BATCH_ANY;
		}
		else if (strcmp(begin, "pquery") == 0) {
			features |= AS_FEATURES_PARTITION_QUERY;
		}

		begin = end;
	}

	// This client requires partition scan support. Partition scans were first
	// supported in server version 4.9. Do not allow any server node into the
	// cluster that is running server version < 4.9.
	if ((features & AS_FEATURES_PARTITION_SCAN) == 0) {
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Node %s %s version < 4.9. This client requires server version >= 4.9",
			node_info->name, addr_name);
		cf_free(response);
		as_node_info_destroy(node_info);
		return AEROSPIKE_ERR_CLIENT;
	}

	node_info->features = features;

	// Process cluster name.
	if (cluster->cluster_name) {
		nv = as_vector_get(&values, args++);
		
		if (strcmp(cluster->cluster_name, nv->value) != 0) {
			char addr_name[AS_IP_ADDRESS_SIZE];
			as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Invalid node %s %s Expected cluster name '%s' Received '%s'",
					node_info->name, addr_name, cluster->cluster_name, nv->value);
			cf_free(response);
			as_node_info_destroy(node_info);
			return AEROSPIKE_ERR_CLIENT;
		}
	}

	if (detect_load_balancer && args < values.size) {
		nv = as_vector_get(&values, args++);
		status = as_set_node_address(cluster, err, nv->value, host->tls_name, node_info);

		if (status != AEROSPIKE_OK) {
			cf_free(response);
			as_node_info_destroy(node_info);
			return status;
		}
	}

	cf_free(response);
	return AEROSPIKE_OK;
	
Error: {
	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
	as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid node info response from %s: %s",
					addr_name, response);
	cf_free(response);
	as_node_info_destroy(node_info);
	return AEROSPIKE_ERR_CLIENT;
	}
}
