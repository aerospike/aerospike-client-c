/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_peers.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_lookup.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdlib.h>

const char*
as_cluster_get_alternate_host(as_cluster* cluster, const char* hostname);

//---------------------------------
// Functions
//---------------------------------

as_node*
as_peers_find_local_node(as_vector* nodes, const char* name)
{
	as_node* node;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		node = as_vector_get_ptr(nodes, i);
		
		if (strcmp(node->name, name) == 0) {
			return node;
		}
	}
	return NULL;
}

static as_node*
as_peers_find_cluster_node(as_cluster* cluster, const char* name)
{
	// Check global list of nodes for duplicate.
	// This function runs in tend thread, so volatile nodes reference not necessary.
	as_nodes* nodes = cluster->nodes;
	as_node* node;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		node = nodes->array[i];
		
		if (strcmp(node->name, name) == 0) {
			return node;
		}
	}
	return NULL;
}

static bool
as_peers_find_node(
	as_peers* peers, as_cluster* cluster, const char* name, as_vector* hosts,
	as_node** replace_node
	)
{
	// Check global node map for existing cluster.
	as_node* node = as_peers_find_cluster_node(cluster, name);

	if (node) {
		// Node name found.
		as_address* address = as_node_get_address(node);

		if (node->failures == 0 || as_address_is_local((struct sockaddr*)&address->addr)) {
			// If the node does not have cluster tend errors or is localhost,
			// reject new peer as the IP address does not need to change.
			node->friends++;
			return true;
		}

		// Retrieve node's IP address and port.
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_short_name((struct sockaddr*)&address->addr, addr_name, sizeof(addr_name));
		uint16_t port = as_address_port((struct sockaddr*)&address->addr);
		as_error err;

		// Match peer hosts with node.
		for (uint32_t i = 0; i < hosts->size; i++) {
			as_host* host = as_vector_get(hosts, i);

			if (host->port == port) {
				// Check for IP address or hostname if it exists.
				if (strcmp(host->name, addr_name) == 0 ||
				   (node->hostname && strcmp(host->name, node->hostname) == 0)) {
					// Main node host is also the same as one of the peer hosts.
					// Peer should not be added.
					node->friends++;
					return true;
				}

				// Peer name might be a hostname. Get peer IP addresses and check with node IP address.
				as_error_reset(&err);
				as_address_iterator iter;
				as_status status = as_lookup_host(&iter, &err, host->name, 0);

				if (status != AEROSPIKE_OK) {
					as_log_error("Invalid peer received by cluster tend: %s", host->name);
					continue;
				}

				struct sockaddr* addr;
				bool found_node = false;

				while (as_lookup_next(&iter, &addr)) {
					if (as_address_equals(addr, (struct sockaddr*)&address->addr) ||
						as_address_is_local(addr)) {
						// Set node hostname for faster future lookups.
						as_node_set_hostname(node, host->name);
						node->friends++;
						found_node = true;
						break;
					}
				}
				as_lookup_end(&iter);

				if (found_node) {
					return true;
				}
			}
		}

		// Node should be replaced with a new node same name and new IP address.
		*replace_node = node;
	}

	// Check local node map for this tend iteration.
	node = as_peers_find_local_node(&peers->nodes, name);

	if (node) {
		node->friends++;
		*replace_node = NULL;
		return true;
	}
	return false;
}

bool
as_peers_find_invalid_host(as_peers* peers, as_host* host)
{
	as_vector* invalid_hosts = &peers->invalid_hosts;
	
	for (uint32_t i = 0; i < invalid_hosts->size; i++) {
		as_host* h = as_vector_get(invalid_hosts, i);

		if (strcmp(h->name, host->name) == 0 && h->port == host->port) {
			return true;
		}
	}
	return false;
}

static inline void
as_peers_create_node(
	as_peers* peers, as_cluster* cluster, as_host* host, bool is_alias, as_node_info* node_info
	)
{
	// Create node.
	as_node* node = as_node_create(cluster, node_info);
	as_node_create_min_connections(node);

	if (is_alias) {
		as_node_set_hostname(node, host->name);
	}
	as_vector_append(&peers->nodes, &node);
}

static bool
as_peers_validate_node(
	as_peers* peers, as_cluster* cluster, as_host* host, const char* expected_name
	)
{
	if (as_peers_find_invalid_host(peers, host)) {
		return false;
	}

	as_error err;
	as_error_init(&err);

	as_address_iterator iter;
	as_status status = as_lookup_host(&iter, &err, host->name, host->port);
	
	if (status != AEROSPIKE_OK) {
		as_log_warn("%s %s", as_error_string(status), err.message);
		as_peers_add_invalid_host(peers, host);
		return false;
	}
	
	as_node_info node_info;
	struct sockaddr* addr;
	bool validated = false;
	
	while (as_lookup_next(&iter, &addr)) {
		status = as_lookup_node(cluster, &err, host, addr, false, &node_info);
		
		if (status == AEROSPIKE_OK) {
			if (expected_name == NULL || strcmp(node_info.name, expected_name) == 0) {
				as_peers_create_node(peers, cluster, host, iter.hostname_is_alias, &node_info);
				validated = true;
				break;
			}
			else {
				as_node_info_destroy(&node_info);
				as_log_warn("Peer node %s is different than actual node %s for host %s %d",
							expected_name, node_info.name, host->name, host->port);
			}
		}
		else {
			as_log_warn("Failed to connect to peer %s %d. %s %s",
						host->name, host->port, as_error_string(status), err.message);
		}
	}
	as_lookup_end(&iter);

	if (! validated) {
		as_peers_add_invalid_host(peers, host);
	}
	return validated;
}

static bool
as_peers_validate(as_peers* peers, as_cluster* cluster, as_vector* hosts, const char* expected_name)
{
	for (uint32_t i = 0; i < hosts->size; i++) {
		as_host* host = as_vector_get(hosts, i);

		host->name = (char*)as_cluster_get_alternate_host(cluster, host->name);

		if (as_peers_validate_node(peers, cluster, host, expected_name)) {
			return true;
		}
	}
	return false;
}

static as_status
as_peers_expected_error(as_error* err, char expected, const char* p)
{
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid peers. Expected %c at %s", expected, p);
}

static char*
as_peers_parse_host(char* p, as_host* host, char* last)
{
	// IPV6 addresses can start with bracket.
	if (*p == '[') {
		host->name = ++p;
		
		while (*p) {
			if (*p == ']') {
				*p++ = 0;
				
				if (*p == ':') {
					p++;
					host->port = (uint16_t)strtol(p, &p, 10);
				}
				*last = *p;
				return p;
			}
			p++;
		}
	}
	else {
		host->name = p;
		
		while (*p) {
			if (*p == ':') {
				*p++ = 0;
				host->port = (uint16_t)strtol(p, &p, 10);
				*last = *p;
				return p;
			}
			else if (*p == ',' || *p == ']') {
				*last = *p;
				*p = 0;
				return p;
			}
			p++;
		}
	}
	return NULL;
}

void
as_peers_append_unique_node(as_vector* /* <as_node*> */ nodes, as_node* node)
{
	// Avoid adding duplicate nodes.
	if (! as_peers_find_local_node(nodes, node->name)) {
		as_vector_append(nodes, &node);
	}
}

as_status
as_peers_parse_peers(as_peers* peers, as_error* err, as_cluster* cluster, as_node* node, char* buf)
{
	//as_log_debug("Node %s peers: %s", node->name, buf);
	char* p = buf;

	node->peers_count = 0;

	// Parse generation.
	uint32_t generation = (uint32_t)strtol(p, &p, 10);
	
	if (*p != ',') {
		return as_peers_expected_error(err, ',', p);
	}
	p++;
	
	// Parse optional default port.
	uint16_t default_port = 3000;
	
	if (*p != ',') {
		default_port = (uint16_t)strtol(p, &p, 10);
		
		if (*p != ',') {
			return as_peers_expected_error(err, ',', p);
		}
	}
	p++;
	
	// Parse peers.
	if (*p != '[') {
		return as_peers_expected_error(err, '[', p);
	}
	p++;
	
	if (*p == ']') {
		// No peers defined.
		node->peers_generation = generation;
		return AEROSPIKE_OK;
	}

	as_vector hosts;
	as_vector_inita(&hosts, sizeof(as_host), 16);

	bool change_peers_generation = true;

	while (*p) {
		// Parse peer
		if (*p != '[') {
			as_vector_destroy(&hosts);
			return as_peers_expected_error(err, '[', p);
		}
		p++;
		
		// Parse peer node name.
		char* node_name = p;
		while (*p) {
			if (*p == ',') {
				*p++ = 0;
				break;
			}
			p++;
		}
		
		node->peers_count++;

		// Parse peer TLS name
		char* tls_name = p;
		while (*p) {
			if (*p == ',') {
				*p++ = 0;
				break;
			}
			p++;
		}
		
		// Parse peer hosts
		if (*p != '[') {
			as_vector_destroy(&hosts);
			return as_peers_expected_error(err, '[', p);
		}
		p++;
		
		as_vector_clear(&hosts);

		while (*p && *p != ']') {
			// Parse host
			as_host host = {.name = NULL, .tls_name = tls_name, .port = default_port};
			char last = 0;
			p = as_peers_parse_host(p, &host, &last);

			if (! p) {
				as_vector_destroy(&hosts);
				return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid peers host: %s", host.name);
			}

			if (last == ']') {
				as_vector_append(&hosts, &host);
				break;
			}

			if (last != ',') {
				as_vector_destroy(&hosts);
				return as_peers_expected_error(err, ',', p);
			}

			p++;
			as_vector_append(&hosts, &host);
		}
		p++;

		as_node* replace_node = NULL;
		bool node_found = as_peers_find_node(peers, cluster, node_name, &hosts, &replace_node);

		if (! node_found) {
			if (as_peers_validate(peers, cluster, &hosts, node_name)) {
				if (replace_node) {
					as_log_info("Replace node %s %s", replace_node->name, as_node_get_address_string(replace_node));
					as_peers_append_unique_node(&peers->nodes_to_remove, replace_node);
				}
			}
			else {
				change_peers_generation = false;
			}
		}

		if (*p != ']') {
			as_vector_destroy(&hosts);
			return as_peers_expected_error(err, ']', p);
		}
		p++;
		
		if (*p == ']') {
			// Only set new peers generation if all referenced peers are added to the cluster.
			if (change_peers_generation) {
				node->peers_generation = generation;
			}
			as_vector_destroy(&hosts);
			return AEROSPIKE_OK;
		}
		
		if (*p != ',') {
			as_vector_destroy(&hosts);
			return as_peers_expected_error(err, ',', p);
		}
		p++;
	}
	as_vector_destroy(&hosts);
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid peers host: %s", buf);
}
