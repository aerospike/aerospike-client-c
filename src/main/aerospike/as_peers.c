/*
 * Copyright 2008-2021 Aerospike, Inc.
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

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

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
as_peers_find_node(as_peers* peers, as_cluster* cluster, const char* name)
{
	// Check global node map for existing cluster.
	as_node* node = as_peers_find_cluster_node(cluster, name);
	
	if (node) {
		node->friends++;
		return true;
	}
	
	// Check local node map for this tend iteration.
	node = as_peers_find_local_node(&peers->nodes, name);
	
	if (node) {
		node->friends++;
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
		as_node_add_alias(node, host->name, host->port);
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
	return 0;
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

	bool peers_validated = true;

	while (*p) {
		// Parse peer
		if (*p != '[') {
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
		
		bool node_validated = false;
		
		if (as_peers_find_node(peers, cluster, node_name)) {
			// Node already exists. Do not even try to connect to hosts.
			node_validated = true;
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
			return as_peers_expected_error(err, '[', p);
		}
		p++;
		
		while (*p) {
			if (*p == ']') {
				p++;
				break;
			}
			
			// Parse host
			as_host host = {.name = NULL, .tls_name = tls_name, .port = default_port};
			char last = 0;
			p = as_peers_parse_host(p, &host, &last);
			
			if (! p) {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid peers host: %s", host.name);
			}

			// Only add the first host that works for a node.
			if (! node_validated) {
				// Check global aliases for existing cluster.
				host.name = (char*)as_cluster_get_alternate_host(cluster, host.name);
				node_validated = as_peers_validate_node(peers, cluster, &host, node_name);
			}
			
			if (last) {
				*p = last;
			}
			
			if (*p == ',') {
				p++;
			}
			else if (*p != ']') {
				return as_peers_expected_error(err, ',', p);
			}
		}

		if (! node_validated) {
			peers_validated = false;
		}

		if (*p != ']') {
			return as_peers_expected_error(err, ']', p);
		}
		p++;
		
		if (*p == ']') {
			// Only set new peers generation if all referenced peers are added to the cluster.
			if (peers_validated) {
				node->peers_generation = generation;
			}
			return AEROSPIKE_OK;
		}
		
		if (*p != ',') {
			return as_peers_expected_error(err, ',', p);
		}
		p++;
	}
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid peers host: %s", buf);
}
