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
#include <aerospike/as_string_builder.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdlib.h>

//---------------------------------
// Declarations
//---------------------------------

static void
as_peers_record_error(as_peers* peers, const as_error* err);

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
	// Create node. Min connections are created when the node is added to the cluster, because a
	// collision can still drop it until then.
	as_node* node = as_node_create(cluster, node_info);

	if (is_alias) {
		as_node_set_hostname(node, host->name);
	}
	as_vector_append(&peers->nodes, &node);
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


//---------------------------------
// Peer records
//---------------------------------

// Temporary record of one advertised peer. Parsing and translation complete for the whole reply
// before any node is validated or created, so collisions between siblings can be detected.
typedef struct as_peer_record_s {
	char* node_name;
	char* tls_name;
	as_vector hosts;       // Translated as_host copies (owned).
	char* advertised;      // Advertised host list snapshot for view change tracking (owned).
	as_error* error;       // First translation error (owned).
	as_node* replace_node;
	uint8_t* shared;       // Per host flag: the host is also an endpoint of another node (owned).
	const char* shared_with; // Other node of the first shared host.
	uint32_t shared_index;   // Index of the first shared host.
	uint32_t shared_count;
	uint32_t advertised_size;
	bool found;
	bool alt_missing;
	bool collided;
} as_peer_record;

static void
as_peer_record_destroy(as_peer_record* rec)
{
	for (uint32_t i = 0; i < rec->hosts.size; i++) {
		as_host_destroy(as_vector_get(&rec->hosts, i));
	}
	as_vector_destroy(&rec->hosts);
	cf_free(rec->advertised);
	cf_free(rec->error);
	cf_free(rec->shared);
}

static void
as_peer_records_destroy(as_vector* records)
{
	for (uint32_t i = 0; i < records->size; i++) {
		as_peer_record_destroy(as_vector_get(records, i));
	}
	as_vector_destroy(records);
}

static void
as_peers_record_error(as_peers* peers, const as_error* err)
{
	bool alternate = err->code == AEROSPIKE_ERR_INVALID_NODE;

	if (! peers->has_topology_error || (alternate && ! peers->alternate_missing)) {
		peers->topology_error = *err;
		peers->has_topology_error = true;
	}

	if (alternate) {
		peers->alternate_missing = true;
	}
}

static const char*
as_peers_translator_name(as_cluster* cluster)
{
	return cluster->translator_registered ? cluster->translator.name : "static-map";
}

static bool
as_peers_node_has_host(as_node* node, const as_host* host)
{
	as_address* address = as_node_get_address(node);

	if (host->port != as_address_port((struct sockaddr*)&address->addr)) {
		return false;
	}

	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_short_name((struct sockaddr*)&address->addr, addr_name, sizeof(addr_name));

	return strcmp(host->name, addr_name) == 0 ||
		(node->hostname && strcmp(host->name, node->hostname) == 0);
}

static inline bool
as_peers_host_equals(const as_host* a, const as_host* b)
{
	return a->port == b->port && strcmp(a->name, b->name) == 0;
}

static inline bool
as_peers_node_collides(as_node* node, const char* name, const as_host* host)
{
	return strcmp(node->name, name) != 0 && as_peers_node_has_host(node, host);
}

// Return the name of a different node that host is also an endpoint of: the refreshing node, a
// peer of the same reply, a live cluster node or a node validated earlier in this tend. The
// comparison is exact host and port, so it needs no DNS or I/O.
static const char*
as_peers_find_host_owner(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index,
	const as_host* host
	)
{
	as_peer_record* rec = as_vector_get(records, index);

	if (node && as_peers_node_collides(node, rec->node_name, host)) {
		return node->name;
	}

	for (uint32_t i = 0; i < records->size; i++) {
		as_peer_record* other = as_vector_get(records, i);

		if (i == index || strcmp(other->node_name, rec->node_name) == 0) {
			continue;
		}

		for (uint32_t k = 0; k < other->hosts.size; k++) {
			if (as_peers_host_equals(host, as_vector_get(&other->hosts, k))) {
				return other->node_name;
			}
		}
	}

	as_nodes* nodes = cluster->nodes;

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* n = nodes->array[i];

		if (n->active && n->failures == 0 && as_peers_node_collides(n, rec->node_name, host)) {
			return n->name;
		}
	}

	for (uint32_t i = 0; i < peers->nodes.size; i++) {
		as_node* n = as_vector_get_ptr(&peers->nodes, i);

		if (as_peers_node_collides(n, rec->node_name, host)) {
			return n->name;
		}
	}
	return NULL;
}

// Flag the hosts of a newcomer that are also endpoints of another node.
static void
as_peers_find_shared_hosts(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index
	)
{
	as_peer_record* rec = as_vector_get(records, index);

	for (uint32_t i = 0; i < rec->hosts.size; i++) {
		const char* other = as_peers_find_host_owner(peers, cluster, node, records, index,
			as_vector_get(&rec->hosts, i));

		if (! other) {
			continue;
		}

		if (! rec->shared) {
			rec->shared = cf_calloc(rec->hosts.size, sizeof(uint8_t));
			rec->shared_with = other;
			rec->shared_index = i;
		}
		rec->shared[i] = 1;
		rec->shared_count++;
	}
}

static void
as_peers_remove_shared_hosts(as_peer_record* rec)
{
	uint32_t size = 0;

	for (uint32_t i = 0; i < rec->hosts.size; i++) {
		as_host* host = as_vector_get(&rec->hosts, i);

		if (rec->shared[i]) {
			as_host_destroy(host);
		}
		else {
			if (size != i) {
				memcpy(as_vector_get(&rec->hosts, size), host, sizeof(as_host));
			}
			size++;
		}
	}
	rec->hosts.size = size;
}

static void
as_peers_record_collision(
	as_peers* peers, as_cluster* cluster, const char* name1, const char* name2, const char* host,
	uint16_t port
	)
{
	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_TRANSLATION_COLLISION,
		"Address translator %s translated nodes %s and %s to the same address %s:%u. "
		"Give each node its own reachable address or mapping",
		as_peers_translator_name(cluster), name1, name2, host, (unsigned)port);
	as_log_warn("%s", err.message);
	as_peers_record_error(peers, &err);
}

//---------------------------------
// Parsing
//---------------------------------

static void
as_peers_record_translation_error(as_peer_record* rec, const as_error* err)
{
	if (! rec->error) {
		rec->error = cf_malloc(sizeof(as_error));
		*rec->error = *err;
	}
}

static as_status
as_peers_parse_record(
	as_peers* peers, as_error* err, as_cluster* cluster, char** pp, uint16_t default_port,
	as_vector* records
	)
{
	char* p = *pp;
	as_peer_record rec;

	// Parse peer node name.
	rec.node_name = p;
	while (*p) {
		if (*p == ',') {
			*p++ = 0;
			break;
		}
		p++;
	}

	// Parse peer TLS name
	rec.tls_name = p;
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

	as_vector_init(&rec.hosts, sizeof(as_host), 2);
	rec.advertised = NULL;
	rec.error = NULL;
	rec.replace_node = NULL;
	rec.shared = NULL;
	rec.shared_with = NULL;
	rec.shared_index = 0;
	rec.shared_count = 0;
	rec.advertised_size = 0;
	rec.found = false;
	rec.alt_missing = false;
	rec.collided = false;

	as_string_builder sb;
	bool track_view = cluster->translation_view_callback;

	if (track_view) {
		as_string_builder_init(&sb, 128, true);
	}

	as_endpoint_source source = as_cluster_endpoint_source(cluster);
	as_status status = AEROSPIKE_OK;

	// as_peers_parse_host() overwrites the closing ']' with a terminator when the last host has
	// no port, so the end of the host list is tracked here instead of being re-read from p.
	bool closed = false;

	while (*p && *p != ']') {
		as_host host = {.name = NULL, .tls_name = rec.tls_name, .port = default_port};
		char last = 0;
		p = as_peers_parse_host(p, &host, &last);

		if (! p) {
			status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid peers host: %s", host.name);
			break;
		}

		if (last != ']' && last != ',') {
			status = as_peers_expected_error(err, ',', p);
			break;
		}

		rec.advertised_size++;

		if (track_view) {
			if (rec.advertised_size > 1) {
				as_string_builder_append_char(&sb, ',');
			}
			as_string_builder_append(&sb, host.name);
			as_string_builder_append_char(&sb, ':');
			as_string_builder_append_uint(&sb, host.port);
		}

		as_error terr;
		as_host translated;
		as_status tstatus = as_cluster_translate_address(cluster, &terr, rec.node_name,
			rec.tls_name, host.name, host.port, source, &translated);

		if (tstatus == AEROSPIKE_OK) {
			as_vector_append(&rec.hosts, &translated);
		}
		else {
			as_peers_record_translation_error(&rec, &terr);
		}

		if (last == ']') {
			closed = true;
			break;
		}
		p++;
	}

	if (status == AEROSPIKE_OK && ! closed && *p != ']') {
		status = as_peers_expected_error(err, ']', p);
	}

	if (track_view) {
		rec.advertised = sb.data;
	}

	if (status != AEROSPIKE_OK) {
		as_peer_record_destroy(&rec);
		return status;
	}
	p++;
	as_vector_append(records, &rec);
	*pp = p;
	return AEROSPIKE_OK;
}

// Scan node names of a peers response without creating nodes. Names are terminated in place.
static as_status
as_peers_parse_names(as_error* err, char* buf, as_vector* /* <char*> */ names)
{
	char* p = strchr(buf, '[');

	if (! p) {
		return as_peers_expected_error(err, '[', buf);
	}
	p++;

	uint32_t depth = 1;

	while (*p && depth > 0) {
		if (*p == '[') {
			depth++;
			p++;

			if (depth == 2) {
				char* name = p;

				while (*p && *p != ',' && *p != ']') {
					p++;
				}

				if (*p == ']') {
					depth--;
				}

				if (*p) {
					*p++ = 0;
				}
				as_vector_append(names, &name);
			}
		}
		else {
			if (*p == ']') {
				depth--;
			}
			p++;
		}
	}
	return AEROSPIKE_OK;
}

static as_peer_record*
as_peers_find_record(as_vector* records, const char* name)
{
	for (uint32_t i = 0; i < records->size; i++) {
		as_peer_record* rec = as_vector_get(records, i);

		if (strcmp(rec->node_name, name) == 0) {
			return rec;
		}
	}
	return NULL;
}

// Compare the advertised host list of a known node with its last snapshot. A node that has no
// snapshot yet is recorded without reporting a change.
static void
as_peers_track_view(as_peers* peers, as_cluster* cluster, as_peer_record* rec)
{
	if (! rec->advertised) {
		return;
	}

	as_node* node = as_peers_find_cluster_node(cluster, rec->node_name);

	if (! node) {
		node = as_peers_find_local_node(&peers->nodes, rec->node_name);
	}

	if (! node) {
		return;
	}

	if (node->advertised_hosts) {
		if (strcmp(node->advertised_hosts, rec->advertised) == 0) {
			return;
		}
		peers->view_changed = true;
		cf_free(node->advertised_hosts);
	}
	node->advertised_hosts = rec->advertised;
	rec->advertised = NULL;
}

static void
as_peers_alt_missing_error(as_peers* peers, as_cluster* cluster, const char* name)
{
	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_INVALID_NODE,
		"Node %s has no alternate-access-address and use_services_alternate is true. "
		"Configure alternate-access-address on the server or set use_services_alternate to false",
		name);
	as_log_error("%s", err.message);
	as_peers_record_error(peers, &err);
}

//---------------------------------
// Validation
//---------------------------------

typedef enum as_peers_validation_e {
	AS_PEERS_VALIDATED,
	AS_PEERS_FAILED,
	AS_PEERS_COLLIDED
} as_peers_validation;

static bool
as_peers_node_has_addr(as_node* node, const struct sockaddr* addr)
{
	as_address* address = as_node_get_address(node);

	return as_address_port((struct sockaddr*)&address->addr) == as_address_port((struct sockaddr*)addr) &&
		as_address_equals((struct sockaddr*)&address->addr, (struct sockaddr*)addr);
}

// Return a node, other than the advertised peer, that a validated peer actually is: a node with
// the validated name, or a healthy node at the validated socket address.
static as_node*
as_peers_find_validated_node(
	as_node** array, uint32_t size, const char* peer_name, const as_node_info* info
	)
{
	for (uint32_t i = 0; i < size; i++) {
		as_node* node = array[i];

		if (strcmp(node->name, peer_name) == 0) {
			continue;
		}

		if (strcmp(node->name, info->name) == 0 ||
			(node->active && node->failures == 0 &&
			 as_peers_node_has_addr(node, (struct sockaddr*)&info->addr))) {
			return node;
		}
	}
	return NULL;
}

// Drop a newcomer validated earlier in this tend. Newcomers have no connections other than their
// tend socket until they are added to the cluster, so the node can be destroyed right away.
static void
as_peers_drop_newcomer(as_peers* peers, as_node* node)
{
	for (uint32_t i = 0; i < peers->nodes.size; i++) {
		if (as_vector_get_ptr(&peers->nodes, i) == node) {
			as_vector_remove(&peers->nodes, i);
			break;
		}
	}

	// The dropped newcomer no longer replaces the node of the same name. Only replacements
	// are queued for removal while peers are parsed.
	for (uint32_t i = 0; i < peers->nodes_to_remove.size; i++) {
		as_node* n = as_vector_get_ptr(&peers->nodes_to_remove, i);

		if (strcmp(n->name, node->name) == 0) {
			as_vector_remove(&peers->nodes_to_remove, i);
			break;
		}
	}
	as_node_destroy(node);
}

static void
as_peers_find_collided(as_peers* peers, const char* name, char* other)
{
	for (uint32_t i = 0; i < peers->collided.size; i++) {
		as_peers_collided* entry = as_vector_get(&peers->collided, i);

		if (strcmp(entry->name, name) == 0) {
			as_strncpy(other, entry->peer, AS_NODE_NAME_SIZE);
			return;
		}
	}
}

static void
as_peers_add_collided(as_peers* peers, const char* name, const char* peer)
{
	for (uint32_t i = 0; i < peers->collided.size; i++) {
		as_peers_collided* entry = as_vector_get(&peers->collided, i);

		if (strcmp(entry->name, name) == 0) {
			as_strncpy(entry->peer, peer, AS_NODE_NAME_SIZE);
			return;
		}
	}

	as_peers_collided* entry = as_vector_reserve(&peers->collided);
	as_strncpy(entry->name, name, AS_NODE_NAME_SIZE);
	as_strncpy(entry->peer, peer, AS_NODE_NAME_SIZE);
}

// After a peer validated, check that it is not a node other than the advertised one: the
// refreshing node, a cluster node, a newcomer of this tend, another advertised peer or a node
// that an earlier peer of this tend collided with. A newcomer it resolves to is dropped, so
// neither peer becomes a node. Returns true and the other node name on a collision.
static bool
as_peers_find_validated_collision(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index,
	const as_node_info* info, char* other
	)
{
	as_peer_record* rec = as_vector_get(records, index);
	as_node* n;
	other[0] = 0;

	if (strcmp(node->name, info->name) == 0 ||
		as_peers_node_has_addr(node, (struct sockaddr*)&info->addr)) {
		as_strncpy(other, node->name, AS_NODE_NAME_SIZE);
	}
	else if ((n = as_peers_find_validated_node(cluster->nodes->array, cluster->nodes->size,
		rec->node_name, info))) {
		as_strncpy(other, n->name, AS_NODE_NAME_SIZE);
	}
	else if ((n = as_peers_find_validated_node((as_node**)peers->nodes.list, peers->nodes.size,
		rec->node_name, info))) {
		as_strncpy(other, n->name, AS_NODE_NAME_SIZE);
		as_peers_drop_newcomer(peers, n);
	}
	else if (strcmp(rec->node_name, info->name) != 0 && as_peers_find_record(records, info->name)) {
		as_strncpy(other, info->name, AS_NODE_NAME_SIZE);
	}
	else {
		as_peers_find_collided(peers, info->name, other);
	}

	if (! other[0]) {
		return false;
	}
	as_peers_add_collided(peers, info->name, rec->node_name);
	return true;
}

static as_peers_validation
as_peers_validate_node(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index,
	as_host* host
	)
{
	if (as_peers_find_invalid_host(peers, host)) {
		return AS_PEERS_FAILED;
	}

	as_peer_record* rec = as_vector_get(records, index);
	as_error err;
	as_error_init(&err);

	as_address_iterator iter;
	as_status status = as_lookup_host(&iter, &err, host->name, host->port);

	if (status != AEROSPIKE_OK) {
		as_log_warn("%s %s", as_error_string(status), err.message);
		as_peers_add_invalid_host(peers, host);
		return AS_PEERS_FAILED;
	}

	as_node_info node_info;
	struct sockaddr* addr;
	char other[AS_NODE_NAME_SIZE];
	as_peers_validation result = AS_PEERS_FAILED;

	while (as_lookup_next(&iter, &addr)) {
		status = as_lookup_node(cluster, &err, host, addr, false, &node_info);

		if (status == AEROSPIKE_OK) {
			if (as_peers_find_validated_collision(peers, cluster, node, records, index, &node_info,
				other)) {
				as_peers_record_collision(peers, cluster, rec->node_name, other, host->name,
					host->port);
				as_node_info_destroy(&node_info);
				result = AS_PEERS_COLLIDED;
				break;
			}

			if (strcmp(node_info.name, rec->node_name) == 0) {
				as_peers_create_node(peers, cluster, host, iter.hostname_is_alias, &node_info);
				result = AS_PEERS_VALIDATED;
				break;
			}
			else {
				as_node_info_destroy(&node_info);
				as_log_warn("Peer node %s is different than actual node %s for host %s %d",
							rec->node_name, node_info.name, host->name, host->port);
			}
		}
		else {
			as_log_warn("Failed to connect to peer %s %d. %s %s",
						host->name, host->port, as_error_string(status), err.message);

			// Keep an actionable error, such as missing alternate addresses, for initialization.
			if (as_peers_is_topology_error(status)) {
				as_peers_record_error(peers, &err);
			}
		}
	}
	as_lookup_end(&iter);

	// A collided host is reachable, so it is not an invalid host.
	if (result == AS_PEERS_FAILED) {
		as_peers_add_invalid_host(peers, host);
	}
	return result;
}

static as_peers_validation
as_peers_validate(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index
	)
{
	as_peer_record* rec = as_vector_get(records, index);

	for (uint32_t i = 0; i < rec->hosts.size; i++) {
		as_host* host = as_vector_get(&rec->hosts, i);
		as_peers_validation result = as_peers_validate_node(peers, cluster, node, records, index,
			host);

		if (result != AS_PEERS_FAILED) {
			return result;
		}
	}
	return AS_PEERS_FAILED;
}

static bool
as_peers_validate_record(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index
	)
{
	as_peer_record* rec = as_vector_get(records, index);

	if (rec->alt_missing) {
		as_peers_alt_missing_error(peers, cluster, rec->node_name);
		return false;
	}

	if (rec->collided) {
		return false;
	}

	if (rec->hosts.size == 0) {
		if (rec->error) {
			as_log_warn("%s", rec->error->message);
			as_peers_record_error(peers, rec->error);
		}
		return false;
	}

	as_peers_validation result = as_peers_validate(peers, cluster, node, records, index);

	if (result != AS_PEERS_VALIDATED) {
		// A collision has already logged its own warning.
		if (result == AS_PEERS_FAILED && rec->error) {
			as_log_warn("%s", rec->error->message);
			as_peers_record_error(peers, rec->error);
		}
		return false;
	}

	as_node* added = as_vector_get_ptr(&peers->nodes, peers->nodes.size - 1);

	if (rec->advertised) {
		added->advertised_hosts = rec->advertised;
		rec->advertised = NULL;
	}

	if (rec->replace_node) {
		as_log_info("Replace node %s %s", rec->replace_node->name,
			as_node_get_address_string(rec->replace_node));
		as_peers_append_unique_node(&peers->nodes_to_remove, rec->replace_node);
	}
	return true;
}

as_status
as_peers_parse_peers(
	as_peers* peers, as_error* err, as_cluster* cluster, as_node* node, char* buf, char* std_buf
	)
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

	as_vector records;
	as_vector_init(&records, sizeof(as_peer_record), 16);

	while (*p && *p != ']') {
		if (*p != '[') {
			as_peer_records_destroy(&records);
			return as_peers_expected_error(err, '[', p);
		}
		p++;

		node->peers_count++;

		as_status status = as_peers_parse_record(peers, err, cluster, &p, default_port, &records);

		if (status != AEROSPIKE_OK) {
			as_peer_records_destroy(&records);
			return status;
		}

		if (*p != ']') {
			as_peer_records_destroy(&records);
			return as_peers_expected_error(err, ']', p);
		}
		p++;

		if (*p == ',') {
			p++;
		}
		else if (*p != ']') {
			as_peer_records_destroy(&records);
			return as_peers_expected_error(err, ',', p);
		}
	}

	if (*p != ']') {
		as_peer_records_destroy(&records);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid peers host: %s", buf);
	}

	if (std_buf) {
		// Alternate peers were requested. Every standard peer must be present in the alternate
		// list with at least one address.
		as_vector names;
		as_vector_inita(&names, sizeof(char*), 16);

		as_status status = as_peers_parse_names(err, std_buf, &names);

		if (status != AEROSPIKE_OK) {
			as_vector_destroy(&names);
			as_peer_records_destroy(&records);
			return status;
		}

		for (uint32_t i = 0; i < names.size; i++) {
			char* name = *(char**)as_vector_get(&names, i);
			as_peer_record* rec = as_peers_find_record(&records, name);

			if (! rec) {
				as_peer_record missing = {
					.node_name = name, .tls_name = "", .advertised = NULL, .error = NULL,
					.replace_node = NULL, .shared = NULL, .shared_with = NULL, .shared_index = 0,
					.shared_count = 0, .advertised_size = 0, .found = false,
					.alt_missing = true, .collided = false
				};
				as_vector_init(&missing.hosts, sizeof(as_host), 1);
				as_vector_append(&records, &missing);
				node->peers_count++;
			}
			else if (rec->advertised_size == 0) {
				rec->alt_missing = true;
			}
		}
		as_vector_destroy(&names);
	}

	// Phase one is complete. Track advertised changes, then resolve nodes that already exist.
	for (uint32_t i = 0; i < records.size; i++) {
		as_peer_record* rec = as_vector_get(&records, i);

		as_peers_track_view(peers, cluster, rec);
		rec->found = as_peers_find_node(peers, cluster, rec->node_name, &rec->hosts, &rec->replace_node);
	}

	// Detect collisions of all newcomers before any of them is validated. All shared hosts are
	// found before any is removed, so the result does not depend on the order of the reply.
	for (uint32_t i = 0; i < records.size; i++) {
		as_peer_record* rec = as_vector_get(&records, i);

		if (rec->found || rec->alt_missing || rec->hosts.size == 0) {
			continue;
		}
		as_peers_find_shared_hosts(peers, cluster, node, &records, i);
	}

	// A shared host is never used, so two nodes cannot become one transport endpoint. With a
	// registered translator a peer with any shared host is not created. The implicit static map
	// keeps a peer that still has hosts of its own, as servers without access-address may
	// advertise an interface address, such as a container bridge, that every node has.
	bool reject_peer = cluster->translator_registered;

	for (uint32_t i = 0; i < records.size; i++) {
		as_peer_record* rec = as_vector_get(&records, i);

		if (rec->shared_count == 0) {
			continue;
		}

		as_host* shared = as_vector_get(&rec->hosts, rec->shared_index);

		if (reject_peer || rec->shared_count == rec->hosts.size) {
			rec->collided = true;
			as_peers_record_collision(peers, cluster, rec->shared_with, rec->node_name,
				shared->name, shared->port);
		}
		else {
			as_log_info("Peer %s skips address %s:%u because node %s has the same address",
				rec->node_name, shared->name, (unsigned)shared->port, rec->shared_with);
			as_peers_remove_shared_hosts(rec);
		}
	}

	// Only set new peers generation if all referenced peers are added to the cluster.
	bool change_peers_generation = true;

	for (uint32_t i = 0; i < records.size; i++) {
		as_peer_record* rec = as_vector_get(&records, i);

		if (rec->found) {
			continue;
		}

		if (! as_peers_validate_record(peers, cluster, node, &records, i)) {
			change_peers_generation = false;
		}
	}

	if (change_peers_generation) {
		node->peers_generation = generation;
	}
	as_peer_records_destroy(&records);
	return AEROSPIKE_OK;
}
