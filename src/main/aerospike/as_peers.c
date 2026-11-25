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
#include <aerospike/as_discovery.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_lookup.h>
#include <aerospike/as_lookup_internal.h>
#include <aerospike/as_string_builder.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdlib.h>
#include <string.h>

//---------------------------------
// Declarations
//---------------------------------

static void
as_peers_record_error(as_peers* peers, const as_error* err, bool alternate);

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
	// Check global list of nodes for duplicate. The last node of a name holds it.
	// This function runs in tend thread, so volatile nodes reference not necessary.
	as_nodes* nodes = cluster->nodes;
	as_node* node;
	
	for (uint32_t i = nodes->size; i > 0; i--) {
		node = nodes->array[i - 1];
		
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
				if (strcasecmp(host->name, addr_name) == 0 ||
				   (node->hostname && strcasecmp(host->name, node->hostname) == 0)) {
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
				bool resolved = false;

				// A loopback match does not stop the search, so a later exact match still
				// makes the node's address an address of the hostname.
				while (as_lookup_next(&iter, &addr)) {
					if (as_address_equals(addr, (struct sockaddr*)&address->addr)) {
						found_node = true;
						resolved = true;
						break;
					}

					if (as_address_is_local(addr)) {
						found_node = true;
					}
				}
				as_lookup_end(&iter);

				if (found_node) {
					// Set node hostname for faster future lookups.
					as_node_set_hostname(node, host->name);
					node->hostname_resolved = resolved;
					node->friends++;
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

		if (strcasecmp(h->name, host->name) == 0 && h->port == host->port) {
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
		// Peers are validated without load balancer detection, so the address is one of the host.
		as_node_set_hostname(node, host->name);
		node->hostname_resolved = true;
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
	// Compare references, not names, so two nodes with one name can both be removed.
	for (uint32_t i = 0; i < nodes->size; i++) {
		if (as_vector_get_ptr(nodes, i) == node) {
			return;
		}
	}
	as_vector_append(nodes, &node);
}


//---------------------------------
// Peer records
//---------------------------------

// A host a peer does not use because another node has it. The owner name is copied, because a
// newcomer that owns it can be dropped later in the tend.
typedef struct as_peer_skip_s {
	as_host host;                  // Copy (owned).
	char owner[AS_NODE_NAME_SIZE];
	bool translated;               // The translator changed the advertised host or port.
} as_peer_skip;

// Temporary record of one advertised peer. Parsing and translation complete for the whole reply
// before any node is validated or created, so collisions between siblings can be detected.
typedef struct as_peer_record_s {
	char* node_name;
	char* tls_name;
	as_vector hosts;       // Translated as_host copies (owned).
	as_vector translated;  // Per host bool: the translator changed the advertised host or port.
	as_vector* skipped;    // as_peer_skip of each skipped host in order, NULL when none (owned).
	char* advertised;      // Advertised host list snapshot for view change tracking (owned).
	as_error* error;       // First translation error (owned).
	as_node* replace_node;
	uint8_t* shared;       // Per host flag: the host is also an endpoint of another node (owned).
	uint32_t shared_count;
	uint32_t advertised_size;
	bool found;
	bool alt_missing;
	bool collided;
} as_peer_record;

static void
as_peer_record_init(as_peer_record* rec, char* node_name, char* tls_name, uint32_t capacity)
{
	memset(rec, 0, sizeof(as_peer_record));
	rec->node_name = node_name;
	rec->tls_name = tls_name;
	as_vector_init(&rec->hosts, sizeof(as_host), capacity);
	as_vector_init(&rec->translated, sizeof(bool), capacity);
}

static void
as_peer_record_append_host(as_peer_record* rec, as_host* host, bool translated)
{
	as_vector_append(&rec->hosts, host);
	as_vector_append(&rec->translated, &translated);
}

static inline bool
as_peer_record_translated(as_peer_record* rec, uint32_t index)
{
	return *(bool*)as_vector_get(&rec->translated, index);
}

static void
as_peer_record_destroy(as_peer_record* rec)
{
	for (uint32_t i = 0; i < rec->hosts.size; i++) {
		as_host_destroy(as_vector_get(&rec->hosts, i));
	}
	as_vector_destroy(&rec->hosts);
	as_vector_destroy(&rec->translated);

	if (rec->skipped) {
		for (uint32_t i = 0; i < rec->skipped->size; i++) {
			as_peer_skip* skip = as_vector_get(rec->skipped, i);
			as_host_destroy(&skip->host);
		}
		as_vector_destroy(rec->skipped);
	}
	cf_free(rec->advertised);
	cf_free(rec->error);
	cf_free(rec->shared);
}

// Add a copy of host and its owner to the skipped hosts of rec.
static as_peer_skip*
as_peer_record_skip(as_peer_record* rec, const as_host* host, const char* owner, bool translated)
{
	if (! rec->skipped) {
		rec->skipped = as_vector_create(sizeof(as_peer_skip), 2);
	}

	as_peer_skip* skip = as_vector_reserve(rec->skipped);
	as_host_copy(host, &skip->host);
	as_strncpy(skip->owner, owner, AS_NODE_NAME_SIZE);
	skip->translated = translated;
	return skip;
}

static void
as_peer_records_destroy(as_vector* records)
{
	for (uint32_t i = 0; i < records->size; i++) {
		as_peer_record_destroy(as_vector_get(records, i));
	}
	as_vector_destroy(records);
}

// alternate is true when err reports a missing alternate address.
static void
as_peers_record_error(as_peers* peers, const as_error* err, bool alternate)
{
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

	return strcasecmp(host->name, addr_name) == 0 ||
		(node->hostname && strcasecmp(host->name, node->hostname) == 0);
}

// Hostnames compare case-insensitively, and nothing is resolved (FR-46).
static inline bool
as_peers_host_equals(const as_host* a, const as_host* b)
{
	return a->port == b->port && strcasecmp(a->name, b->name) == 0;
}

static inline bool
as_peers_node_collides(as_node* node, const char* name, const as_host* host)
{
	return strcmp(node->name, name) != 0 && as_peers_node_has_host(node, host);
}

// Return the name of a different node that host is also an endpoint of: the refreshing node, a
// peer of the same reply, a live cluster node or a node validated earlier in this tend. Hosts are
// compared as in as_peers_host_equals(), so no DNS or I/O is needed. node, cluster and peers are
// NULL for the conformance entry point, which models the peers of one reply only.
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

	if (! cluster || ! peers) {
		return NULL;
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

// Flag the hosts of a newcomer that are also endpoints of another node, and add each to the
// skipped hosts with its owner.
static void
as_peers_find_shared_hosts(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index
	)
{
	as_peer_record* rec = as_vector_get(records, index);

	for (uint32_t i = 0; i < rec->hosts.size; i++) {
		as_host* host = as_vector_get(&rec->hosts, i);
		const char* other = as_peers_find_host_owner(peers, cluster, node, records, index, host);

		if (! other) {
			continue;
		}

		if (! rec->shared) {
			rec->shared = cf_calloc(rec->hosts.size, sizeof(uint8_t));
		}
		rec->shared[i] = 1;
		rec->shared_count++;
		as_peer_record_skip(rec, host, other, as_peer_record_translated(rec, i));
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
				memcpy(as_vector_get(&rec->translated, size), as_vector_get(&rec->translated, i),
					sizeof(bool));
			}
			size++;
		}
	}
	rec->hosts.size = size;
	rec->translated.size = size;
}

#define AS_PEERS_SKIPS_SHOWN 3

// The only place that builds the message of a peer left without an address of its own, shared by
// the cluster and the conformance entry point. It lists the skipped hosts of the peer with their
// owners, the first AS_PEERS_SKIPS_SHOWN of them and then how many more.
static void
as_peers_collision_error(as_error* err, const char* translator, const as_peer_record* rec)
{
	char list[AS_ERROR_MESSAGE_MAX_SIZE];
	as_string_builder sb;
	as_string_builder_assign(&sb, sizeof(list), list);

	uint32_t size = rec->skipped ? rec->skipped->size : 0;
	uint32_t shown = size < AS_PEERS_SKIPS_SHOWN ? size : AS_PEERS_SKIPS_SHOWN;

	for (uint32_t i = 0; i < shown; i++) {
		as_peer_skip* skip = as_vector_get(rec->skipped, i);

		if (i > 0) {
			as_string_builder_append(&sb, ", ");
		}
		as_string_builder_append(&sb, skip->host.name);
		as_string_builder_append_char(&sb, ':');
		as_string_builder_append_uint(&sb, skip->host.port);
		as_string_builder_append(&sb, " is used by ");
		as_string_builder_append(&sb, skip->owner);
	}

	if (size > shown) {
		as_string_builder_append(&sb, " and ");
		as_string_builder_append_uint(&sb, size - shown);
		as_string_builder_append(&sb, " more");
	}

	as_error_update(err, AEROSPIKE_ERR_TRANSLATION_COLLISION,
		"Peer %s has no address of its own: %s (address translator %s). "
		"Give each node its own reachable address or mapping",
		rec->node_name, sb.data, translator);
}

// Log a host that a peer skips because another node has it, before any connect: at info level
// for the server's own address, as a warning when the translator produced it.
static void
as_peers_log_shared_host(const char* translator, const char* name, const as_peer_skip* skip)
{
	if (skip->translated) {
		as_log_warn("Address translator %s gave peer %s the address %s:%u of node %s. "
			"The peer uses its other addresses", translator, name, skip->host.name,
			(unsigned)skip->host.port, skip->owner);
	}
	else {
		as_log_info("Peer %s skips address %s:%u because node %s has the same address",
			name, skip->host.name, (unsigned)skip->host.port, skip->owner);
	}
}

// Receives each collision found by as_peers_check_collisions().
typedef struct as_peers_collision_sink_s {
	void (*report)(const as_error* err, void* udata);
	void* udata;
} as_peers_collision_sink;

// Cluster sink: udata is the as_peers of the tend.
static void
as_peers_report_collision(const as_error* err, void* udata)
{
	as_log_warn("%s", err->message);
	as_peers_record_error(udata, err, false);
}

// A peer host validated as node name2 at a socket where node name1 already is.
static void
as_peers_record_collision(
	as_peers* peers, as_cluster* cluster, const char* name1, const char* name2, const char* host,
	uint16_t port
	)
{
	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_TRANSLATION_COLLISION,
		"Nodes %s and %s answer at the same address %s:%u (address translator %s). "
		"Give each node its own reachable address or mapping",
		name1, name2, host, (unsigned)port, as_peers_translator_name(cluster));
	as_peers_report_collision(&err, peers);
}

// Detect collisions of all newcomers before any of them is validated. All shared hosts are found
// before any is removed, so the result does not depend on the order of the reply.
//
// A shared host is never used, for every translator, so two nodes cannot become one transport
// endpoint (FR-46). A peer that still has hosts of its own is created through them, as servers
// without access-address may advertise an interface address, such as a container bridge, that
// every node has. A peer left with none collides and is reported once, listing its shared hosts
// and their owners.
static void
as_peers_check_collisions(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records,
	const char* translator, const as_peers_collision_sink* sink
	)
{
	for (uint32_t i = 0; i < records->size; i++) {
		as_peer_record* rec = as_vector_get(records, i);

		if (rec->found || rec->alt_missing || rec->hosts.size == 0) {
			continue;
		}
		as_peers_find_shared_hosts(peers, cluster, node, records, i);
	}

	for (uint32_t i = 0; i < records->size; i++) {
		as_peer_record* rec = as_vector_get(records, i);

		if (rec->shared_count == 0) {
			continue;
		}

		if (rec->shared_count == rec->hosts.size) {
			rec->collided = true;

			as_error err;
			as_peers_collision_error(&err, translator, rec);
			sink->report(&err, sink->udata);
			continue;
		}

		for (uint32_t k = 0; k < rec->skipped->size; k++) {
			as_peers_log_shared_host(translator, rec->node_name, as_vector_get(rec->skipped, k));
		}
		as_peers_remove_shared_hosts(rec);
	}
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
	char* node_name;
	char* tls_name;

	// Parse peer node name.
	node_name = p;
	while (*p) {
		if (*p == ',') {
			*p++ = 0;
			break;
		}
		p++;
	}

	// Parse peer TLS name
	tls_name = p;
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

	as_peer_record rec;
	as_peer_record_init(&rec, node_name, tls_name, 2);

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
			bool changed = translated.port != host.port ||
				strcasecmp(translated.name, host.name) != 0;
			as_peer_record_append_host(&rec, &translated, changed);
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
		"Node %s has no alternate-access-address and use_services_alternate "
		"(useServicesAlternate) is true. "
		"Configure alternate-access-address on the server or set use_services_alternate to false",
		name);
	as_log_error("%s", err.message);
	as_peers_record_error(peers, &err, true);
}

//---------------------------------
// Validation
//---------------------------------

typedef enum as_peers_validation_e {
	AS_PEERS_VALIDATED,
	AS_PEERS_FAILED,
	AS_PEERS_COLLIDED,

	// No address validated, and at least one answered as a known node and was skipped.
	AS_PEERS_SKIPPED
} as_peers_validation;

// What a validated address of a peer is, besides the peer.
typedef enum as_peers_found_e {
	AS_PEERS_FOUND_NONE,

	// Another name is already at the socket, so two nodes would share one transport endpoint.
	AS_PEERS_FOUND_SOCKET,

	// The address answers as a node the client already knows, so it is not the peer's (FR-47).
	AS_PEERS_FOUND_KNOWN
} as_peers_found;

static bool
as_peers_node_has_addr(as_node* node, const struct sockaddr* addr)
{
	as_address* address = as_node_get_address(node);

	return as_address_port((struct sockaddr*)&address->addr) == as_address_port((struct sockaddr*)addr) &&
		as_address_equals((struct sockaddr*)&address->addr, (struct sockaddr*)addr);
}

// Return a healthy node at the validated socket address whose name is neither the validated name
// nor peer_name.
static as_node*
as_peers_find_socket_node(
	as_node** array, uint32_t size, const char* peer_name, const as_node_info* info
	)
{
	for (uint32_t i = 0; i < size; i++) {
		as_node* node = array[i];

		if (strcmp(node->name, peer_name) == 0 || strcmp(node->name, info->name) == 0) {
			continue;
		}

		if (node->active && node->failures == 0 &&
			as_peers_node_has_addr(node, (struct sockaddr*)&info->addr)) {
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

// After a peer address validated, check what else it is, and copy the other node name to other.
//
// Socket: the refreshing node, a healthy cluster node or a newcomer of this tend is at the
// validated socket under another name, or an earlier socket collision of this tend recorded the
// validated name. A newcomer at the socket is dropped, so neither node becomes a node there.
//
// Known: the validated name is not the peer's but is the name of the refreshing node, a cluster
// node, a newcomer or another advertised peer. That node keeps its address and nothing is dropped.
static as_peers_found
as_peers_find_validated_collision(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index,
	const as_node_info* info, char* other
	)
{
	as_peer_record* rec = as_vector_get(records, index);
	as_node* n;
	other[0] = 0;

	if (strcmp(node->name, info->name) != 0 && strcmp(node->name, rec->node_name) != 0 &&
		as_peers_node_has_addr(node, (struct sockaddr*)&info->addr)) {
		as_strncpy(other, node->name, AS_NODE_NAME_SIZE);
	}
	else if ((n = as_peers_find_socket_node(cluster->nodes->array, cluster->nodes->size,
		rec->node_name, info))) {
		as_strncpy(other, n->name, AS_NODE_NAME_SIZE);
	}
	else if ((n = as_peers_find_socket_node((as_node**)peers->nodes.list, peers->nodes.size,
		rec->node_name, info))) {
		as_strncpy(other, n->name, AS_NODE_NAME_SIZE);
		as_peers_drop_newcomer(peers, n);
	}
	else {
		as_peers_find_collided(peers, info->name, other);
	}

	if (other[0]) {
		as_peers_add_collided(peers, info->name, rec->node_name);
		return AS_PEERS_FOUND_SOCKET;
	}

	if (strcmp(info->name, rec->node_name) == 0) {
		return AS_PEERS_FOUND_NONE;
	}

	if (strcmp(node->name, info->name) == 0 || as_peers_find_cluster_node(cluster, info->name) ||
		as_peers_find_local_node(&peers->nodes, info->name) ||
		as_peers_find_record(records, info->name)) {
		as_strncpy(other, info->name, AS_NODE_NAME_SIZE);
		return AS_PEERS_FOUND_KNOWN;
	}
	return AS_PEERS_FOUND_NONE;
}

// Accept check of the addresses of one peer host.
typedef struct as_peers_accept_s {
	as_peers* peers;
	as_cluster* cluster;
	as_node* node;
	as_vector* records;
	uint32_t index;
	as_host* host;
	uint32_t mismatches;
	bool translated;
	bool collided;
	bool skipped;
} as_peers_accept;

// Log an address of a peer that answered as the known node owner: at info level for the server's
// own address, such as a server-side alias, as a warning when the translator produced it.
static void
as_peers_log_known_host(as_cluster* cluster, const char* name, const as_host* host,
	const char* owner, bool translated)
{
	if (translated) {
		as_log_warn("Address translator %s gave peer %s the address %s:%u, which answers as "
			"node %s. The peer skips it", as_peers_translator_name(cluster), name, host->name,
			(unsigned)host->port, owner);
	}
	else {
		as_log_info("Peer %s skips address %s:%u because it answers as node %s", name, host->name,
			(unsigned)host->port, owner);
	}
}

// A collision and a known node are checked before the name, so neither is reported as a
// mismatch. A socket collision stops the peer. An address that answers as a known node is skipped
// like a shared address (FR-47): the next address is tried and the known node is untouched. The
// first skip of a host is logged and added to the skipped hosts of the peer, which a -26 lists
// when the peer has nothing left.
static as_lookup_verdict
as_peers_accept_node(as_node_info* info, void* udata)
{
	as_peers_accept* a = udata;
	as_peer_record* rec = as_vector_get(a->records, a->index);
	char other[AS_NODE_NAME_SIZE];

	switch (as_peers_find_validated_collision(a->peers, a->cluster, a->node, a->records, a->index,
		info, other)) {
		case AS_PEERS_FOUND_SOCKET:
			as_peers_record_collision(a->peers, a->cluster, rec->node_name, other, a->host->name,
				a->host->port);
			a->collided = true;
			return AS_LOOKUP_STOP;

		case AS_PEERS_FOUND_KNOWN:
			if (! a->skipped) {
				as_peers_log_known_host(a->cluster, rec->node_name, a->host, other, a->translated);
				as_peer_record_skip(rec, a->host, other, a->translated);
				a->skipped = true;
			}
			return AS_LOOKUP_NEXT;

		default:
			break;
	}

	if (strcmp(info->name, rec->node_name) == 0) {
		return AS_LOOKUP_ACCEPT;
	}

	as_log_warn("Peer node %s is different than actual node %s for host %s %d",
				rec->node_name, info->name, a->host->name, a->host->port);
	a->mismatches++;
	return AS_LOOKUP_NEXT;
}

static as_peers_validation
as_peers_validate_node(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index,
	as_host* host, bool translated
	)
{
	if (as_peers_find_invalid_host(peers, host)) {
		return AS_PEERS_FAILED;
	}

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
	as_peers_validation result = AS_PEERS_FAILED;
	bool alternate_missing;
	as_peers_accept accept = {
		.peers = peers,
		.cluster = cluster,
		.node = node,
		.records = records,
		.index = index,
		.host = host,
		.mismatches = 0,
		.translated = translated,
		.collided = false,
		.skipped = false
	};

	// A host with several addresses validates the first address whose connect completes, and
	// goes on to its other addresses when that node is not the peer.
	status = as_lookup_validate_any(cluster, &err, host, &iter, as_peers_accept_node, &accept,
		&node_info, &alternate_missing);

	if (status == AEROSPIKE_OK) {
		as_peers_create_node(peers, cluster, host, iter.hostname_is_alias, &node_info);
		result = AS_PEERS_VALIDATED;
	}
	else {
		if (accept.collided) {
			result = AS_PEERS_COLLIDED;
		}
		else if (accept.skipped && accept.mismatches == 0) {
			// The skip has already been logged. An address that answered as an unknown node
			// was not shown to be shared, so a host with one fails as before.
			result = AS_PEERS_SKIPPED;
		}
		else if (accept.mismatches == 0) {
			// A mismatch has already logged its own warning.
			as_log_warn("Failed to connect to peer %s %d. %s %s",
						host->name, host->port, as_error_string(status), err.message);
		}

		// Keep an actionable error, such as missing alternate addresses, for initialization,
		// even when another address was the wrong node or collided.
		if (alternate_missing || as_peers_is_topology_error(status)) {
			as_peers_record_error(peers, &err, alternate_missing);
		}
	}
	as_lookup_end(&iter);

	// A collided or skipped host is reachable, so it is not an invalid host.
	if (result == AS_PEERS_FAILED) {
		as_peers_add_invalid_host(peers, host);
	}
	return result;
}

// Validate the hosts of a peer in order until one validates or collides. Returns AS_PEERS_SKIPPED
// when every host was skipped, and AS_PEERS_FAILED when any failed otherwise.
static as_peers_validation
as_peers_validate(
	as_peers* peers, as_cluster* cluster, as_node* node, as_vector* records, uint32_t index
	)
{
	as_peer_record* rec = as_vector_get(records, index);
	bool failed = false;

	for (uint32_t i = 0; i < rec->hosts.size; i++) {
		as_host* host = as_vector_get(&rec->hosts, i);
		as_peers_validation result = as_peers_validate_node(peers, cluster, node, records, index,
			host, as_peer_record_translated(rec, i));

		if (result == AS_PEERS_FAILED) {
			failed = true;
		}
		else if (result != AS_PEERS_SKIPPED) {
			return result;
		}
	}
	return failed ? AS_PEERS_FAILED : AS_PEERS_SKIPPED;
}

static void
as_peers_report_translation_error(as_peers* peers, const as_peer_record* rec)
{
	if (rec->error) {
		as_log_warn("%s", rec->error->message);
		as_peers_record_error(peers, rec->error, false);
	}
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

	// A host dropped by a translation error, such as a strict miss, is reported however the other
	// hosts end. A collision of the peer is reported first, so it stays the recorded error.
	if (rec->collided) {
		as_peers_report_translation_error(peers, rec);
		return false;
	}

	if (rec->hosts.size == 0) {
		as_peers_report_translation_error(peers, rec);
		return false;
	}

	as_peers_validation result = as_peers_validate(peers, cluster, node, records, index);

	if (result == AS_PEERS_SKIPPED) {
		// Every host left after the shared ones were removed answered as a known node, so the
		// peer has no address of its own. as_peers_validate() returns AS_PEERS_FAILED instead
		// when a host failed otherwise, which keeps the failure warning and reports no -26.
		as_error err;
		as_peers_collision_error(&err, as_peers_translator_name(cluster), rec);
		as_peers_report_collision(&err, peers);
		as_peers_report_translation_error(peers, rec);
		return false;
	}

	if (result != AS_PEERS_VALIDATED) {
		// A collision has already logged its own warning.
		if (result == AS_PEERS_FAILED) {
			as_peers_report_translation_error(peers, rec);
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
				as_peer_record missing;
				as_peer_record_init(&missing, name, "", 1);
				missing.alt_missing = true;
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

	as_peers_collision_sink sink = {.report = as_peers_report_collision, .udata = peers};
	as_peers_check_collisions(peers, cluster, node, &records, as_peers_translator_name(cluster),
		&sink);

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

//---------------------------------
// Address move
//---------------------------------

// Copy the address of iter after the one tried last to out, skipping the node's address. Return
// false when no address is left.
static bool
as_peers_next_address(as_node* node, as_address_iterator* iter, struct sockaddr_storage* out)
{
	as_vector list;
	as_vector_inita(&list, sizeof(struct sockaddr_storage), 8);
	struct sockaddr* addr;

	while (as_lookup_next(iter, &addr)) {
		as_address_copy_storage(addr, as_vector_reserve(&list));
	}

	bool found = false;

	if (list.size > 1) {
		uint32_t start = 0;

		if (node->has_move_address) {
			for (uint32_t i = 0; i < list.size; i++) {
				if (as_address_equals(as_vector_get(&list, i),
					(struct sockaddr*)&node->move_address)) {
					start = i + 1;
					break;
				}
			}
		}

		struct sockaddr* current = (struct sockaddr*)&as_node_get_address(node)->addr;

		for (uint32_t i = 0; i < list.size; i++) {
			struct sockaddr_storage* candidate = as_vector_get(&list, (start + i) % list.size);

			if (! as_address_equals((struct sockaddr*)candidate, current)) {
				memcpy(out, candidate, sizeof(struct sockaddr_storage));
				found = true;
				break;
			}
		}
	}
	as_vector_destroy(&list);
	return found;
}

// Return true and the other node name when the node of name validated at a candidate address is
// also another node: a healthy cluster node, or a node moved earlier in this tend, at that
// address. Moves run before peers are parsed, so no other newcomer or collided peer exists yet.
// A colliding moved node is dropped.
static bool
as_peers_find_move_collision(
	as_peers* peers, as_cluster* cluster, const char* name, const as_node_info* info, char* other
	)
{
	as_node* n = as_peers_find_socket_node(cluster->nodes->array, cluster->nodes->size, name,
		info);

	if (n) {
		as_strncpy(other, n->name, AS_NODE_NAME_SIZE);
		return true;
	}

	n = as_peers_find_socket_node((as_node**)peers->nodes.list, peers->nodes.size, name, info);

	if (n) {
		as_strncpy(other, n->name, AS_NODE_NAME_SIZE);
		as_peers_drop_newcomer(peers, n);
		return true;
	}
	return false;
}

void
as_peers_move_node(as_peers* peers, as_cluster* cluster, as_node* node)
{
	if (node->failed_tends < AS_NODE_MOVE_FAILED_TENDS || ! node->active || ! node->hostname ||
		! node->hostname_resolved) {
		return;
	}

	uint16_t port = as_address_port((struct sockaddr*)&as_node_get_address(node)->addr);
	as_error err;
	as_error_init(&err);
	as_address_iterator iter;

	if (as_lookup_host(&iter, &err, node->hostname, port) != AEROSPIKE_OK) {
		return;
	}

	struct sockaddr_storage candidate;
	bool found = as_peers_next_address(node, &iter, &candidate);
	as_lookup_end(&iter);

	if (! found) {
		return;
	}
	memcpy(&node->move_address, &candidate, sizeof(candidate));
	node->has_move_address = true;

	// A zero connect timeout means no deadline, so it only bounds the move when set.
	uint32_t bound = cluster->tend_interval / 2;

	if (cluster->conn_timeout_ms > 0 && cluster->conn_timeout_ms < bound) {
		bound = cluster->conn_timeout_ms;
	}

	struct sockaddr* addr = (struct sockaddr*)&candidate;
	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_name(addr, addr_name, sizeof(addr_name));

	as_host host = {.name = node->hostname, .tls_name = node->tls_name, .port = port};
	as_node_info info;
	as_status status = as_lookup_node_until(cluster, &err, &host, addr, false, &info, NULL,
		as_socket_deadline(bound), true);

	if (status != AEROSPIKE_OK) {
		as_log_debug("Address %s of %s failed: %s %s", addr_name, node->hostname,
			as_error_string(status), err.message);
		return;
	}

	if (strcmp(info.name, node->name) != 0) {
		as_log_debug("Address %s of %s is node %s, not node %s", addr_name, node->hostname,
			info.name, node->name);
		as_node_info_destroy(&info);
		return;
	}

	char other[AS_NODE_NAME_SIZE];

	if (as_peers_find_move_collision(peers, cluster, node->name, &info, other)) {
		as_log_warn("Address %s of %s is also node %s, so node %s does not move to it", addr_name,
			node->hostname, other, node->name);
		as_node_info_destroy(&info);
		return;
	}

	// Min connections are created when the swap adds the node.
	as_node* moved = as_node_create(cluster, &info);
	as_node_set_hostname(moved, node->hostname);
	moved->hostname_resolved = true;

	if (node->advertised_hosts) {
		moved->advertised_hosts = cf_strdup(node->advertised_hosts);
	}
	as_vector_append(&peers->nodes, &moved);
	as_peers_append_unique_node(&peers->nodes_to_remove, node);
	as_lookup_set_valid_address(cluster, &host, addr, moved->name);

	as_log_info("Move node %s %s from %s to %s", node->name, node->hostname,
		as_node_get_address_string(node), as_node_get_address_string(moved));
}

//---------------------------------
// Conformance
//---------------------------------

// Conformance sink: copies each collision message.
typedef struct as_peers_collision_copy_s {
	as_vector messages; // char* (owned)
	bool failed;
} as_peers_collision_copy;

static void
as_peers_copy_collision(const as_error* err, void* udata)
{
	as_peers_collision_copy* copy = udata;
	char* message = cf_strdup(err->message);

	if (! message) {
		copy->failed = true;
		return;
	}
	as_vector_append(&copy->messages, &message);
}

// Same names as the conformance translate entry point uses.
static const char*
as_peers_translator_kind_name(as_translator_kind kind)
{
	switch (kind) {
	case AS_TRANSLATOR_STATIC_MAP:
		return "static-map";
	case AS_TRANSLATOR_PORT_MAP:
		return "port-map";
	case AS_TRANSLATOR_PASS_THROUGH:
		return "pass-through";
	default:
		return NULL;
	}
}

static inline bool
as_peers_conformance_same_peer(const as_endpoint* peers, uint32_t i)
{
	return i > 0 && strcmp(peers[i].node_name, peers[i - 1].node_name) == 0;
}

static inline bool
as_peers_tls_name_equals(const char* a, const char* b)
{
	return (! a && ! b) || (a && b && strcmp(a, b) == 0);
}

static as_status
as_peers_conformance_validate(as_error* err, const as_endpoint* peers, uint32_t size)
{
	// First endpoint of the current peer.
	uint32_t first = 0;

	for (uint32_t i = 0; i < size; i++) {
		const as_endpoint* peer = &peers[i];

		if (! peer->node_name || ! peer->node_name[0] || ! peer->host) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Invalid conformance collision peer %u: node name and host are required", i);
		}

		if (peer->source != AS_ENDPOINT_SOURCE_STANDARD &&
			peer->source != AS_ENDPOINT_SOURCE_ALTERNATE) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Invalid conformance collision peer %u: unknown source %d", i, (int)peer->source);
		}

		if (peer->source != peers[0].source) {
			return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
				"Conformance collision peers mix the standard and alternate lists");
		}

		if (! as_peers_conformance_same_peer(peers, i)) {
			for (uint32_t k = 0; k < i; k++) {
				if (strcmp(peers[k].node_name, peer->node_name) == 0) {
					return as_error_update(err, AEROSPIKE_ERR_PARAM,
						"Conformance collision peers repeat node name %s", peer->node_name);
				}
			}
			first = i;
			continue;
		}

		if (! as_peers_tls_name_equals(peer->tls_name, peers[first].tls_name)) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Conformance collision peer %s has more than one TLS name", peer->node_name);
		}

		for (uint32_t k = first; k < i; k++) {
			if (peers[k].port == peer->port && strcasecmp(peers[k].host, peer->host) == 0) {
				return as_error_update(err, AEROSPIKE_ERR_PARAM,
					"Conformance collision peer %s repeats host %s:%u", peer->node_name,
					peer->host, (unsigned)peer->port);
			}
		}
	}
	return AEROSPIKE_OK;
}

static inline bool
as_peers_conformance_is_node(const as_peer_record* rec)
{
	return rec->hosts.size > 0 && ! rec->collided;
}

as_status
as_discovery_conformance_collision(as_error* err, const as_conformance_translation* translation,
	const as_endpoint* peers, uint32_t peers_size, as_conformance_collisions* out)
{
	as_error_reset(err);

	if (out) {
		memset(out, 0, sizeof(as_conformance_collisions));
	}

	if (! translation || ! peers || peers_size == 0 || ! out) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid conformance collision input");
	}

	const char* translator = as_peers_translator_kind_name(translation->kind);

	if (! translator) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown translator kind %d",
			(int)translation->kind);
	}

	as_status status = as_peers_conformance_validate(err, peers, peers_size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	// Every peer is a newcomer of one reply: there is no refreshing, live or validated node.
	// Consecutive endpoints of one node name are the hosts of one peer, in order.
	as_vector records;
	as_vector_init(&records, sizeof(as_peer_record), peers_size);

	for (uint32_t i = 0; i < peers_size; i++) {
		if (! as_peers_conformance_same_peer(peers, i)) {
			as_peer_record rec;
			as_peer_record_init(&rec, (char*)peers[i].node_name, (char*)peers[i].tls_name, 1);
			as_vector_append(&records, &rec);
		}

		as_peer_record* rec = as_vector_get(&records, records.size - 1);
		as_host host;
		status = as_discovery_conformance_translate(err, translation, &peers[i], &host);

		if (status == AEROSPIKE_OK) {
			bool changed = host.port != peers[i].port || strcasecmp(host.name, peers[i].host) != 0;
			as_peer_record_append_host(rec, &host, changed);
		}
		else if (status != AEROSPIKE_ERR_TRANSLATION_MISS) {
			as_peer_records_destroy(&records);
			return status;
		}
		// A strict miss drops only that host. A peer left without hosts is neither a node nor a
		// collision.
	}
	as_error_reset(err);

	as_peers_collision_copy copy = {.failed = false};
	as_vector_init(&copy.messages, sizeof(char*), 4);
	as_peers_collision_sink sink = {.report = as_peers_copy_collision, .udata = &copy};

	as_peers_check_collisions(NULL, NULL, NULL, &records, translator, &sink);

	uint32_t nodes_size = 0;

	for (uint32_t i = 0; i < records.size; i++) {
		if (as_peers_conformance_is_node(as_vector_get(&records, i))) {
			nodes_size++;
		}
	}

	bool ok = ! copy.failed;

	if (ok && nodes_size > 0) {
		out->nodes = cf_calloc(nodes_size, sizeof(char*));
		ok = out->nodes != NULL;

		for (uint32_t i = 0; ok && i < records.size; i++) {
			as_peer_record* rec = as_vector_get(&records, i);

			if (! as_peers_conformance_is_node(rec)) {
				continue;
			}

			char* name = cf_strdup(rec->node_name);

			if (! name) {
				ok = false;
				break;
			}
			out->nodes[out->nodes_size++] = name;
		}
	}

	if (ok && copy.messages.size > 0) {
		out->errors = cf_malloc(copy.messages.size * sizeof(char*));
		ok = out->errors != NULL;

		if (ok) {
			memcpy(out->errors, copy.messages.list, copy.messages.size * sizeof(char*));
			out->errors_size = copy.messages.size;
			copy.messages.size = 0;
		}
	}

	for (uint32_t i = 0; i < copy.messages.size; i++) {
		cf_free(*(char**)as_vector_get(&copy.messages, i));
	}
	as_vector_destroy(&copy.messages);
	as_peer_records_destroy(&records);

	if (! ok) {
		as_conformance_collisions_destroy(out);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to copy the conformance collision result");
	}
	return AEROSPIKE_OK;
}

void
as_conformance_collisions_destroy(as_conformance_collisions* out)
{
	if (! out) {
		return;
	}

	for (uint32_t i = 0; i < out->nodes_size; i++) {
		cf_free(out->nodes[i]);
	}
	cf_free(out->nodes);

	for (uint32_t i = 0; i < out->errors_size; i++) {
		cf_free(out->errors[i]);
	}
	cf_free(out->errors);
	memset(out, 0, sizeof(as_conformance_collisions));
}
