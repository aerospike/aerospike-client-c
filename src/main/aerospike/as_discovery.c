/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/as_discovery.h>
#include <aerospike/as_config.h>

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

//---------------------------------
// Types
//---------------------------------

typedef struct port_map_endpoint_s {
	char* host;
	uint16_t port;
	char* target_host;
	uint16_t target_port;
} port_map_endpoint;

typedef struct port_map_node_s {
	char* name;
	char* target_host;
	uint16_t target_port;
} port_map_node;

// Immutable after creation. Both tables are sorted for binary search.
typedef struct port_map_state_s {
	port_map_endpoint* endpoints;
	uint32_t endpoints_size;
	port_map_node* nodes;
	uint32_t nodes_size;
} port_map_state;

#define MAX_HOST_LENGTH 255

//---------------------------------
// Static Functions
//---------------------------------

static const char*
translator_kind_name(as_translator_kind kind)
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

static as_status
static_seed_provider_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates, void* udata)
{
	(void)udata;
	return as_discovery_static_seed_refresh(err, context, candidates);
}

static bool
static_seed_provider_false(void* udata)
{
	(void)udata;
	return false;
}

static bool
static_seed_provider_true(void* udata)
{
	(void)udata;
	return true;
}

static const char*
static_map_lookup(const as_addr_map* entries, uint32_t size, const char* host)
{
	for (uint32_t i = 0; i < size; i++) {
		if (entries[i].orig && entries[i].alt && strcmp(entries[i].orig, host) == 0) {
			return entries[i].alt;
		}
	}
	return NULL;
}

static const char*
source_name(as_endpoint_source source)
{
	return source == AS_ENDPOINT_SOURCE_ALTERNATE ? "alternate" : "standard";
}

static int
compare_port_map_endpoint(const void* a, const void* b)
{
	const port_map_endpoint* x = a;
	const port_map_endpoint* y = b;
	int rv = strcmp(x->host, y->host);

	if (rv != 0) {
		return rv;
	}
	return (x->port > y->port) - (x->port < y->port);
}

static int
compare_port_map_node(const void* a, const void* b)
{
	return strcmp(((const port_map_node*)a)->name, ((const port_map_node*)b)->name);
}

static void
port_map_destroy(void* udata)
{
	port_map_state* state = udata;

	if (!state) {
		return;
	}

	for (uint32_t i = 0; i < state->endpoints_size; i++) {
		cf_free(state->endpoints[i].host);
		cf_free(state->endpoints[i].target_host);
	}

	for (uint32_t i = 0; i < state->nodes_size; i++) {
		cf_free(state->nodes[i].name);
		cf_free(state->nodes[i].target_host);
	}
	cf_free(state->endpoints);
	cf_free(state->nodes);
	cf_free(state);
}

// Parse "host:port" or "[ipv6]:port" into a copied host and a port.
// Returns NULL on success or a static reason.
static const char*
port_map_parse_address(const char* address, char** host_out, uint16_t* port_out)
{
	const char* colon = strrchr(address, ':');

	if (!colon) {
		return "is not host:port";
	}

	const char* host = address;
	size_t len = (size_t)(colon - address);

	if (len > 0 && host[0] == '[') {
		if (len < 3 || host[len - 1] != ']') {
			return "has an invalid bracketed IPv6 host";
		}
		host++;
		len -= 2;
	}
	else {
		for (size_t i = 0; i < len; i++) {
			if (host[i] == ':') {
				return "has an IPv6 host that is not in brackets";
			}
		}
	}

	if (len == 0) {
		return "has an empty host";
	}

	if (len > MAX_HOST_LENGTH) {
		return "has a host that is too long";
	}

	const char* digits = colon + 1;

	if (!*digits) {
		return "has an empty port";
	}

	uint32_t port = 0;

	for (const char* p = digits; *p; p++) {
		if (!isdigit((unsigned char)*p)) {
			return "has an invalid port";
		}

		port = port * 10 + (uint32_t)(*p - '0');

		if (port > 65535) {
			return "has a port out of range 1-65535";
		}
	}

	if (port == 0) {
		return "has a port out of range 1-65535";
	}

	char* copy = cf_malloc(len + 1);

	if (!copy) {
		return "could not be copied";
	}

	memcpy(copy, host, len);
	copy[len] = 0;
	*host_out = copy;
	*port_out = (uint16_t)port;
	return NULL;
}

static as_status
port_map_invalid(as_error* err, const as_addr_map* entry, const char* reason)
{
	return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid port-map entry %s: %s: %s",
		entry->orig ? entry->orig : "(null)", entry->alt ? entry->alt : "(null)", reason);
}

static as_status
port_map_create(as_error* err, const as_addr_map* entries, uint32_t size, port_map_state** out)
{
	if (size && !entries) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid port-map: map is null");
	}

	port_map_state* state = cf_calloc(1, sizeof(port_map_state));
	uint32_t cap = size ? size : 1;

	if (!state) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate port-map");
	}

	state->endpoints = cf_calloc(cap, sizeof(port_map_endpoint));
	state->nodes = cf_calloc(cap, sizeof(port_map_node));

	if (!state->endpoints || !state->nodes) {
		port_map_destroy(state);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate port-map");
	}

	for (uint32_t i = 0; i < size; i++) {
		const as_addr_map* entry = &entries[i];
		const char* reason;

		if (!entry->orig || !entry->orig[0]) {
			port_map_destroy(state);
			return port_map_invalid(err, entry, "key is empty");
		}

		if (!entry->alt || !entry->alt[0]) {
			port_map_destroy(state);
			return port_map_invalid(err, entry, "value is empty");
		}

		char* target_host = NULL;
		uint16_t target_port = 0;
		reason = port_map_parse_address(entry->alt, &target_host, &target_port);

		if (reason) {
			port_map_destroy(state);
			return port_map_invalid(err, entry, reason);
		}

		if (strchr(entry->orig, ':')) {
			char* host = NULL;
			uint16_t port = 0;
			reason = port_map_parse_address(entry->orig, &host, &port);

			if (reason) {
				cf_free(target_host);
				port_map_destroy(state);
				return port_map_invalid(err, entry, reason);
			}

			port_map_endpoint* ep = &state->endpoints[state->endpoints_size++];
			ep->host = host;
			ep->port = port;
			ep->target_host = target_host;
			ep->target_port = target_port;
		}
		else {
			port_map_node* node = &state->nodes[state->nodes_size++];
			node->name = cf_strdup(entry->orig);
			node->target_host = target_host;
			node->target_port = target_port;

			if (!node->name) {
				port_map_destroy(state);
				return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
					"Failed to copy port-map entry");
			}
		}
	}

	qsort(state->endpoints, state->endpoints_size, sizeof(port_map_endpoint),
		compare_port_map_endpoint);
	qsort(state->nodes, state->nodes_size, sizeof(port_map_node), compare_port_map_node);

	for (uint32_t i = 1; i < state->endpoints_size; i++) {
		if (compare_port_map_endpoint(&state->endpoints[i - 1], &state->endpoints[i]) == 0) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Invalid port-map entry %s:%u: duplicates another key", state->endpoints[i].host,
				(unsigned)state->endpoints[i].port);
			port_map_destroy(state);
			return AEROSPIKE_ERR_PARAM;
		}
	}

	for (uint32_t i = 1; i < state->nodes_size; i++) {
		if (compare_port_map_node(&state->nodes[i - 1], &state->nodes[i]) == 0) {
			as_error_update(err, AEROSPIKE_ERR_PARAM,
				"Invalid port-map entry %s: duplicates another key", state->nodes[i].name);
			port_map_destroy(state);
			return AEROSPIKE_ERR_PARAM;
		}
	}

	*out = state;
	return AEROSPIKE_OK;
}

// Fill out with a copy of the target host. The TLS name is left NULL.
static bool
port_map_lookup(const port_map_state* state, const as_endpoint* endpoint, as_host* out)
{
	const char* host = NULL;
	uint16_t port = 0;

	port_map_endpoint key = {.host = (char*)endpoint->host, .port = endpoint->port};
	const port_map_endpoint* ep = bsearch(&key, state->endpoints, state->endpoints_size,
		sizeof(port_map_endpoint), compare_port_map_endpoint);

	if (ep) {
		host = ep->target_host;
		port = ep->target_port;
	}
	else if (endpoint->node_name) {
		port_map_node nkey = {.name = (char*)endpoint->node_name};
		const port_map_node* node = bsearch(&nkey, state->nodes, state->nodes_size,
			sizeof(port_map_node), compare_port_map_node);

		if (node) {
			host = node->target_host;
			port = node->target_port;
		}
	}

	if (!host) {
		return false;
	}

	as_host_copy_fields(out, host, NULL, port);
	return true;
}

static bool
port_map_translate(const as_endpoint* endpoint, as_host* out, void* udata)
{
	if (!endpoint || !endpoint->host || !out || !udata) {
		return false;
	}
	return port_map_lookup(udata, endpoint, out);
}

static bool
pass_through_translate(const as_endpoint* endpoint, as_host* out, void* udata)
{
	(void)udata;

	if (!endpoint || !endpoint->host || !out) {
		return false;
	}

	as_host_copy_fields(out, endpoint->host, NULL, endpoint->port);
	return true;
}

static bool
contains_host(const as_vector* hosts, const as_host* candidate)
{
	// Host identity is name and port. The TLS name of the first occurrence is kept.
	for (uint32_t i = 0; i < hosts->size; i++) {
		const as_host* host = as_vector_get((as_vector*)hosts, i);

		if (host->port == candidate->port && strcmp(host->name, candidate->name) == 0) {
			return true;
		}
	}
	return false;
}

static bool
append_unique_hosts(const as_vector* input, as_seed_candidates* output)
{
	if (!input) {
		return true;
	}

	for (uint32_t i = 0; i < input->size; i++) {
		const as_host* host = as_vector_get((as_vector*)input, i);

		if (!host->name) {
			return false;
		}

		if (!contains_host(output->hosts, host) &&
			!as_seed_candidates_append(output, host->name, host->tls_name, host->port)) {
			return false;
		}
	}
	return true;
}

static bool
copy_candidates(const as_seed_candidates* src, as_seed_candidates* trg)
{
	if (!as_seed_candidates_init(trg, src->hosts->size)) {
		return false;
	}

	if (!append_unique_hosts(src->hosts, trg)) {
		as_seed_candidates_destroy(trg);
		return false;
	}
	return true;
}

//---------------------------------
// Functions
//---------------------------------

bool
as_seed_candidates_init(as_seed_candidates* candidates, uint32_t capacity)
{
	if (!candidates) {
		return false;
	}

	candidates->hosts = as_vector_create(sizeof(as_host), capacity ? capacity : 1);
	return candidates->hosts != NULL;
}

void
as_seed_candidates_destroy(as_seed_candidates* candidates)
{
	if (!candidates || !candidates->hosts) {
		return;
	}

	for (uint32_t i = 0; i < candidates->hosts->size; i++) {
		as_host_destroy(as_vector_get(candidates->hosts, i));
	}
	as_vector_destroy(candidates->hosts);
	candidates->hosts = NULL;
}

bool
as_seed_candidates_append(as_seed_candidates* candidates, const char* name, const char* tls_name,
	uint16_t port)
{
	if (!candidates || !candidates->hosts || !name || !name[0]) {
		return false;
	}

	as_host host;
	host.name = cf_strdup(name);

	if (!host.name) {
		return false;
	}

	if (tls_name && tls_name[0]) {
		host.tls_name = cf_strdup(tls_name);

		if (!host.tls_name) {
			cf_free(host.name);
			return false;
		}
	}
	else {
		host.tls_name = NULL;
	}

	host.port = port;
	as_vector_append(candidates->hosts, &host);
	return true;
}

void
as_discovery_static_seed_provider(as_seed_provider* provider)
{
	memset(provider, 0, sizeof(as_seed_provider));
	provider->size = sizeof(as_seed_provider);
	provider->api_version = AS_DISCOVERY_API_VERSION;
	provider->name = "static";
	provider->refresh = static_seed_provider_refresh;
	provider->needs_periodic_refresh = static_seed_provider_false;
	provider->supports_deadline = static_seed_provider_true;
}

as_status
as_discovery_static_seed_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates)
{
	if (!context || !context->configured_seeds || !candidates || !candidates->hosts) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid static seed refresh arguments");
	}

	for (uint32_t i = 0; i < context->configured_seeds->size; i++) {
		const as_host* host = as_vector_get((as_vector*)context->configured_seeds, i);

		if (!as_seed_candidates_append(candidates, host->name, host->tls_name, host->port)) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
				"Failed to append static seed candidate");
		}
	}
	return AEROSPIKE_OK;
}

bool
as_discovery_static_map_translate(const as_addr_map* entries, uint32_t size,
	const as_endpoint* endpoint, as_host* out)
{
	if (!endpoint || !endpoint->host || !out || (size && !entries)) {
		return false;
	}

	const char* alt = static_map_lookup(entries, size, endpoint->host);

	if (!alt) {
		return false;
	}

	char* name = cf_strdup(alt);

	if (!name) {
		return false;
	}

	out->name = name;
	out->tls_name = NULL;
	out->port = endpoint->port;
	return true;
}

bool
as_discovery_merge_hosts(as_seed_merge_policy policy, bool init, const as_vector* static_hosts,
	const as_vector* discovery_hosts, as_seed_candidates* output)
{
	if (!output || !output->hosts) {
		return false;
	}

	switch (policy) {
	case AS_SEED_MERGE:
		return append_unique_hosts(static_hosts, output) &&
			append_unique_hosts(discovery_hosts, output);

	case AS_SEED_REPLACE:
		if (init) {
			return append_unique_hosts(static_hosts, output) &&
				append_unique_hosts(discovery_hosts, output);
		}
		return append_unique_hosts(discovery_hosts, output);

	case AS_SEED_DISCOVERY_ONLY:
		return (discovery_hosts && discovery_hosts->size) ?
			append_unique_hosts(discovery_hosts, output) :
			append_unique_hosts(static_hosts, output);

	default:
		return false;
	}
}

as_status
as_discovery_port_map_translator(as_error* err, const as_addr_map* entries, uint32_t size,
	as_address_translator* translator)
{
	as_error_reset(err);

	if (!translator) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid port-map translator");
	}

	port_map_state* state = NULL;
	as_status status = port_map_create(err, entries, size, &state);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	memset(translator, 0, sizeof(as_address_translator));
	translator->size = sizeof(as_address_translator);
	translator->api_version = AS_DISCOVERY_API_VERSION;
	translator->name = "port-map";
	translator->translate = port_map_translate;
	translator->destroy = port_map_destroy;
	translator->udata = state;
	return AEROSPIKE_OK;
}

void
as_discovery_pass_through_translator(as_address_translator* translator)
{
	memset(translator, 0, sizeof(as_address_translator));
	translator->size = sizeof(as_address_translator);
	translator->api_version = AS_DISCOVERY_API_VERSION;
	translator->name = "pass-through";
	translator->translate = pass_through_translate;
}

as_status
as_discovery_complete_translation(as_error* err, const char* translator_name,
	as_translation_miss_policy on_miss, const as_endpoint* endpoint, bool mapped,
	as_host* mapped_host, as_host* out)
{
	out->name = NULL;
	out->tls_name = NULL;
	out->port = 0;

	const char* node = endpoint->node_name ? endpoint->node_name : "";

	if (mapped) {
		const char* name = mapped_host->name;
		uint16_t port = mapped_host->port;
		bool valid = name && name[0] && strlen(name) <= MAX_HOST_LENGTH && port != 0;

		if (valid) {
			as_host_copy_fields(out, name, endpoint->tls_name, port);
		}

		as_host_destroy(mapped_host);
		mapped_host->name = NULL;
		mapped_host->tls_name = NULL;

		if (!valid) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Address translator %s returned an invalid host or port for node %s advertised "
				"address %s:%u in the %s list", translator_name, node, endpoint->host,
				(unsigned)endpoint->port, source_name(endpoint->source));
		}
		return AEROSPIKE_OK;
	}

	if (on_miss == AS_TRANSLATION_STRICT) {
		return as_error_update(err, AEROSPIKE_ERR_TRANSLATION_MISS,
			"Address translator %s has no mapping for node %s advertised address %s:%u in the "
			"%s list. Add a mapping for that endpoint or node, or set translation.on_miss to "
			"pass_through", translator_name, node, endpoint->host, (unsigned)endpoint->port,
			source_name(endpoint->source));
	}

	as_host_copy_fields(out, endpoint->host, endpoint->tls_name, endpoint->port);
	return AEROSPIKE_OK;
}

as_status
as_discovery_conformance_translate(as_error* err, const as_conformance_translation* translation,
	const as_endpoint* endpoint, as_host* out)
{
	as_error_reset(err);

	if (!translation || !endpoint || !endpoint->host || !out ||
		(translation->map_size && !translation->map)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid conformance translate input");
	}

	const char* name = translator_kind_name(translation->kind);

	if (!name) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown translator kind %d",
			(int)translation->kind);
	}

	if (translation->on_miss != AS_TRANSLATION_PASS_THROUGH &&
		translation->on_miss != AS_TRANSLATION_STRICT) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown on_miss value %d",
			(int)translation->on_miss);
	}

	as_host mapped = {0};
	bool found;

	switch (translation->kind) {
	case AS_TRANSLATOR_STATIC_MAP:
		found = as_discovery_static_map_translate(translation->map, translation->map_size,
			endpoint, &mapped);
		break;

	case AS_TRANSLATOR_PORT_MAP: {
		port_map_state* state = NULL;
		as_status status = port_map_create(err, translation->map, translation->map_size, &state);

		if (status != AEROSPIKE_OK) {
			return status;
		}
		found = port_map_lookup(state, endpoint, &mapped);
		port_map_destroy(state);
		break;
	}

	default:
		found = pass_through_translate(endpoint, &mapped, NULL);
		break;
	}
	return as_discovery_complete_translation(err, name, translation->on_miss, endpoint, found,
		&mapped, out);
}

as_status
as_discovery_conformance_merge(as_error* err, as_seed_merge_policy policy,
	const as_vector* static_hosts, const as_vector* const* results, uint32_t results_size,
	as_seed_candidates* snapshots)
{
	as_error_reset(err);

	if (!results || results_size == 0 || !snapshots) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid conformance merge input");
	}

	if (policy != AS_SEED_MERGE && policy != AS_SEED_REPLACE && policy != AS_SEED_DISCOVERY_ONLY) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Unknown merge policy %d", (int)policy);
	}

	for (uint32_t i = 0; i < results_size; i++) {
		bool ok;

		if (i > 0 && !results[i]) {
			// Failed refresh keeps the previous snapshot.
			ok = copy_candidates(&snapshots[i - 1], &snapshots[i]);
		}
		else if (!as_seed_candidates_init(&snapshots[i], 4)) {
			ok = false;
		}
		else {
			ok = as_discovery_merge_hosts(policy, i == 0, static_hosts, results[i], &snapshots[i]);

			if (ok && i > 0 && snapshots[i].hosts->size == 0) {
				// An empty merge after initialization keeps the previous snapshot.
				as_seed_candidates_destroy(&snapshots[i]);
				ok = copy_candidates(&snapshots[i - 1], &snapshots[i]);
			}
			else if (!ok) {
				as_seed_candidates_destroy(&snapshots[i]);
			}
		}

		if (!ok) {
			for (uint32_t j = 0; j < i; j++) {
				as_seed_candidates_destroy(&snapshots[j]);
			}
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to merge seed hosts");
		}
	}
	return AEROSPIKE_OK;
}
