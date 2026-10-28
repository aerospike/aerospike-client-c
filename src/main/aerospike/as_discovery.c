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

#include <string.h>

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

	if (translation->kind != AS_TRANSLATOR_STATIC_MAP) {
		return as_error_update(err, AEROSPIKE_ERR_UNSUPPORTED_FEATURE,
			"Translator %s is not implemented", name);
	}

	const char* host = static_map_lookup(translation->map, translation->map_size, endpoint->host);

	if (!host) {
		if (translation->on_miss == AS_TRANSLATION_STRICT) {
			return as_error_update(err, AEROSPIKE_ERR_TRANSLATION_MISS,
				"Address translator %s has no mapping for node %s advertised address %s:%u in the "
				"%s list. Add a mapping for that endpoint or node, or set translation.on_miss to "
				"pass_through", name, endpoint->node_name ? endpoint->node_name : "",
				endpoint->host, (unsigned)endpoint->port,
				endpoint->source == AS_ENDPOINT_SOURCE_ALTERNATE ? "alternate" : "standard");
		}
		host = endpoint->host;
	}

	as_seed_candidates tmp;

	if (!as_seed_candidates_init(&tmp, 1)) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate host");
	}

	if (!as_seed_candidates_append(&tmp, host, endpoint->tls_name, endpoint->port)) {
		as_seed_candidates_destroy(&tmp);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to copy host");
	}

	// Move the copied host into out.
	*out = *(as_host*)as_vector_get(tmp.hosts, 0);
	as_vector_destroy(tmp.hosts);
	return AEROSPIKE_OK;
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
