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
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_sleep.h>
#include <citrusleaf/cf_clock.h>

#include "test.h"
#include "aerospike_test.h"

// Coverage: U-1 (contract shape and error channel only), U-2, U-3, U-5, U-7,
// U-8, U-11 and the static-map/port-map/pass-through/merge L1 translate,
// collision and merge entry points; src/test/conformance runs the shared
// fixture rows through them. Runtime degradation in U-1 and M-7 is covered by
// discovery_seed_manager.

extern aerospike* as;

//---------------------------------
// Static Functions
//---------------------------------

static as_status
refresh(as_error* err, const as_seed_candidate_context* context, as_seed_candidates* candidates,
	void* udata)
{
	(void)err;
	(void)context;
	(void)candidates;
	(void)udata;
	return AEROSPIKE_OK;
}

static as_status
refresh_fail(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates, void* udata)
{
	(void)context;
	(void)candidates;
	(void)udata;
	return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "refresh failed");
}

static bool
needs_periodic_refresh(void* udata)
{
	(void)udata;
	return false;
}

static void
destroy(void* udata)
{
	(*(uint32_t*)udata)++;
}

static bool
translate(const as_endpoint* endpoint, as_host* out, void* udata)
{
	(void)endpoint;
	(void)out;
	(void)udata;
	return false;
}

static void
init_provider(as_seed_provider* provider, const char* name, uint32_t* counter)
{
	memset(provider, 0, sizeof(as_seed_provider));
	provider->size = sizeof(as_seed_provider);
	provider->api_version = AS_DISCOVERY_API_VERSION;
	provider->name = name;
	provider->refresh = refresh;
	provider->needs_periodic_refresh = needs_periodic_refresh;
	provider->destroy = destroy;
	provider->udata = counter;
}

static void
init_translator(as_address_translator* translator, const char* name, uint32_t* counter)
{
	memset(translator, 0, sizeof(as_address_translator));
	translator->size = sizeof(as_address_translator);
	translator->api_version = AS_DISCOVERY_API_VERSION;
	translator->name = name;
	translator->translate = translate;
	translator->destroy = destroy;
	translator->udata = counter;
}

static as_vector*
hosts_create(void)
{
	return as_vector_create(sizeof(as_host), 4);
}

static void
hosts_add(as_vector* hosts, const char* name, const char* tls_name, uint16_t port)
{
	as_host host;
	as_host_copy_fields(&host, name, tls_name, port);
	as_vector_append(hosts, &host);
}

static void
hosts_destroy(as_vector* hosts)
{
	for (uint32_t i = 0; i < hosts->size; i++) {
		as_host_destroy(as_vector_get(hosts, i));
	}
	as_vector_destroy(hosts);
}

// Format hosts as "name:port,..." for order-sensitive comparison.
static const char*
hosts_str(const as_vector* hosts, char* buf, size_t size)
{
	buf[0] = 0;
	size_t len = 0;

	for (uint32_t i = 0; i < hosts->size; i++) {
		as_host* host = as_vector_get((as_vector*)hosts, i);
		len += snprintf(buf + len, size - len, "%s%s:%u", i ? "," : "", host->name, host->port);

		if (len >= size) {
			break;
		}
	}
	return buf;
}

static int
thread_count(void)
{
#if defined(__linux__)
	FILE* f = fopen("/proc/self/status", "r");

	if (!f) {
		return -1;
	}

	char line[256];
	int count = -1;

	while (fgets(line, sizeof(line), f)) {
		if (strncmp(line, "Threads:", 8) == 0) {
			count = atoi(line + 8);
			break;
		}
	}
	fclose(f);
	return count;
#else
	return -1;
#endif
}

// A thread joined by an earlier test still counts for a moment after
// pthread_join() returns, so a baseline is only taken once the count holds.
static int
stable_thread_count(void)
{
	int count = thread_count();

	for (int i = 0, same = 0; count >= 0 && i < 200 && same < 5; i++) {
		as_sleep(10);
		int next = thread_count();
		same = next == count ? same + 1 : 0;
		count = next;
	}
	return count;
}

static int
cmp_u64(const void* a, const void* b)
{
	uint64_t x = *(const uint64_t*)a;
	uint64_t y = *(const uint64_t*)b;
	return (x > y) - (x < y);
}

static bool
file_contains_ci(const char* path, const char* needle, bool* found)
{
	FILE* f = fopen(path, "r");

	if (!f) {
		return false;
	}

	char line[1024];
	size_t n = strlen(needle);
	*found = false;

	while (!*found && fgets(line, sizeof(line), f)) {
		for (char* p = line; *p && !*found; p++) {
			size_t i = 0;

			while (i < n && p[i] && tolower((unsigned char)p[i]) == needle[i]) {
				i++;
			}
			*found = (i == n);
		}
	}
	fclose(f);
	return true;
}

//---------------------------------
// Tests: API shape and status
//---------------------------------

TEST(discovery_status_codes, "U-1 discovery status values and strings are fixed")
{
	assert_int_eq(AEROSPIKE_ERR_TRANSLATION_MISS, -25);
	assert_int_eq(AEROSPIKE_ERR_TRANSLATION_COLLISION, -26);
	assert_int_eq(AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, -27);
	assert_int_eq(AEROSPIKE_METRICS_CONFLICT, -20);
	assert_string_eq(as_error_string(AEROSPIKE_ERR_TRANSLATION_MISS),
		"AEROSPIKE_ERR_TRANSLATION_MISS");
	assert_string_eq(as_error_string(AEROSPIKE_ERR_TRANSLATION_COLLISION),
		"AEROSPIKE_ERR_TRANSLATION_COLLISION");
	assert_string_eq(as_error_string(AEROSPIKE_ERR_SEED_DISCOVERY_FAILED),
		"AEROSPIKE_ERR_SEED_DISCOVERY_FAILED");
}

TEST(discovery_topology_neutral_names, "U-11 public discovery surface has no topology-specific names")
{
	const char* files[] = {
		AS_START_DIR "src/include/aerospike/as_discovery.h",
		AS_START_DIR "src/include/aerospike/as_config.h"
	};

	for (uint32_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
		bool found = true;
		assert_true(file_contains_ci(files[i], "nlb", &found));
		assert_false(found);
	}
}

//---------------------------------
// Tests: defaults
//---------------------------------

TEST(discovery_defaults, "U-3 defaults are static seeds, static map, merge, pass-through, thread")
{
	int threads = stable_thread_count();
	as_config config;
	as_config_init(&config);

	assert_false(config.discovery.provider_registered);
	assert_false(config.discovery.binding_provider);
	assert_false(config.discovery.registration_locked);
	assert_false(config.translation.provider_registered);
	assert_null(config.discovery.provider.udata);
	assert_null(config.translation.provider.udata);
	assert_int_eq(config.discovery.merge_policy, AS_SEED_MERGE);
	assert_int_eq(config.discovery.execution_mode, AS_DISCOVERY_EXECUTION_THREAD);
	assert_int_eq(config.discovery.refresh_interval_ms, 30000);
	assert_int_eq(config.discovery.refresh_timeout_ms, 10000);
	assert_int_eq(config.discovery.tend_refresh_deadline_ms, 100);
	assert_int_eq(config.translation.on_miss, AS_TRANSLATION_PASS_THROUGH);

	aerospike client;
	aerospike_init(&client, &config);
	aerospike_destroy(&client);

	if (threads >= 0) {
		assert_int_eq(thread_count(), threads);
	}
}

TEST(discovery_static_provider, "U-3 static provider copies configured seeds in order")
{
	as_seed_provider provider;
	as_discovery_static_seed_provider(&provider);

	assert_int_eq(provider.size, sizeof(as_seed_provider));
	assert_int_eq(provider.api_version, AS_DISCOVERY_API_VERSION);
	assert_string_eq(provider.name, "static");
	assert_false(provider.needs_periodic_refresh(provider.udata));
	assert_true(provider.supports_deadline(provider.udata));
	assert_null(provider.destroy);

	as_vector* seeds = hosts_create();
	hosts_add(seeds, "b.example", "tls-b", 3000);
	hosts_add(seeds, "a.example", NULL, 3100);
	hosts_add(seeds, "c.example", "", 3200);

	as_seed_candidate_context context = {
		.cluster_name = "c1",
		.configured_seeds = seeds,
		.tend_iteration = 0
	};
	as_seed_candidates candidates;
	as_error err;

	assert_true(as_seed_candidates_init(&candidates, 0));
	assert_int_eq(provider.refresh(&err, &context, &candidates, provider.udata), AEROSPIKE_OK);
	assert_int_eq(candidates.hosts->size, 3);

	as_host* first = as_vector_get(candidates.hosts, 0);
	as_host* second = as_vector_get(candidates.hosts, 1);
	as_host* seed = as_vector_get(seeds, 0);

	assert_string_eq(first->name, "b.example");
	assert_string_eq(first->tls_name, "tls-b");
	assert_int_eq(first->port, 3000);
	assert_true(first->name != seed->name);
	assert_true(first->tls_name != seed->tls_name);
	assert_string_eq(second->name, "a.example");
	assert_null(second->tls_name);
	assert_int_eq(second->port, 3100);

	// An empty TLS name is the same as none.
	as_host* third = as_vector_get(candidates.hosts, 2);
	assert_string_eq(third->name, "c.example");
	assert_null(third->tls_name);

	as_seed_candidates direct;
	assert_true(as_seed_candidates_init(&direct, 0));
	assert_int_eq(as_discovery_static_seed_refresh(&err, &context, &direct), AEROSPIKE_OK);
	assert_int_eq(direct.hosts->size, 3);
	assert_null(((as_host*)as_vector_get(direct.hosts, 2))->tls_name);
	as_seed_candidates_destroy(&direct);

	as_seed_candidates_destroy(&candidates);
	assert_null(candidates.hosts);
	hosts_destroy(seeds);
}

TEST(discovery_candidates_append, "U-1 candidate append copies strings and rejects invalid hosts")
{
	as_seed_candidates candidates;
	char name[] = "host-1";
	char tls[] = "tls-1";

	assert_true(as_seed_candidates_init(&candidates, 1));
	assert_true(as_seed_candidates_append(&candidates, name, tls, 3000));
	assert_false(as_seed_candidates_append(&candidates, NULL, NULL, 3000));
	assert_false(as_seed_candidates_append(&candidates, "", NULL, 3000));
	name[0] = 'X';
	tls[0] = 'X';

	// An empty TLS name is the same as none.
	assert_true(as_seed_candidates_append(&candidates, "host-2", "", 3000));
	assert_true(as_seed_candidates_append(&candidates, "host-3", "x", 3000));

	as_host* host = as_vector_get(candidates.hosts, 0);
	assert_int_eq(candidates.hosts->size, 3);
	assert_string_eq(host->name, "host-1");
	assert_string_eq(host->tls_name, "tls-1");
	host = as_vector_get(candidates.hosts, 1);
	assert_string_eq(host->name, "host-2");
	assert_null(host->tls_name);
	host = as_vector_get(candidates.hosts, 2);
	assert_string_eq(host->tls_name, "x");
	as_seed_candidates_destroy(&candidates);
}

TEST(discovery_empty_tls_configured_seed, "U-1 an empty configured TLS name gets the default")
{
	as_config config;
	as_error err;

	as_config_init(&config);
	assert_true(as_config_add_hosts(&config, "seed-host::3000", 3000));
	assert_string_eq(((as_host*)as_vector_get(config.hosts, 0))->tls_name, "");
	config.tls.enable = true;
	as_config_tls_set_cert_blacklist(&config, "/nonexistent/plan1-blacklist");

	// The blacklist read fails after TLS names are defaulted and before any network I/O.
	aerospike client;
	aerospike_init(&client, &config);
	assert_int_eq(aerospike_connect(&client, &err), AEROSPIKE_ERR_TLS_ERROR);
	assert_null(client.cluster);
	assert_string_eq(((as_host*)as_vector_get(client.config.hosts, 0))->tls_name, "seed-host");

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		as_config_destroy(&client.config);
	}
	else {
		aerospike_destroy(&client);
	}
}

//---------------------------------
// Tests: static map
//---------------------------------

TEST(discovery_static_map, "U-2/U-7 static map is exact, host-only, keeps port and omits TLS")
{
	as_addr_map entries[] = {
		{"10.0.0.1", "198.51.100.1"},
		{"fd00::1", "2001:db8::1"},
		{"node.internal", "node.example.com"}
	};
	as_endpoint endpoint = {
		.node_name = "BB9020011AC4202",
		.tls_name = "node-a.example",
		.host = "10.0.0.1",
		.port = 3010,
		.source = AS_ENDPOINT_SOURCE_STANDARD
	};
	as_host out = {0};

	assert_true(as_discovery_static_map_translate(entries, 3, &endpoint, &out));
	assert_string_eq(out.name, "198.51.100.1");
	assert_null(out.tls_name);
	assert_int_eq(out.port, 3010);
	assert_string_eq(endpoint.host, "10.0.0.1");
	assert_string_eq(endpoint.tls_name, "node-a.example");
	as_host_destroy(&out);

	endpoint.host = "fd00::1";
	endpoint.source = AS_ENDPOINT_SOURCE_ALTERNATE;
	assert_true(as_discovery_static_map_translate(entries, 3, &endpoint, &out));
	assert_string_eq(out.name, "2001:db8::1");
	as_host_destroy(&out);

	// IPv6 equality is string equality, as in legacy ip_map.
	memset(&out, 0, sizeof(out));
	endpoint.host = "fd00:0::1";
	assert_false(as_discovery_static_map_translate(entries, 3, &endpoint, &out));
	assert_null(out.name);

	endpoint.host = "NODE.INTERNAL";
	assert_false(as_discovery_static_map_translate(entries, 3, &endpoint, &out));
	endpoint.host = "10.0.0.1";
	assert_false(as_discovery_static_map_translate(entries, 0, &endpoint, &out));
	assert_false(as_discovery_static_map_translate(NULL, 0, &endpoint, &out));
	assert_null(out.name);
}

TEST(discovery_static_map_legacy_parity, "U-7 static map matches legacy ip_map byte for byte")
{
	as_addr_map entries[] = {
		{"10.0.0.1", "198.51.100.1"},
		{"10.0.0.1", "198.51.100.99"},
		{"10.0.0.2", "10.0.0.2"},
		{"seed.internal", "seed.example.com"},
		{"fd00::1", "2001:db8::1"}
	};
	uint32_t entries_size = sizeof(entries) / sizeof(entries[0]);
	const char* inputs[] = {
		"10.0.0.1", "10.0.0.2", "10.0.0.3", "seed.internal", "fd00::1", "fd00::01", "", "10.0.0.1 "
	};

	// Same layout the cluster builds from as_config.ip_map; configured seeds use this path too.
	as_cluster cluster;
	memset(&cluster, 0, sizeof(cluster));
	cluster.ip_map = as_vector_create(sizeof(as_addr_map), entries_size);

	for (uint32_t i = 0; i < entries_size; i++) {
		as_vector_append(cluster.ip_map, &entries[i]);
	}

	for (uint32_t i = 0; i < sizeof(inputs) / sizeof(inputs[0]); i++) {
		as_endpoint endpoint = {
			.node_name = "A1",
			.tls_name = NULL,
			.host = inputs[i],
			.port = 3000,
			.source = AS_ENDPOINT_SOURCE_STANDARD
		};
		as_host out = {0};
		const char* legacy = as_cluster_get_alternate_host(&cluster, inputs[i]);
		bool mapped = as_discovery_static_map_translate(entries, entries_size, &endpoint, &out);
		const char* actual = mapped ? out.name : endpoint.host;

		assert_int_eq(strlen(actual), strlen(legacy));
		assert_string_eq(actual, legacy);
		assert_int_eq(out.port, mapped ? 3000 : 0);

		if (mapped) {
			as_host_destroy(&out);
		}
	}
	as_vector_destroy(cluster.ip_map);
}

TEST(discovery_static_map_latency, "U-8 static map translate p99 is below 1 ms")
{
	as_addr_map entries[64];
	char orig[64][32];
	char alt[64][32];

	for (uint32_t i = 0; i < 64; i++) {
		snprintf(orig[i], sizeof(orig[i]), "10.0.0.%u", i);
		snprintf(alt[i], sizeof(alt[i]), "198.51.100.%u", i);
		entries[i].orig = orig[i];
		entries[i].alt = alt[i];
	}

	as_endpoint endpoint = {
		.node_name = "A1",
		.tls_name = "tls",
		.host = "10.0.0.63",
		.port = 3000,
		.source = AS_ENDPOINT_SOURCE_STANDARD
	};
	enum { SAMPLES = 10000 };
	uint64_t* samples = malloc(sizeof(uint64_t) * SAMPLES);
	assert_not_null(samples);

	for (uint32_t i = 0; i < SAMPLES; i++) {
		as_host out;
		uint64_t begin = cf_getns();
		bool mapped = as_discovery_static_map_translate(entries, 64, &endpoint, &out);
		samples[i] = cf_getns() - begin;

		if (mapped) {
			as_host_destroy(&out);
		}
	}

	qsort(samples, SAMPLES, sizeof(uint64_t), cmp_u64);
	uint64_t p99 = samples[SAMPLES * 99 / 100];
	free(samples);
	assert_true(p99 < 1000000);
}

//---------------------------------
// Tests: merge
//---------------------------------

TEST(discovery_merge, "U-5 merge policies order, init behavior and name:port deduplication")
{
	char buf[256];
	as_vector* statics = hosts_create();
	as_vector* discovered = hosts_create();
	as_vector* empty = hosts_create();
	as_seed_candidates out;

	hosts_add(statics, "s1", NULL, 3000);
	hosts_add(statics, "s2", NULL, 3000);
	hosts_add(statics, "s1", NULL, 3000);
	hosts_add(discovered, "d1", "tls-d1", 3000);
	hosts_add(discovered, "s2", "other-tls", 3000);
	hosts_add(discovered, "s1", NULL, 3001);

	// merge: static first then discovered; duplicates by name and port only.
	assert_true(as_seed_candidates_init(&out, 0));
	assert_true(as_discovery_merge_hosts(AS_SEED_MERGE, false, statics, discovered, &out));
	assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), "s1:3000,s2:3000,d1:3000,s1:3001");
	assert_null(((as_host*)as_vector_get(out.hosts, 1))->tls_name);
	assert_string_eq(((as_host*)as_vector_get(out.hosts, 2))->tls_name, "tls-d1");
	as_seed_candidates_destroy(&out);

	assert_true(as_seed_candidates_init(&out, 0));
	assert_true(as_discovery_merge_hosts(AS_SEED_MERGE, true, statics, empty, &out));
	assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), "s1:3000,s2:3000");
	as_seed_candidates_destroy(&out);

	// replace: merge at init, discovered only afterwards.
	assert_true(as_seed_candidates_init(&out, 0));
	assert_true(as_discovery_merge_hosts(AS_SEED_REPLACE, true, statics, discovered, &out));
	assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), "s1:3000,s2:3000,d1:3000,s1:3001");
	as_seed_candidates_destroy(&out);

	assert_true(as_seed_candidates_init(&out, 0));
	assert_true(as_discovery_merge_hosts(AS_SEED_REPLACE, false, statics, discovered, &out));
	assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), "d1:3000,s2:3000,s1:3001");
	assert_string_eq(((as_host*)as_vector_get(out.hosts, 1))->tls_name, "other-tls");
	as_seed_candidates_destroy(&out);

	assert_true(as_seed_candidates_init(&out, 0));
	assert_true(as_discovery_merge_hosts(AS_SEED_REPLACE, false, statics, empty, &out));
	assert_int_eq(out.hosts->size, 0);
	as_seed_candidates_destroy(&out);

	// discovery_only: discovered unless empty, at init and afterwards.
	for (int init = 0; init <= 1; init++) {
		assert_true(as_seed_candidates_init(&out, 0));
		assert_true(as_discovery_merge_hosts(AS_SEED_DISCOVERY_ONLY, init, statics, discovered, &out));
		assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), "d1:3000,s2:3000,s1:3001");
		as_seed_candidates_destroy(&out);

		assert_true(as_seed_candidates_init(&out, 0));
		assert_true(as_discovery_merge_hosts(AS_SEED_DISCOVERY_ONLY, init, statics, NULL, &out));
		assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), "s1:3000,s2:3000");
		as_seed_candidates_destroy(&out);
	}

	assert_true(as_seed_candidates_init(&out, 0));
	assert_false(as_discovery_merge_hosts((as_seed_merge_policy)99, true, statics, discovered, &out));
	as_seed_candidates_destroy(&out);

	// An empty TLS name is the same as none, for configured and discovered hosts.
	as_vector* empty_tls_statics = hosts_create();
	as_vector* empty_tls_discovered = hosts_create();
	hosts_add(empty_tls_statics, "e1", "", 3000);
	hosts_add(empty_tls_discovered, "e2", "", 3000);
	assert_true(as_seed_candidates_init(&out, 0));
	assert_true(as_discovery_merge_hosts(AS_SEED_MERGE, true, empty_tls_statics,
		empty_tls_discovered, &out));
	assert_string_eq(hosts_str(out.hosts, buf, sizeof(buf)), "e1:3000,e2:3000");
	assert_null(((as_host*)as_vector_get(out.hosts, 0))->tls_name);
	assert_null(((as_host*)as_vector_get(out.hosts, 1))->tls_name);
	as_seed_candidates_destroy(&out);
	hosts_destroy(empty_tls_statics);
	hosts_destroy(empty_tls_discovered);

	hosts_destroy(statics);
	hosts_destroy(discovered);
	hosts_destroy(empty);
}

//---------------------------------
// Tests: conformance entry points
//---------------------------------

TEST(discovery_conformance_translate, "L1 translate rows run against the C built-ins")
{
	as_addr_map entries[] = {{"10.0.0.1", "198.51.100.1"}};
	as_conformance_translation translation = {
		.kind = AS_TRANSLATOR_STATIC_MAP,
		.on_miss = AS_TRANSLATION_PASS_THROUGH,
		.map = entries,
		.map_size = 1
	};
	as_endpoint endpoint = {
		.node_name = "A1",
		.tls_name = "node-a.example",
		.host = "10.0.0.1",
		.port = 3010,
		.source = AS_ENDPOINT_SOURCE_ALTERNATE
	};
	as_error err;
	as_host out;

	// Hit: translated transport, TLS identity from the endpoint.
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_OK);
	assert_string_eq(out.name, "198.51.100.1");
	assert_string_eq(out.tls_name, "node-a.example");
	assert_true(out.tls_name != endpoint.tls_name);
	assert_int_eq(out.port, 3010);
	as_host_destroy(&out);

	// Pass-through miss.
	endpoint.host = "10.0.0.9";
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_OK);
	assert_string_eq(out.name, "10.0.0.9");
	assert_string_eq(out.tls_name, "node-a.example");
	assert_int_eq(out.port, 3010);
	as_host_destroy(&out);

	// Strict miss names the node, endpoint, source list and translator.
	translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_ERR_TRANSLATION_MISS);
	assert_not_null(strstr(err.message, "A1"));
	assert_not_null(strstr(err.message, "10.0.0.9:3010"));
	assert_not_null(strstr(err.message, "alternate"));
	assert_not_null(strstr(err.message, "static-map"));

	// Default translator: static map over ip_map with no map at all.
	translation.on_miss = AS_TRANSLATION_PASS_THROUGH;
	translation.map = NULL;
	translation.map_size = 0;
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_OK);
	assert_string_eq(out.name, "10.0.0.9");
	as_host_destroy(&out);

	// Port-map: endpoint key first, then node name; unmapped under strict is a miss.
	as_addr_map port_entries[] = {{"10.0.0.9:3010", "198.51.100.9:4010"}, {"A1", "node-a.example:5000"}};
	translation.kind = AS_TRANSLATOR_PORT_MAP;
	translation.map = port_entries;
	translation.map_size = 2;
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_OK);
	assert_string_eq(out.name, "198.51.100.9");
	assert_int_eq(out.port, 4010);
	assert_string_eq(out.tls_name, "node-a.example");
	as_host_destroy(&out);

	endpoint.host = "10.0.0.8";
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_OK);
	assert_string_eq(out.name, "node-a.example");
	assert_int_eq(out.port, 5000);
	as_host_destroy(&out);

	endpoint.node_name = "B2";
	translation.on_miss = AS_TRANSLATION_STRICT;
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_ERR_TRANSLATION_MISS);
	assert_not_null(strstr(err.message, "port-map"));

	// Pass-through is an explicit mapping, so it succeeds even under strict.
	translation.kind = AS_TRANSLATOR_PASS_THROUGH;
	translation.map = NULL;
	translation.map_size = 0;
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_OK);
	assert_string_eq(out.name, "10.0.0.8");
	assert_string_eq(out.tls_name, "node-a.example");
	assert_int_eq(out.port, 3010);
	as_host_destroy(&out);

	translation.kind = (as_translator_kind)99;
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_ERR_PARAM);

	translation.kind = AS_TRANSLATOR_STATIC_MAP;
	translation.map_size = 1;
	assert_int_eq(as_discovery_conformance_translate(&err, &translation, &endpoint, &out),
		AEROSPIKE_ERR_PARAM);
}

TEST(discovery_conformance_merge, "L1 merge rows publish one snapshot per discovery result")
{
	char buf[256];
	as_vector* statics = hosts_create();
	as_vector* d1 = hosts_create();
	as_vector* d2 = hosts_create();
	as_vector* empty = hosts_create();
	as_error err;

	hosts_add(statics, "s1", NULL, 3000);
	hosts_add(d1, "d1", NULL, 3000);
	hosts_add(d2, "d2", NULL, 3000);

	const as_vector* results[] = {d1, empty, NULL, d2};
	as_seed_candidates snapshots[4];

	// replace: init merges, empty and failed refreshes keep the previous snapshot.
	assert_int_eq(as_discovery_conformance_merge(&err, AS_SEED_REPLACE, statics, results, 4,
		snapshots), AEROSPIKE_OK);
	assert_string_eq(hosts_str(snapshots[0].hosts, buf, sizeof(buf)), "s1:3000,d1:3000");
	assert_string_eq(hosts_str(snapshots[1].hosts, buf, sizeof(buf)), "s1:3000,d1:3000");
	assert_string_eq(hosts_str(snapshots[2].hosts, buf, sizeof(buf)), "s1:3000,d1:3000");
	assert_string_eq(hosts_str(snapshots[3].hosts, buf, sizeof(buf)), "d2:3000");

	for (uint32_t i = 0; i < 4; i++) {
		as_seed_candidates_destroy(&snapshots[i]);
	}

	// discovery_only: empty discovery falls back to static.
	assert_int_eq(as_discovery_conformance_merge(&err, AS_SEED_DISCOVERY_ONLY, statics, results, 4,
		snapshots), AEROSPIKE_OK);
	assert_string_eq(hosts_str(snapshots[0].hosts, buf, sizeof(buf)), "d1:3000");
	assert_string_eq(hosts_str(snapshots[1].hosts, buf, sizeof(buf)), "s1:3000");
	assert_string_eq(hosts_str(snapshots[2].hosts, buf, sizeof(buf)), "s1:3000");
	assert_string_eq(hosts_str(snapshots[3].hosts, buf, sizeof(buf)), "d2:3000");

	for (uint32_t i = 0; i < 4; i++) {
		as_seed_candidates_destroy(&snapshots[i]);
	}

	// merge: static always first.
	assert_int_eq(as_discovery_conformance_merge(&err, AS_SEED_MERGE, statics, results, 4,
		snapshots), AEROSPIKE_OK);
	assert_string_eq(hosts_str(snapshots[0].hosts, buf, sizeof(buf)), "s1:3000,d1:3000");
	assert_string_eq(hosts_str(snapshots[1].hosts, buf, sizeof(buf)), "s1:3000");
	assert_string_eq(hosts_str(snapshots[2].hosts, buf, sizeof(buf)), "s1:3000");
	assert_string_eq(hosts_str(snapshots[3].hosts, buf, sizeof(buf)), "s1:3000,d2:3000");

	for (uint32_t i = 0; i < 4; i++) {
		as_seed_candidates_destroy(&snapshots[i]);
	}

	assert_int_eq(as_discovery_conformance_merge(&err, AS_SEED_MERGE, statics, results, 0,
		snapshots), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_discovery_conformance_merge(&err, (as_seed_merge_policy)99, statics, results,
		4, snapshots), AEROSPIKE_ERR_PARAM);

	hosts_destroy(statics);
	hosts_destroy(d1);
	hosts_destroy(d2);
	hosts_destroy(empty);
}

// The cluster's collision message for name2 colliding with the earlier owner name1.
static const char*
collision_text(char* buf, size_t size, const char* translator, const char* name1,
	const char* name2, const char* host, uint16_t port)
{
	snprintf(buf, size, "Address translator %s translated nodes %s and %s to the same address "
		"%s:%u. Give each node its own reachable address or mapping", translator, name1, name2,
		host, (unsigned)port);
	return buf;
}

static uint32_t
collision_errors_naming(const as_conformance_collisions* out, const char* text)
{
	uint32_t count = 0;

	for (uint32_t i = 0; i < out->errors_size; i++) {
		if (strstr(out->errors[i], text)) {
			count++;
		}
	}
	return count;
}

TEST(discovery_conformance_collision, "L1 collision rows report every colliding peer and create the rest")
{
	char text[512];
	as_error err;
	as_conformance_collisions out;

	// Static map: two hosts to one target, a third peer with its own mapping.
	as_addr_map ip_entries[] = {
		{"10.0.0.1", "203.0.113.1"}, {"10.0.0.2", "203.0.113.1"}, {"10.0.0.3", "203.0.113.3"}
	};
	as_conformance_translation translation = {
		.kind = AS_TRANSLATOR_STATIC_MAP,
		.on_miss = AS_TRANSLATION_PASS_THROUGH,
		.map = ip_entries,
		.map_size = 3
	};
	as_endpoint peers[] = {
		{.node_name = "A", .host = "10.0.0.1", .port = 3000},
		{.node_name = "B", .host = "10.0.0.2", .port = 3000},
		{.node_name = "C", .host = "10.0.0.3", .port = 3000}
	};

	assert_int_eq(as_discovery_conformance_collision(&err, &translation, peers, 3, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 1);
	assert_string_eq(out.nodes[0], "C");
	assert_int_eq(out.errors_size, 2);
	// One message per colliding peer, each naming the other peer first, as the cluster logs.
	assert_string_eq(out.errors[0],
		collision_text(text, sizeof(text), "static-map", "B", "A", "203.0.113.1", 3000));
	assert_string_eq(out.errors[1],
		collision_text(text, sizeof(text), "static-map", "A", "B", "203.0.113.1", 3000));
	as_conformance_collisions_destroy(&out);
	assert_null(out.nodes);
	assert_null(out.errors);
	assert_int_eq(out.errors_size, 0);

	// Three peers on one endpoint: no node, every peer reported.
	as_endpoint shared[] = {
		{.node_name = "A", .host = "10.0.0.5", .port = 3000},
		{.node_name = "B", .host = "10.0.0.5", .port = 3000},
		{.node_name = "C", .host = "10.0.0.5", .port = 3000}
	};
	translation.kind = AS_TRANSLATOR_PASS_THROUGH;
	translation.map = NULL;
	translation.map_size = 0;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, shared, 3, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 0);
	assert_null(out.nodes);
	assert_int_eq(out.errors_size, 3);
	assert_int_eq(collision_errors_naming(&out, "pass-through"), 3);
	assert_int_eq(collision_errors_naming(&out, "10.0.0.5:3000"), 3);
	assert_true(collision_errors_naming(&out, " A ") > 0);
	assert_true(collision_errors_naming(&out, " B ") > 0);
	assert_true(collision_errors_naming(&out, " C ") > 0);
	as_conformance_collisions_destroy(&out);

	// Port map: two node keys to one value.
	as_addr_map port_entries[] = {{"A", "lb.example.com:30001"}, {"B", "lb.example.com:30001"}};
	translation.kind = AS_TRANSLATOR_PORT_MAP;
	translation.map = port_entries;
	translation.map_size = 2;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, peers, 2, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 0);
	assert_int_eq(out.errors_size, 2);
	assert_string_eq(out.errors[0],
		collision_text(text, sizeof(text), "port-map", "B", "A", "lb.example.com", 30001));
	assert_string_eq(out.errors[1],
		collision_text(text, sizeof(text), "port-map", "A", "B", "lb.example.com", 30001));
	as_conformance_collisions_destroy(&out);

	// Same host, distinct ports: no collision.
	as_endpoint ports[] = {
		{.node_name = "A", .host = "10.0.0.1", .port = 3000},
		{.node_name = "B", .host = "10.0.0.1", .port = 3001}
	};
	translation.kind = AS_TRANSLATOR_PASS_THROUGH;
	translation.map = NULL;
	translation.map_size = 0;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, ports, 2, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 2);
	assert_string_eq(out.nodes[0], "A");
	assert_string_eq(out.nodes[1], "B");
	assert_int_eq(out.errors_size, 0);
	assert_null(out.errors);
	as_conformance_collisions_destroy(&out);

	// A strict miss drops that peer only.
	as_endpoint miss[] = {
		{.node_name = "A", .host = "10.0.0.1", .port = 3000},
		{.node_name = "M", .host = "10.0.0.9", .port = 3000},
		{.node_name = "B", .host = "10.0.0.2", .port = 3000},
		{.node_name = "C", .host = "10.0.0.3", .port = 3000}
	};
	translation.kind = AS_TRANSLATOR_STATIC_MAP;
	translation.on_miss = AS_TRANSLATION_STRICT;
	translation.map = ip_entries;
	translation.map_size = 3;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, miss, 4, &out),
		AEROSPIKE_OK);
	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 1);
	assert_string_eq(out.nodes[0], "C");
	assert_int_eq(out.errors_size, 2);
	assert_int_eq(collision_errors_naming(&out, " M "), 0);
	as_conformance_collisions_destroy(&out);

	// Alternate source.
	for (uint32_t i = 0; i < 3; i++) {
		shared[i].source = AS_ENDPOINT_SOURCE_ALTERNATE;
	}
	translation.kind = AS_TRANSLATOR_PASS_THROUGH;
	translation.on_miss = AS_TRANSLATION_PASS_THROUGH;
	translation.map = NULL;
	translation.map_size = 0;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, shared, 2, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.errors_size, 2);
	as_conformance_collisions_destroy(&out);

	// Invalid input leaves out zeroed.
	shared[1].source = AS_ENDPOINT_SOURCE_STANDARD;
	out.nodes_size = 7;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, shared, 3, &out),
		AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "standard and alternate"));
	assert_null(out.nodes);
	assert_int_eq(out.nodes_size, 0);
	assert_null(out.errors);
	assert_int_eq(out.errors_size, 0);

	as_endpoint repeated[] = {
		{.node_name = "A", .host = "10.0.0.1", .port = 3000},
		{.node_name = "A", .host = "10.0.0.2", .port = 3000}
	};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, repeated, 2, &out),
		AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "repeat node name A"));

	ports[1].host = NULL;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, ports, 2, &out),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(out.nodes_size, 0);

	as_addr_map bad_entries[] = {{"A", "lb.example.com"}};
	translation.kind = AS_TRANSLATOR_PORT_MAP;
	translation.map = bad_entries;
	translation.map_size = 1;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, peers, 2, &out),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(out.nodes_size, 0);
	assert_null(out.nodes);

	assert_int_eq(as_discovery_conformance_collision(&err, &translation, peers, 0, &out),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_discovery_conformance_collision(&err, NULL, peers, 2, &out),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, peers, 2, NULL),
		AEROSPIKE_ERR_PARAM);
	as_conformance_collisions_destroy(NULL);
}

//---------------------------------
// Tests: registration and ownership
//---------------------------------

TEST(discovery_provider_validation, "U-1 provider registration enforces required callbacks")
{
	as_config config;
	as_error err;
	uint32_t destroyed = 0;
	as_seed_provider provider;

	as_config_init(&config);

	init_provider(&provider, "p", &destroyed);
	provider.refresh = NULL;
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_ERR_PARAM);

	init_provider(&provider, "p", &destroyed);
	provider.needs_periodic_refresh = NULL;
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_ERR_PARAM);

	init_provider(&provider, "p", &destroyed);
	provider.size = sizeof(as_seed_provider) - 1;
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_ERR_PARAM);

	init_provider(&provider, "p", &destroyed);
	provider.api_version = AS_DISCOVERY_API_VERSION + 1;
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_ERR_PARAM);

	init_provider(&provider, NULL, &destroyed);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_ERR_PARAM);

	init_provider(&provider, "", &destroyed);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_set_seed_provider(&config, NULL, &err), AEROSPIKE_ERR_PARAM);
	assert_false(config.discovery.provider_registered);

	// Optional callbacks may be NULL.
	char name[] = "optional";
	init_provider(&provider, name, &destroyed);
	provider.supports_deadline = NULL;
	provider.destroy = NULL;
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	name[0] = 'X';
	assert_string_eq(config.discovery.provider.name, "optional");
	assert_null(config.discovery.provider.supports_deadline);
	assert_false(config.discovery.binding_provider);

	as_config_destroy(&config);
	assert_int_eq(destroyed, 0);
}

TEST(discovery_translator_validation, "U-2 translator registration enforces required callbacks")
{
	as_config config;
	as_error err;
	uint32_t destroyed = 0;
	as_address_translator translator;

	as_config_init(&config);

	init_translator(&translator, "t", &destroyed);
	translator.translate = NULL;
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_ERR_PARAM);

	init_translator(&translator, "t", &destroyed);
	translator.size = 0;
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_ERR_PARAM);

	init_translator(&translator, "t", &destroyed);
	translator.api_version = 0;
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_ERR_PARAM);

	init_translator(&translator, NULL, &destroyed);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_ERR_PARAM);
	assert_false(config.translation.provider_registered);

	init_translator(&translator, "t", &destroyed);
	translator.on_cluster_view_changed = NULL;
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);
	assert_true(config.translation.provider_registered);
	assert_int_eq(destroyed, 0);

	as_config_destroy(&config);
	assert_int_eq(destroyed, 1);
}

TEST(discovery_registration_ownership, "U-1 replacement destroys only the replaced registration")
{
	as_config config;
	as_error err;
	uint32_t first = 0;
	uint32_t second = 0;
	uint32_t t_first = 0;
	uint32_t t_second = 0;
	as_seed_provider provider;
	as_address_translator translator;

	as_config_init(&config);
	init_provider(&provider, "first", &first);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	init_provider(&provider, "second", &second);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(first, 1);
	assert_int_eq(second, 0);
	assert_string_eq(config.discovery.provider.name, "second");

	init_translator(&translator, "first", &t_first);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);
	init_translator(&translator, "second", &t_second);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);
	assert_int_eq(t_first, 1);
	assert_int_eq(t_second, 0);

	// A rejected replacement keeps the current registration.
	provider.refresh = NULL;
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(second, 0);
	assert_true(config.discovery.provider_registered);

	as_config_destroy(&config);
	assert_int_eq(first, 1);
	assert_int_eq(second, 1);
	assert_int_eq(t_first, 1);
	assert_int_eq(t_second, 1);
}

TEST(discovery_client_destroy_once, "U-1 unconnected client destroy releases registrations once")
{
	as_config config;
	as_error err;
	uint32_t destroyed = 0;
	uint32_t t_destroyed = 0;
	as_seed_provider provider;
	as_address_translator translator;

	as_config_init(&config);
	init_provider(&provider, "p", &destroyed);
	init_translator(&translator, "t", &t_destroyed);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		// aerospike_destroy() does nothing in async mode, so close releases the registrations.
		aerospike client;
		aerospike_init(&client, &config);
		aerospike_close(&client, &err);
		assert_int_eq(destroyed, 1);
		assert_int_eq(t_destroyed, 1);
		assert_true(client.config.discovery.provider_released);
		assert_true(client.config.translation.provider_released);

		aerospike_close(&client, &err);
		assert_int_eq(destroyed, 1);
		assert_int_eq(t_destroyed, 1);

		// A released registration blocks connect until it is registered again.
		assert_int_eq(aerospike_connect(&client, &err), AEROSPIKE_ERR_PARAM);
		assert_not_null(strstr(err.message, "Seed provider was released"));

		assert_int_eq(as_config_set_seed_provider(&client.config, &provider, &err), AEROSPIKE_OK);
		assert_int_eq(aerospike_connect(&client, &err), AEROSPIKE_ERR_PARAM);
		assert_not_null(strstr(err.message, "Address translator was released"));

		assert_int_eq(as_config_set_address_translator(&client.config, &translator, &err),
			AEROSPIKE_OK);
		assert_false(client.config.discovery.provider_released);
		assert_false(client.config.translation.provider_released);

		aerospike_close(&client, &err);
		assert_int_eq(destroyed, 2);
		assert_int_eq(t_destroyed, 2);

		// Frees the remaining config fields; both registrations are already released.
		as_config_destroy(&client.config);
		assert_int_eq(destroyed, 2);
		assert_int_eq(t_destroyed, 2);
		return;
	}

	aerospike* client = aerospike_new(&config);
	assert_not_null(client);
	assert_int_eq(destroyed, 0);

	// Close without a cluster is a no-op for registrations in sync mode.
	aerospike_close(client, &err);
	aerospike_close(client, &err);
	assert_int_eq(destroyed, 0);
	assert_false(client->config.discovery.provider_released);
	assert_false(client->config.translation.provider_released);

	aerospike_destroy(client);
	assert_int_eq(destroyed, 1);
	assert_int_eq(t_destroyed, 1);
}

TEST(discovery_registration_same_udata, "U-1 re-registering the same udata destroys nothing")
{
	as_config config;
	as_error err;
	uint32_t destroyed = 0;
	uint32_t t_destroyed = 0;
	as_seed_provider provider;
	as_address_translator translator;

	as_config_init(&config);
	init_provider(&provider, "p", &destroyed);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);

	// Same table again.
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(destroyed, 0);
	assert_string_eq(config.discovery.provider.name, "p");

	// The registered table itself.
	assert_int_eq(as_config_set_seed_provider(&config, &config.discovery.provider, &err),
		AEROSPIKE_OK);
	assert_int_eq(destroyed, 0);
	assert_string_eq(config.discovery.provider.name, "p");

	// Same udata, different name and refresh.
	as_seed_provider renamed;
	init_provider(&renamed, "renamed", &destroyed);
	renamed.refresh = refresh_fail;
	assert_int_eq(as_config_set_seed_provider(&config, &renamed, &err), AEROSPIKE_OK);
	assert_int_eq(destroyed, 0);
	assert_string_eq(config.discovery.provider.name, "renamed");
	assert_true(config.discovery.provider.refresh == renamed.refresh);
	assert_true(config.discovery.provider_registered);

	init_translator(&translator, "t", &t_destroyed);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);
	assert_int_eq(t_destroyed, 0);
	assert_string_eq(config.translation.provider.name, "t");

	assert_int_eq(as_config_set_address_translator(&config, &config.translation.provider, &err),
		AEROSPIKE_OK);
	assert_int_eq(t_destroyed, 0);
	assert_string_eq(config.translation.provider.name, "t");

	as_address_translator t_renamed;
	init_translator(&t_renamed, "t-renamed", &t_destroyed);
	assert_int_eq(as_config_set_address_translator(&config, &t_renamed, &err), AEROSPIKE_OK);
	assert_int_eq(t_destroyed, 0);
	assert_string_eq(config.translation.provider.name, "t-renamed");
	assert_true(config.translation.provider_registered);

	as_config_destroy(&config);
	assert_int_eq(destroyed, 1);
	assert_int_eq(t_destroyed, 1);
}

TEST(discovery_binding_mode, "U-1 binding seed callbacks are thread mode only")
{
	as_config config;
	as_error err;
	uint32_t destroyed = 0;

	as_config_init(&config);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(as_config_set_binding_seed_provider(&config, "binding", refresh,
		false, destroy, &destroyed, &err), AEROSPIKE_ERR_PARAM);
	assert_false(config.discovery.provider_registered);
	assert_int_eq(destroyed, 0);

	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	assert_int_eq(as_config_set_binding_seed_provider(&config, "binding", NULL,
		false, destroy, &destroyed, &err), AEROSPIKE_ERR_PARAM);

	// The periodic flag is stated at registration; the client never calls into the binding for it.
	uint32_t periodic_destroyed = 0;
	assert_int_eq(as_config_set_binding_seed_provider(&config, "periodic", refresh,
		true, destroy, &periodic_destroyed, &err), AEROSPIKE_OK);
	assert_true(config.discovery.provider.needs_periodic_refresh(NULL));

	assert_int_eq(as_config_set_binding_seed_provider(&config, "binding", refresh,
		false, destroy, &destroyed, &err), AEROSPIKE_OK);
	assert_int_eq(periodic_destroyed, 1);
	assert_false(config.discovery.provider.needs_periodic_refresh(NULL));
	assert_true(config.discovery.binding_provider);
	assert_string_eq(config.discovery.provider.name, "binding");
	assert_null(config.discovery.provider.supports_deadline);

	// Switching to tend after registration fails connect before any cluster work.
	as_config_add_host(&config, "127.0.0.1", 3000);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;

	aerospike client;
	aerospike_init(&client, &config);
	assert_int_eq(aerospike_connect(&client, &err), AEROSPIKE_ERR_PARAM);
	assert_null(client.cluster);
	assert_int_eq(destroyed, 0);

	// A regular provider replaces the binding provider and clears the flag.
	uint32_t other = 0;
	as_seed_provider provider;
	init_provider(&provider, "regular", &other);
	assert_int_eq(as_config_set_seed_provider(&client.config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(destroyed, 1);
	assert_false(client.config.discovery.binding_provider);

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		// aerospike_destroy() does nothing in async mode, so close releases the registration.
		aerospike_close(&client, &err);
		assert_int_eq(other, 1);
		as_config_destroy(&client.config);
	}
	else {
		aerospike_destroy(&client);
		assert_int_eq(other, 1);
	}
	assert_int_eq(destroyed, 1);
}

TEST(discovery_live_lifecycle, "U-1 registrations survive close/reconnect and are destroyed once")
{
	if (as->config.tls.enable) {
		info("skipped: lifecycle test does not copy TLS configuration");
		return;
	}

	as_config config;
	as_error err;
	uint32_t destroyed = 0;
	uint32_t t_destroyed = 0;
	as_seed_provider provider;
	as_address_translator translator;

	as_config_init(&config);
	as_config_add_hosts(&config, g_host, (uint16_t)g_port);
	as_config_set_user(&config, as->config.user, as->config.password);
	config.use_services_alternate = as->config.use_services_alternate;
	config.auth_mode = as->config.auth_mode;

	init_provider(&provider, "p", &destroyed);
	init_translator(&translator, "t", &t_destroyed);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);

	aerospike* client = aerospike_new(&config);
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	assert_true(client->config.discovery.registration_locked);

	// Replacement while connected is rejected and does not destroy anything.
	uint32_t other = 0;
	as_seed_provider replacement;
	init_provider(&replacement, "other", &other);
	assert_int_eq(as_config_set_seed_provider(&client->config, &replacement, &err),
		AEROSPIKE_ERR_PARAM);
	as_address_translator t_replacement;
	init_translator(&t_replacement, "other", &other);
	assert_int_eq(as_config_set_address_translator(&client->config, &t_replacement, &err),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(destroyed, 0);
	assert_int_eq(t_destroyed, 0);

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		// Async close destroys the client once pending commands complete. Closing from a
		// non-event-loop thread completes that synchronously.
		aerospike_close(client, &err);
		assert_int_eq(destroyed, 1);
		assert_int_eq(t_destroyed, 1);
		assert_int_eq(other, 0);
		return;
	}

	aerospike_close(client, &err);
	assert_false(client->config.discovery.registration_locked);
	assert_int_eq(destroyed, 0);
	assert_int_eq(t_destroyed, 0);

	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	aerospike_close(client, &err);
	aerospike_close(client, &err);
	assert_int_eq(destroyed, 0);
	assert_int_eq(t_destroyed, 0);

	aerospike_destroy(client);
	assert_int_eq(destroyed, 1);
	assert_int_eq(t_destroyed, 1);
	assert_int_eq(other, 0);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(discovery_contract, "pluggable discovery contract tests")
{
	suite_add(discovery_status_codes);
	suite_add(discovery_topology_neutral_names);
	suite_add(discovery_defaults);
	suite_add(discovery_static_provider);
	suite_add(discovery_candidates_append);
	suite_add(discovery_empty_tls_configured_seed);
	suite_add(discovery_static_map);
	suite_add(discovery_static_map_legacy_parity);
	suite_add(discovery_static_map_latency);
	suite_add(discovery_merge);
	suite_add(discovery_conformance_translate);
	suite_add(discovery_conformance_merge);
	suite_add(discovery_conformance_collision);
	suite_add(discovery_provider_validation);
	suite_add(discovery_translator_validation);
	suite_add(discovery_registration_ownership);
	suite_add(discovery_client_destroy_once);
	suite_add(discovery_registration_same_udata);
	suite_add(discovery_binding_mode);
	suite_add(discovery_live_lifecycle);
}
