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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_config_internal.h>
#include <aerospike/as_conn_pool.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_orderedmap.h>
#include <aerospike/as_pair.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_stringmap.h>
#include <citrusleaf/cf_clock.h>

#include "test.h"
#include "aerospike_test.h"

// Coverage: U-1 (contract shape and error channel only), U-2, U-3, U-5, U-7,
// U-8, U-11, U-12 and the static-map/port-map/pass-through/merge L1 translate,
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
// Static Functions: factories
//---------------------------------

// A registered factory. Each instance gets its own heap udata that points back here.
typedef struct plugin_s {
	const char* name;
	as_status fail;
	bool invalid;
	uint32_t calls;
	uint32_t destroyed;
	uint32_t released;
	uint32_t destroyed_seq;
	uint32_t released_seq;
	as_val* options;
} plugin;

static uint32_t plugin_seq;

static void
plugin_reset(plugin* p, const char* name)
{
	memset(p, 0, sizeof(plugin));
	p->name = name;
}

static void
plugin_done(plugin* p)
{
	as_val_destroy(p->options);
	p->options = NULL;
}

static void
plugin_instance_destroy(void* udata)
{
	plugin* p = *(plugin**)udata;
	p->destroyed++;
	p->destroyed_seq = ++plugin_seq;
	free(udata);
}

static void
plugin_release(void* udata)
{
	plugin* p = udata;
	p->released++;
	p->released_seq = ++plugin_seq;
}

// Record the call and keep the options. Returns the instance udata, or NULL on a failure.
static void*
plugin_call(plugin* p, as_error* err, const as_map* options, as_status* status)
{
	p->calls++;
	as_val_destroy(p->options);
	p->options = as_val_reserve((as_val*)options);

	if (p->fail != AEROSPIKE_OK) {
		*status = as_error_update(err, p->fail, "%s unavailable", p->name);
		return NULL;
	}

	plugin** udata = malloc(sizeof(plugin*));
	*udata = p;
	*status = AEROSPIKE_OK;
	return udata;
}

static as_status
provider_factory(as_error* err, const as_map* options, as_seed_provider* provider, void* udata)
{
	as_status status;
	void* instance = plugin_call(udata, err, options, &status);

	if (instance) {
		init_provider(provider, ((plugin*)udata)->name, NULL);
		provider->destroy = plugin_instance_destroy;
		provider->udata = instance;

		if (((plugin*)udata)->invalid) {
			provider->refresh = NULL;
		}
	}
	return status;
}

static as_status
translator_factory(as_error* err, const as_map* options, as_address_translator* translator,
	void* udata)
{
	as_status status;
	void* instance = plugin_call(udata, err, options, &status);

	if (instance) {
		init_translator(translator, ((plugin*)udata)->name, NULL);
		translator->destroy = plugin_instance_destroy;
		translator->udata = instance;

		if (((plugin*)udata)->invalid) {
			translator->translate = NULL;
		}
	}
	return status;
}

static bool
is_orderedmap(const as_val* val)
{
	as_orderedmap probe;
	as_orderedmap_init(&probe, 1);
	bool rv = as_val_type(val) == AS_MAP && ((const as_map*)val)->hooks == probe._.hooks;
	as_orderedmap_destroy(&probe);
	return rv;
}

// as_val_tostring() of val compared with expected.
static bool
val_str_eq(const as_val* val, const char* expected)
{
	char* s = as_val_tostring(val);
	bool rv = s && strcmp(s, expected) == 0;

	if (!rv) {
		info("got %s, expected %s", s ? s : "(null)", expected);
	}
	free(s);
	return rv;
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

	// replace: init merges, an empty refresh publishes empty, a failure keeps it.
	assert_int_eq(as_discovery_conformance_merge(&err, AS_SEED_REPLACE, statics, results, 4,
		snapshots), AEROSPIKE_OK);
	assert_string_eq(hosts_str(snapshots[0].hosts, buf, sizeof(buf)), "s1:3000,d1:3000");
	assert_string_eq(hosts_str(snapshots[1].hosts, buf, sizeof(buf)), "");
	assert_string_eq(hosts_str(snapshots[2].hosts, buf, sizeof(buf)), "");
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

// The cluster's collision message for peer, whose shared hosts and their owners are list.
static const char*
collision_text(char* buf, size_t size, const char* translator, const char* peer,
	const char* list)
{
	snprintf(buf, size, "Peer %s has no address of its own: %s (address translator %s). "
		"Give each node its own reachable address or mapping", peer, list, translator);
	return buf;
}

static bool
collision_nodes_are(const as_conformance_collisions* out, const char* const* names,
	uint32_t size)
{
	if (out->nodes_size != size) {
		return false;
	}

	for (uint32_t i = 0; i < size; i++) {
		if (strcmp(out->nodes[i], names[i]) != 0) {
			return false;
		}
	}
	return true;
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
	// One message per colliding peer, naming the other peer as the owner, as the cluster logs.
	assert_string_eq(out.errors[0],
		collision_text(text, sizeof(text), "static-map", "A", "203.0.113.1:3000 is used by B"));
	assert_string_eq(out.errors[1],
		collision_text(text, sizeof(text), "static-map", "B", "203.0.113.1:3000 is used by A"));
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
		collision_text(text, sizeof(text), "port-map", "A", "lb.example.com:30001 is used by B"));
	assert_string_eq(out.errors[1],
		collision_text(text, sizeof(text), "port-map", "B", "lb.example.com:30001 is used by A"));
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

	// Consecutive endpoints of one name are one peer; a name after another peer is a repeat.
	as_endpoint repeated[] = {
		{.node_name = "A", .host = "10.0.0.1", .port = 3000},
		{.node_name = "B", .host = "10.0.0.2", .port = 3000},
		{.node_name = "A", .host = "10.0.0.3", .port = 3000}
	};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, repeated, 3, &out),
		AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "repeat node name A"));
	assert_null(out.nodes);

	as_endpoint two_tls[] = {
		{.node_name = "A", .tls_name = "a1", .host = "10.0.0.1", .port = 3000},
		{.node_name = "A", .tls_name = "a2", .host = "10.0.0.2", .port = 3000}
	};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, two_tls, 2, &out),
		AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "peer A has more than one TLS name"));
	two_tls[1].tls_name = NULL;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, two_tls, 2, &out),
		AEROSPIKE_ERR_PARAM);

	// A host repeated within a peer, also when only the case differs.
	as_endpoint repeated_host[] = {
		{.node_name = "A", .host = "node-a.example", .port = 3000},
		{.node_name = "A", .host = "Node-A.example", .port = 3000}
	};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, repeated_host, 2, &out),
		AEROSPIKE_ERR_PARAM);
	assert_not_null(strstr(err.message, "peer A repeats host Node-A.example:3000"));
	repeated_host[1].port = 3001;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, repeated_host, 2, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 1);
	assert_int_eq(out.errors_size, 0);
	as_conformance_collisions_destroy(&out);

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

TEST(discovery_conformance_collision_hosts, "FR-46 collision peers with several hosts skip shared hosts and keep their own")
{
	char text[512];
	as_error err;
	as_conformance_collisions out;
	as_conformance_translation translation = {
		.kind = AS_TRANSLATOR_PASS_THROUGH,
		.on_miss = AS_TRANSLATION_PASS_THROUGH
	};

	// Partial overlap: each peer keeps its other host, for every translator.
	as_endpoint partial[] = {
		{.node_name = "A", .host = "h", .port = 31001},
		{.node_name = "A", .host = "h", .port = 31002},
		{.node_name = "B", .host = "h", .port = 31001},
		{.node_name = "B", .host = "h", .port = 31003}
	};
	const char* both[] = {"A", "B"};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, partial, 4, &out),
		AEROSPIKE_OK);
	assert_true(collision_nodes_are(&out, both, 2));
	assert_int_eq(out.errors_size, 0);
	as_conformance_collisions_destroy(&out);

	as_addr_map port_entries[] = {
		{"10.0.0.1:3000", "lb.example.com:30001"}, {"10.0.0.2:3000", "lb.example.com:30001"},
		{"10.0.0.3:3000", "lb.example.com:30003"}, {"10.0.0.4:3000", "lb.example.com:30004"}
	};
	as_endpoint mapped[] = {
		{.node_name = "A", .host = "10.0.0.1", .port = 3000},
		{.node_name = "A", .host = "10.0.0.3", .port = 3000},
		{.node_name = "B", .host = "10.0.0.2", .port = 3000},
		{.node_name = "B", .host = "10.0.0.4", .port = 3000}
	};
	translation.kind = AS_TRANSLATOR_PORT_MAP;
	translation.map = port_entries;
	translation.map_size = 4;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, mapped, 4, &out),
		AEROSPIKE_OK);
	assert_true(collision_nodes_are(&out, both, 2));
	assert_int_eq(out.errors_size, 0);
	as_conformance_collisions_destroy(&out);

	// One side overlap through static-map: only the peer left without a host collides.
	as_addr_map ip_entries[] = {
		{"10.0.0.1", "203.0.113.1"}, {"10.0.0.2", "203.0.113.1"}, {"10.0.0.3", "203.0.113.3"}
	};
	as_endpoint one_side[] = {
		{.node_name = "A", .host = "10.0.0.1", .port = 3000},
		{.node_name = "A", .host = "10.0.0.3", .port = 3000},
		{.node_name = "B", .host = "10.0.0.2", .port = 3000}
	};
	const char* only_a[] = {"A"};
	translation.kind = AS_TRANSLATOR_STATIC_MAP;
	translation.map = ip_entries;
	translation.map_size = 3;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, one_side, 3, &out),
		AEROSPIKE_OK);
	assert_true(collision_nodes_are(&out, only_a, 1));
	assert_int_eq(out.errors_size, 1);
	assert_string_eq(out.errors[0],
		collision_text(text, sizeof(text), "static-map", "B", "203.0.113.1:3000 is used by A"));
	as_conformance_collisions_destroy(&out);

	// Shared hosts are all found before any is removed: B shares :31001 with A and :31002 with
	// C, so B and C have nothing left while A keeps :31003.
	as_endpoint before_removal[] = {
		{.node_name = "A", .host = "h", .port = 31001},
		{.node_name = "A", .host = "h", .port = 31003},
		{.node_name = "B", .host = "h", .port = 31001},
		{.node_name = "B", .host = "h", .port = 31002},
		{.node_name = "C", .host = "h", .port = 31002}
	};
	translation.kind = AS_TRANSLATOR_PASS_THROUGH;
	translation.map = NULL;
	translation.map_size = 0;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, before_removal, 5, &out),
		AEROSPIKE_OK);
	assert_true(collision_nodes_are(&out, only_a, 1));
	assert_int_eq(out.errors_size, 2);
	assert_string_eq(out.errors[0], collision_text(text, sizeof(text), "pass-through", "B",
		"h:31001 is used by A, h:31002 is used by C"));
	assert_string_eq(out.errors[1],
		collision_text(text, sizeof(text), "pass-through", "C", "h:31002 is used by B"));
	as_conformance_collisions_destroy(&out);

	// A chain shares every host, and B's message names both of its owners.
	as_endpoint chain[] = {
		{.node_name = "A", .host = "h", .port = 31001},
		{.node_name = "B", .host = "h", .port = 31001},
		{.node_name = "B", .host = "h", .port = 31002},
		{.node_name = "C", .host = "h", .port = 31002}
	};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, chain, 4, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 0);
	assert_int_eq(out.errors_size, 3);
	assert_string_eq(out.errors[1], collision_text(text, sizeof(text), "pass-through", "B",
		"h:31001 is used by A, h:31002 is used by C"));
	as_conformance_collisions_destroy(&out);

	// Every host shared: one message per peer, listing both hosts.
	as_endpoint every[] = {
		{.node_name = "A", .host = "h", .port = 31001},
		{.node_name = "A", .host = "h", .port = 31002},
		{.node_name = "B", .host = "h", .port = 31001},
		{.node_name = "B", .host = "h", .port = 31002}
	};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, every, 4, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 0);
	assert_int_eq(out.errors_size, 2);
	assert_string_eq(out.errors[0], collision_text(text, sizeof(text), "pass-through", "A",
		"h:31001 is used by B, h:31002 is used by B"));
	assert_string_eq(out.errors[1], collision_text(text, sizeof(text), "pass-through", "B",
		"h:31001 is used by A, h:31002 is used by A"));
	as_conformance_collisions_destroy(&out);

	// Hostnames compare case-insensitively, and a peer is reported on its own spelling.
	as_endpoint cased[] = {
		{.node_name = "A", .host = "AS.Priv.Example.com", .port = 31001},
		{.node_name = "B", .host = "as.priv.example.com", .port = 31001},
		{.node_name = "B", .host = "as.priv.example.com", .port = 31002}
	};
	const char* only_b[] = {"B"};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, cased, 3, &out),
		AEROSPIKE_OK);
	assert_true(collision_nodes_are(&out, only_b, 1));
	assert_int_eq(out.errors_size, 1);
	assert_string_eq(out.errors[0], collision_text(text, sizeof(text), "pass-through", "A",
		"AS.Priv.Example.com:31001 is used by B"));
	as_conformance_collisions_destroy(&out);

	// Five shared hosts: the first three are listed, then how many more.
	as_endpoint many[] = {
		{.node_name = "A", .host = "h", .port = 1},
		{.node_name = "A", .host = "h", .port = 2},
		{.node_name = "A", .host = "h", .port = 3},
		{.node_name = "A", .host = "h", .port = 4},
		{.node_name = "A", .host = "h", .port = 5},
		{.node_name = "B", .host = "h", .port = 1},
		{.node_name = "C", .host = "h", .port = 2},
		{.node_name = "D", .host = "h", .port = 3},
		{.node_name = "E", .host = "h", .port = 4},
		{.node_name = "F", .host = "h", .port = 5}
	};
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, many, 10, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 0);
	assert_int_eq(out.errors_size, 6);
	assert_string_eq(out.errors[0], collision_text(text, sizeof(text), "pass-through", "A",
		"h:1 is used by B, h:2 is used by C, h:3 is used by D and 2 more"));
	as_conformance_collisions_destroy(&out);

	// A strict miss drops only that host: A keeps 203.0.113.3. M misses on every host, so it is
	// neither a node nor a collision.
	as_endpoint miss[] = {
		{.node_name = "A", .host = "10.0.0.9", .port = 3000},
		{.node_name = "A", .host = "10.0.0.3", .port = 3000},
		{.node_name = "M", .host = "10.0.0.8", .port = 3000},
		{.node_name = "M", .host = "10.0.0.9", .port = 3000},
		{.node_name = "B", .host = "10.0.0.1", .port = 3000},
		{.node_name = "C", .host = "10.0.0.2", .port = 3000}
	};
	translation.kind = AS_TRANSLATOR_STATIC_MAP;
	translation.on_miss = AS_TRANSLATION_STRICT;
	translation.map = ip_entries;
	translation.map_size = 3;
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, miss, 6, &out),
		AEROSPIKE_OK);
	assert_true(collision_nodes_are(&out, only_a, 1));
	assert_int_eq(out.errors_size, 2);
	assert_int_eq(collision_errors_naming(&out, "Peer M "), 0);
	assert_int_eq(collision_errors_naming(&out, "used by M"), 0);
	as_conformance_collisions_destroy(&out);

	// The strict miss leaves A only the shared host, so A collides too.
	miss[1].host = "10.0.0.1";
	assert_int_eq(as_discovery_conformance_collision(&err, &translation, miss, 6, &out),
		AEROSPIKE_OK);
	assert_int_eq(out.nodes_size, 0);
	assert_int_eq(out.errors_size, 3);
	assert_string_eq(out.errors[0],
		collision_text(text, sizeof(text), "static-map", "A", "203.0.113.1:3000 is used by B"));
	as_conformance_collisions_destroy(&out);
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

	plugin p;
	plugin t;
	plugin_reset(&p, "consul");
	plugin_reset(&t, "nat-table");
	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_register_address_translator(&config, "nat-table", translator_factory,
		plugin_release, &t, &err), AEROSPIKE_OK);

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		// aerospike_destroy() does nothing in async mode, so close releases the registrations.
		aerospike client;
		aerospike_init(&client, &config);
		aerospike_close(&client, &err);
		assert_int_eq(destroyed, 1);
		assert_int_eq(t_destroyed, 1);
		assert_int_eq(p.released, 1);
		assert_int_eq(t.released, 1);
		assert_true(client.config.discovery.provider_released);
		assert_true(client.config.translation.provider_released);

		aerospike_close(&client, &err);
		assert_int_eq(destroyed, 1);
		assert_int_eq(t_destroyed, 1);
		assert_int_eq(p.released, 1);
		assert_int_eq(t.released, 1);

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
		assert_int_eq(p.released, 1);
		assert_int_eq(t.released, 1);
		return;
	}

	aerospike* client = aerospike_new(&config);
	assert_not_null(client);
	assert_int_eq(destroyed, 0);

	// Close without a cluster is a no-op for registrations in sync mode.
	aerospike_close(client, &err);
	aerospike_close(client, &err);
	assert_int_eq(destroyed, 0);
	assert_int_eq(p.released, 0);
	assert_false(client->config.discovery.provider_released);
	assert_false(client->config.translation.provider_released);

	aerospike_destroy(client);
	assert_int_eq(destroyed, 1);
	assert_int_eq(t_destroyed, 1);
	assert_int_eq(p.released, 1);
	assert_int_eq(t.released, 1);
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

#define assert_message(__err, __needle) \
	if (!strstr((__err).message, __needle)) { \
		fail("message '%s' lacks '%s'", (__err).message, __needle); \
	}

#define fail(fmt, ...) \
	return atf_assert_log(__result__, "", __FILE__, __LINE__, fmt, ##__VA_ARGS__)

// State of a binding factory registration: counts the factory calls, the instance destroys
// and the registration (udata) destroys.
typedef struct binding_ctx_s {
	uint32_t factory_calls;
	uint32_t instance_destroyed;
	uint32_t udata_destroyed;
	bool periodic;
	bool invalid;
	as_status fail;
	uint32_t options_size;
} binding_ctx;

static bool
binding_periodic(void* udata)
{
	(void)udata;
	return true;
}

static as_status
binding_factory(as_error* err, const as_map* options, as_seed_provider* provider, void* udata)
{
	binding_ctx* ctx = udata;
	ctx->factory_calls++;

	if (ctx->fail != AEROSPIKE_OK) {
		return as_error_set_message(err, ctx->fail, "binding failure");
	}

	ctx->options_size = as_map_size(options);
	init_provider(provider, ctx->invalid ? NULL : "binding", &ctx->instance_destroyed);

	if (ctx->periodic) {
		provider->needs_periodic_refresh = binding_periodic;
	}
	return AEROSPIKE_OK;
}

static void
binding_udata_destroy(void* udata)
{
	((binding_ctx*)udata)->udata_destroyed++;
}

static as_status
binding_register(as_config* config, binding_ctx* ctx, as_error* err)
{
	return as_config_set_binding_seed_provider(config, binding_factory, binding_udata_destroy,
		ctx, err);
}

static void
live_config(as_config* config)
{
	as_config_init(config);
	as_config_add_hosts(config, g_host, (uint16_t)g_port);
	as_config_set_user(config, as->config.user, as->config.password);
	config->use_services_alternate = as->config.use_services_alternate;
	config->auth_mode = as->config.auth_mode;
}

TEST(discovery_binding_mode, "U-1 binding seed callbacks are thread mode only")
{
	as_config config;
	as_error err;
	binding_ctx ctx = {0};

	as_config_init(&config);
	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(binding_register(&config, &ctx, &err), AEROSPIKE_ERR_PARAM);
	assert_false(config.discovery.provider_registered);
	assert_int_eq(ctx.factory_calls, 0);
	assert_int_eq(ctx.udata_destroyed, 0);

	config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_THREAD;
	assert_int_eq(as_config_set_binding_seed_provider(&config, NULL, binding_udata_destroy, &ctx,
		&err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(ctx.udata_destroyed, 0);

	// The periodic flag is set by the factory; the client never calls into the binding for it.
	binding_ctx periodic = {.periodic = true};
	assert_int_eq(binding_register(&config, &periodic, &err), AEROSPIKE_OK);
	assert_int_eq(periodic.factory_calls, 1);
	assert_int_eq(periodic.options_size, 0);
	assert_true(config.discovery.provider.needs_periodic_refresh(NULL));

	assert_int_eq(binding_register(&config, &ctx, &err), AEROSPIKE_OK);
	assert_int_eq(periodic.instance_destroyed, 1);
	assert_int_eq(periodic.udata_destroyed, 1);
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
	assert_int_eq(ctx.instance_destroyed, 0);

	// A regular provider replaces the binding provider, ends its registration and clears the flag.
	uint32_t other = 0;
	as_seed_provider provider;
	init_provider(&provider, "regular", &other);
	assert_int_eq(as_config_set_seed_provider(&client.config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(ctx.instance_destroyed, 1);
	assert_int_eq(ctx.udata_destroyed, 1);
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
	assert_int_eq(ctx.instance_destroyed, 1);
	assert_int_eq(ctx.udata_destroyed, 1);
}

TEST(discovery_binding_factory_errors, "U-1 a binding factory failure or invalid table fails registration and leaves udata to the caller")
{
	as_config config;
	as_error err;
	binding_ctx first = {0};

	as_config_init(&config);
	assert_int_eq(binding_register(&config, &first, &err), AEROSPIKE_OK);

	binding_ctx failing = {.fail = AEROSPIKE_ERR_CLIENT};
	assert_int_eq(binding_register(&config, &failing, &err), AEROSPIKE_ERR_CLIENT);
	assert_message(err, "factory failed: binding failure");
	assert_int_eq(failing.factory_calls, 1);
	assert_int_eq(failing.udata_destroyed, 0);

	// The previous registration is unchanged.
	assert_int_eq(first.instance_destroyed, 0);
	assert_int_eq(first.udata_destroyed, 0);
	assert_true(config.discovery.provider_registered);
	assert_true(config.discovery.binding_provider);

	// An invalid table destroys that table's udata, not the registration udata.
	binding_ctx invalid = {.invalid = true};
	assert_int_eq(binding_register(&config, &invalid, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "factory returned an invalid provider");
	assert_int_eq(invalid.instance_destroyed, 1);
	assert_int_eq(invalid.udata_destroyed, 0);
	assert_int_eq(first.instance_destroyed, 0);
	assert_int_eq(first.udata_destroyed, 0);

	// Replacing the registration ends it once.
	binding_ctx second = {0};
	assert_int_eq(binding_register(&config, &second, &err), AEROSPIKE_OK);
	assert_int_eq(first.instance_destroyed, 1);
	assert_int_eq(first.udata_destroyed, 1);

	as_config_destroy(&config);
	assert_int_eq(second.instance_destroyed, 1);
	assert_int_eq(second.udata_destroyed, 1);
	assert_int_eq(first.udata_destroyed, 1);
}

TEST(discovery_binding_unconnected_release, "U-1 closing or destroying an unconnected client with a binding provider ends the registration once")
{
	as_config config;
	as_error err;
	binding_ctx ctx = {0};

	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);
	assert_int_eq(binding_register(&config, &ctx, &err), AEROSPIKE_OK);

	aerospike client;
	aerospike_init(&client, &config);

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		aerospike_close(&client, &err);
		as_config_destroy(&client.config);
	}
	else {
		aerospike_destroy(&client);
	}
	assert_int_eq(ctx.factory_calls, 1);
	assert_int_eq(ctx.instance_destroyed, 1);
	assert_int_eq(ctx.udata_destroyed, 1);
}

TEST(discovery_binding_reconnect, "U-1 sync close destroys the binding instance, and connect builds it again from the factory")
{
	if (as->config.tls.enable) {
		info("skipped: lifecycle test does not copy TLS configuration");
		return;
	}

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		info("skipped: async close does not keep the registration");
		return;
	}

	as_config config;
	as_error err;
	binding_ctx ctx = {0};

	live_config(&config);
	assert_int_eq(binding_register(&config, &ctx, &err), AEROSPIKE_OK);
	assert_int_eq(ctx.factory_calls, 1);

	aerospike* client = aerospike_new(&config);
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	assert_true(client->config.discovery.binding_provider);
	assert_int_eq(ctx.factory_calls, 1);

	aerospike_close(client, &err);
	assert_int_eq(ctx.instance_destroyed, 1);
	assert_int_eq(ctx.udata_destroyed, 0);
	assert_false(client->config.discovery.provider_released);
	assert_false(client->config.discovery.binding_provider);

	// A factory failure fails that connect, and the next connect calls the factory again.
	ctx.fail = AEROSPIKE_ERR_CLIENT;
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_ERR_CLIENT);
	assert_message(err, "Failed to rebuild the seed provider: factory failed: binding failure");
	assert_int_eq(ctx.factory_calls, 2);
	assert_null(client->cluster);

	ctx.fail = AEROSPIKE_OK;
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	assert_int_eq(ctx.factory_calls, 3);
	assert_true(client->config.discovery.binding_provider);
	assert_int_eq(ctx.udata_destroyed, 0);

	aerospike_close(client, &err);
	assert_int_eq(ctx.instance_destroyed, 2);
	assert_int_eq(ctx.udata_destroyed, 0);

	// Tend mode is still rejected for the rebuilt binding provider.
	client->config.discovery.execution_mode = AS_DISCOVERY_EXECUTION_TEND;
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "Binding seed provider requires thread execution mode");
	assert_null(client->cluster);
	assert_int_eq(ctx.factory_calls, 4);
	assert_int_eq(ctx.instance_destroyed, 2);

	aerospike_destroy(client);
	assert_int_eq(ctx.instance_destroyed, 3);
	assert_int_eq(ctx.udata_destroyed, 1);
}

TEST(discovery_live_lifecycle, "U-1 sync close destroys direct instances once; reconnect needs them registered again")
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
	assert_int_eq(destroyed, 1);
	assert_int_eq(t_destroyed, 1);
	assert_true(client->config.discovery.provider_released);
	assert_true(client->config.translation.provider_released);

	// Direct instances have no recipe, so each must be registered again.
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message,
		"Seed provider was released by aerospike_close(). Register it again");
	assert_int_eq(as_config_set_seed_provider(&client->config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_ERR_PARAM);
	assert_string_eq(err.message,
		"Address translator was released by aerospike_close(). Register it again");
	assert_int_eq(as_config_set_address_translator(&client->config, &translator, &err),
		AEROSPIKE_OK);
	assert_null(client->cluster);

	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	aerospike_close(client, &err);
	aerospike_close(client, &err);
	assert_int_eq(destroyed, 2);
	assert_int_eq(t_destroyed, 2);

	aerospike_destroy(client);
	assert_int_eq(destroyed, 2);
	assert_int_eq(t_destroyed, 2);
	assert_int_eq(other, 0);
}

// Connect to the test cluster with translation.on_miss set, then close. connected is true when
// connect created a cluster.
static as_status
connect_on_miss(as_translation_miss_policy on_miss, as_error* err, bool* connected)
{
	as_config config;
	as_config_init(&config);
	as_config_add_hosts(&config, g_host, (uint16_t)g_port);
	as_config_set_user(&config, as->config.user, as->config.password);
	config.use_services_alternate = as->config.use_services_alternate;
	config.auth_mode = as->config.auth_mode;
	config.translation.on_miss = on_miss;

	aerospike* client = aerospike_new(&config);
	as_status status = aerospike_connect(client, err);
	*connected = client->cluster != NULL;

	if (client->cluster) {
		as_error close_err;
		aerospike_close(client, &close_err);

		if (as_event_loop_size == 0 || as_event_single_thread) {
			aerospike_destroy(client);
		}
	}
	else {
		// aerospike_destroy() does nothing in async mode, and aerospike_destroy_internal() is not
		// exported from the Windows DLL.
		as_config_destroy(&client->config);
		cf_free(client);
	}
	return status;
}

TEST(discovery_on_miss_range, "U-2 FR-44 an on_miss other than pass-through or strict fails connect with AEROSPIKE_ERR_PARAM")
{
	if (as->config.tls.enable) {
		info("skipped: on_miss test does not copy TLS configuration");
		return;
	}

	as_error err;
	bool connected;
	const int invalid[] = {2, -1};

	for (int i = 0; i < 2; i++) {
		char expected[64];
		snprintf(expected, sizeof(expected), "Invalid translation on_miss: %d", invalid[i]);
		as_status status = connect_on_miss((as_translation_miss_policy)invalid[i], &err,
			&connected);
		assert_int_eq(status, AEROSPIKE_ERR_PARAM);
		assert_not_null(strstr(err.message, expected));
		assert_false(connected);
	}

	assert_int_eq(connect_on_miss(AS_TRANSLATION_PASS_THROUGH, &err, &connected), AEROSPIKE_OK);
	assert_true(connected);
	assert_int_eq(connect_on_miss(AS_TRANSLATION_STRICT, &err, &connected), AEROSPIKE_OK);
	assert_true(connected);
}

//---------------------------------
// Tests: named factories
//---------------------------------

TEST(discovery_factory_registration, "registration rejects built-in, duplicate and invalid names, and a failed registration keeps udata")
{
	as_config config;
	as_error err;
	plugin p;
	plugin_reset(&p, "consul");
	as_config_init(&config);

	static const char* const providers[] = {"static", "dns", "aws-tags"};
	static const char* const translators[] = {"static-map", "port-map", "pass-through"};
	char needle[64];

	for (uint32_t i = 0; i < 3; i++) {
		assert_int_eq(as_config_register_seed_provider(&config, providers[i], provider_factory,
			plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
		snprintf(needle, sizeof(needle), "'%s': built-in name", providers[i]);
		assert_message(err, needle);

		assert_int_eq(as_config_register_address_translator(&config, translators[i],
			translator_factory, plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
		snprintf(needle, sizeof(needle), "'%s': built-in name", translators[i]);
		assert_message(err, needle);
	}

	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_register_address_translator(&config, "nat-table", translator_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);

	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid seed provider name 'consul': already registered");
	assert_int_eq(as_config_register_address_translator(&config, "nat-table", translator_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid address translator name 'nat-table': already registered");

	// Providers and translators have separate names.
	assert_int_eq(as_config_register_address_translator(&config, "consul", translator_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);

	assert_int_eq(as_config_register_seed_provider(&config, NULL, provider_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_register_seed_provider(&config, "", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_register_seed_provider(&config, "etcd", NULL, plugin_release, &p,
		&err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid seed provider factory for 'etcd': null");
	assert_int_eq(as_config_register_seed_provider(NULL, "etcd", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_register_address_translator(&config, NULL, translator_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_register_address_translator(&config, "etcd", NULL, plugin_release,
		&p, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_register_address_translator(NULL, "etcd", translator_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);

	// A NULL destroy is allowed.
	assert_int_eq(as_config_register_seed_provider(&config, "etcd", provider_factory, NULL, &p,
		&err), AEROSPIKE_OK);

	// Registering builds nothing, and a failed registration leaves udata with the caller.
	assert_int_eq(p.calls, 0);
	assert_int_eq(p.released, 0);

	as_config_destroy(&config);
	assert_int_eq(p.released, 3);
	assert_int_eq(p.calls, 0);
}

TEST(discovery_factory_select, "select builds from a registered factory with a client-owned ordered copy of the options")
{
	as_config config;
	as_error err;
	plugin p;
	plugin t;
	plugin_reset(&p, "consul");
	plugin_reset(&t, "nat-table");
	as_config_init(&config);

	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_register_address_translator(&config, "nat-table", translator_factory,
		plugin_release, &t, &err), AEROSPIKE_OK);

	// NULL options: an empty map.
	assert_int_eq(as_config_select_seed_provider(&config, "consul", NULL, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 1);
	assert_true(config.discovery.provider_registered);
	assert_string_eq(config.discovery.provider.name, "consul");
	assert_true(is_orderedmap(p.options));
	assert_true(val_str_eq(p.options, "{}"));

	// A stack hashmap destroyed right after the call arrives as an ordered copy, a pair as a list.
	as_hashmap map;
	as_hashmap_init(&map, 8);
	as_stringmap_set_str((as_map*)&map, "service", "aerospike");
	as_stringmap_set_int64((as_map*)&map, "port", 3000);
	as_arraylist* endpoints = as_arraylist_new(2, 0);
	as_arraylist_append_str(endpoints, "127.0.0.1:1");
	as_stringmap_set_list((as_map*)&map, "endpoints", (as_list*)endpoints);
	as_stringmap_set((as_map*)&map, "pair", (as_val*)as_pair_new((as_val*)as_integer_new(1),
		(as_val*)as_string_new_strdup("x")));
	as_stringmap_set_double((as_map*)&map, "ratio", 0.5);
	as_stringmap_set((as_map*)&map, "none", (as_val*)&as_nil);

	assert_int_eq(as_config_select_seed_provider(&config, "consul", (as_map*)&map, &err),
		AEROSPIKE_OK);
	as_hashmap_destroy(&map);
	assert_int_eq(p.calls, 2);
	assert_int_eq(p.destroyed, 1);
	assert_true(is_orderedmap(p.options));
	assert_true(val_str_eq(p.options, "{\"endpoints\":[\"127.0.0.1:1\"], \"none\":NIL, "
		"\"pair\":[1, \"x\"], \"port\":3000, \"ratio\":0.5, \"service\":\"aerospike\"}"));

	// A value that cannot be copied fails the select and keeps the current instance.
	as_orderedmap bad;
	as_orderedmap_init(&bad, 1);
	as_stringmap_set((as_map*)&bad, "rec", (as_val*)as_record_new(1));
	assert_int_eq(as_config_select_seed_provider(&config, "consul", (as_map*)&bad, &err),
		AEROSPIKE_ERR_PARAM);
	as_orderedmap_destroy(&bad);
	assert_message(err, "Invalid seed provider 'consul': Invalid options: cannot copy");
	assert_int_eq(p.calls, 2);
	assert_int_eq(p.destroyed, 1);
	assert_true(config.discovery.provider_registered);

	// A factory failure keeps the current instance.
	p.fail = AEROSPIKE_ERR_CLIENT;
	assert_int_eq(as_config_select_seed_provider(&config, "consul", NULL, &err),
		AEROSPIKE_ERR_CLIENT);
	assert_message(err, "Invalid seed provider 'consul': factory failed: consul unavailable");
	assert_int_eq(p.calls, 3);
	assert_int_eq(p.destroyed, 1);
	assert_true(config.discovery.provider_registered);
	p.fail = AEROSPIKE_OK;

	// An invalid result is destroyed once and keeps the current instance.
	p.invalid = true;
	assert_int_eq(as_config_select_seed_provider(&config, "consul", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid seed provider 'consul': factory returned an invalid provider");
	assert_int_eq(p.calls, 4);
	assert_int_eq(p.destroyed, 2);
	assert_true(config.discovery.provider_registered);
	p.invalid = false;

	assert_int_eq(as_config_select_address_translator(&config, "nat-table", NULL, &err),
		AEROSPIKE_OK);
	assert_int_eq(t.calls, 1);
	assert_string_eq(config.translation.provider.name, "nat-table");

	t.invalid = true;
	assert_int_eq(as_config_select_address_translator(&config, "nat-table", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err,
		"Invalid address translator 'nat-table': factory returned an invalid translator");
	assert_int_eq(t.destroyed, 1);
	assert_true(config.translation.provider_registered);

	// A code setter replaces a selected instance.
	uint32_t code = 0;
	as_address_translator translator;
	init_translator(&translator, "code", &code);
	assert_int_eq(as_config_set_address_translator(&config, &translator, &err), AEROSPIKE_OK);
	assert_int_eq(t.destroyed, 2);

	// Instances are destroyed before the factories are released.
	as_config_destroy(&config);
	assert_int_eq(p.destroyed, 3);
	assert_int_eq(p.released, 1);
	assert_true(p.released_seq > p.destroyed_seq);
	assert_int_eq(t.destroyed, 2);
	assert_int_eq(t.released, 1);
	assert_int_eq(code, 1);
	plugin_done(&p);
	plugin_done(&t);
}

TEST(discovery_factory_select_rejections, "select rejects a NULL config, a missing, built-in or unregistered name, and a connected client")
{
	as_config config;
	as_error err;
	plugin p;
	plugin_reset(&p, "consul");
	as_config_init(&config);

	assert_int_eq(as_config_select_seed_provider(&config, "consul", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid seed provider name 'consul': not registered");

	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_register_address_translator(&config, "nat-table", translator_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);

	assert_int_eq(as_config_select_seed_provider(NULL, "consul", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_select_seed_provider(&config, NULL, NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_select_seed_provider(&config, "", NULL, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_select_seed_provider(&config, "eureka", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid seed provider name 'eureka': not registered");
	assert_int_eq(as_config_select_seed_provider(&config, "static", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid seed provider name 'static': built-in name, use its own setter");

	assert_int_eq(as_config_select_address_translator(NULL, "nat-table", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_select_address_translator(&config, NULL, NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_select_address_translator(&config, "consul", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid address translator name 'consul': not registered");
	assert_int_eq(as_config_select_address_translator(&config, "pass-through", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err,
		"Invalid address translator name 'pass-through': built-in name, use its own setter");

	// As while connected.
	config.discovery.registration_locked = true;
	assert_int_eq(as_config_select_seed_provider(&config, "consul", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err, "Seed provider cannot be replaced while the client is connected");
	assert_int_eq(as_config_select_address_translator(&config, "nat-table", NULL, &err),
		AEROSPIKE_ERR_PARAM);
	assert_message(err, "Address translator cannot be replaced while the client is connected");
	assert_int_eq(as_config_register_seed_provider(&config, "etcd", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "Invalid seed provider name 'etcd': cannot register while the client is "
		"connected");
	config.discovery.registration_locked = false;

	assert_int_eq(p.calls, 0);
	assert_false(config.discovery.provider_registered);
	assert_false(config.translation.provider_registered);
	as_config_destroy(&config);
	assert_int_eq(p.released, 2);
}

TEST(discovery_factory_recipe, "a selected factory is rebuilt from its index and its own options after a close, also after more registrations")
{
	as_config config;
	as_error err;
	plugin p;
	plugin t;
	plugin extra;
	plugin_reset(&p, "consul");
	plugin_reset(&t, "nat-table");
	plugin_reset(&extra, "extra");
	as_config_init(&config);

	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_register_address_translator(&config, "nat-table", translator_factory,
		plugin_release, &t, &err), AEROSPIKE_OK);

	// The caller's map is gone before the rebuild.
	as_hashmap map;
	as_hashmap_init(&map, 4);
	as_stringmap_set_str((as_map*)&map, "service", "aerospike");
	as_stringmap_set_int64((as_map*)&map, "port", 3000);
	assert_int_eq(as_config_select_seed_provider(&config, "consul", (as_map*)&map, &err),
		AEROSPIKE_OK);
	as_hashmap_destroy(&map);
	assert_int_eq(as_config_select_address_translator(&config, "nat-table", NULL, &err),
		AEROSPIKE_OK);

	// The recipe and the factory each hold the copied options.
	assert_int_eq(p.options->count, 2);

	// Ten more registrations grow the registry; the recipe keeps its index.
	char names[10][16];

	for (uint32_t i = 0; i < 10; i++) {
		snprintf(names[i], sizeof(names[i]), "extra%u", i);
		assert_int_eq(as_config_register_seed_provider(&config, names[i], provider_factory,
			plugin_release, &extra, &err), AEROSPIKE_OK);
	}

	// What the sync close does.
	as_config_release_plugins(&config, true);
	assert_int_eq(p.destroyed, 1);
	assert_int_eq(t.destroyed, 1);
	assert_false(config.discovery.provider_registered);
	assert_false(config.translation.provider_registered);
	assert_false(config.discovery.provider_released);
	assert_false(config.translation.provider_released);
	assert_not_null(config.discovery.recipe);
	assert_int_eq(p.released, 0);

	assert_int_eq(as_config_rebuild_plugins(&config, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 2);
	assert_int_eq(t.calls, 2);
	assert_int_eq(extra.calls, 0);
	assert_string_eq(config.discovery.provider.name, "consul");
	assert_string_eq(config.translation.provider.name, "nat-table");
	assert_true(is_orderedmap(p.options));
	assert_true(val_str_eq(p.options, "{\"port\":3000, \"service\":\"aerospike\"}"));

	// A rebuild with an instance in place builds nothing.
	assert_int_eq(as_config_rebuild_plugins(&config, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 2);

	// A failed rebuild keeps the recipe, so a later connect tries again.
	as_config_release_plugins(&config, true);
	p.fail = AEROSPIKE_ERR_CLIENT;
	assert_int_eq(as_config_rebuild_plugins(&config, &err), AEROSPIKE_ERR_CLIENT);
	assert_message(err, "Failed to rebuild the seed provider: factory failed: consul unavailable");
	assert_false(config.discovery.provider_registered);
	assert_false(config.translation.provider_registered);
	assert_not_null(config.discovery.recipe);
	p.fail = AEROSPIKE_OK;
	assert_int_eq(as_config_rebuild_plugins(&config, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 4);
	assert_int_eq(t.calls, 3);
	assert_int_eq(p.destroyed, 2);

	// A code setter drops the recipe and its options reference, once.
	uint32_t code = 0;
	as_seed_provider provider;
	init_provider(&provider, "code", &code);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(p.destroyed, 3);
	assert_null(config.discovery.recipe);
	assert_int_eq(p.options->count, 1);

	// Without a recipe the code instance is released for good.
	as_config_release_plugins(&config, true);
	assert_int_eq(code, 1);
	assert_true(config.discovery.provider_released);
	assert_false(config.translation.provider_released);

	as_config_destroy(&config);
	assert_int_eq(p.destroyed, 3);
	assert_int_eq(t.destroyed, 3);
	assert_int_eq(p.released, 1);
	assert_int_eq(t.released, 1);
	assert_int_eq(extra.released, 10);
	assert_true(t.released_seq > t.destroyed_seq);
	assert_int_eq(code, 1);
	plugin_done(&p);
	plugin_done(&t);
	plugin_done(&extra);
}

TEST(discovery_recipe_unconnected_close, "the close of an unconnected async client drops recipes and registries for good")
{
	as_config config;
	as_error err;
	plugin p;
	plugin_reset(&p, "consul");
	as_config_init(&config);

	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_select_seed_provider(&config, "consul", NULL, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_pass_through_translator(&config, &err), AEROSPIKE_OK);
	assert_not_null(config.translation.recipe);

	as_config_release_plugins(&config, false);
	assert_int_eq(p.destroyed, 1);
	assert_int_eq(p.released, 1);
	assert_true(p.released_seq > p.destroyed_seq);
	assert_null(config.discovery.recipe);
	assert_null(config.translation.recipe);
	assert_true(config.discovery.provider_released);
	assert_true(config.translation.provider_released);

	assert_int_eq(as_config_rebuild_plugins(&config, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 1);
	assert_false(config.discovery.provider_registered);

	as_config_destroy(&config);
	assert_int_eq(p.destroyed, 1);
	assert_int_eq(p.released, 1);
	plugin_done(&p);
}

TEST(discovery_live_factory_reconnect, "a selected factory is destroyed at sync close and rebuilt by reconnect; each instance is destroyed once")
{
	if (as->config.tls.enable) {
		info("skipped: lifecycle test does not copy TLS configuration");
		return;
	}

	as_config config;
	as_error err;
	plugin p;
	plugin t;
	plugin_reset(&p, "consul");
	plugin_reset(&t, "nat-table");

	as_config_init(&config);
	as_config_add_hosts(&config, g_host, (uint16_t)g_port);
	as_config_set_user(&config, as->config.user, as->config.password);
	config.use_services_alternate = as->config.use_services_alternate;
	config.auth_mode = as->config.auth_mode;

	assert_int_eq(as_config_register_seed_provider(&config, "consul", provider_factory,
		plugin_release, &p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_register_address_translator(&config, "nat-table", translator_factory,
		plugin_release, &t, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_select_seed_provider(&config, "consul", NULL, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_select_address_translator(&config, "nat-table", NULL, &err),
		AEROSPIKE_OK);

	aerospike* client = aerospike_new(&config);
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		// Async close destroys the client, instances first.
		aerospike_close(client, &err);
		assert_int_eq(p.destroyed, 1);
		assert_int_eq(t.destroyed, 1);
		assert_int_eq(p.released, 1);
		assert_true(p.released_seq > p.destroyed_seq);
		plugin_done(&p);
		plugin_done(&t);
		return;
	}

	aerospike_close(client, &err);
	assert_int_eq(p.destroyed, 1);
	assert_int_eq(t.destroyed, 1);
	assert_int_eq(p.released, 0);

	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 2);
	assert_int_eq(t.calls, 2);
	assert_string_eq(client->config.discovery.provider.name, "consul");
	aerospike_close(client, &err);
	aerospike_close(client, &err);
	assert_int_eq(p.destroyed, 2);
	assert_int_eq(t.destroyed, 2);

	aerospike_destroy(client);
	assert_int_eq(p.destroyed, 2);
	assert_int_eq(t.destroyed, 2);
	assert_int_eq(p.calls, 2);
	assert_int_eq(p.released, 1);
	assert_int_eq(t.released, 1);
	plugin_done(&p);
	plugin_done(&t);
}

//---------------------------------
// Tests: factories set without a name
//---------------------------------

TEST(discovery_factory_setter, "a factory setter builds at once from empty options, keeps udata on failure and ends the registration once")
{
	as_config config;
	as_error err;
	plugin p;
	plugin q;
	plugin t;
	plugin u;
	plugin_reset(&p, "consul");
	plugin_reset(&q, "etcd");
	plugin_reset(&t, "nat-table");
	plugin_reset(&u, "nat-other");

	as_config_init(&config);
	assert_int_eq(as_config_set_seed_provider_factory(NULL, provider_factory, plugin_release, &p,
		&err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_set_seed_provider_factory(&config, NULL, plugin_release, &p, &err),
		AEROSPIKE_ERR_PARAM);
	assert_int_eq(as_config_set_address_translator_factory(&config, NULL, plugin_release, &t,
		&err), AEROSPIKE_ERR_PARAM);

	config.discovery.registration_locked = true;
	assert_int_eq(as_config_set_seed_provider_factory(&config, provider_factory, plugin_release,
		&p, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "cannot be replaced while the client is connected");
	assert_int_eq(as_config_set_address_translator_factory(&config, translator_factory,
		plugin_release, &t, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "cannot be replaced while the client is connected");
	config.discovery.registration_locked = false;
	assert_int_eq(p.calls, 0);
	assert_int_eq(t.calls, 0);

	assert_int_eq(as_config_set_seed_provider_factory(&config, provider_factory, plugin_release,
		&p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_address_translator_factory(&config, translator_factory,
		plugin_release, &t, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 1);
	assert_int_eq(t.calls, 1);
	assert_true(is_orderedmap(p.options));
	assert_int_eq(as_map_size((as_map*)p.options), 0);
	assert_true(is_orderedmap(t.options));
	assert_true(config.discovery.provider_registered);
	assert_false(config.discovery.binding_provider);
	assert_string_eq(config.discovery.provider.name, "consul");
	assert_string_eq(config.translation.provider.name, "nat-table");

	// A failing factory or an invalid table keeps the previous registration and leaves udata to
	// the caller.
	q.fail = AEROSPIKE_ERR_CLIENT;
	assert_int_eq(as_config_set_seed_provider_factory(&config, provider_factory, plugin_release,
		&q, &err), AEROSPIKE_ERR_CLIENT);
	assert_message(err, "factory failed: etcd unavailable");
	q.fail = AEROSPIKE_OK;
	q.invalid = true;
	assert_int_eq(as_config_set_seed_provider_factory(&config, provider_factory, plugin_release,
		&q, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "factory returned an invalid provider");
	assert_int_eq(q.destroyed, 1);
	assert_int_eq(q.released, 0);

	u.invalid = true;
	assert_int_eq(as_config_set_address_translator_factory(&config, translator_factory,
		plugin_release, &u, &err), AEROSPIKE_ERR_PARAM);
	assert_message(err, "factory returned an invalid translator");
	assert_int_eq(u.destroyed, 1);
	assert_int_eq(u.released, 0);
	assert_int_eq(p.destroyed, 0);
	assert_int_eq(t.destroyed, 0);
	assert_string_eq(config.discovery.provider.name, "consul");
	assert_string_eq(config.translation.provider.name, "nat-table");

	// Registering the same udata again is a new registration: the previous one ends.
	assert_int_eq(as_config_set_seed_provider_factory(&config, provider_factory, plugin_release,
		&p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_address_translator_factory(&config, translator_factory,
		plugin_release, &t, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 2);
	assert_int_eq(p.destroyed, 1);
	assert_int_eq(p.released, 1);
	assert_int_eq(t.destroyed, 1);
	assert_int_eq(t.released, 1);

	// A direct table replaces the factory registration.
	uint32_t direct = 0;
	as_seed_provider provider;
	init_provider(&provider, "direct", &direct);
	assert_int_eq(as_config_set_seed_provider(&config, &provider, &err), AEROSPIKE_OK);
	assert_int_eq(p.destroyed, 2);
	assert_int_eq(p.released, 2);
	assert_null(config.discovery.recipe);

	as_config_destroy(&config);
	assert_int_eq(direct, 1);
	assert_int_eq(p.destroyed, 2);
	assert_int_eq(p.released, 2);
	assert_int_eq(t.destroyed, 2);
	assert_int_eq(t.released, 2);
	assert_int_eq(q.released, 0);
	assert_int_eq(u.released, 0);
	plugin_done(&p);
	plugin_done(&q);
	plugin_done(&t);
	plugin_done(&u);
}

// Instances that share one reference-counted udata pointer, as a binding that hands out its
// runtime object would. The factory udata is a holder of that pointer.
typedef struct shared_ref_s {
	int32_t refs;
} shared_ref;

static void
shared_unref(void* udata)
{
	((shared_ref*)udata)->refs--;
}

static as_status
shared_provider_factory(as_error* err, const as_map* options, as_seed_provider* provider,
	void* udata)
{
	(void)err;
	(void)options;
	shared_ref* s = *(shared_ref**)udata;
	s->refs++;
	init_provider(provider, "shared", NULL);
	provider->destroy = shared_unref;
	provider->udata = s;
	return AEROSPIKE_OK;
}

static as_status
shared_translator_factory(as_error* err, const as_map* options,
	as_address_translator* translator, void* udata)
{
	(void)err;
	(void)options;
	shared_ref* s = *(shared_ref**)udata;
	s->refs++;
	init_translator(translator, "shared", NULL);
	translator->destroy = shared_unref;
	translator->udata = s;
	return AEROSPIKE_OK;
}

TEST(discovery_factory_shared_udata, "replacing a factory instance destroys it even when the new instance has the same udata pointer")
{
	as_config config;
	as_error err;
	shared_ref s = {0};
	shared_ref ts = {0};
	shared_ref* holder = &s;
	shared_ref* t_holder = &ts;

	as_config_init(&config);
	assert_int_eq(as_config_set_seed_provider_factory(&config, shared_provider_factory, NULL,
		&holder, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_seed_provider_factory(&config, shared_provider_factory, NULL,
		&holder, &err), AEROSPIKE_OK);
	assert_int_eq(s.refs, 1);

	assert_int_eq(as_config_register_seed_provider(&config, "shared", shared_provider_factory,
		NULL, &holder, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_select_seed_provider(&config, "shared", NULL, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_select_seed_provider(&config, "shared", NULL, &err), AEROSPIKE_OK);
	assert_int_eq(s.refs, 1);

	assert_int_eq(as_config_set_address_translator_factory(&config, shared_translator_factory,
		NULL, &t_holder, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_address_translator_factory(&config, shared_translator_factory,
		NULL, &t_holder, &err), AEROSPIKE_OK);
	assert_int_eq(ts.refs, 1);

	as_config_destroy(&config);
	assert_int_eq(s.refs, 0);
	assert_int_eq(ts.refs, 0);
}

TEST(discovery_live_factory_setter_reconnect, "a factory setter's instances are destroyed at sync close and rebuilt by reconnect; a rebuild failure fails only that connect")
{
	if (as->config.tls.enable) {
		info("skipped: lifecycle test does not copy TLS configuration");
		return;
	}

	as_config config;
	as_error err;
	plugin p;
	plugin t;
	plugin_reset(&p, "consul");
	plugin_reset(&t, "nat-table");

	live_config(&config);
	assert_int_eq(as_config_set_seed_provider_factory(&config, provider_factory, plugin_release,
		&p, &err), AEROSPIKE_OK);
	assert_int_eq(as_config_set_address_translator_factory(&config, translator_factory,
		plugin_release, &t, &err), AEROSPIKE_OK);

	aerospike* client = aerospike_new(&config);
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 1);
	assert_int_eq(t.calls, 1);

	if (as_event_loop_size > 0 && !as_event_single_thread) {
		// Async close destroys the client, instances first.
		aerospike_close(client, &err);
		assert_int_eq(p.destroyed, 1);
		assert_int_eq(t.destroyed, 1);
		assert_int_eq(p.released, 1);
		assert_int_eq(t.released, 1);
		assert_true(p.released_seq > p.destroyed_seq);
		plugin_done(&p);
		plugin_done(&t);
		return;
	}

	aerospike_close(client, &err);
	assert_int_eq(p.destroyed, 1);
	assert_int_eq(t.destroyed, 1);
	assert_int_eq(p.released, 0);
	assert_int_eq(t.released, 0);
	assert_false(client->config.discovery.provider_released);
	assert_false(client->config.translation.provider_released);

	// The provider is rebuilt before the translator fails; the next connect keeps it.
	t.fail = AEROSPIKE_ERR_CLIENT;
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_ERR_CLIENT);
	assert_message(err,
		"Failed to rebuild the address translator: factory failed: nat-table unavailable");
	assert_null(client->cluster);
	assert_int_eq(p.calls, 2);
	assert_int_eq(t.calls, 2);

	t.fail = AEROSPIKE_OK;
	assert_int_eq(aerospike_connect(client, &err), AEROSPIKE_OK);
	assert_int_eq(p.calls, 2);
	assert_int_eq(t.calls, 3);
	assert_false(client->config.discovery.binding_provider);
	assert_string_eq(client->config.discovery.provider.name, "consul");
	assert_string_eq(client->config.translation.provider.name, "nat-table");
	aerospike_close(client, &err);
	assert_int_eq(p.destroyed, 2);
	assert_int_eq(t.destroyed, 2);

	aerospike_destroy(client);
	assert_int_eq(p.destroyed, 2);
	assert_int_eq(t.destroyed, 2);
	assert_int_eq(p.released, 1);
	assert_int_eq(t.released, 1);
	assert_true(p.released_seq > p.destroyed_seq);
	plugin_done(&p);
	plugin_done(&t);
}

#undef fail

// AWS documented TCP idle timeout of Network Load Balancer listeners (default) and PrivateLink
// endpoints.
#define AWS_NLB_TCP_IDLE_TIMEOUT_SEC 350
#define MAX_SOCKET_IDLE_UNDER_NLB 340
#define NS_PER_SEC ((uint64_t)1000 * 1000 * 1000)

static uint64_t
idle_for(uint64_t sec)
{
	return cf_getns() - sec * NS_PER_SEC;
}

// Only total and min_size are read by as_conn_pool_excess(), so the pool is not initialized.
static void
pool_with_total(as_conn_pool* pool, uint32_t total)
{
	memset(pool, 0, sizeof(as_conn_pool));
	pool->min_size = 2;
	pool->queue.total = total;
}

static void
idle_client_close(aerospike* client)
{
	as_error err;
	aerospike_close(client, &err);

	// Async close destroys a connected client once pending commands complete.
	if (as_event_loop_size == 0 || as_event_single_thread) {
		aerospike_destroy(client);
	}
}

// A client of the harness host, connected as discovery_live_lifecycle does. TLS is not copied.
static aerospike*
idle_client_connect(uint32_t max_socket_idle, uint32_t tender_interval, as_error* err)
{
	as_config config;
	as_config_init(&config);
	as_config_add_hosts(&config, g_host, (uint16_t)g_port);
	as_config_set_user(&config, as->config.user, as->config.password);
	config.use_services_alternate = as->config.use_services_alternate;
	config.auth_mode = as->config.auth_mode;
	config.max_socket_idle = max_socket_idle;
	config.tender_interval = tender_interval;

	aerospike* client = aerospike_new(&config);

	if (aerospike_connect(client, err) != AEROSPIKE_OK) {
		idle_client_close(client);
		return NULL;
	}
	return client;
}

// Sync connections of every node, pooled or in use.
static uint32_t
sync_conn_total(as_cluster* cluster)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t total = 0;

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];

		for (uint32_t j = 0; j < cluster->conn_pools_per_node; j++) {
			total += as_load_uint32(&node->sync_conn_pools[j].queue.total);
		}
	}
	as_nodes_release(nodes);
	return total;
}

// Mark every pooled sync connection as idle for the NLB timeout. Returns the number marked.
static uint32_t
age_pooled_sync_conns(as_cluster* cluster)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint64_t last_used = idle_for(AWS_NLB_TCP_IDLE_TIMEOUT_SEC);
	uint32_t aged = 0;

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];

		for (uint32_t j = 0; j < cluster->conn_pools_per_node; j++) {
			as_conn_pool* pool = &node->sync_conn_pools[j];
			pthread_mutex_lock(&pool->lock);

			for (uint32_t k = pool->queue.head; k < pool->queue.tail; k++) {
				as_socket* sock = as_queue_get(&pool->queue, k);
				sock->last_used = last_used;
				aged++;
			}
			pthread_mutex_unlock(&pool->lock);
		}
	}
	as_nodes_release(nodes);
	return aged;
}

TEST(discovery_idle_reap_under_lb_timeout, "U-12 idle-reaping defaults sit under the AWS NLB TCP idle timeout")
{
	as_config config;
	as_config_init(&config);
	assert_int_eq(config.max_socket_idle, 0);
	assert_int_eq(config.min_conns_per_node, 0);
	assert_int_eq(config.async_min_conns_per_node, 0);

	uint64_t trim_ns = (uint64_t)AS_CLUSTER_IDLE_TRIM_DEFAULT_SEC * NS_PER_SEC;
	uint64_t nominal_ms = (uint64_t)AS_CLUSTER_IDLE_TRIM_DEFAULT_SEC * 1000 +
		(uint64_t)AS_CLUSTER_BALANCE_TENDS * config.tender_interval;
	assert_true(nominal_ms < (uint64_t)AWS_NLB_TCP_IDLE_TIMEOUT_SEC * 1000);

	// Above the min count, a connection idle for the NLB timeout is trimmed.
	as_conn_pool pool;
	pool_with_total(&pool, 3);
	assert_int_eq(as_conn_pool_excess(&pool), 1);
	assert_false(as_socket_current_trim(idle_for(AWS_NLB_TCP_IDLE_TIMEOUT_SEC), trim_ns));

	// At the min count nothing is trimmed, and with max_socket_idle 0 checkout keeps a reserved
	// connection however long it was idle.
	pool_with_total(&pool, 2);
	assert_int_eq(as_conn_pool_excess(&pool), 0);
	assert_true(as_socket_current_tran(idle_for(AWS_NLB_TCP_IDLE_TIMEOUT_SEC), 0));

	// max_socket_idle under the NLB timeout: checkout discards a connection idle past it.
	uint64_t tran_ns = (uint64_t)MAX_SOCKET_IDLE_UNDER_NLB * NS_PER_SEC;
	assert_false(as_socket_current_tran(idle_for(AWS_NLB_TCP_IDLE_TIMEOUT_SEC), tran_ns));
	assert_false(as_socket_current_tran(idle_for(MAX_SOCKET_IDLE_UNDER_NLB + 1), tran_ns));
	assert_true(as_socket_current_tran(idle_for(MAX_SOCKET_IDLE_UNDER_NLB - 10), tran_ns));

	// The harness client runs with the as_config_init() default and applies the constant.
	assert_log(as->config.max_socket_idle != 0,
		"harness max_socket_idle is %u, not the as_config_init() default 0",
		as->config.max_socket_idle);
	assert_true(as->cluster->max_socket_idle_ns_tran == 0);
	assert_true(as->cluster->max_socket_idle_ns_trim == trim_ns);

	if (as->config.tls.enable) {
		info("skipped: max_socket_idle client does not copy TLS configuration");
		return;
	}

	// A client with max_socket_idle under the NLB timeout checks it at checkout and trims with it.
	as_error err;
	aerospike* client = idle_client_connect(MAX_SOCKET_IDLE_UNDER_NLB, config.tender_interval,
		&err);
	assert_log(client == NULL, "connect failed: %d %s", err.code, err.message);
	uint64_t client_tran = client->cluster->max_socket_idle_ns_tran;
	uint64_t client_trim = client->cluster->max_socket_idle_ns_trim;
	idle_client_close(client);
	assert_true(client_tran == tran_ns);
	assert_true(client_trim == tran_ns);
}

#define IDLE_TRIM_WAIT_MS 120000
#define IDLE_TRIM_POLL_MS 20

// Polling lag: the trim is seen at most a few tends after the balance run that made it.
#define IDLE_TRIM_TEND_SLACK 3

TEST(discovery_idle_reap_live, "U-12 a pooled connection idle for the AWS NLB TCP idle timeout is trimmed within one balance period")
{
	if (as->config.tls.enable) {
		info("skipped: idle trim client does not copy TLS configuration");
		return;
	}

	// Defaults except the shortest tend interval, so a balance period is short. max_socket_idle 0
	// means checkout never checks idle time: only the balance trim closes the connection.
	as_error err;
	aerospike* client = idle_client_connect(0, AS_TEND_INTERVAL_MIN, &err);
	assert_log(client == NULL, "connect failed: %d %s", err.code, err.message);
	as_cluster* cluster = client->cluster;

	as_key key;
	as_key_init_str(&key, "test", "u12", "idle");
	as_record* rec = NULL;
	as_status status = aerospike_key_exists(client, &err, NULL, &key, &rec);

	if (rec) {
		as_record_destroy(rec);
	}
	as_key_destroy(&key);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		idle_client_close(client);
		assert_int_eq(status, AEROSPIKE_OK);
	}

	uint32_t aged = age_pooled_sync_conns(cluster);
	uint32_t start = as_load_uint32(&cluster->tend_count);
	uint32_t total = sync_conn_total(cluster);

	for (uint32_t ms = 0; total > 0 && ms < IDLE_TRIM_WAIT_MS; ms += IDLE_TRIM_POLL_MS) {
		as_sleep(IDLE_TRIM_POLL_MS);
		total = sync_conn_total(cluster);
	}

	uint32_t tends = as_load_uint32(&cluster->tend_count) - start;
	idle_client_close(client);

	assert_log(aged == 0, "no pooled sync connection after the command");
	assert_log(total != 0, "%u sync connections left after %u tends", total, tends);
	assert_log(tends > AS_CLUSTER_BALANCE_TENDS + IDLE_TRIM_TEND_SLACK,
		"trimmed after %u tends, more than one balance period of %u tends", tends,
		AS_CLUSTER_BALANCE_TENDS);
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
	suite_add(discovery_conformance_collision_hosts);
	suite_add(discovery_provider_validation);
	suite_add(discovery_translator_validation);
	suite_add(discovery_registration_ownership);
	suite_add(discovery_client_destroy_once);
	suite_add(discovery_registration_same_udata);
	suite_add(discovery_binding_mode);
	suite_add(discovery_binding_factory_errors);
	suite_add(discovery_binding_unconnected_release);
	suite_add(discovery_binding_reconnect);
	suite_add(discovery_live_lifecycle);
	suite_add(discovery_on_miss_range);
	suite_add(discovery_factory_registration);
	suite_add(discovery_factory_select);
	suite_add(discovery_factory_select_rejections);
	suite_add(discovery_factory_recipe);
	suite_add(discovery_recipe_unconnected_close);
	suite_add(discovery_live_factory_reconnect);
	suite_add(discovery_factory_setter);
	suite_add(discovery_factory_shared_udata);
	suite_add(discovery_live_factory_setter_reconnect);
	suite_add(discovery_idle_reap_under_lb_timeout);
	suite_add(discovery_idle_reap_live);
}
