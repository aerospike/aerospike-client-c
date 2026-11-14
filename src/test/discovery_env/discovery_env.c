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

/*
 * The client side of three test environments of aerospike-client-conformance (envs/README.md
 * there), selected by ENV_NAME from the environment's values file:
 *
 *   dns           N-4: DNS seed discovery with A and SRV records in thread and tend mode, a DNS
 *                 outage, and a changed SRV record.
 *   collision     N-18: a node translated to a live node's address under pass-through, static-map
 *                 and port-map is never created, and the live node serves.
 *   tls-matching  N-6: TLS through the proxy cluster validates against each node's tls-name, not
 *                 the dialed host; the env's certificate name matching cases, sync and async,
 *                 against C's declarations. With DISCOVERY_ENV_SUITE=n17 only N-17: a CN-only
 *                 certificate is rejected, red until the next major release (FR-45).
 *
 * src/test/discovery_env/run.sh builds and runs it with the values loaded. Verbs that change the
 * environment run as "$ENV_CTL" <verb>, and every change is undone before the next case. The exit
 * status is non-zero when a case fails, a value is missing or an undo did not complete. Every TLS
 * client runs in a forked child, because the client warns about a legacy match once per name per
 * process; tls-matching needs an event library. Built against installed headers only by
 * "make discovery-env"; POSIX only.
 */
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_address.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_event.h>
#include <aerospike/as_host.h>
#include <aerospike/as_key.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_vector.h>
#include <aerospike/version.h>

#include "l2_run/live.h"
#include "test.h"

//---------------------------------
// Macros
//---------------------------------

#define ENV_SEEDS "seeds.dns.test"
#define ENV_SRV "_aerospike._tcp.dns.test"
#define ENV_CHANGE "_change._tcp.dns.test"
#define ENV_SET "discovery-env"
#define ENV_REFRESH_MS 1000
#define ENV_WAIT_MS 15000
#define ENV_DNS_POLL_MS 200
#define ENV_DNS_TIMEOUT_MS 80
#define ENV_SAMPLE_MS 100
#define ENV_KEYS 100
#define ENV_WARNS 3
#define ENV_QUIET_MS 3000
#define ENV_LINE_SIZE 2048
#define ENV_TLS_TIMEOUT_MS 5000
#define ENV_ASYNC_WAIT_MS 10000
#define ENV_CHILD_BOUND_MS 120000
#define ENV_EVENT_LOOPS 2
#define ENV_TLS_CASES_MAX 64
#define ENV_N6_SYNC_KEYS 100
#define ENV_N6_ASYNC_KEYS 50
#define ENV_CHILD_FREE_MAX 8
#define ENV_LEGACY_WARN "only through the legacy name check"
#define ENV_BLACKLISTED_WARN "CERT: BLACKLISTED"
#define ENV_FR45_REASON "legacy name check kept until the next major version (deferring per FR-45)"

//---------------------------------
// Types
//---------------------------------

typedef struct env_node_s {
	const char* name;
	const char* host;
	uint16_t port;
	char address[300];
	char advertised[300];
} env_node;

// A change of the environment and how to undo it: run verb, then wait until name answers expected.
typedef struct env_restore_s {
	char verb[128];
	char name[64];
	as_dns_record_type type;
	char expected[ENV_LINE_SIZE];
} env_restore;

typedef enum env_translator_e {
	ENV_PASS_THROUGH,
	ENV_STATIC_MAP,
	ENV_PORT_MAP
} env_translator;

// One certificate name matching case of the env. bootstrap and revoke are "" when unset.
typedef struct env_tls_case_s {
	const char* name;
	const char* tls_name;
	const char* ca;
	const char* bootstrap;
	const char* revoke;
	uint16_t port;
	bool accept;
	bool warning;
} env_tls_case;

//---------------------------------
// Globals
//---------------------------------

int g_argc = 0;
char** g_argv = NULL;

static const char* env_name;
static const char* env_host;
static const char* env_ctl;
static const char* env_ns;
static const char* env_dns_server;
static bool env_setup_failed;

static env_node env_a;
static env_node env_b;
static env_node env_c;
static env_node env_control_a;
static env_node env_control_b;
static env_node env_collision_a;
static env_node env_collision_b;

// Sorted "host:port" lists: nodes A and B, and node C.
static char env_ab[ENV_LINE_SIZE];
static char env_only_c[ENV_LINE_SIZE];

static const char* env_proxy_tls_name;
static const char* env_proxy_ca;
static env_node env_proxy_a;
static env_node env_proxy_b;
static char env_tls_names[ENV_LINE_SIZE * 2];
static env_tls_case env_tls_cases[ENV_TLS_CASES_MAX];
static uint32_t env_tls_cases_size;

//---------------------------------
// Values and verbs
//---------------------------------

static void
env_node_read(env_node* n, const char* prefix)
{
	char key[64];
	snprintf(key, sizeof(key), "%s_NAME", prefix);
	n->name = live_value(key);
	snprintf(key, sizeof(key), "%s_HOST", prefix);
	n->host = live_value(key);
	snprintf(key, sizeof(key), "%s_PORT", prefix);
	n->port = live_port_value(key);
	snprintf(n->address, sizeof(n->address), "%s:%u", n->host, n->port);
}

// The alternate address the node advertises, "<prefix>_ADVERTISED_HOST:<prefix>_ADVERTISED_PORT".
static void
env_node_read_advertised(env_node* n, const char* prefix)
{
	char key[64];
	snprintf(key, sizeof(key), "%s_ADVERTISED_HOST", prefix);
	const char* host = live_value(key);
	snprintf(key, sizeof(key), "%s_ADVERTISED_PORT", prefix);
	snprintf(n->advertised, sizeof(n->advertised), "%s:%u", host, live_port_value(key));
}

// Run "$ENV_CTL" verb; its output goes to the log.
static bool
env_verb(const char* verb)
{
	char command[1024];
	snprintf(command, sizeof(command), "\"%s\" %s", env_ctl, verb);
	printf("ENV-VERB %s\n", verb);
	fflush(stdout);
	uint64_t begin = live_now();
	int rc = system(command);
	bool ok = rc != -1 && WIFEXITED(rc) && WEXITSTATUS(rc) == 0;
	printf("ENV-VERB %s %s in %" PRIu64 " ms (status %d)\n", verb, ok ? "done" : "FAILED", live_now() - begin, rc);
	return ok;
}

//---------------------------------
// DNS
//---------------------------------

static void
env_dns_config(as_dns_seed_provider_config* dns, const char* const* names, const char* const* servers,
	as_dns_record_type type)
{
	as_dns_seed_provider_config_init(dns);
	dns->names = names;
	dns->names_size = 1;
	dns->record_type = type;
	// A records take this port; the default 3000 could reach a local server.
	dns->port = env_a.port;
	dns->servers = servers;
	dns->servers_size = 1;
	dns->timeout_ms = ENV_DNS_TIMEOUT_MS;
}

// One refresh of provider: the sorted hosts, or "error: <message>".
static void
env_dns_refresh(as_seed_provider* provider, char* answer, size_t size)
{
	as_vector configured;
	as_vector_init(&configured, sizeof(as_host), 1);
	as_seed_candidate_context context = {
		.cluster_name = NULL,
		.configured_seeds = &configured,
		.tend_iteration = 0
	};
	as_seed_candidates candidates;
	as_seed_candidates_init(&candidates, 4);

	as_error err;
	as_error_reset(&err);

	if (provider->refresh(&err, &context, &candidates, provider->udata) == AEROSPIKE_OK) {
		live_hosts_join(candidates.hosts, answer, size);
	}
	else {
		snprintf(answer, size, "error: %.1000s", err.message);
	}
	as_seed_candidates_destroy(&candidates);
	as_vector_destroy(&configured);
}

// Call the DNS provider directly, as the test's own client, every ENV_DNS_POLL_MS for at most
// ENV_WAIT_MS until name answers exactly expected; last receives the last answer.
static bool
env_dns_wait(const char* name, as_dns_record_type type, const char* expected, char* last, size_t size)
{
	const char* names[] = {name};
	const char* servers[] = {env_dns_server};
	as_dns_seed_provider_config dns;
	env_dns_config(&dns, names, servers, type);

	as_seed_provider provider;
	as_error err;

	if (as_discovery_dns_seed_provider(&err, &dns, &provider) != AEROSPIKE_OK) {
		snprintf(last, size, "provider not created: %.1000s", err.message);
		return false;
	}

	uint64_t begin = live_now();
	uint32_t calls = 0;
	bool ok;

	while (true) {
		env_dns_refresh(&provider, last, size);
		calls++;
		ok = strcmp(last, expected) == 0;

		if (ok || live_now() - begin >= ENV_WAIT_MS) {
			break;
		}
		as_sleep(ENV_DNS_POLL_MS);
	}

	if (provider.destroy) {
		provider.destroy(provider.udata);
	}
	printf("ENV-DNS-WAIT %s %s %s after %u calls in %" PRIu64 " ms: %s\n", name,
		type == AS_DNS_RECORD_SRV ? "SRV" : "A", ok ? "answered" : "FAILED", calls, live_now() - begin, last);
	return ok;
}

static bool
env_restore_undo(void* ptr)
{
	env_restore* r = ptr;
	char last[ENV_LINE_SIZE];
	bool ran = env_verb(r->verb);
	bool answered = env_dns_wait(r->name, r->type, r->expected, last, sizeof(last));
	return ran && answered;
}

// Register the undo of verb, then run verb.
static bool
env_change(const char* verb, const char* restore, const char* name, as_dns_record_type type,
	const char* expected)
{
	env_restore* r = calloc(1, sizeof(env_restore));

	if (! r) {
		return false;
	}
	as_strncpy(r->verb, restore, sizeof(r->verb));
	as_strncpy(r->name, name, sizeof(r->name));
	r->type = type;
	as_strncpy(r->expected, expected, sizeof(r->expected));
	live_cleanup_push(restore, env_restore_undo, r);
	return env_verb(verb);
}

// A client seeded only by the DNS provider on name.
static aerospike*
env_dns_connect(const char* name, as_dns_record_type type, as_discovery_execution_mode mode,
	as_error* err)
{
	as_config config;
	as_config_init(&config);
	config.use_services_alternate = true;
	config.discovery.refresh_interval_ms = ENV_REFRESH_MS;
	config.discovery.execution_mode = mode;

	const char* names[] = {name};
	const char* servers[] = {env_dns_server};
	as_dns_seed_provider_config dns;
	env_dns_config(&dns, names, servers, type);

	if (as_config_set_dns_seed_provider(&config, &dns, err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		return NULL;
	}
	return live_connect(&config, err);
}

static void
env_snapshot(aerospike* as, char* buf, size_t size)
{
	as_seed_snapshot* snapshot = as_seed_manager_reserve(as->cluster->seed_manager);
	live_hosts_join(as_seed_snapshot_hosts(snapshot), buf, size);
	as_seed_snapshot_release(snapshot);
}

static const char*
env_mode_name(as_discovery_execution_mode mode)
{
	return mode == AS_DISCOVERY_EXECUTION_TEND ? "tend" : "thread";
}

// The client runs in mode: a discovery thread in thread mode; none, with no overrun and not
// tripped, in tend mode.
static bool
env_mode_ok(aerospike* as, as_discovery_execution_mode mode, char* msg, size_t size)
{
	as_seed_manager_stats s;
	as_seed_manager_get_stats(as->cluster->seed_manager, &s);
	snprintf(msg, size, "mode=%s threadRunning=%s overruns=%u tripped=%s refreshes=%u failures=%u",
		env_mode_name(mode), s.thread_running ? "true" : "false", s.overruns, s.tripped ? "true" : "false",
		s.refreshes, s.failures);
	printf("ENV-N4-MODE %s\n", msg);

	if (mode == AS_DISCOVERY_EXECUTION_THREAD) {
		return s.thread_running;
	}
	return ! s.thread_running && s.overruns == 0 && ! s.tripped;
}

// Both nodes held with every partition owned, and ENV_KEYS keys written and read.
static bool
env_serves_ab(const char* label, aerospike* as, char* msg, size_t size)
{
	char names[256];
	char* items[] = {(char*)env_a.name, (char*)env_b.name};
	live_sort_join(items, 2, names, sizeof(names));

	char last[LIVE_LIST_SIZE];

	if (! live_wait_nodes(as, env_ns, names, ENV_WAIT_MS, last, sizeof(last))) {
		snprintf(msg, size, "%.63s: expected nodes %s with every partition owned: %s", label, names, last);
		return false;
	}

	char first[1024];
	uint32_t failures = live_round_trip(as, env_ns, ENV_SET, label, NULL, ENV_KEYS, first, sizeof(first));
	printf("ENV-N4-RESULT %s %s keys=%u failures=%u\n", label, last, ENV_KEYS, failures);

	if (failures > 0) {
		snprintf(msg, size, "%s: %u of %u keys failed, first %s", label, failures, ENV_KEYS, first);
		return false;
	}
	return true;
}

//---------------------------------
// dns
//---------------------------------

static void
env_joins(atf_test_result* __result__, const char* label, const char* name, as_dns_record_type type,
	as_discovery_execution_mode mode)
{
	live_cleanup();
	as_error err;
	aerospike* as = env_dns_connect(name, type, mode, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	char msg[ENV_LINE_SIZE];
	check(env_serves_ab(label, as, msg, sizeof(msg)), "%s", msg);

	char snapshot[ENV_LINE_SIZE];
	env_snapshot(as, snapshot, sizeof(snapshot));
	printf("ENV-N4-SEEDS %s %s\n", label, snapshot);
	check(strcmp(snapshot, env_ab) == 0, "seeds are %s, not %s", snapshot, env_ab);
	check(env_mode_ok(as, mode, msg, sizeof(msg)), "%s", msg);
	live_cleanup();
}

TEST(n4_a_records_thread, "N-4: A records seed both nodes in thread mode")
{
	env_joins(__result__, "a-thread", ENV_SEEDS, AS_DNS_RECORD_A, AS_DISCOVERY_EXECUTION_THREAD);
}

TEST(n4_srv_tend, "N-4: SRV records seed both nodes in tend mode")
{
	env_joins(__result__, "srv-tend", ENV_SRV, AS_DNS_RECORD_SRV, AS_DISCOVERY_EXECUTION_TEND);
}

TEST(n4_outage_keeps_running, "N-4: a DNS outage keeps the client serving and counts the failed refreshes")
{
	live_cleanup();
	as_error err;
	aerospike* as = env_dns_connect(ENV_SEEDS, AS_DNS_RECORD_A, AS_DISCOVERY_EXECUTION_THREAD, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	char msg[ENV_LINE_SIZE];
	check(env_serves_ab("outage-before", as, msg, sizeof(msg)), "%s", msg);

	as_seed_manager_stats before;
	as_seed_manager_get_stats(as->cluster->seed_manager, &before);
	uint32_t mark = live_log_mark();
	check(env_change("stop-dns", "start-dns", ENV_SEEDS, AS_DNS_RECORD_A, env_ab), "stop-dns failed");

	const char* needles[] = {"Seed provider dns refresh failed", "DNS seed discovery failed for every name",
		ENV_SEEDS};
	uint64_t begin = live_now();
	uint32_t warns;
	uint32_t failures;

	while (true) {
		as_seed_manager_stats now;
		as_seed_manager_get_stats(as->cluster->seed_manager, &now);
		warns = live_log_count(mark, needles, 3);
		failures = now.failures - before.failures;

		if ((warns >= ENV_WARNS && failures >= ENV_WARNS) || live_now() - begin >= ENV_WAIT_MS) {
			break;
		}
		as_sleep(ENV_SAMPLE_MS);
	}

	const char* overrun[] = {"exceeding refresh_timeout_ms"};
	uint32_t overruns = live_log_count(mark, overrun, 1);
	printf("ENV-N4-RESULT outage refreshFailedWarns=%u failures=%u overrunWarns=%u in %" PRIu64 " ms\n", warns,
		failures, overruns, live_now() - begin);
	check(warns >= ENV_WARNS, "%u refresh failure warnings within %u ms, expected %u", warns, ENV_WAIT_MS,
		ENV_WARNS);
	check(failures >= ENV_WARNS, "failures rose by %u, expected %u", failures, ENV_WARNS);
	check(overruns == 0, "%u refreshes exceeded refresh_timeout_ms", overruns);
	check(env_serves_ab("outage", as, msg, sizeof(msg)), "%s", msg);
	check(env_mode_ok(as, AS_DISCOVERY_EXECUTION_THREAD, msg, sizeof(msg)), "%s", msg);
	live_cleanup();
}

static void
env_changed_srv(atf_test_result* __result__, as_discovery_execution_mode mode)
{
	live_cleanup();
	as_error err;
	aerospike* as = env_dns_connect(ENV_CHANGE, AS_DNS_RECORD_SRV, mode, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	char label[32];
	snprintf(label, sizeof(label), "change-%s", env_mode_name(mode));
	char msg[ENV_LINE_SIZE];
	check(env_serves_ab(label, as, msg, sizeof(msg)), "%s", msg);

	char snapshot[ENV_LINE_SIZE];
	env_snapshot(as, snapshot, sizeof(snapshot));
	printf("ENV-N4-SEEDS %s before %s\n", label, snapshot);
	check(strcmp(snapshot, env_ab) == 0, "seeds before the change are %s, not %s", snapshot, env_ab);

	check(env_change("set-change-srv node-c", "set-change-srv node-a node-b", ENV_CHANGE, AS_DNS_RECORD_SRV,
		env_ab), "set-change-srv node-c failed");

	uint64_t begin = live_now();

	while (true) {
		env_snapshot(as, snapshot, sizeof(snapshot));

		if (strcmp(snapshot, env_only_c) == 0 || live_now() - begin >= ENV_WAIT_MS) {
			break;
		}
		as_sleep(ENV_SAMPLE_MS);
	}
	printf("ENV-N4-SEEDS %s after %s in %" PRIu64 " ms\n", label, snapshot, live_now() - begin);
	check(strcmp(snapshot, env_only_c) == 0, "seeds are %s %u ms after the change, not %s", snapshot,
		ENV_WAIT_MS, env_only_c);
	check(env_serves_ab(label, as, msg, sizeof(msg)), "%s", msg);
	check(env_mode_ok(as, mode, msg, sizeof(msg)), "%s", msg);
	live_cleanup();
}

TEST(n4_changed_srv_thread, "N-4: a changed SRV record replaces the seeds in thread mode")
{
	env_changed_srv(__result__, AS_DISCOVERY_EXECUTION_THREAD);
}

TEST(n4_changed_srv_tend, "N-4: a changed SRV record replaces the seeds in tend mode")
{
	env_changed_srv(__result__, AS_DISCOVERY_EXECUTION_TEND);
}

//---------------------------------
// collision
//---------------------------------

static const char*
env_translator_name(env_translator t)
{
	switch (t) {
	case ENV_PASS_THROUGH:
		return "pass-through";
	case ENV_STATIC_MAP:
		return "static-map";
	default:
		return "port-map";
	}
}

// A client seeded at seed. Pass-through and static-map use the alternate addresses; port-map
// maps both nodes' names to seed's address.
static aerospike*
env_collision_connect(env_translator t, const env_node* seed, const env_node* other, as_error* err)
{
	as_config config;
	as_config_init(&config);
	as_config_add_host(&config, seed->host, seed->port);
	as_status status = AEROSPIKE_OK;

	switch (t) {
	case ENV_PASS_THROUGH:
		config.use_services_alternate = true;
		status = as_config_set_pass_through_translator(&config, err);
		break;
	case ENV_STATIC_MAP:
		config.use_services_alternate = true;
		break;
	case ENV_PORT_MAP: {
		as_addr_map map[2] = {
			{.orig = (char*)seed->name, .alt = (char*)seed->address},
			{.orig = (char*)other->name, .alt = (char*)seed->address}
		};
		status = as_config_set_port_map_translator(&config, map, 2, err);
		break;
	}
	}

	if (status != AEROSPIKE_OK) {
		as_config_destroy(&config);
		return NULL;
	}
	return live_connect(&config, err);
}

TEST(n18_control_pass_through, "N-18 control: pass-through holds both nodes of the control cluster")
{
	live_cleanup();
	uint32_t mark = live_log_mark();
	as_error err;
	aerospike* as = env_collision_connect(ENV_PASS_THROUGH, &env_control_a, &env_control_b, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	char names[ENV_LINE_SIZE];
	char* items[] = {(char*)env_control_a.name, (char*)env_control_b.name};
	live_sort_join(items, 2, names, sizeof(names));
	char last[ENV_LINE_SIZE];
	check(live_wait_nodes(as, env_ns, names, ENV_WAIT_MS, last, sizeof(last)), "expected %s: %s", names, last);

	as_sleep(ENV_QUIET_MS);
	const char* needles[] = {"to the same address"};
	uint32_t collisions = live_log_count(mark, needles, 1);
	char first[1024];
	uint32_t failures = live_round_trip(as, env_ns, ENV_SET, "control", NULL, ENV_KEYS, first, sizeof(first));
	printf("ENV-N18-RESULT control translator=pass-through %s collisionWarns=%u keys=%u failures=%u\n", last,
		collisions, ENV_KEYS, failures);
	check(collisions == 0, "%u collision warnings in the control cluster", collisions);
	check(failures == 0, "%u of %u keys failed, first %s", failures, ENV_KEYS, first);
	live_cleanup();
}

// The newcomer translates to the address of live, the seed: it never becomes a node, every
// colliding tend logs a warning naming both nodes, the translator and the address, and live
// serves the keys it masters (replication factor 1).
static void
env_collision_case(atf_test_result* __result__, env_translator t, const env_node* live_node,
	const env_node* newcomer)
{
	live_cleanup();
	const char* translator = env_translator_name(t);
	// Port-map maps both names to the live node's address; otherwise the newcomer advertises the
	// live node's alternate address.
	const char* address = t == ENV_PORT_MAP ? live_node->address : newcomer->advertised;

	uint32_t mark = live_log_mark();
	uint64_t begin = live_now();
	as_error err;
	aerospike* as = env_collision_connect(t, live_node, newcomer, &err);
	check(as, "client not created: %d %s", err.code, err.message);

	uint32_t forward = 0;
	uint32_t reverse = 0;
	uint32_t warns;
	uint32_t samples = 0;
	uint32_t bad = 0;
	char bad_sample[LIVE_LIST_SIZE] = "";

	while (true) {
		char held[LIVE_LIST_SIZE];
		live_nodes(as, held, sizeof(held));
		samples++;

		if (strcmp(held, live_node->name) != 0 && bad++ == 0) {
			as_strncpy(bad_sample, held, sizeof(bad_sample));
		}
		warns = live_collisions(mark, translator, live_node->name, newcomer->name, address, &forward, &reverse);

		if (warns >= ENV_WARNS || live_now() - begin >= ENV_WAIT_MS) {
			break;
		}
		as_sleep(ENV_SAMPLE_MS);
	}

	uint64_t elapsed = live_now() - begin;
	uint64_t deadline = live_now() + ENV_WAIT_MS;

	while (live_owned(as, env_ns, live_node->name) == 0 && live_now() < deadline) {
		as_sleep(ENV_SAMPLE_MS);
	}

	uint32_t owned = live_owned(as, env_ns, live_node->name);
	char first[1024];
	uint32_t failures = live_round_trip(as, env_ns, ENV_SET, translator, live_node->name, ENV_KEYS, first,
		sizeof(first));
	printf("ENV-N18-RESULT translator=%s live=%s newcomer=%s address=%s warns=%u forward=%u reverse=%u "
		"elapsedMs=%" PRIu64 " samples=%u badSamples=%u liveOwned=%u keys=%u failures=%u\n", translator,
		live_node->name, newcomer->name, address, warns, forward, reverse, elapsed, samples, bad, owned, ENV_KEYS,
		failures);
	check(bad == 0, "%u of %u samples held other nodes than %s, first %s", bad, samples, live_node->name,
		bad_sample);
	check(warns >= ENV_WARNS, "%u collision warnings naming %s, %s, %s and %s within %u ms, expected %u", warns,
		translator, live_node->name, newcomer->name, address, ENV_WAIT_MS, ENV_WARNS);
	check(failures == 0, "%u of %u keys mastered by %s failed, first %s", failures, ENV_KEYS, live_node->name,
		first);
	live_cleanup();
}

TEST(n18_pass_through_live_node, "N-18: pass-through never creates a node translated to a live node's address")
{
	env_collision_case(__result__, ENV_PASS_THROUGH, &env_collision_a, &env_collision_b);
}

TEST(n18_static_map_live_node, "N-18: static-map never creates a node whose only address is a live node's")
{
	env_collision_case(__result__, ENV_STATIC_MAP, &env_collision_a, &env_collision_b);
}

TEST(n18_port_map_live_node, "N-18: port-map never creates a node mapped to a live node's address")
{
	env_collision_case(__result__, ENV_PORT_MAP, &env_control_a, &env_control_b);
}

//---------------------------------
// tls-matching
//---------------------------------

#if AS_EVENT_LIB_DEFINED

static env_tls_case*
env_tls_case_find(const char* name)
{
	for (uint32_t i = 0; i < env_tls_cases_size; i++) {
		if (strcmp(env_tls_cases[i].name, name) == 0) {
			return &env_tls_cases[i];
		}
	}
	return NULL;
}

typedef enum env_declared_e {
	ENV_DECLARED_PASS,
	ENV_DECLARED_FAIL,
	ENV_DECLARED_BLOCKED
} env_declared;

// An env case in tls/name-matching.yaml terms: its fixture row ("-" for none) and outcome; l1, the
// row's c-declared.yaml declaration (C against the fixture); declared, C against the env's
// expectation; and the reason of an l1 fail (c-declared.yaml) or of a blocked row (the fixture's
// blockedBy). The env expects the legacy name check's compatibility result, so four rows C fails at
// L1 pass against the env; a declared fail row is one where the legacy check accepts, with one
// warning, what the env expects rejected. Follows envs/tls-matching.sh, tls/name-matching.yaml and
// c-declared.yaml.
typedef struct env_tls_row_s {
	const char* env_case;
	const char* row;
	const char* fixture;
	const char* l1;
	env_declared declared;
	const char* reason;
} env_tls_row;

static const env_tls_row ENV_TLS_ROWS[] = {
	{"dns_exact", "dns-id-equals-dns-name", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"dns_case", "dns-id-differs-only-in-case", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"ipv4_ip_san", "ipv4-id-equals-ip-address", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"ipv6_ip_san", "ipv6-id-equals-ip-address", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"ipv6_ip_san_equivalent", "ipv6-id-differently-spelled", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"ipv6_bracketed", "ipv6-id-bracketed", "blocked", "blocked", ENV_DECLARED_BLOCKED,
		"undecided whether a bracketed IPv6 tlsName is an IP-ID"},
	{"ipv6_scoped", "ipv6-id-with-zone", "blocked", "blocked", ENV_DECLARED_BLOCKED,
		"undecided how a scoped IPv6 tlsName (fe80::1%en0) compares"},
	{"ipv4_leading_zero", "ipv4-id-with-leading-zeros", "blocked", "blocked", ENV_DECLARED_BLOCKED,
		"undecided whether 127.000.0.1 is an IP-ID"},
	{"dns_only_as_ip", "dns-id-resolving-to-ip-address", "reject", "pass", ENV_DECLARED_PASS, NULL},
	{"wildcard_one_label", "wildcard-matches-one-label", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"wildcard_case", "wildcard-matches-case-insensitively", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"wildcard_bare_domain", "wildcard-does-not-match-parent", "reject", "pass", ENV_DECLARED_PASS, NULL},
	{"wildcard_two_labels", "wildcard-does-not-match-two-labels", "reject", "pass", ENV_DECLARED_PASS, NULL},
	{"wildcard_partial_label", "partial-label-wildcard-rejected", "reject", "pass", ENV_DECLARED_PASS, NULL},
	{"wildcard_double", "multiple-wildcards-rejected", "reject", "pass", ENV_DECLARED_PASS, NULL},
	{"wildcard_inner_label", "inner-label-wildcard-rejected", "reject", "pass", ENV_DECLARED_PASS, NULL},
	{"ip_vs_wildcard", "ip-id-against-wildcard", "reject", "fail", ENV_DECLARED_FAIL, ENV_FR45_REASON},
	{"cn_case_only", "cn-differs-only-in-case-no-san", "reject", "fail", ENV_DECLARED_FAIL, ENV_FR45_REASON},
	{"legacy_cn_only", "cn-only-no-san", "reject", "fail", ENV_DECLARED_PASS, ENV_FR45_REASON},
	{"legacy_cn_nonmatching_dns", "cn-equal-dns-name-differs", "reject", "fail", ENV_DECLARED_PASS,
		ENV_FR45_REASON},
	{"legacy_ip_as_dns", "ip-id-only-as-dns-name", "reject", "fail", ENV_DECLARED_PASS, ENV_FR45_REASON},
	{"dns_san_nonmatching_cn", "dns-id-with-non-matching-cn", "accept", "pass", ENV_DECLARED_PASS, NULL},
	{"ci_setup_aerospike_server", "ci-server-certificate-shape", "reject", "fail", ENV_DECLARED_PASS,
		ENV_FR45_REASON},
	{"ci_setup_aerospike_server_fixed", "ci-server-certificate-shape-fixed", "accept", "pass", ENV_DECLARED_PASS,
		NULL},
	{"revoked_serial", "-", "-", "-", ENV_DECLARED_PASS, NULL}
};

#define ENV_TLS_ROWS_SIZE (sizeof(ENV_TLS_ROWS) / sizeof(ENV_TLS_ROWS[0]))

typedef enum env_outcome_e {
	ENV_ACCEPT,
	ENV_REJECT,
	ENV_ERROR
} env_outcome;

// Runs in a forked child: returns whether it passed, with its report or reason in msg.
typedef bool (*env_child_fn)(void* arg, char* msg, size_t size);

// The one async command a child waits for. gen tells a late callback from the current one.
typedef struct env_async_s {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	uint64_t gen;
	bool done;
	as_status status;
	int64_t value;
	char message[AS_ERROR_MESSAGE_MAX_SIZE];
} env_async;

static env_async env_async_op = {
	.lock = PTHREAD_MUTEX_INITIALIZER,
	.cond = PTHREAD_COND_INITIALIZER
};

// What a child frees or removes once its clients are closed.
static char* env_child_free[ENV_CHILD_FREE_MAX];
static uint32_t env_child_free_size;
static char env_child_files[ENV_CHILD_FREE_MAX][64];
static uint32_t env_child_files_size;

static const env_tls_row*
env_tls_row_find(const char* env_case)
{
	for (uint32_t i = 0; i < ENV_TLS_ROWS_SIZE; i++) {
		if (strcmp(ENV_TLS_ROWS[i].env_case, env_case) == 0) {
			return &ENV_TLS_ROWS[i];
		}
	}
	return NULL;
}

static const char*
env_declared_name(env_declared d)
{
	switch (d) {
	case ENV_DECLARED_PASS:
		return "pass";
	case ENV_DECLARED_FAIL:
		return "fail";
	default:
		return "blocked";
	}
}

static const char*
env_outcome_name(env_outcome o)
{
	switch (o) {
	case ENV_ACCEPT:
		return "accept";
	case ENV_REJECT:
		return "reject";
	default:
		return "error";
	}
}

//---------------------------------
// tls-matching: child processes
//---------------------------------

// Run fn in a forked child with its own event loops, and wait at most ENV_CHILD_BOUND_MS. Returns
// whether the child passed and exited 0; msg receives its report or why not.
static bool
env_child(const char* label, env_child_fn fn, void* arg, char* msg, size_t size)
{
	int fds[2];

	if (pipe(fds) != 0) {
		snprintf(msg, size, "%s: pipe: %s", label, strerror(errno));
		return false;
	}
	fflush(stdout);
	fflush(stderr);
	pid_t pid = fork();

	if (pid < 0) {
		snprintf(msg, size, "%s: fork: %s", label, strerror(errno));
		close(fds[0]);
		close(fds[1]);
		return false;
	}

	if (pid == 0) {
		close(fds[0]);
		live_cleanup_forget();
		char report[ENV_LINE_SIZE] = "";
		bool ok = false;

		if (as_event_create_loops(ENV_EVENT_LOOPS)) {
			ok = fn(arg, report, sizeof(report));
		}
		else {
			snprintf(report, sizeof(report), "cannot create %u event loops", ENV_EVENT_LOOPS);
		}

		live_cleanup();

		if (ok && ! live_cleanup_ok()) {
			ok = false;
			snprintf(report, sizeof(report), "an undo did not complete (ENV-CLEANUP-FAILED above)");
		}

		if (! ok) {
			printf("ENV-TLS-FAILED %s %s\n", label, report);
		}
		as_event_close_loops();
		live_shutdown();

		for (uint32_t i = 0; i < env_child_free_size; i++) {
			free(env_child_free[i]);
		}

		for (uint32_t i = 0; i < env_child_files_size; i++) {
			unlink(env_child_files[i]);
		}

		char line[ENV_LINE_SIZE + 2];
		int len = snprintf(line, sizeof(line), "%c%s", ok ? '1' : '0', report);

		if (write(fds[1], line, (size_t)len) != len) {
			ok = false;
		}
		close(fds[1]);
		exit(ok ? 0 : 1);
	}

	close(fds[1]);
	uint64_t deadline = live_now() + ENV_CHILD_BOUND_MS;
	int status = 0;
	bool killed = false;

	while (true) {
		pid_t r = waitpid(pid, &status, WNOHANG);

		if (r == pid || (r < 0 && errno != EINTR)) {
			break;
		}

		if (live_now() >= deadline) {
			kill(pid, SIGKILL);
			waitpid(pid, &status, 0);
			killed = true;
			break;
		}
		as_sleep(100);
	}

	char line[ENV_LINE_SIZE + 2];
	size_t len = 0;
	ssize_t n;

	while (len < sizeof(line) - 1 && (n = read(fds[0], line + len, sizeof(line) - 1 - len)) != 0) {
		if (n < 0) {
			if (errno == EINTR) {
				continue;
			}
			break;
		}
		len += (size_t)n;
	}
	line[len] = '\0';
	close(fds[0]);

	char how[64];

	if (killed) {
		snprintf(how, sizeof(how), "killed-after-%us", ENV_CHILD_BOUND_MS / 1000);
	}
	else if (WIFSIGNALED(status)) {
		snprintf(how, sizeof(how), "signal=%d", WTERMSIG(status));
	}
	else {
		snprintf(how, sizeof(how), "exit=%d", WEXITSTATUS(status));
	}
	printf("ENV-CHILD %s %s\n", label, how);

	bool exited_0 = ! killed && WIFEXITED(status) && WEXITSTATUS(status) == 0;
	bool reported = len > 0;
	bool passed = reported && line[0] == '1';
	const char* report = reported ? line + 1 : "";

	if (! reported) {
		snprintf(msg, size, "%s: %s with no report", label, how);
	}
	else if (passed && ! exited_0) {
		snprintf(msg, size, "%s: %s after reporting %.1000s", label, how, report);
	}
	else {
		as_strncpy(msg, report, size);
	}
	return passed && exited_0;
}

// Free p once the child's clients are closed.
static void
env_child_defer_free(char* p)
{
	if (env_child_free_size == ENV_CHILD_FREE_MAX) {
		fprintf(stderr, "ENV: too many deferred frees\n");
		exit(1);
	}
	env_child_free[env_child_free_size++] = p;
}

static void
env_tls_config(as_config* config, const char* host, uint16_t port, const char* tls_name, const char* ca,
	const char* blacklist)
{
	as_config_init(config);
	config->tls.enable = true;
	as_config_tls_add_host(config, host, tls_name, port);
	as_config_tls_set_cafile(config, ca);

	if (blacklist) {
		as_config_tls_set_cert_blacklist(config, blacklist);
	}
	config->conn_timeout_ms = ENV_TLS_TIMEOUT_MS;
	config->policies.read.base.total_timeout = ENV_TLS_TIMEOUT_MS;
	config->policies.write.base.total_timeout = ENV_TLS_TIMEOUT_MS;
}

// Rewrite the blacklist file at path: serial on one line, or empty when serial is NULL.
static bool
env_blacklist_write(const char* path, const char* serial)
{
	FILE* f = fopen(path, "w");

	if (! f) {
		return false;
	}

	if (serial) {
		fprintf(f, "%s\n", serial);
	}
	return fclose(f) == 0;
}

// A new blacklist file, removed at the end of the child.
static bool
env_blacklist_file(const char* serial, char* path, size_t size)
{
	if (env_child_files_size == ENV_CHILD_FREE_MAX) {
		return false;
	}
	snprintf(path, size, "/tmp/discovery_env_blacklist_XXXXXX");
	int fd = mkstemp(path);

	if (fd < 0) {
		return false;
	}
	close(fd);
	as_strncpy(env_child_files[env_child_files_size++], path, sizeof(env_child_files[0]));
	return env_blacklist_write(path, serial);
}

//---------------------------------
// tls-matching: warnings
//---------------------------------

static uint32_t
env_legacy_warns(uint32_t mark, const char* name)
{
	char quoted[300];
	snprintf(quoted, sizeof(quoted), "'%s'", name);
	const char* needles[] = {ENV_LEGACY_WARN, quoted};
	return live_log_count(mark, needles, 2);
}

static uint32_t
env_mismatch_warns(uint32_t mark, const char* name)
{
	char needle[300];
	snprintf(needle, sizeof(needle), "TLS name '%s' mismatch", name);
	const char* needles[] = {needle};
	return live_log_count(mark, needles, 1);
}

static uint32_t
env_blacklisted_warns(uint32_t mark)
{
	const char* needles[] = {ENV_BLACKLISTED_WARN};
	return live_log_count(mark, needles, 1);
}

//---------------------------------
// tls-matching: async
//---------------------------------

static void
env_async_complete(void* udata, as_error* err, int64_t value)
{
	env_async* op = &env_async_op;
	pthread_mutex_lock(&op->lock);

	if ((uint64_t)(uintptr_t)udata == op->gen) {
		op->done = true;
		op->status = err ? err->code : AEROSPIKE_OK;
		as_strncpy(op->message, err ? err->message : "", sizeof(op->message));
		op->value = value;
		pthread_cond_broadcast(&op->cond);
	}
	pthread_mutex_unlock(&op->lock);
}

static void
env_write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	(void)event_loop;
	env_async_complete(udata, err, 0);
}

// The client destroys record when the listener returns.
static void
env_record_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	(void)event_loop;
	env_async_complete(udata, err, record ? as_record_get_int64(record, "v", -1) : -1);
}

// One async put (write) or get of key on loop, waited for at most ENV_ASYNC_WAIT_MS.
static as_status
env_async_run(aerospike* as, as_event_loop* loop, bool write, as_key* key, int64_t value, int64_t* got,
	char* msg, size_t size)
{
	env_async* op = &env_async_op;
	pthread_mutex_lock(&op->lock);
	uint64_t gen = ++op->gen;
	op->done = false;
	pthread_mutex_unlock(&op->lock);

	void* udata = (void*)(uintptr_t)gen;
	as_error err;
	as_status status;

	if (write) {
		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, "v", value);
		status = aerospike_key_put_async(as, &err, NULL, key, &rec, env_write_listener, udata, loop, NULL);
		as_record_destroy(&rec);
	}
	else {
		status = aerospike_key_get_async(as, &err, NULL, key, env_record_listener, udata, loop, NULL);
	}

	if (status != AEROSPIKE_OK) {
		snprintf(msg, size, "%d %s", err.code, err.message);
		return status;
	}

	uint64_t deadline = live_now() + ENV_ASYNC_WAIT_MS;
	pthread_mutex_lock(&op->lock);

	while (! op->done && live_now() < deadline) {
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_nsec += 100 * 1000000L;

		if (ts.tv_nsec >= 1000000000L) {
			ts.tv_sec++;
			ts.tv_nsec -= 1000000000L;
		}
		pthread_cond_timedwait(&op->cond, &op->lock, &ts);
	}

	if (! op->done) {
		pthread_mutex_unlock(&op->lock);
		snprintf(msg, size, "no callback within %u ms", ENV_ASYNC_WAIT_MS);
		return AEROSPIKE_ERR_TIMEOUT;
	}
	status = op->status;
	snprintf(msg, size, "%d %s", op->status, op->message);
	*got = op->value;
	pthread_mutex_unlock(&op->lock);
	return status;
}

// An async put of name with value, then an async get of it on loop. Returns the first failed
// status, with what failed in msg.
static as_status
env_async_put_get(aerospike* as, as_event_loop* loop, const char* name, int64_t value, char* msg, size_t size)
{
	as_key key;
	as_key_init_str(&key, env_ns, ENV_SET, name);
	char text[AS_ERROR_MESSAGE_MAX_SIZE + 32];
	int64_t got = -1;
	as_status status = env_async_run(as, loop, true, &key, value, &got, text, sizeof(text));

	if (status != AEROSPIKE_OK) {
		snprintf(msg, size, "async put %s: %.900s", name, text);
		as_key_destroy(&key);
		return status;
	}
	status = env_async_run(as, loop, false, &key, 0, &got, text, sizeof(text));
	as_key_destroy(&key);

	if (status != AEROSPIKE_OK) {
		snprintf(msg, size, "async get %s: %.900s", name, text);
		return status;
	}

	if (got != value) {
		snprintf(msg, size, "async get %s: v is %" PRId64 ", not %" PRId64, name, got, value);
		return AEROSPIKE_ERR_CLIENT;
	}
	msg[0] = '\0';
	return AEROSPIKE_OK;
}

//---------------------------------
// tls-matching: N-6
//---------------------------------

// Each node is held at its forwarder with the proxy's tls-name.
static bool
env_n6_nodes(aerospike* as, char* msg, size_t size)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	bool ok = true;
	msg[0] = '\0';

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		as_address* address = as_node_get_address(node);
		char host[INET6_ADDRSTRLEN];
		as_address_short_name((struct sockaddr*)&address->addr, host, sizeof(host));
		uint16_t port = as_address_port((struct sockaddr*)&address->addr);
		const char* tls_name = node->tls_name ? node->tls_name : "(none)";
		printf("ENV-N6-NODE name=%s endpoint=%s:%u tlsName=%s\n", node->name, host, port, tls_name);

		const env_node* expected = strcmp(node->name, env_proxy_a.name) == 0 ? &env_proxy_a :
			strcmp(node->name, env_proxy_b.name) == 0 ? &env_proxy_b : NULL;

		if (ok && ! expected) {
			snprintf(msg, size, "unexpected node %s", node->name);
			ok = false;
		}
		else if (ok && (strcmp(host, expected->host) != 0 || port != expected->port)) {
			snprintf(msg, size, "node %s is held at %s:%u, not its forwarder %s", node->name, host, port,
				expected->address);
			ok = false;
		}
		else if (ok && strcmp(tls_name, env_proxy_tls_name) != 0) {
			snprintf(msg, size, "node %s has tls-name %s, not %s", node->name, tls_name, env_proxy_tls_name);
			ok = false;
		}
	}
	as_nodes_release(nodes);
	return ok;
}

static bool
env_n6_proxy(void* arg, char* msg, size_t size)
{
	(void)arg;
	uint32_t mark = live_log_mark();
	as_config config;
	env_tls_config(&config, env_proxy_a.host, env_proxy_a.port, env_proxy_tls_name, env_proxy_ca, NULL);
	as_addr_map map[2] = {
		{.orig = (char*)env_proxy_a.name, .alt = env_proxy_a.address},
		{.orig = (char*)env_proxy_b.name, .alt = env_proxy_b.address}
	};
	as_error err;

	if (as_config_set_port_map_translator(&config, map, 2, &err) != AEROSPIKE_OK) {
		as_config_destroy(&config);
		snprintf(msg, size, "port-map: %s", err.message);
		return false;
	}

	aerospike* as = live_connect(&config, &err);

	if (! as) {
		snprintf(msg, size, "client not created: %d %s", err.code, err.message);
		return false;
	}

	char names[256];
	char* items[] = {(char*)env_proxy_a.name, (char*)env_proxy_b.name};
	live_sort_join(items, 2, names, sizeof(names));
	char last[LIVE_LIST_SIZE];

	if (! live_wait_nodes(as, env_ns, names, ENV_WAIT_MS, last, sizeof(last))) {
		snprintf(msg, size, "expected nodes %s with every partition owned: %s", names, last);
		return false;
	}

	if (! env_n6_nodes(as, msg, size)) {
		return false;
	}

	char first[1024];
	uint32_t sync_failures = live_round_trip(as, env_ns, ENV_SET, "n6", NULL, ENV_N6_SYNC_KEYS, first,
		sizeof(first));
	uint32_t async_failures = 0;
	char async_first[1024] = "";

	for (uint32_t loop = 0; loop < ENV_EVENT_LOOPS; loop++) {
		for (uint32_t i = 0; i < ENV_N6_ASYNC_KEYS; i++) {
			char name[64];
			snprintf(name, sizeof(name), "n6-async-%u-%u", loop, i);
			char text[1024];

			if (env_async_put_get(as, as_event_loop_get_by_index(loop), name, i, text, sizeof(text)) !=
				AEROSPIKE_OK) {
				if (async_failures++ == 0) {
					as_strncpy(async_first, text, sizeof(async_first));
				}
			}
		}
	}

	uint32_t legacy = env_legacy_warns(mark, env_proxy_tls_name);
	uint32_t mismatch = env_mismatch_warns(mark, env_proxy_tls_name);
	printf("ENV-N6-RESULT %s syncKeys=%u syncFailures=%u asyncKeys=%u asyncFailures=%u legacyWarns=%u "
		"mismatchWarns=%u\n", last, ENV_N6_SYNC_KEYS, sync_failures, ENV_N6_ASYNC_KEYS * ENV_EVENT_LOOPS,
		async_failures, legacy, mismatch);

	if (sync_failures > 0) {
		snprintf(msg, size, "%u of %u sync keys failed, first %s", sync_failures, ENV_N6_SYNC_KEYS, first);
		return false;
	}

	if (async_failures > 0) {
		snprintf(msg, size, "%u of %u async keys failed, first %s", async_failures,
			ENV_N6_ASYNC_KEYS * ENV_EVENT_LOOPS, async_first);
		return false;
	}

	if (legacy > 0 || mismatch > 0) {
		snprintf(msg, size, "%u legacy and %u mismatch warnings for %s", legacy, mismatch, env_proxy_tls_name);
		return false;
	}
	snprintf(msg, size, "pass");
	return true;
}

static bool
env_n6_dialed(void* arg, char* msg, size_t size)
{
	(void)arg;
	const char* dialed = env_proxy_a.host;
	uint32_t mark = live_log_mark();
	as_config config;
	env_tls_config(&config, env_proxy_a.host, env_proxy_a.port, dialed, env_proxy_ca, NULL);
	as_error err;
	aerospike* as = live_connect(&config, &err);

	if (as) {
		snprintf(msg, size, "client created with tls-name %s, the host it dials", dialed);
		return false;
	}

	uint32_t mismatch = env_mismatch_warns(mark, dialed);
	uint32_t legacy = env_legacy_warns(mark, dialed);
	printf("ENV-N6-DIALED tlsName=%s status=%d mismatchWarns=%u legacyWarns=%u message=%s\n", dialed,
		err.code, mismatch, legacy, err.message);

	if (mismatch == 0) {
		snprintf(msg, size, "client not created (%d %s), but no \"TLS name '%s' mismatch\" warning", err.code,
			err.message, dialed);
		return false;
	}

	if (legacy > 0) {
		snprintf(msg, size, "%u legacy warnings for %s", legacy, dialed);
		return false;
	}
	snprintf(msg, size, "pass");
	return true;
}

#endif

TEST(n6_proxy_validates_node_tls_name, "N-6: TLS through the proxy validates each node against its tls-name")
{
#if AS_EVENT_LIB_DEFINED
	char msg[ENV_LINE_SIZE];
	check(env_child("n6-proxy", env_n6_proxy, NULL, msg, sizeof(msg)), "%s", msg);
#else
	fail("built without an event library: build with EVENT_LIB=libev");
#endif
}

TEST(n6_dialed_host_is_not_the_tls_name, "N-6: a client using the dialed host as tls-name is not created")
{
#if AS_EVENT_LIB_DEFINED
	char msg[ENV_LINE_SIZE];
	check(env_child("n6-dialed", env_n6_dialed, NULL, msg, sizeof(msg)), "%s", msg);
#else
	fail("built without an event library: build with EVENT_LIB=libev");
#endif
}

//---------------------------------
// tls-matching: cases
//---------------------------------

#if AS_EVENT_LIB_DEFINED

// A client with the case name: accept is connected plus one key written and read; reject is not
// created with the mismatch warning, or the blacklisted one for a revoked case.
static env_outcome
env_tls_sync(const env_tls_case* c, const char* blacklist, char* text, size_t size)
{
	uint32_t mark = live_log_mark();
	as_config config;
	env_tls_config(&config, env_host, c->port, c->tls_name, c->ca, blacklist);
	as_error err;
	aerospike* as = live_connect(&config, &err);

	if (! as) {
		uint32_t evidence = c->revoke[0] ? env_blacklisted_warns(mark) : env_mismatch_warns(mark, c->tls_name);
		printf("ENV-TLS-LEG %s sync not created: %d %s\n", c->name, err.code, err.message);
		snprintf(text, size, "%s", evidence ? "reject" : "error");
		return evidence ? ENV_REJECT : ENV_ERROR;
	}

	char first[1024];
	uint32_t failures = live_round_trip(as, env_ns, ENV_SET, c->name, NULL, 1, first, sizeof(first));
	live_cleanup();

	if (failures > 0) {
		printf("ENV-TLS-LEG %s sync created, key failed: %s\n", c->name, first);
		snprintf(text, size, "error");
		return ENV_ERROR;
	}
	snprintf(text, size, "accept");
	return ENV_ACCEPT;
}

// A second client with the case name and one async put and get on loop 0, chosen by outcome as
// Java: a revoked case starts with an empty blacklist and revokes through a reload; a case whose
// client is not created but has a bootstrap name connects with it and presents the case name to
// one async command through the node's tls_name.
static env_outcome
env_tls_async(const env_tls_case* c, char* text, size_t size)
{
	bool revoke = c->revoke[0] != '\0';
	char blacklist[64] = "";

	if (revoke && ! env_blacklist_file(NULL, blacklist, sizeof(blacklist))) {
		snprintf(text, size, "error(blacklist)");
		return ENV_ERROR;
	}

	uint32_t mark = live_log_mark();
	as_config config;
	env_tls_config(&config, env_host, c->port, c->tls_name, c->ca, revoke ? blacklist : NULL);
	as_error err;
	aerospike* as = live_connect(&config, &err);
	const char* via = "";
	as_node* node = NULL;
	char* original = NULL;

	if (! as) {
		printf("ENV-TLS-LEG %s async not created: %d %s\n", c->name, err.code, err.message);

		if (! c->bootstrap[0]) {
			bool evidence = env_mismatch_warns(mark, c->tls_name) > 0;
			snprintf(text, size, "%s(not-created)", evidence ? "reject" : "error");
			return evidence ? ENV_REJECT : ENV_ERROR;
		}

		env_tls_config(&config, env_host, c->port, c->bootstrap, c->ca, NULL);
		as = live_connect(&config, &err);

		if (! as) {
			printf("ENV-TLS-LEG %s bootstrap %s not created: %d %s\n", c->name, c->bootstrap, err.code,
				err.message);
			snprintf(text, size, "error(bootstrap)");
			return ENV_ERROR;
		}

		as_nodes* nodes = as_nodes_reserve(as->cluster);

		if (nodes->size != 1) {
			printf("ENV-TLS-LEG %s bootstrap holds %u nodes, not 1\n", c->name, nodes->size);
			as_nodes_release(nodes);
			snprintf(text, size, "error(bootstrap)");
			return ENV_ERROR;
		}
		node = nodes->array[0];
		// Held until the name is swapped back, so tend cannot destroy the node meanwhile. The copy
		// is freed after the client is closed: a socket created in the window keeps a pointer to it.
		as_node_reserve(node);
		as_nodes_release(nodes);
		char* copy = strdup(c->tls_name);

		if (! copy) {
			fprintf(stderr, "ENV: out of memory\n");
			exit(1);
		}
		env_child_defer_free(copy);
		original = node->tls_name;
		as_store_ptr((void**)&node->tls_name, copy);
		via = "(bootstrap)";
	}
	else if (revoke) {
		if (! env_blacklist_write(blacklist, c->revoke)) {
			snprintf(text, size, "error(blacklist)");
			return ENV_ERROR;
		}

		if (aerospike_reload_tls_config(as, &err) != AEROSPIKE_OK) {
			printf("ENV-TLS-LEG %s reload failed: %d %s\n", c->name, err.code, err.message);
			snprintf(text, size, "error(reload)");
			return ENV_ERROR;
		}
	}

	uint32_t async_mark = live_log_mark();
	char name[128];
	snprintf(name, sizeof(name), "%s-async", c->name);
	char msg[1024];
	as_status status = env_async_put_get(as, as_event_loop_get_by_index(0), name, 1, msg, sizeof(msg));

	if (node) {
		as_store_ptr((void**)&node->tls_name, original);
		as_node_release(node);
	}

	env_outcome outcome;

	if (status == AEROSPIKE_OK) {
		outcome = ENV_ACCEPT;
	}
	else {
		uint32_t evidence = revoke ? env_blacklisted_warns(async_mark) : env_mismatch_warns(async_mark, c->tls_name);
		outcome = status == AEROSPIKE_ERR_TLS_ERROR && evidence ? ENV_REJECT : ENV_ERROR;
		printf("ENV-TLS-LEG %s async%s %s\n", c->name, via, msg);
	}
	live_cleanup();
	snprintf(text, size, "%s%s", env_outcome_name(outcome), via);
	return outcome;
}

// Each leg runs in its own child, since the client warns once per name per process: its report
// is "<outcome> <legacy warnings> <text>", the warnings counted in that process alone.
static bool
env_tls_leg_sync(void* arg, char* msg, size_t size)
{
	const env_tls_case* c = arg;
	bool revoke = c->revoke[0] != '\0';
	char blacklist[64] = "";

	if (revoke && ! env_blacklist_file(c->revoke, blacklist, sizeof(blacklist))) {
		snprintf(msg, size, "cannot write the blacklist file");
		return false;
	}

	uint32_t mark = live_log_mark();
	char text[64];
	env_outcome outcome = env_tls_sync(c, revoke ? blacklist : NULL, text, sizeof(text));
	snprintf(msg, size, "%d %u %s", (int)outcome, env_legacy_warns(mark, c->tls_name), text);
	return true;
}

static bool
env_tls_leg_async(void* arg, char* msg, size_t size)
{
	const env_tls_case* c = arg;
	uint32_t mark = live_log_mark();
	char text[64];
	env_outcome outcome = env_tls_async(c, text, sizeof(text));
	snprintf(msg, size, "%d %u %s", (int)outcome, env_legacy_warns(mark, c->tls_name), text);
	return true;
}

static bool
env_tls_leg(env_tls_case* c, const char* leg, env_child_fn fn, env_outcome* outcome, uint32_t* warns,
	char* text, char* msg, size_t size)
{
	char label[160];
	snprintf(label, sizeof(label), "%s-%s", c->name, leg);
	char report[ENV_LINE_SIZE];

	if (! env_child(label, fn, c, report, sizeof(report))) {
		snprintf(msg, size, "%s: %.900s", label, report);
		return false;
	}

	int o;

	// text holds 64 bytes.
	if (sscanf(report, "%d %u %63s", &o, warns, text) != 3) {
		snprintf(msg, size, "%s: unreadable report %.900s", label, report);
		return false;
	}
	*outcome = (env_outcome)o;
	return true;
}

// Whether the case ran as C declares it; msg receives PASS, DECLARED-FAIL or BLOCKED, or why not.
static bool
env_tls_case_run(env_tls_case* c, char* msg, size_t size)
{
	const env_tls_row* row = env_tls_row_find(c->name);
	env_outcome sync_outcome;
	env_outcome async_outcome;
	uint32_t warns_sync;
	uint32_t warns_async;
	char sync[64];
	char async[64];

	if (! env_tls_leg(c, "sync", env_tls_leg_sync, &sync_outcome, &warns_sync, sync, msg, size) ||
		! env_tls_leg(c, "async", env_tls_leg_async, &async_outcome, &warns_async, async, msg, size)) {
		return false;
	}

	env_outcome env_expect = c->accept ? ENV_ACCEPT : ENV_REJECT;
	uint32_t env_warns = c->warning ? 1 : 0;
	bool env_match = sync_outcome == env_expect && async_outcome == env_expect && warns_sync == env_warns &&
		warns_async == env_warns;
	bool declared_match;

	switch (row->declared) {
	case ENV_DECLARED_PASS:
		declared_match = env_match;
		break;
	case ENV_DECLARED_FAIL:
		declared_match = sync_outcome == ENV_ACCEPT && async_outcome == ENV_ACCEPT && warns_sync == 1 &&
			warns_async == 1;
		break;
	default:
		declared_match = true;
		break;
	}

	const char* result = row->declared == ENV_DECLARED_BLOCKED ? "BLOCKED" : ! declared_match ? "WRONG" :
		env_match ? "PASS" : "DECLARED-FAIL";
	char reason[300] = "";

	if (row->reason) {
		snprintf(reason, sizeof(reason), " reason=\"%s\"", row->reason);
	}
	printf("ENV-TLS-CASE %s row=%s fixture=%s l1=%s env=%s%s declared=%s sync=%s async=%s warnsSync=%u "
		"warnsAsync=%u result=%s%s\n", c->name, row->row, row->fixture, row->l1, c->accept ? "accept" : "reject",
		c->warning ? "+warn" : "", env_declared_name(row->declared), sync, async, warns_sync, warns_async,
		result, reason);

	if (! declared_match) {
		snprintf(msg, size, "%s: declared %s, got sync=%s async=%s warnsSync=%u warnsAsync=%u", c->name,
			env_declared_name(row->declared), sync, async, warns_sync, warns_async);
		return false;
	}
	snprintf(msg, size, "%s", result);
	return true;
}

// Every env case has a table row and every row an env case.
static bool
env_tls_table_ok(char* msg, size_t size)
{
	for (uint32_t i = 0; i < env_tls_cases_size; i++) {
		if (! env_tls_row_find(env_tls_cases[i].name)) {
			snprintf(msg, size, "env case %s has no row in ENV_TLS_ROWS", env_tls_cases[i].name);
			return false;
		}
	}

	for (uint32_t i = 0; i < ENV_TLS_ROWS_SIZE; i++) {
		if (! env_tls_case_find(ENV_TLS_ROWS[i].env_case)) {
			snprintf(msg, size, "ENV_TLS_ROWS case %s is not in TLS_CASES", ENV_TLS_ROWS[i].env_case);
			return false;
		}
	}
	return true;
}

#endif

TEST(tls_matching_cases, "TLS name matching: every env case, sync and async, as C declares it")
{
#if AS_EVENT_LIB_DEFINED
	char msg[ENV_LINE_SIZE];
	check(env_tls_table_ok(msg, sizeof(msg)), "stale case table: %s", msg);

	uint32_t passed = 0;
	uint32_t failed = 0;
	uint32_t blocked = 0;
	uint32_t wrong = 0;
	char first[ENV_LINE_SIZE] = "";

	for (uint32_t i = 0; i < env_tls_cases_size; i++) {
		env_tls_case* c = &env_tls_cases[i];
		char verb[128];
		snprintf(verb, sizeof(verb), "serve %s", c->name);

		if (! env_verb(verb)) {
			snprintf(msg, sizeof(msg), "%s: %s failed", c->name, verb);
		}
		else if (env_tls_case_run(c, msg, sizeof(msg))) {
			if (strcmp(msg, "PASS") == 0) {
				passed++;
			}
			else if (strcmp(msg, "DECLARED-FAIL") == 0) {
				failed++;
			}
			else {
				blocked++;
			}
			continue;
		}

		if (wrong++ == 0) {
			as_strncpy(first, msg, sizeof(first));
		}
	}
	printf("ENV-TLS-SUMMARY cases=%u pass=%u declaredFail=%u blocked=%u wrong=%u\n", env_tls_cases_size, passed,
		failed, blocked, wrong);
	check(wrong == 0, "%u of %u cases did not run or not as declared, first %s", wrong, env_tls_cases_size, first);
#else
	fail("built without an event library: build with EVENT_LIB=libev");
#endif
}

//---------------------------------
// tls-matching: N-17
//---------------------------------

#if AS_EVENT_LIB_DEFINED

// Prints ENV-N17-RESULT accepted-through-legacy only when the CN-only certificate is accepted
// through the legacy check and a key round-trips; run.sh reads it as the expected FR-45 red.
static bool
env_n17_run(void* arg, char* msg, size_t size)
{
	const env_tls_case* c = arg;
	uint32_t mark = live_log_mark();
	as_config config;
	env_tls_config(&config, env_host, c->port, c->tls_name, c->ca, NULL);
	as_error err;
	aerospike* as = live_connect(&config, &err);

	if (! as) {
		uint32_t mismatch = env_mismatch_warns(mark, c->tls_name);
		printf("ENV-N17-CONNECT not created: %d %s mismatchWarns=%u\n", err.code, err.message, mismatch);

		if (mismatch == 0) {
			snprintf(msg, size, "client not created (%d %s), but no \"TLS name '%s' mismatch\" warning",
				err.code, err.message, c->tls_name);
			return false;
		}
		printf("ENV-N17-RESULT rejected\n");
		snprintf(msg, size, "rejected");
		return true;
	}

	char first[1024];
	uint32_t failures = live_round_trip(as, env_ns, ENV_SET, "n17", NULL, 1, first, sizeof(first));

	if (failures > 0) {
		snprintf(msg, size, "client created with the CN-only certificate, but the key failed: %s", first);
		return false;
	}

	uint32_t legacy = env_legacy_warns(mark, c->tls_name);

	if (legacy == 0) {
		snprintf(msg, size, "CN-only certificate accepted without a legacy name check warning");
		return false;
	}
	printf("ENV-N17-RESULT accepted-through-legacy legacyWarns=%u\n", legacy);
	snprintf(msg, size, "N-17: CN-only certificate accepted through the legacy name check; C keeps CN "
		"acceptance, deprecated, until the next major release (PRD FR-45, design E-6)");
	return false;
}

#endif

TEST(n17_cn_only_certificate_rejected, "N-17: a certificate with the tls-name only as its CN is rejected")
{
#if AS_EVENT_LIB_DEFINED
	env_tls_case* c = env_tls_case_find("legacy_cn_only");
	check(c, "TLS_CASES has no legacy_cn_only");
	check(env_verb("serve legacy_cn_only"), "serve legacy_cn_only failed");
	char msg[ENV_LINE_SIZE];
	check(env_child("n17", env_n17_run, c, msg, sizeof(msg)), "%s", msg);
#else
	fail("built without an event library: build with EVENT_LIB=libev");
#endif
}

//---------------------------------
// Suites
//---------------------------------

// Wait until DNS serves the records the cases start from, so a slow up cannot fail the first one.
static bool
env_dns_before(atf_suite* suite)
{
	(void)suite;
	char last[ENV_LINE_SIZE];

	if (! env_dns_wait(ENV_SEEDS, AS_DNS_RECORD_A, env_ab, last, sizeof(last)) ||
		! env_dns_wait(ENV_CHANGE, AS_DNS_RECORD_SRV, env_ab, last, sizeof(last))) {
		printf("ENV dns: DNS does not serve the records the cases start from\n");
		env_setup_failed = true;
		return false;
	}
	return true;
}

static bool
env_suite_after(atf_suite* suite)
{
	(void)suite;
	live_cleanup();
	return true;
}

static bool
env_plan_after(atf_plan* plan)
{
	(void)plan;
	live_cleanup();
	live_shutdown();

	if (env_setup_failed) {
		printf("ENV %s: the suite did not start\n", env_name);
		return false;
	}

	if (! live_cleanup_ok()) {
		printf("ENV %s: an undo did not complete; the environment may be left changed\n", env_name);
		return false;
	}
	return true;
}

SUITE(env_dns_suite, "N-4 DNS seed discovery (envs/dns.sh)")
{
	suite_before(env_dns_before);
	suite_after(env_suite_after);
	suite_add(n4_a_records_thread);
	suite_add(n4_srv_tend);
	suite_add(n4_outage_keeps_running);
	suite_add(n4_changed_srv_thread);
	suite_add(n4_changed_srv_tend);
}

SUITE(env_collision_suite, "N-18 translated address collisions against a live node (envs/collision.sh)")
{
	suite_after(env_suite_after);
	suite_add(n18_control_pass_through);
	suite_add(n18_pass_through_live_node);
	suite_add(n18_static_map_live_node);
	suite_add(n18_port_map_live_node);
}

SUITE(env_tls_suite, "N-6 TLS through the proxy and certificate name matching (envs/tls-matching.sh)")
{
	suite_after(env_suite_after);
	suite_add(n6_proxy_validates_node_tls_name);
	suite_add(n6_dialed_host_is_not_the_tls_name);
	suite_add(tls_matching_cases);
}

SUITE(env_tls_n17_suite, "N-17 CN-only certificate rejected, red until the next major (envs/tls-matching.sh)")
{
	suite_after(env_suite_after);
	suite_add(n17_cn_only_certificate_rejected);
}

static bool
env_bool_value(const char* name, const char* yes, const char* no)
{
	const char* value = live_value(name);

	if (strcmp(value, yes) != 0 && strcmp(value, no) != 0) {
		printf("ENV: %s is %s, not %s or %s\n", name, value, yes, no);
		exit(1);
	}
	return strcmp(value, yes) == 0;
}

static const char*
env_optional_value(const char* name)
{
	const char* value = getenv(name);
	return value ? value : "";
}

static void
env_tls_read(void)
{
	env_proxy_tls_name = live_value("PROXY_TLS_NAME");
	env_proxy_ca = live_value("PROXY_CA");
	env_node_read(&env_proxy_a, "PROXY_NODE_A");
	env_node_read(&env_proxy_b, "PROXY_NODE_B");

	const char* cases = live_value("TLS_CASES");

	if (strlen(cases) >= sizeof(env_tls_names)) {
		printf("ENV tls-matching: TLS_CASES is longer than %zu\n", sizeof(env_tls_names) - 1);
		exit(1);
	}
	as_strncpy(env_tls_names, cases, sizeof(env_tls_names));
	char* save = NULL;

	for (char* name = strtok_r(env_tls_names, ",", &save); name; name = strtok_r(NULL, ",", &save)) {
		if (env_tls_cases_size == ENV_TLS_CASES_MAX) {
			printf("ENV tls-matching: more than %u cases in TLS_CASES\n", ENV_TLS_CASES_MAX);
			exit(1);
		}

		char prefix[96];
		int len = snprintf(prefix, sizeof(prefix), "TLS_%s", name);

		for (int i = 0; i < len; i++) {
			prefix[i] = (char)toupper((unsigned char)prefix[i]);
		}

		char key[128];
		env_tls_case* c = &env_tls_cases[env_tls_cases_size++];
		c->name = name;
		snprintf(key, sizeof(key), "%s_PORT", prefix);
		c->port = live_port_value(key);
		snprintf(key, sizeof(key), "%s_NAME", prefix);
		c->tls_name = live_value(key);
		snprintf(key, sizeof(key), "%s_CA", prefix);
		c->ca = live_value(key);
		snprintf(key, sizeof(key), "%s_EXPECT", prefix);
		c->accept = env_bool_value(key, "accept", "reject");
		snprintf(key, sizeof(key), "%s_WARNING", prefix);
		c->warning = env_bool_value(key, "true", "false");
		snprintf(key, sizeof(key), "%s_BOOTSTRAP", prefix);
		c->bootstrap = env_optional_value(key);
		snprintf(key, sizeof(key), "%s_REVOKE", prefix);
		c->revoke = env_optional_value(key);
	}
}

PLAN(discovery_env)
{
	live_init("ENV");
	env_name = live_value("ENV_NAME");
	env_host = live_value("ENV_HOST");
	env_ctl = live_value("ENV_CTL");
	env_ns = live_value("ENV_NAMESPACE");

	if (strcmp(env_name, "dns") == 0) {
		env_node_read(&env_a, "NODE_A");
		env_node_read(&env_b, "NODE_B");
		env_node_read(&env_c, "NODE_C");
		env_dns_server = live_value("DNS_SERVER");

		// A records carry no port; the provider gives both hosts NODE_A_PORT.
		if (env_a.port != env_b.port) {
			printf("ENV dns: NODE_B_PORT %u is not NODE_A_PORT %u\n", env_b.port, env_a.port);
			exit(1);
		}

		char* ab[] = {env_a.address, env_b.address};
		live_sort_join(ab, 2, env_ab, sizeof(env_ab));
		as_strncpy(env_only_c, env_c.address, sizeof(env_only_c));
		plan_add(env_dns_suite);
	}
	else if (strcmp(env_name, "collision") == 0) {
		env_node_read(&env_control_a, "CONTROL_NODE_A");
		env_node_read(&env_control_b, "CONTROL_NODE_B");
		env_node_read(&env_collision_a, "COLLISION_NODE_A");
		env_node_read(&env_collision_b, "COLLISION_NODE_B");
		env_node_read_advertised(&env_collision_a, "COLLISION_NODE_A");
		env_node_read_advertised(&env_collision_b, "COLLISION_NODE_B");

		if (strcmp(env_collision_a.advertised, env_collision_b.advertised) != 0) {
			printf("ENV collision: COLLISION_NODE_B advertises %s, not COLLISION_NODE_A's %s\n",
				env_collision_b.advertised, env_collision_a.advertised);
			exit(1);
		}
		plan_add(env_collision_suite);
	}
	else if (strcmp(env_name, "tls-matching") == 0) {
		env_tls_read();
		const char* suite = env_optional_value("DISCOVERY_ENV_SUITE");

		if (strcmp(suite, "n17") == 0) {
			plan_add(env_tls_n17_suite);
		}
		else if (suite[0] == '\0') {
			plan_add(env_tls_suite);
		}
		else {
			printf("ENV tls-matching: DISCOVERY_ENV_SUITE is %s, not n17 or empty\n", suite);
			exit(1);
		}
	}
	else {
		printf("ENV %s: no client test for this environment; run it in dns, collision or tls-matching\n",
			env_name);
		exit(1);
	}
	printf("ENV %s: client %s, host %s, namespace %s\n", env_name, aerospike_client_version, env_host, env_ns);
	plan_after(env_plan_after);
}
