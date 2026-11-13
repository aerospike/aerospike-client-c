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
 * The client side of two test environments of aerospike-client-conformance (envs/README.md
 * there), selected by ENV_NAME from the environment's values file:
 *
 *   dns        N-4: DNS seed discovery with A and SRV records in thread and tend mode, a DNS
 *              outage, and a changed SRV record.
 *   collision  N-18: a node translated to a live node's address under pass-through, static-map
 *              and port-map is never created, and the live node serves.
 *
 * src/test/discovery_env/run.sh builds and runs it with the values loaded. Verbs that change the
 * environment run as "$ENV_CTL" <verb>, and every change is undone before the next case. The exit
 * status is non-zero when a case fails, a value is missing or an undo did not complete. Built
 * against installed headers only by "make discovery-env"; POSIX only.
 */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config.h>
#include <aerospike/as_discovery.h>
#include <aerospike/as_host.h>
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
	else {
		printf("ENV %s: no client test for this environment; run it in dns or collision\n", env_name);
		exit(1);
	}
	printf("ENV %s: client %s, host %s, namespace %s\n", env_name, aerospike_client_version, env_host, env_ns);
	plan_after(env_plan_after);
}
