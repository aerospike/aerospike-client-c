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

// Server query run-time guardrails: query-max-run-time-ms and the cap on
// concurrent Top-K jobs. Each test checks what the application sees when a
// guardrail stops a query, and that the client does not retry it. The tests
// skip themselves on servers that do not have the settings.

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_info.h>
#include <aerospike/as_key.h>
#if AS_EVENT_LIB_DEFINED
#include <aerospike/as_monitor.h>
#endif
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_subcode.h>
#include <aerospike/as_val.h>
#include <citrusleaf/cf_clock.h>

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../test.h"
#include "../util/info_helper.h"

//---------------------------------
// Global Variables
//---------------------------------

extern aerospike* as;

#define NAMESPACE "test"
#define SET "query_guardrails"
#define N_RECORDS 400
#define TOP_K 5

// The rate keeps a query running past the limits and timeouts below. With 400
// records, a query runs for about 5 seconds on a single node and about 2 seconds
// on each node of a three-node cluster, against about 50 milliseconds without
// the rate.
#define RPS 2

// The server's run-time limit for a query that the limit must stop.
#define SERVER_RUN_TIME_LIMIT_MS "500"

// The server's run-time limit for the query that holds a Top-K job slot, which
// is how long that query runs. It must be shorter than the query's run time
// (see RPS), so the job cap tests need a single node, or more records on a
// larger cluster.
#define SERVER_BLOCKER_RUN_TIME_LIMIT_MS "3000"

// A client total timeout shorter than the throttled query's run time.
#define TOTAL_TIMEOUT_MS 800

// The server's setting for how long a long query may run, and its namespace
// statistic that counts the long queries that timed out.
#define SERVER_RUN_TIME_LIMIT_NAME "query-max-run-time-ms"
#define SERVER_TIMEOUT_STAT "pi_query_long_basic_timeout"

// The server's setting for how many Top-K queries may run on a node at once.
#define SERVER_JOBS_CAP_NAME "order-by-jobs-limit"

static char g_server_run_time_limit_original[32];
static bool g_server_has_run_time_limit;
static char g_server_jobs_cap_original[32];

//---------------------------------
// Server Configuration
//---------------------------------

static bool
set_server_config_callback(const as_error* err, const as_node* node, const char* req, char* res,
	void* udata)
{
	uint32_t* n_failed = (uint32_t*)udata;
	char* value = NULL;
	as_status status = as_info_parse_single_response(res, &value);

	if (status != AEROSPIKE_OK || strcmp(value, "ok") != 0) {
		error("%s failed on node %s: %s", req, node->name, res);
		(*n_failed)++;
	}
	return true;
}

// Apply a set-config command on every node.
static bool
set_server_config(const char* command)
{
	uint32_t n_failed = 0;
	as_error err;
	as_status status = aerospike_info_foreach(as, &err, NULL, command, set_server_config_callback,
		&n_failed);

	return status == AEROSPIKE_OK && n_failed == 0;
}

static bool
set_server_run_time_limit(const char* value)
{
	char command[128];
	snprintf(command, sizeof(command),
		"set-config:context=namespace;id=" NAMESPACE ";" SERVER_RUN_TIME_LIMIT_NAME "=%s", value);
	return set_server_config(command);
}

static bool
set_server_jobs_cap(const char* value)
{
	char command[128];
	snprintf(command, sizeof(command), "set-config:context=service;" SERVER_JOBS_CAP_NAME "=%s",
		value);
	return set_server_config(command);
}

//---------------------------------
// Server Statistics
//---------------------------------

typedef struct stat_sum_s {
	const char* name;
	uint64_t sum;
} stat_sum;

static bool
sum_server_stat_callback(const as_error* err, const as_node* node, const char* req, char* res,
	void* udata)
{
	stat_sum* ctx = (stat_sum*)udata;
	char* stats = NULL;
	as_status status = as_info_parse_single_response(res, &stats);

	if (status != AEROSPIKE_OK) {
		return true;
	}

	size_t name_len = strlen(ctx->name);
	char* stat = stats;

	while (stat) {
		if (strncmp(stat, ctx->name, name_len) == 0 && stat[name_len] == '=') {
			ctx->sum += strtoull(stat + name_len + 1, NULL, 10);
			break;
		}

		stat = strchr(stat, ';');

		if (stat) {
			stat++;
		}
	}
	return true;
}

// Sum a namespace statistic over every node.
static uint64_t
sum_server_namespace_stat(const char* name)
{
	stat_sum ctx = {.name = name, .sum = 0};
	as_error err;

	aerospike_info_foreach(as, &err, NULL, "namespace/" NAMESPACE, sum_server_stat_callback, &ctx);
	return ctx.sum;
}

static uint32_t
cluster_size(void)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	uint32_t size = nodes->size;
	as_nodes_release(nodes);
	return size;
}

//---------------------------------
// Running Queries
//---------------------------------

typedef enum {
	RUN_QUERY,
	RUN_TOP_K_QUERY,
	RUN_SCAN
} run_kind;

typedef struct run_s {
	// Inputs.
	run_kind kind;
	bool async;
	uint32_t socket_timeout;
	uint32_t total_timeout;
	uint8_t error_detail_verbosity;

	// Outputs.
	as_status status;
	char message[AS_ERROR_MESSAGE_MAX_SIZE];
	uint32_t count;
	uint64_t elapsed_ms;

#if AS_EVENT_LIB_DEFINED
	as_monitor monitor;
#endif
} run;

static void
init_base_policy(as_policy_base* base, const run* r)
{
	// Zero keeps the policy's default socket timeout.
	if (r->socket_timeout > 0) {
		base->socket_timeout = r->socket_timeout;
	}

	base->total_timeout = r->total_timeout;
	base->error_detail_verbosity = r->error_detail_verbosity;
}

static void
init_query(as_query* query, const run* r)
{
	as_query_init(query, NAMESPACE, SET);
	query->records_per_second = RPS;

	if (r->kind == RUN_TOP_K_QUERY) {
		as_query_order_by(query, "score", AS_QUERY_ORDER_BY_INTEGER, AS_ORDER_DESCENDING,
			AS_QUERY_ORDER_BY_FLAGS_DEFAULT);
		as_query_top_k(query, TOP_K);
	}
}

static void
set_error(run* r, const as_error* err)
{
	r->status = err->code;
	snprintf(r->message, sizeof(r->message), "%s", err->message);
}

static bool
count_callback(const as_val* val, void* udata)
{
	if (val) {
		run* r = (run*)udata;
		r->count++;
	}
	return true;
}

static void
run_sync_query(run* r)
{
	as_policy_query policy;
	as_policy_query_init(&policy);
	init_base_policy(&policy.base, r);

	as_query query;
	init_query(&query, r);

	as_error err;
	aerospike_query_foreach(as, &err, &policy, &query, count_callback, r);
	set_error(r, &err);
	as_query_destroy(&query);
}

static void
run_sync_scan(run* r)
{
	as_policy_scan policy;
	as_policy_scan_init(&policy);
	init_base_policy(&policy.base, r);
	policy.records_per_second = RPS;

	as_scan scan;
	as_scan_init(&scan, NAMESPACE, SET);

	as_error err;
	aerospike_scan_foreach(as, &err, &policy, &scan, count_callback, r);
	set_error(r, &err);
	as_scan_destroy(&scan);
}

#if AS_EVENT_LIB_DEFINED
static bool
async_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	run* r = (run*)udata;

	if (err) {
		set_error(r, err);
		as_monitor_notify(&r->monitor);
		return false;
	}

	if (! record) {
		r->status = AEROSPIKE_OK;
		as_monitor_notify(&r->monitor);
		return false;
	}

	r->count++;
	return true;
}

static void
run_async_query(run* r)
{
	as_policy_query policy;
	as_policy_query_init(&policy);
	init_base_policy(&policy.base, r);

	as_query query;
	init_query(&query, r);

	as_monitor_init(&r->monitor);
	as_monitor_begin(&r->monitor);

	as_error err;
	as_status status = aerospike_query_async(as, &err, &policy, &query, async_listener, r, NULL);

	if (status == AEROSPIKE_OK) {
		as_monitor_wait(&r->monitor);
	}
	else {
		set_error(r, &err);
	}

	as_monitor_destroy(&r->monitor);
	as_query_destroy(&query);
}
#endif

static void
run_query(run* r)
{
	uint64_t begin_ms = cf_getms();

	if (r->kind == RUN_SCAN) {
		run_sync_scan(r);
	}
#if AS_EVENT_LIB_DEFINED
	else if (r->async) {
		run_async_query(r);
	}
#endif
	else {
		run_sync_query(r);
	}

	r->elapsed_ms = cf_getms() - begin_ms;
}

static void*
run_query_thread(void* udata)
{
	run_query((run*)udata);
	return NULL;
}

// Run a query that outlasts query-max-run-time-ms. Returns how many long-query
// timeouts the servers counted for it. Each node that ran the query counts its
// own, so a retry would raise the count. A query runs on all nodes at once and
// each one times out, while a scan runs one node at a time and stops at the
// first timeout.
static uint64_t
run_and_count_server_timeouts(run* r)
{
	uint64_t timeouts_before = sum_server_namespace_stat(SERVER_TIMEOUT_STAT);

	run_query(r);

	// A node may count the timeout after it has sent the error.
	as_sleep(500);
	return sum_server_namespace_stat(SERVER_TIMEOUT_STAT) - timeouts_before;
}

// Run a Top-K query while another Top-K query holds the only job slot on every
// node. Returns the status of the query that holds the slot.
static as_status
run_with_jobs_capped(run* refused)
{
	// A Top-K job that an earlier test left behind can take a moment to end.
	as_sleep(1000);

	run blocker = {.kind = RUN_TOP_K_QUERY};
	pthread_t thread;
	int rc = pthread_create(&thread, NULL, run_query_thread, &blocker);

	if (rc != 0) {
		error("pthread_create() returned %d", rc);
		return AEROSPIKE_ERR_CLIENT;
	}

	// Let the blocker take its slot.
	as_sleep(500);

	run_query(refused);
	pthread_join(thread, NULL);
	return blocker.status;
}

//---------------------------------
// Test Preconditions
//---------------------------------

// The run-time limit and the Top-K job cap come together, so a server that has
// the limit but not the cap fails the job cap tests instead of skipping them. A
// server with neither, such as 8.1.2.0, skips every test.
static bool
server_supports_guardrails(void)
{
	if (! g_server_has_run_time_limit) {
		info("skipping guardrail test; the server has no " SERVER_RUN_TIME_LIMIT_NAME);
	}
	return g_server_has_run_time_limit;
}

//---------------------------------
// Run-Time Limit Tests
//---------------------------------

// Each test checks the timeout count that run_and_count_server_timeouts()
// describes, which a retry would raise.

TEST(query_guardrails_query_run_time_limit,
	"a long query stopped by query-max-run-time-ms returns QUERY_TIMEOUT and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_RUN_TIME_LIMIT_MS));

	run r = {.kind = RUN_QUERY};
	uint64_t timeouts = run_and_count_server_timeouts(&r);

	assert_int_eq(r.status, AEROSPIKE_ERR_QUERY_TIMEOUT);
	uint32_t n_nodes = cluster_size();
	assert_int_eq(timeouts, n_nodes);
}

TEST(query_guardrails_top_k_run_time_limit,
	"a Top-K query stopped by query-max-run-time-ms returns QUERY_TIMEOUT, not a partial result, and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_RUN_TIME_LIMIT_MS));

	run r = {.kind = RUN_TOP_K_QUERY};
	uint64_t timeouts = run_and_count_server_timeouts(&r);

	assert_int_eq(r.status, AEROSPIKE_ERR_QUERY_TIMEOUT);
	assert_int_eq(r.count, 0);
	uint32_t n_nodes = cluster_size();
	assert_int_eq(timeouts, n_nodes);
}

TEST(query_guardrails_scan_run_time_limit,
	"a scan stopped by query-max-run-time-ms returns QUERY_TIMEOUT and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_RUN_TIME_LIMIT_MS));

	run r = {.kind = RUN_SCAN};
	uint64_t timeouts = run_and_count_server_timeouts(&r);

	assert_int_eq(r.status, AEROSPIKE_ERR_QUERY_TIMEOUT);
	assert_int_eq(timeouts, 1);
}

#if AS_EVENT_LIB_DEFINED
TEST(query_guardrails_query_run_time_limit_async,
	"an async long query stopped by query-max-run-time-ms returns QUERY_TIMEOUT and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_RUN_TIME_LIMIT_MS));

	run r = {.kind = RUN_QUERY, .async = true};
	uint64_t timeouts = run_and_count_server_timeouts(&r);

	assert_int_eq(r.status, AEROSPIKE_ERR_QUERY_TIMEOUT);
	uint32_t n_nodes = cluster_size();
	assert_int_eq(timeouts, n_nodes);
}

TEST(query_guardrails_top_k_run_time_limit_async,
	"an async Top-K query stopped by query-max-run-time-ms returns QUERY_TIMEOUT, not a partial result, and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_RUN_TIME_LIMIT_MS));

	run r = {.kind = RUN_TOP_K_QUERY, .async = true};
	uint64_t timeouts = run_and_count_server_timeouts(&r);

	assert_int_eq(r.status, AEROSPIKE_ERR_QUERY_TIMEOUT);
	assert_int_eq(r.count, 0);
	uint32_t n_nodes = cluster_size();
	assert_int_eq(timeouts, n_nodes);
}
#endif

//---------------------------------
// Total Timeout Tests
//---------------------------------

// A query that passes its total timeout ends as a timeout, whether the client
// or the server notices first, and does not run on past it. No server run-time
// limit is set, so the total timeout is the only thing that can stop the query.
// A retry that restarted the timeout would run for at least another
// TOTAL_TIMEOUT_MS, so the query must end less than half of that past the total
// timeout.
#define TOTAL_TIMEOUT_MAX_ELAPSED_MS (TOTAL_TIMEOUT_MS * 3 / 2)

TEST(query_guardrails_query_total_timeout,
	"a long query that passes its total timeout returns a timeout without running on")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit("0"));

	run r = {.kind = RUN_QUERY, .total_timeout = TOTAL_TIMEOUT_MS};
	run_query(&r);

	assert_true(r.status == AEROSPIKE_ERR_TIMEOUT || r.status == AEROSPIKE_ERR_QUERY_TIMEOUT);
	assert_true(r.elapsed_ms < TOTAL_TIMEOUT_MAX_ELAPSED_MS);
}

TEST(query_guardrails_top_k_total_timeout,
	"a Top-K query that passes its total timeout returns a timeout without running on")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit("0"));

	run r = {.kind = RUN_TOP_K_QUERY, .total_timeout = TOTAL_TIMEOUT_MS};
	run_query(&r);

	assert_true(r.status == AEROSPIKE_ERR_TIMEOUT || r.status == AEROSPIKE_ERR_QUERY_TIMEOUT);
	assert_int_eq(r.count, 0);
	assert_true(r.elapsed_ms < TOTAL_TIMEOUT_MAX_ELAPSED_MS);
}

//---------------------------------
// Top-K Socket Timeout Tests
//---------------------------------

// A Top-K node sends nothing until it has scanned everything, so a socket
// timeout shorter than the scan ends the query. A retry would wait for another
// socket timeout, so the query must come back less than half of that past the
// first one.
#define SOCKET_TIMEOUT_MS 500
#define SOCKET_TIMEOUT_MAX_ELAPSED_MS (SOCKET_TIMEOUT_MS * 3 / 2)

// The server's run-time limit for a job whose client hung up after its socket
// timeout. Such a job can run for several seconds before the server notices.
// The limit is above the socket timeout, so the client always gives up first,
// and it ends the job soon so that it does not hold a Top-K slot in the tests
// that follow.
#define SERVER_ABANDONED_JOB_RUN_TIME_LIMIT_MS "1000"

TEST(query_guardrails_top_k_socket_timeout,
	"a Top-K query that exceeds its socket timeout returns TIMEOUT and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_ABANDONED_JOB_RUN_TIME_LIMIT_MS));

	run r = {
		.kind = RUN_TOP_K_QUERY,
		.socket_timeout = SOCKET_TIMEOUT_MS
	};
	run_query(&r);

	assert_int_eq(r.status, AEROSPIKE_ERR_TIMEOUT);
	assert_int_eq(r.count, 0);
	assert_true(r.elapsed_ms < SOCKET_TIMEOUT_MAX_ELAPSED_MS);
}

#if AS_EVENT_LIB_DEFINED
TEST(query_guardrails_top_k_socket_timeout_async,
	"an async Top-K query that exceeds its socket timeout returns TIMEOUT and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_ABANDONED_JOB_RUN_TIME_LIMIT_MS));

	run r = {
		.kind = RUN_TOP_K_QUERY,
		.async = true,
		.socket_timeout = SOCKET_TIMEOUT_MS
	};
	run_query(&r);

	assert_int_eq(r.status, AEROSPIKE_ERR_TIMEOUT);
	assert_int_eq(r.count, 0);
	assert_true(r.elapsed_ms < SOCKET_TIMEOUT_MAX_ELAPSED_MS);
}
#endif

//---------------------------------
// Top-K Job Cap Tests
//---------------------------------

TEST(query_guardrails_top_k_jobs_cap,
	"a Top-K query refused by the job cap returns FAIL_FORBIDDEN with the server's reason and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_BLOCKER_RUN_TIME_LIMIT_MS));
	assert_true(set_server_jobs_cap("1"));

	run refused = {
		.kind = RUN_TOP_K_QUERY,
		.error_detail_verbosity = AS_ERROR_DETAIL_MESSAGE
	};
	as_status blocker_status = run_with_jobs_capped(&refused);

	assert_int_eq(blocker_status, AEROSPIKE_ERR_QUERY_TIMEOUT);
	assert_int_eq(refused.status, AEROSPIKE_ERR_FAIL_FORBIDDEN);
	assert_int_eq(refused.count, 0);
	assert_true(strstr(refused.message, "retry later") != NULL);
}

#if AS_EVENT_LIB_DEFINED
TEST(query_guardrails_top_k_jobs_cap_async,
	"an async Top-K query refused by the job cap returns FAIL_FORBIDDEN with the server's reason and is not retried")
{
	if (! server_supports_guardrails()) {
		return;
	}

	assert_true(set_server_run_time_limit(SERVER_BLOCKER_RUN_TIME_LIMIT_MS));
	assert_true(set_server_jobs_cap("1"));

	run refused = {
		.kind = RUN_TOP_K_QUERY,
		.async = true,
		.error_detail_verbosity = AS_ERROR_DETAIL_MESSAGE
	};
	as_status blocker_status = run_with_jobs_capped(&refused);

	assert_int_eq(blocker_status, AEROSPIKE_ERR_QUERY_TIMEOUT);
	assert_int_eq(refused.status, AEROSPIKE_ERR_FAIL_FORBIDDEN);
	assert_int_eq(refused.count, 0);
	assert_true(strstr(refused.message, "retry later") != NULL);
}
#endif

//---------------------------------
// Suite
//---------------------------------

static bool
before(atf_suite* suite)
{
	get_info_field("get-config:context=namespace;id=" NAMESPACE, SERVER_RUN_TIME_LIMIT_NAME,
		g_server_run_time_limit_original, sizeof(g_server_run_time_limit_original));
	g_server_has_run_time_limit = g_server_run_time_limit_original[0] != 0;

	if (! g_server_has_run_time_limit) {
		return true;
	}

	get_info_field("get-config:context=service", SERVER_JOBS_CAP_NAME,
		g_server_jobs_cap_original, sizeof(g_server_jobs_cap_original));

	as_record rec;
	as_record_inita(&rec, 1);

	for (uint32_t i = 0; i < N_RECORDS; i++) {
		as_record_set_int64(&rec, "score", (int64_t)i);

		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		as_error err;
		as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

		if (status != AEROSPIKE_OK) {
			error("aerospike_key_put() returned %d - %s", err.code, err.message);
			as_record_destroy(&rec);
			return false;
		}
	}

	as_record_destroy(&rec);
	return true;
}

static bool
after(atf_suite* suite)
{
	if (! g_server_has_run_time_limit) {
		return true;
	}

	bool restored = set_server_run_time_limit(g_server_run_time_limit_original);

	if (g_server_jobs_cap_original[0] != 0) {
		restored = set_server_jobs_cap(g_server_jobs_cap_original) && restored;
	}

	for (uint32_t i = 0; i < N_RECORDS; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		as_error err;
		aerospike_key_remove(as, &err, NULL, &key);
	}

	return restored;
}

SUITE(query_guardrails, "query run-time guardrail tests")
{
	suite_before(before);
	suite_after(after);

	suite_add(query_guardrails_query_run_time_limit);
	suite_add(query_guardrails_top_k_run_time_limit);
	suite_add(query_guardrails_scan_run_time_limit);
#if AS_EVENT_LIB_DEFINED
	suite_add(query_guardrails_query_run_time_limit_async);
	suite_add(query_guardrails_top_k_run_time_limit_async);
#endif
	suite_add(query_guardrails_query_total_timeout);
	suite_add(query_guardrails_top_k_total_timeout);
	suite_add(query_guardrails_top_k_socket_timeout);
#if AS_EVENT_LIB_DEFINED
	suite_add(query_guardrails_top_k_socket_timeout_async);
#endif
	suite_add(query_guardrails_top_k_jobs_cap);
#if AS_EVENT_LIB_DEFINED
	suite_add(query_guardrails_top_k_jobs_cap_async);
#endif
}
