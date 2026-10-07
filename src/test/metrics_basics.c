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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_key.h>
#include <aerospike/as_metrics.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>

#include <citrusleaf/alloc.h>

#include <stdio.h>
#include <string.h>

#if defined(_MSC_VER)
#include <windows.h>
#else
#include <dirent.h>
#include <unistd.h>
#endif

#include "test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

typedef struct metrics_test_exporter_s {
	as_metrics_exporter base;
	uint32_t calls;
	uint32_t enabled_calls;
	bool fail;
} metrics_test_exporter;

static as_status metrics_test_export(
	as_metrics_exporter* exporter, as_error* err, const as_metrics_snapshot* snapshot);

typedef struct metrics_listener_counts_s {
	int enable;
	int disable;
	int snapshot;
	int node_close;
} metrics_listener_counts;

static void
metrics_disable(void)
{
	as_error err;
	aerospike_disable_metrics(as, &err);
}

static void
metrics_policy_init_off(as_metrics_policy* policy)
{
	as_metrics_policy_init(policy);
	// Empty report_dir installs no file exporter. The policy default is ".".
	policy->report_dir[0] = '\0';
}

static as_status
metrics_enable(const as_metrics_policy* policy, as_error* err)
{
	metrics_disable();
	as_error_reset(err);
	return aerospike_enable_metrics(as, err, policy);
}

static as_status
metrics_put(as_error* err, const char* key_name)
{
	as_key key;
	as_key_init_str(&key, "test", "metrics", key_name);

	as_record rec;
	as_record_init(&rec, 1);
	as_record_set_int64(&rec, "v", 1);

	as_status status = aerospike_key_put(as, err, NULL, &key, &rec);
	as_record_destroy(&rec);
	as_key_destroy(&key);
	return status;
}

static metrics_test_exporter*
metrics_exporter_new(bool fail)
{
	metrics_test_exporter* exporter = cf_calloc(1, sizeof(metrics_test_exporter));
	exporter->base.export_fn = metrics_test_export;
	exporter->fail = fail;
	return exporter;
}

static as_status
metrics_test_export(
	as_metrics_exporter* exporter, as_error* err, const as_metrics_snapshot* snapshot)
{
	metrics_test_exporter* self = (metrics_test_exporter*)exporter;
	as_incr_uint32(&self->calls);

	if (snapshot && snapshot->metrics_enabled) {
		as_incr_uint32(&self->enabled_calls);
	}

	if (self->fail) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "metrics test exporter failed");
	}
	return AEROSPIKE_OK;
}

static as_status
metrics_on_enable(as_error* err, void* udata)
{
	(void)err;
	((metrics_listener_counts*)udata)->enable++;
	return AEROSPIKE_OK;
}

static as_status
metrics_on_disable(as_error* err, struct as_cluster_s* cluster, void* udata)
{
	(void)err;
	(void)cluster;
	((metrics_listener_counts*)udata)->disable++;
	return AEROSPIKE_OK;
}

static as_status
metrics_on_snapshot(as_error* err, struct as_cluster_s* cluster, void* udata)
{
	(void)err;
	(void)cluster;
	((metrics_listener_counts*)udata)->snapshot++;
	return AEROSPIKE_OK;
}

static as_status
metrics_on_node_close(as_error* err, struct as_node_s* node, void* udata)
{
	(void)err;
	(void)node;
	((metrics_listener_counts*)udata)->node_close++;
	return AEROSPIKE_OK;
}

static bool
metrics_create_temp_dir_path(char* out_path, size_t out_path_size)
{
#if defined(_MSC_VER)
	char tmp[MAX_PATH];
	char file[MAX_PATH];

	if (GetTempPathA(MAX_PATH, tmp) == 0 || GetTempFileNameA(tmp, "asm", 0, file) == 0) {
		return false;
	}

	DeleteFileA(file);

	if (!CreateDirectoryA(file, NULL)) {
		return false;
	}

	snprintf(out_path, out_path_size, "%s", file);
	return true;
#else
	if (out_path_size < sizeof("/tmp/as-metrics-XXXXXX")) {
		return false;
	}

	snprintf(out_path, out_path_size, "/tmp/as-metrics-XXXXXX");
	return mkdtemp(out_path) != NULL;
#endif
}

static bool
metrics_is_log_filename(const char* filename)
{
	size_t n = strlen(filename);

	// metrics-YYYYMMDDHHMMSS.log
	if (n != 26 || strncmp(filename, "metrics-", 8) != 0 || strcmp(filename + 22, ".log") != 0) {
		return false;
	}

	for (int i = 8; i < 22; i++) {
		if (filename[i] < '0' || filename[i] > '9') {
			return false;
		}
	}
	return true;
}

static int
metrics_count_logs(const char* dir)
{
	int count = 0;

#if defined(_MSC_VER)
	char pattern[MAX_PATH];
	WIN32_FIND_DATAA data;

	snprintf(pattern, sizeof(pattern), "%s\\metrics-*.log", dir);
	HANDLE handle = FindFirstFileA(pattern, &data);

	if (handle == INVALID_HANDLE_VALUE) {
		return 0;
	}

	do {
		if (metrics_is_log_filename(data.cFileName)) {
			count++;
		}
	} while (FindNextFileA(handle, &data));

	FindClose(handle);
#else
	DIR* directory = opendir(dir);

	if (!directory) {
		return -1;
	}

	struct dirent* entry;

	while ((entry = readdir(directory)) != NULL) {
		if (metrics_is_log_filename(entry->d_name)) {
			count++;
		}
	}
	closedir(directory);
#endif
	return count;
}

static bool
metrics_read_header(const char* dir, char* header, size_t header_size)
{
	header[0] = '\0';

#if defined(_MSC_VER)
	char pattern[MAX_PATH];
	WIN32_FIND_DATAA data;

	snprintf(pattern, sizeof(pattern), "%s\\metrics-*.log", dir);
	HANDLE handle = FindFirstFileA(pattern, &data);

	if (handle == INVALID_HANDLE_VALUE) {
		return false;
	}

	bool found = false;

	do {
		if (!metrics_is_log_filename(data.cFileName)) {
			continue;
		}

		char path[MAX_PATH];
		snprintf(path, sizeof(path), "%s\\%s", dir, data.cFileName);
		FILE* file = fopen(path, "r");

		if (file && fgets(header, (int)header_size, file)) {
			found = true;
		}

		if (file) {
			fclose(file);
		}
		break;
	} while (FindNextFileA(handle, &data));

	FindClose(handle);
	return found;
#else
	DIR* directory = opendir(dir);

	if (!directory) {
		return false;
	}

	bool found = false;
	struct dirent* entry;

	while ((entry = readdir(directory)) != NULL) {
		if (!metrics_is_log_filename(entry->d_name)) {
			continue;
		}

		char path[1024];
		snprintf(path, sizeof(path), "%s/%s", dir, entry->d_name);
		FILE* file = fopen(path, "r");

		if (file && fgets(header, (int)header_size, file)) {
			found = true;
		}

		if (file) {
			fclose(file);
		}
		break;
	}
	closedir(directory);
	return found;
#endif
}

static void
metrics_remove_dir(const char* dir)
{
#if defined(_MSC_VER)
	char pattern[MAX_PATH];
	WIN32_FIND_DATAA data;

	snprintf(pattern, sizeof(pattern), "%s\\*", dir);
	HANDLE handle = FindFirstFileA(pattern, &data);

	if (handle != INVALID_HANDLE_VALUE) {
		do {
			if (strcmp(data.cFileName, ".") == 0 || strcmp(data.cFileName, "..") == 0) {
				continue;
			}

			char path[MAX_PATH];
			snprintf(path, sizeof(path), "%s\\%s", dir, data.cFileName);
			DeleteFileA(path);
		} while (FindNextFileA(handle, &data));

		FindClose(handle);
	}
	RemoveDirectoryA(dir);
#else
	DIR* directory = opendir(dir);

	if (directory) {
		struct dirent* entry;

		while ((entry = readdir(directory)) != NULL) {
			if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
				continue;
			}

			char path[1024];
			snprintf(path, sizeof(path), "%s/%s", dir, entry->d_name);
			unlink(path);
		}
		closedir(directory);
	}
	rmdir(dir);
#endif
}

static bool
metrics_identity_ok(const as_metrics_snapshot* snap)
{
	if (!snap->cluster_name || !snap->client_type || !snap->client_version || !snap->app_id) {
		return false;
	}

	if (strcmp(snap->client_type, "c") != 0 || snap->client_version[0] == '\0' ||
			snap->app_id[0] == '\0') {
		return false;
	}

	// Local timestamp is "YYYY-MM-DD HH:MM:SS".
	if (strlen(snap->timestamp) != 19 || snap->timestamp[4] != '-' || snap->timestamp[7] != '-' ||
			snap->timestamp[10] != ' ' || snap->timestamp[13] != ':' || snap->timestamp[16] != ':') {
		return false;
	}

	// On-demand snapshots do not attach nodes removed since the last periodic export.
	return snap->nodes_departed_count == 0 && snap->nodes_departed == NULL;
}

static bool
metrics_nodes_ok(const as_metrics_snapshot* snap)
{
	if (snap->nodes_count == 0 || !snap->nodes) {
		return false;
	}

	bool pools = false;

	for (uint32_t i = 0; i < snap->nodes_count; i++) {
		as_metrics_node_snapshot* node = snap->nodes[i];

		if (!node || !node->name || node->name[0] == '\0' || !node->address || node->address[0] == '\0' ||
				node->port == 0) {
			return false;
		}

		if (node->sync.opened > 0 || node->sync.in_pool > 0 || node->sync.in_use > 0) {
			pools = true;
		}
	}
	return pools;
}

static bool
metrics_conn_failures_zero(const as_metrics_snapshot* snap)
{
	for (uint32_t i = 0; i < snap->nodes_count; i++) {
		as_metrics_node_snapshot* node = snap->nodes[i];

		if (node->conn_open_failures != 0 || node->conn_tls_handshake_failures != 0 ||
				node->conn_auth_failures != 0) {
			return false;
		}
	}
	return true;
}

static bool
metrics_namespaces_empty(const as_metrics_snapshot* snap)
{
	for (uint32_t i = 0; i < snap->nodes_count; i++) {
		if (snap->nodes[i]->namespace_count != 0) {
			return false;
		}
	}
	return true;
}

static uint64_t
metrics_write_latency_total(const as_metrics_snapshot* snap)
{
	uint64_t total = 0;

	for (uint32_t i = 0; i < snap->nodes_count; i++) {
		as_metrics_node_snapshot* node = snap->nodes[i];

		for (uint32_t j = 0; j < node->namespace_count; j++) {
			as_metrics_namespace_snapshot* ns = &node->namespaces[j];

			if (!ns->name || strcmp(ns->name, "test") != 0) {
				continue;
			}

			as_metrics_latency_snapshot* latency = &ns->latencies[AS_LATENCY_TYPE_WRITE];

			for (uint8_t k = 0; k < latency->bucket_count; k++) {
				total += latency->buckets[k];
			}
		}
	}
	return total;
}

static bool
metrics_test_namespace_ok(const as_metrics_snapshot* snap, uint8_t bucket_count)
{
	bool found = false;

	for (uint32_t i = 0; i < snap->nodes_count; i++) {
		as_metrics_node_snapshot* node = snap->nodes[i];

		for (uint32_t j = 0; j < node->namespace_count; j++) {
			as_metrics_namespace_snapshot* ns = &node->namespaces[j];

			if (!ns->name || strcmp(ns->name, "test") != 0) {
				continue;
			}

			found = true;
			as_metrics_latency_snapshot* latency = &ns->latencies[AS_LATENCY_TYPE_WRITE];

			if (latency->type != AS_LATENCY_TYPE_WRITE || latency->bucket_count != bucket_count ||
					!latency->buckets) {
				return false;
			}
		}
	}
	return found;
}

static bool
metrics_header_ok(const char* header, const char* latency_token)
{
	if (!strstr(header, " header(2) ")) {
		return false;
	}

	const char* required[] = {
		"client_type",
		"client_version",
		"app_id",
		"invalid_node_count",
		"command_count",
		"retry_count",
		"delay_queue_timeout_count",
		"eventloop",
		"process_size",
		"queue_size",
		"sync_conn",
		"in_use",
		"in_pool",
		"key_busy",
		"bytes_in",
		"bytes_out",
		latency_token,
		NULL
	};

	for (int i = 0; required[i]; i++) {
		if (!strstr(header, required[i])) {
			return false;
		}
	}

	// Field names in the file are snake_case. eventloop stays a single lowercase word.
	const char* rejected[] = {
		"clientType",
		"keyBusy",
		"bytesIn",
		"bytesOut",
		"eventLoop",
		NULL
	};

	for (int i = 0; rejected[i]; i++) {
		if (strstr(header, rejected[i])) {
			return false;
		}
	}
	return true;
}

static bool
metrics_suite_cleanup(atf_suite* suite)
{
	(void)suite;
	metrics_disable();
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(metrics_policy_defaults, "metrics policy defaults leave operational and usage off")
{
	as_metrics_policy policy;
	as_metrics_policy_init(&policy);

	assert_false(policy.operational_enabled);
	assert_false(policy.usage_enabled);
	assert_false(policy.enable);
	assert_int_eq(policy.interval, 30);
	assert_int_eq(policy.latency_columns, 7);
	assert_int_eq(policy.latency_shift, 1);
	assert_int_eq(policy.latency_unit, AS_METRICS_LATENCY_MILLISECONDS);
	assert_string_eq(policy.report_dir, ".");
	assert_null(policy.labels);
	assert_null(policy.exporters);
	assert_null(policy.metrics_listeners.enable_listener);
	assert_null(policy.metrics_listeners.snapshot_listener);
	assert_null(policy.metrics_listeners.node_close_listener);
	assert_null(policy.metrics_listeners.disable_listener);

	as_metrics_policy_destroy(&policy);
}

TEST(metrics_snapshot_without_cluster, "on-demand snapshot requires a connected cluster")
{
	as_config config;
	as_config_init(&config);

	aerospike client;
	aerospike_init(&client, &config);

	as_error err;
	as_metrics_snapshot* snap = NULL;
	as_status status = aerospike_get_metrics_snapshot(&client, &err, &snap);

	assert_int_eq(status, AEROSPIKE_ERR_CLIENT);
	assert_null(snap);

	aerospike_destroy(&client);
}

TEST(metrics_snapshot_before_enable, "on-demand snapshot works while periodic export is off")
{
	metrics_disable();

	as_error err;
	as_metrics_snapshot* snap = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &snap), AEROSPIKE_OK);
	assert_not_null(snap);

	assert_false(snap->metrics_enabled);
	assert_false(snap->operational_metrics_enabled);
	assert_false(snap->usage_metrics_enabled);
	assert_int_eq(snap->latency_columns, 0);
	assert_int_eq(snap->latency_shift, 0);
	assert_int_eq(snap->latency_unit, AS_METRICS_LATENCY_MILLISECONDS);
	assert_int_eq(snap->cpu, 0);
	assert_int_eq((int64_t)snap->mem, 0);
	assert_int_eq(snap->label_count, 0);
	assert_int_eq(snap->event_loop_count, as_event_loop_size);
	assert_true(metrics_identity_ok(snap));
	assert_true(metrics_nodes_ok(snap));
	assert_true(metrics_conn_failures_zero(snap));
	assert_true(metrics_namespaces_empty(snap));

	if (as_event_loop_size > 0) {
		assert_not_null(snap->event_loops);
	}
	else {
		assert_null(snap->event_loops);
	}

	as_metrics_snapshot_destroy(snap);
}

TEST(metrics_enable_leaves_operational_off, "metrics.enabled does not turn on operational or usage")
{
	as_metrics_policy policy;
	metrics_policy_init_off(&policy);

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);

	assert_int_eq(metrics_put(&err, "metrics-master-only"), AEROSPIKE_OK);

	as_metrics_snapshot* snap = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &snap), AEROSPIKE_OK);
	assert_true(snap->metrics_enabled);
	assert_false(snap->operational_metrics_enabled);
	assert_false(snap->usage_metrics_enabled);
	assert_int_eq(snap->latency_columns, 7);
	assert_int_eq(snap->latency_shift, 1);
	assert_int_eq(snap->latency_unit, AS_METRICS_LATENCY_MILLISECONDS);
	assert_int_eq(snap->cpu, 0);
	assert_int_eq((int64_t)snap->mem, 0);
	assert_true(metrics_namespaces_empty(snap));
	assert_true(metrics_conn_failures_zero(snap));
	assert_true(metrics_nodes_ok(snap));
	as_metrics_snapshot_destroy(snap);

	metrics_disable();

	// Pull stays available after periodic export stops. Counters are not reset.
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &snap), AEROSPIKE_OK);
	assert_false(snap->metrics_enabled);
	assert_false(snap->operational_metrics_enabled);
	assert_true(metrics_nodes_ok(snap));
	assert_true(snap->command_count > 0);
	as_metrics_snapshot_destroy(snap);

	as_metrics_policy_destroy(&policy);
}

TEST(metrics_operational_namespace_latency, "operational metrics copy namespace histograms and process memory")
{
	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	policy.operational_enabled = true;

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);
	assert_int_eq(metrics_put(&err, "metrics-operational-1"), AEROSPIKE_OK);

	as_metrics_snapshot* first = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &first), AEROSPIKE_OK);
	assert_true(first->metrics_enabled);
	assert_true(first->operational_metrics_enabled);
	assert_false(first->usage_metrics_enabled);
	assert_true(first->mem > 0);
	assert_true(metrics_test_namespace_ok(first, 7));
	assert_true(metrics_conn_failures_zero(first));

	uint64_t before = metrics_write_latency_total(first);
	assert_true(before > 0);
	as_metrics_snapshot_destroy(first);

	assert_int_eq(metrics_put(&err, "metrics-operational-2"), AEROSPIKE_OK);

	as_metrics_snapshot* second = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &second), AEROSPIKE_OK);
	assert_true(metrics_write_latency_total(second) > before);
	as_metrics_snapshot_destroy(second);

	metrics_disable();
	as_metrics_policy_destroy(&policy);
}

TEST(metrics_latency_unit_microseconds, "latency_unit microseconds is stored on the snapshot")
{
	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	policy.operational_enabled = true;
	policy.latency_unit = AS_METRICS_LATENCY_MICROSECONDS;

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);
	assert_int_eq(metrics_put(&err, "metrics-microseconds"), AEROSPIKE_OK);

	as_metrics_snapshot* snap = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &snap), AEROSPIKE_OK);
	assert_int_eq(snap->latency_unit, AS_METRICS_LATENCY_MICROSECONDS);
	assert_int_eq(snap->latency_columns, 7);
	assert_int_eq(snap->latency_shift, 1);
	assert_true(metrics_test_namespace_ok(snap, 7));
	assert_true(metrics_write_latency_total(snap) > 0);
	as_metrics_snapshot_destroy(snap);

	metrics_disable();
	as_metrics_policy_destroy(&policy);
}

TEST(metrics_labels, "static labels are copied onto the snapshot")
{
	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	as_metrics_policy_add_label(&policy, "region", "us-west");

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);

	as_metrics_snapshot* snap = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &snap), AEROSPIKE_OK);
	assert_int_eq(snap->label_count, 1);
	assert_not_null(snap->labels);
	assert_string_eq(snap->labels[0].name, "region");
	assert_string_eq(snap->labels[0].value, "us-west");
	as_metrics_snapshot_destroy(snap);

	metrics_disable();
	as_metrics_policy_destroy(&policy);
}

TEST(metrics_file_header, "report_dir installs a snake_case metrics log")
{
	char dir[256];
	assert_true(metrics_create_temp_dir_path(dir, sizeof(dir)));

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	as_metrics_policy_set_report_dir(&policy, dir);

	as_error err;
	as_status status = metrics_enable(&policy, &err);
	int logs = metrics_count_logs(dir);

	metrics_disable();

	char header[2048];
	bool header_ok = metrics_read_header(dir, header, sizeof(header)) &&
		metrics_header_ok(header, "latency(milliseconds,7,1)");
	metrics_remove_dir(dir);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(logs, 1);
	assert_true(header_ok);
	as_metrics_policy_destroy(&policy);
}

TEST(metrics_file_header_microseconds, "file header records latency_unit microseconds")
{
	char dir[256];
	assert_true(metrics_create_temp_dir_path(dir, sizeof(dir)));

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	policy.latency_unit = AS_METRICS_LATENCY_MICROSECONDS;
	as_metrics_policy_set_report_dir(&policy, dir);

	as_error err;
	as_status status = metrics_enable(&policy, &err);

	metrics_disable();

	char header[2048];
	bool header_ok = metrics_read_header(dir, header, sizeof(header)) &&
		metrics_header_ok(header, "latency(microseconds,7,1)");
	metrics_remove_dir(dir);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(header_ok);
	as_metrics_policy_destroy(&policy);
}

TEST(metrics_empty_report_dir, "empty report_dir does not install the file exporter")
{
	char dir[256];
	char previous[1024];

	assert_true(metrics_create_temp_dir_path(dir, sizeof(dir)));
#if defined(_MSC_VER)
	assert_true(GetCurrentDirectoryA(sizeof(previous), previous) > 0);
	assert_true(SetCurrentDirectoryA(dir));
#else
	assert_true(getcwd(previous, sizeof(previous)) != NULL);
	assert_int_eq(chdir(dir), 0);
#endif

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);

	as_error err;
	as_status status = metrics_enable(&policy, &err);
	int logs = metrics_count_logs(".");

	metrics_disable();
	as_metrics_policy_destroy(&policy);

#if defined(_MSC_VER)
	SetCurrentDirectoryA(previous);
#else
	assert_int_eq(chdir(previous), 0);
#endif
	metrics_remove_dir(dir);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(logs, 0);
}

TEST(metrics_exporter_suppresses_file, "a registered exporter receives the snapshot and skips report_dir")
{
	char dir[256];
	assert_true(metrics_create_temp_dir_path(dir, sizeof(dir)));

	metrics_test_exporter* exporter = metrics_exporter_new(false);

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	as_metrics_policy_set_report_dir(&policy, dir);
	as_metrics_policy_add_exporter(&policy, &exporter->base);

	as_error err;
	as_status status = metrics_enable(&policy, &err);
	uint32_t calls_before_disable = as_load_uint32(&exporter->calls);

	metrics_disable();

	uint32_t calls = as_load_uint32(&exporter->calls);
	int logs = metrics_count_logs(dir);
	metrics_remove_dir(dir);
	as_metrics_policy_destroy(&policy);
	cf_free(exporter);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(calls_before_disable, 0);
	assert_int_eq(calls, 1);
	assert_int_eq(logs, 0);
}

TEST(metrics_exporter_isolation, "one exporter failure does not skip the others")
{
	metrics_test_exporter* failing = metrics_exporter_new(true);
	metrics_test_exporter* healthy = metrics_exporter_new(false);

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	as_metrics_policy_add_exporter(&policy, &failing->base);
	as_metrics_policy_add_exporter(&policy, &healthy->base);

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);

	// Disable force-exports every exporter. A failure is isolated to that exporter.
	assert_int_eq(aerospike_disable_metrics(as, &err), AEROSPIKE_OK);
	assert_int_eq(as_load_uint32(&failing->calls), 1);
	assert_int_eq(as_load_uint32(&healthy->calls), 1);

	as_metrics_policy_destroy(&policy);
	cf_free(failing);
	cf_free(healthy);
}

TEST(metrics_exporter_suspend, "an exporter is suspended after consecutive failures")
{
	metrics_test_exporter* failing = metrics_exporter_new(true);
	metrics_test_exporter* healthy = metrics_exporter_new(false);

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	policy.interval = 1;
	as_metrics_policy_add_exporter(&policy, &failing->base);
	as_metrics_policy_add_exporter(&policy, &healthy->base);

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);

	// interval 1 sleeps one tend interval (1s). Three failures suspend the exporter
	// for three further cycles, while the healthy exporter keeps running.
	as_sleep(8000);
	metrics_disable();

	// Disable joins the metrics thread, so these counts are stable.
	uint32_t failing_calls = as_load_uint32(&failing->calls);
	uint32_t healthy_calls = as_load_uint32(&healthy->calls);
	as_metrics_policy_destroy(&policy);
	cf_free(failing);
	cf_free(healthy);

	assert_true(failing_calls >= 3);
	assert_true(healthy_calls >= 4);
	assert_true(healthy_calls > failing_calls);
}

TEST(metrics_periodic_export_stops_on_disable, "disable stops the export thread and still exports once")
{
	metrics_test_exporter* exporter = metrics_exporter_new(false);

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	policy.interval = 1;
	as_metrics_policy_add_exporter(&policy, &exporter->base);

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);

	as_sleep(3000);
	assert_true(as_load_uint32(&exporter->enabled_calls) >= 1);

	uint32_t calls = as_load_uint32(&exporter->calls);
	assert_int_eq(aerospike_disable_metrics(as, &err), AEROSPIKE_OK);

	uint32_t after_disable = as_load_uint32(&exporter->calls);
	assert_true(after_disable > calls);

	as_sleep(1500);
	assert_int_eq(as_load_uint32(&exporter->calls), after_disable);

	as_metrics_policy_destroy(&policy);
	cf_free(exporter);
}

TEST(metrics_invalid_report_dir, "file exporter open failure leaves metrics disabled")
{
	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
#if defined(_MSC_VER)
	as_metrics_policy_set_report_dir(&policy, "C:\\no\\such\\aerospike-metrics-dir");
#else
	as_metrics_policy_set_report_dir(&policy, "/no/such/aerospike-metrics-dir");
#endif

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_ERR_CLIENT);

	as_metrics_snapshot* snap = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &snap), AEROSPIKE_OK);
	assert_false(snap->metrics_enabled);
	as_metrics_snapshot_destroy(snap);

	as_metrics_policy_destroy(&policy);
}

TEST(metrics_listeners_require_all_callbacks, "a partial metrics listener is rejected")
{
	metrics_listener_counts counts;
	memset(&counts, 0, sizeof(counts));

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	policy.metrics_listeners.enable_listener = metrics_on_enable;
	policy.metrics_listeners.udata = &counts;

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_ERR_PARAM);
	assert_int_eq(counts.enable, 0);

	as_metrics_policy_set_listeners(&policy, metrics_on_enable, metrics_on_disable,
		metrics_on_node_close, metrics_on_snapshot, NULL);
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_ERR_PARAM);

	as_metrics_snapshot* snap = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &snap), AEROSPIKE_OK);
	assert_false(snap->metrics_enabled);
	as_metrics_snapshot_destroy(snap);

	as_metrics_policy_destroy(&policy);
}

TEST(metrics_deprecated_listeners, "deprecated listeners still run on enable and disable")
{
	char dir[256];
	assert_true(metrics_create_temp_dir_path(dir, sizeof(dir)));

	metrics_listener_counts counts;
	memset(&counts, 0, sizeof(counts));

	as_metrics_policy policy;
	metrics_policy_init_off(&policy);
	as_metrics_policy_set_report_dir(&policy, dir);
	as_metrics_policy_set_listeners(&policy, metrics_on_enable, metrics_on_disable,
		metrics_on_node_close, metrics_on_snapshot, &counts);

	as_error err;
	as_status status = metrics_enable(&policy, &err);
	int enable_calls = counts.enable;

	metrics_disable();

	int logs = metrics_count_logs(dir);
	int disable_calls = counts.disable;
	metrics_remove_dir(dir);
	as_metrics_policy_destroy(&policy);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(enable_calls, 1);
	assert_int_eq(disable_calls, 1);
	assert_int_eq(logs, 0);
}

TEST(metrics_command_count_is_cumulative, "command_count grows while enabled and stays after disable")
{
	as_metrics_policy policy;
	metrics_policy_init_off(&policy);

	as_error err;
	assert_int_eq(metrics_enable(&policy, &err), AEROSPIKE_OK);

	as_metrics_snapshot* before = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &before), AEROSPIKE_OK);
	uint64_t count = before->command_count;
	as_metrics_snapshot_destroy(before);

	assert_int_eq(metrics_put(&err, "metrics-command-count"), AEROSPIKE_OK);

	as_metrics_snapshot* after = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &after), AEROSPIKE_OK);
	assert_true(after->command_count > count);
	uint64_t enabled_count = after->command_count;
	as_metrics_snapshot_destroy(after);

	metrics_disable();
	assert_int_eq(metrics_put(&err, "metrics-command-count-off"), AEROSPIKE_OK);

	as_metrics_snapshot* frozen = NULL;
	assert_int_eq(aerospike_get_metrics_snapshot(as, &err, &frozen), AEROSPIKE_OK);
	assert_false(frozen->metrics_enabled);
	assert_true(frozen->command_count == enabled_count);
	as_metrics_snapshot_destroy(frozen);

	as_metrics_policy_destroy(&policy);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(metrics_basics, "metrics snapshot and exporter tests")
{
	suite_before(metrics_suite_cleanup);
	suite_after(metrics_suite_cleanup);

	suite_add(metrics_policy_defaults);
	suite_add(metrics_snapshot_without_cluster);
	suite_add(metrics_snapshot_before_enable);
	suite_add(metrics_enable_leaves_operational_off);
	suite_add(metrics_operational_namespace_latency);
	suite_add(metrics_latency_unit_microseconds);
	suite_add(metrics_labels);
	suite_add(metrics_file_header);
	suite_add(metrics_file_header_microseconds);
	suite_add(metrics_empty_report_dir);
	suite_add(metrics_exporter_suppresses_file);
	suite_add(metrics_exporter_isolation);
	suite_add(metrics_exporter_suspend);
	suite_add(metrics_periodic_export_stops_on_disable);
	suite_add(metrics_invalid_report_dir);
	suite_add(metrics_listeners_require_all_callbacks);
	suite_add(metrics_deprecated_listeners);
	suite_add(metrics_command_count_is_cumulative);
}
