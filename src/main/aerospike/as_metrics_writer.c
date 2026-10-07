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
#include <aerospike/as_metrics_writer.h>
#include <aerospike/as_latency.h>
#include <aerospike/as_string.h>
#include <aerospike/as_string_builder.h>

#include <citrusleaf/alloc.h>

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

//---------------------------------
// Macros
//---------------------------------

#define MIN_FILE_SIZE 1000000

#ifdef _MSC_VER
static char as_dir_sep = '\\';
#else
static char as_dir_sep = '/';
#endif

//---------------------------------
// Types
//---------------------------------

struct as_metrics_file_exporter_s {
	as_metrics_exporter base;
	char report_dir[256];
	FILE* file;
	as_vector* labels;
	as_metrics_cpu_state* cpu;
	uint64_t max_size;
	uint64_t size;
	as_metrics_latency_unit latency_unit;
	uint8_t latency_columns;
	uint8_t latency_shift;
	bool legacy_active;
};

//---------------------------------
// Static Functions
//---------------------------------

static as_status
as_metrics_file_export(as_metrics_exporter* exporter, as_error* err, const as_metrics_snapshot* snapshot);

static struct tm*
as_metrics_localtime(const time_t* now, struct tm* out)
{
#if defined(_MSC_VER)
	return localtime_s(out, now) == 0 ? out : NULL;
#else
	return localtime_r(now, out);
#endif
}

static void
timestamp_to_string(char* str, size_t str_size)
{
	time_t now = time(NULL);
	struct tm storage;
	struct tm* local = as_metrics_localtime(&now, &storage);

	if (!local) {
		snprintf(str, str_size, "0000-00-00 00:00:00");
		return;
	}

	snprintf(str, str_size,
		"%4d-%02d-%02d %02d:%02d:%02d",
		1900 + local->tm_year, local->tm_mon + 1, local->tm_mday,
		local->tm_hour, local->tm_min, local->tm_sec);
}

static void
timestamp_to_string_filename(char* str, size_t str_size)
{
	time_t now = time(NULL);
	struct tm storage;
	struct tm* local = as_metrics_localtime(&now, &storage);

	if (!local) {
		snprintf(str, str_size, "00000000000000");
		return;
	}

	snprintf(str, str_size,
		"%4d%02d%02d%02d%02d%02d",
		1900 + local->tm_year, local->tm_mon + 1, local->tm_mday,
		local->tm_hour, local->tm_min, local->tm_sec);
}

static as_status
as_metrics_open_writer(as_metrics_file_exporter* mw, as_error* err);

static as_status
as_metrics_write_line(as_metrics_file_exporter* mw, const char* data, as_error* err)
{
	int written = fprintf(mw->file, "%s", data);

	if (written <= 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Failed to write metrics data: %d,%s", written, mw->report_dir);
	}
	mw->size += written;

	if (mw->max_size > 0 && mw->size >= mw->max_size) {
		uint32_t result = fclose(mw->file);
		mw->file = NULL;

		if (result != 0) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"File stream did not close successfully: %s", mw->report_dir);
		}
		return as_metrics_open_writer(mw, err);
	}

	return AEROSPIKE_OK;
}

static as_status
as_metrics_open_writer(as_metrics_file_exporter* mw, as_error* err)
{
	as_error_reset(err);

	if (mw->report_dir[0] == '\0') {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Metrics report_dir is empty");
	}

	char now_file_str[128];
	timestamp_to_string_filename(now_file_str, sizeof(now_file_str));

	as_string_builder file_name;
	as_string_builder_inita(&file_name, 256, false);
	as_string_builder_append(&file_name, mw->report_dir);
	char last_char = mw->report_dir[strlen(mw->report_dir) - 1];

	if (last_char != '/' && last_char != '\\') {
		as_string_builder_append_char(&file_name, as_dir_sep);
	}
	as_string_builder_append(&file_name, "metrics-");
	as_string_builder_append(&file_name, now_file_str);
	as_string_builder_append(&file_name, ".log");
	mw->file = fopen(file_name.data, "w");

	if (!mw->file) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to open file: %s", file_name.data);
	}

	mw->size = 0;
	char now_str[128];
	timestamp_to_string(now_str, sizeof(now_str));

	const char* latency_unit = mw->latency_unit == AS_METRICS_LATENCY_MICROSECONDS ?
		"microseconds" : "milliseconds";
	char data[1024];
	int rv = snprintf(data, sizeof(data), "%s header(2) cluster[name,client_type,client_version,app_id,label[],cpu,mem,invalid_node_count,command_count,retry_count,delay_queue_timeout_count,eventloop[],node[]] label[name,value] eventloop[process_size,queue_size] node[name,address,port,sync_conn,async_conn,namespace[]] conn[in_use,in_pool,opened,closed,recovered,aborted] namespace[name,errors,timeouts,key_busy,bytes_in,bytes_out,latency[]] latency(%s,%u,%u)[type[l1,l2,l3...]]\n",
		now_str, latency_unit, mw->latency_columns, mw->latency_shift);

	if (rv <= 0) {
		fclose(mw->file);
		mw->file = NULL;
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Failed to write metrics header: %d,%s", rv, file_name.data);
	}

	as_status status = as_metrics_write_line(mw, data, err);

	if (status != AEROSPIKE_OK) {
		if (mw->file) {
			fclose(mw->file);
			mw->file = NULL;
		}
	}
	return status;
}

static as_status
as_metrics_ensure_open(as_metrics_file_exporter* mw, as_error* err)
{
	if (mw->file) {
		return AEROSPIKE_OK;
	}
	return as_metrics_open_writer(mw, err);
}

static void
as_metrics_write_conn_snapshot(as_string_builder* sb, const as_metrics_conn_snapshot* stats)
{
	as_string_builder_append_uint(sb, stats->in_use);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->in_pool);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->opened);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->closed);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->recovered);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->aborted);
}

static void
as_metrics_write_latencies(as_string_builder* sb, const as_metrics_namespace_snapshot* namespace_snapshot)
{
	for (uint8_t i = 0; i < AS_LATENCY_TYPE_MAX; i++) {
		const as_metrics_latency_snapshot* latency_snapshot = &namespace_snapshot->latencies[i];

		if (i > 0) {
			as_string_builder_append_char(sb, ',');
		}
		as_string_builder_append(sb, as_latency_type_to_string(latency_snapshot->type));
		as_string_builder_append_char(sb, '[');

		for (uint8_t j = 0; j < latency_snapshot->bucket_count; j++) {
			if (j > 0) {
				as_string_builder_append_char(sb, ',');
			}
			as_string_builder_append_uint64(sb, latency_snapshot->buckets[j]);
		}
		as_string_builder_append_char(sb, ']');
	}
}

static void
as_metrics_write_node_snapshot(as_string_builder* sb, const as_metrics_node_snapshot* node_snapshot)
{
	as_string_builder_append_char(sb, '[');
	as_string_builder_append(sb, node_snapshot->name ? node_snapshot->name : "");
	as_string_builder_append_char(sb, ',');
	as_string_builder_append(sb, node_snapshot->address ? node_snapshot->address : "");
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, node_snapshot->port);
	as_string_builder_append_char(sb, ',');
	as_metrics_write_conn_snapshot(sb, &node_snapshot->sync);
	as_string_builder_append_char(sb, ',');
	as_metrics_write_conn_snapshot(sb, &node_snapshot->async);
	as_string_builder_append(sb, ",[");

	for (uint32_t i = 0; i < node_snapshot->namespace_count; i++) {
		const as_metrics_namespace_snapshot* namespace_snapshot = &node_snapshot->namespaces[i];

		if (i > 0) {
			as_string_builder_append_char(sb, ',');
		}

		as_string_builder_append(sb, namespace_snapshot->name ? namespace_snapshot->name : "");
		as_string_builder_append_char(sb, ',');
		as_string_builder_append_uint64(sb, namespace_snapshot->errors);
		as_string_builder_append_char(sb, ',');
		as_string_builder_append_uint64(sb, namespace_snapshot->timeouts);
		as_string_builder_append_char(sb, ',');
		as_string_builder_append_uint64(sb, namespace_snapshot->key_busy);
		as_string_builder_append_char(sb, ',');
		as_string_builder_append_uint64(sb, namespace_snapshot->bytes_in);
		as_string_builder_append_char(sb, ',');
		as_string_builder_append_uint64(sb, namespace_snapshot->bytes_out);
		as_string_builder_append(sb, ",[");
		as_metrics_write_latencies(sb, namespace_snapshot);
		as_string_builder_append_char(sb, ']');
	}
	as_string_builder_append(sb, "]]");
}

static as_status
as_metrics_write_cluster_line(as_error* err, as_metrics_file_exporter* mw, const as_metrics_snapshot* snapshot)
{
	as_string_builder sb;
	as_string_builder_inita(&sb, 16384, true);
	as_string_builder_append(&sb, snapshot->timestamp);
	as_string_builder_append(&sb, " cluster[");
	as_string_builder_append(&sb, snapshot->cluster_name ? snapshot->cluster_name : "");
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append(&sb, snapshot->client_type ? snapshot->client_type : "");
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append(&sb, snapshot->client_version ? snapshot->client_version : "");
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append(&sb, snapshot->app_id ? snapshot->app_id : "");
	as_string_builder_append(&sb, ",[");

	for (uint32_t i = 0; i < snapshot->label_count; i++) {
		as_metrics_label* label = &snapshot->labels[i];

		if (i > 0) {
			as_string_builder_append_char(&sb, ',');
		}
		as_string_builder_append_char(&sb, '[');
		as_string_builder_append(&sb, label->name ? label->name : "");
		as_string_builder_append_char(&sb, ',');
		as_string_builder_append(&sb, label->value ? label->value : "");
		as_string_builder_append_char(&sb, ']');
	}

	as_string_builder_append(&sb, "],");
	as_string_builder_append_int(&sb, (int)snapshot->cpu);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint64(&sb, snapshot->mem);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint(&sb, snapshot->invalid_node_count);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint64(&sb, snapshot->command_count);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint64(&sb, snapshot->retry_count);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint64(&sb, snapshot->delay_queue_timeout_count);
	as_string_builder_append(&sb, ",[");

	for (uint32_t i = 0; i < snapshot->event_loop_count; i++) {
		const as_metrics_event_loop_snapshot* loop = &snapshot->event_loops[i];

		if (i > 0) {
			as_string_builder_append_char(&sb, ',');
		}
		as_string_builder_append_char(&sb, '[');
		as_string_builder_append_int(&sb, loop->process_size);
		as_string_builder_append_char(&sb, ',');
		as_string_builder_append_uint(&sb, loop->queue_size);
		as_string_builder_append_char(&sb, ']');
	}
	as_string_builder_append(&sb, "],[");

	for (uint32_t i = 0; i < snapshot->nodes_count; i++) {
		if (i > 0) {
			as_string_builder_append_char(&sb, ',');
		}
		as_metrics_write_node_snapshot(&sb, snapshot->nodes[i]);
	}
	as_string_builder_append(&sb, "]]");
	as_string_builder_append_newline(&sb);

	as_status status = as_metrics_write_line(mw, sb.data, err);
	as_string_builder_destroy(&sb);
	return status;
}

static as_status
as_metrics_write_departed(as_error* err, as_metrics_file_exporter* mw, const as_metrics_snapshot* snapshot)
{
	for (uint32_t i = 0; i < snapshot->nodes_departed_count; i++) {
		as_string_builder sb;
		as_string_builder_inita(&sb, 16384, true);
		as_string_builder_append(&sb, snapshot->timestamp);
		as_string_builder_append_char(&sb, ' ');
		as_metrics_write_node_snapshot(&sb, snapshot->nodes_departed[i]);
		as_string_builder_append_newline(&sb);

		as_status status = as_metrics_write_line(mw, sb.data, err);
		as_string_builder_destroy(&sb);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_metrics_file_export(as_metrics_exporter* exporter, as_error* err, const as_metrics_snapshot* snapshot)
{
	as_metrics_file_exporter* mw = (as_metrics_file_exporter*)exporter;
	as_status status = as_metrics_ensure_open(mw, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_metrics_write_cluster_line(err, mw, snapshot);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_metrics_write_departed(err, mw, snapshot);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (fflush(mw->file) != 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"File stream did not flush successfully: %s", mw->report_dir);
	}
	return AEROSPIKE_OK;
}

//---------------------------------
// Public Functions
//---------------------------------

as_status
as_metrics_file_exporter_create(
	as_error* err, const as_metrics_policy* policy, as_metrics_exporter** exporter
	)
{
	if (policy->report_size_limit != 0 && policy->report_size_limit < MIN_FILE_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Metrics policy report_size_limit %" PRIu64 " must be at least %d",
			policy->report_size_limit, MIN_FILE_SIZE);
	}

	as_metrics_file_exporter* mw = cf_calloc(1, sizeof(as_metrics_file_exporter));
	mw->base.export_fn = as_metrics_file_export;
	as_strncpy(mw->report_dir, policy->report_dir, sizeof(mw->report_dir));
	mw->max_size = policy->report_size_limit;
	mw->latency_unit = policy->latency_unit;
	mw->latency_columns = policy->latency_columns;
	mw->latency_shift = policy->latency_shift;
	*exporter = &mw->base;
	return AEROSPIKE_OK;
}

as_status
as_metrics_file_exporter_open(as_error* err, as_metrics_exporter* exporter)
{
	return as_metrics_ensure_open((as_metrics_file_exporter*)exporter, err);
}

void
as_metrics_file_exporter_destroy(as_metrics_exporter* exporter)
{
	if (!exporter) {
		return;
	}

	as_metrics_file_exporter* mw = (as_metrics_file_exporter*)exporter;

	if (mw->file) {
		fclose(mw->file);
		mw->file = NULL;
	}
	as_metrics_labels_destroy(mw->labels);
	as_metrics_cpu_state_destroy(mw->cpu);
	cf_free(mw);
}

as_status
as_metrics_writer_create(as_error* err, const as_metrics_policy* policy, as_metrics_listeners* listeners)
{
	as_metrics_exporter* exporter = NULL;
	as_status status = as_metrics_file_exporter_create(err, policy, &exporter);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_metrics_file_exporter* mw = (as_metrics_file_exporter*)exporter;
	mw->labels = as_metrics_labels_copy(policy->labels);
	mw->cpu = as_metrics_cpu_state_create();
	listeners->enable_listener = as_metrics_writer_enable;
	listeners->snapshot_listener = as_metrics_writer_snapshot;
	listeners->node_close_listener = as_metrics_writer_node_close;
	listeners->disable_listener = as_metrics_writer_disable;
	listeners->udata = mw;
	return AEROSPIKE_OK;
}

as_status
as_metrics_writer_enable(as_error* err, void* udata)
{
	as_metrics_file_exporter* mw = udata;
	as_status status = as_metrics_ensure_open(mw, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	mw->legacy_active = true;
	return AEROSPIKE_OK;
}

as_status
as_metrics_writer_snapshot(as_error* err, as_cluster* cluster, void* udata)
{
	as_error_reset(err);
	as_metrics_file_exporter* mw = udata;

	if (!mw->legacy_active || !mw->file) {
		return AEROSPIKE_OK;
	}

	as_metrics_snapshot* snapshot = NULL;
	as_status status = as_metrics_snapshot_create(err, cluster, mw->labels, mw->cpu, &snapshot);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_metrics_file_export(&mw->base, err, snapshot);
	as_metrics_snapshot_destroy(snapshot);
	return status;
}

as_status
as_metrics_writer_node_close(as_error* err, as_node* node, void* udata)
{
	as_error_reset(err);
	as_metrics_file_exporter* mw = udata;

	if (!mw->legacy_active || !mw->file) {
		return AEROSPIKE_OK;
	}

	as_metrics_node_snapshot* snapshot = NULL;
	as_metrics_node_snapshot_create(node, &snapshot);

	char now_str[128];
	timestamp_to_string(now_str, sizeof(now_str));

	as_string_builder sb;
	as_string_builder_inita(&sb, 16384, true);
	as_string_builder_append(&sb, now_str);
	as_string_builder_append_char(&sb, ' ');
	as_metrics_write_node_snapshot(&sb, snapshot);
	as_string_builder_append_newline(&sb);
	as_status status = as_metrics_write_line(mw, sb.data, err);
	as_string_builder_destroy(&sb);
	as_metrics_node_snapshot_destroy(snapshot);
	return status;
}

as_status
as_metrics_writer_disable(as_error* err, as_cluster* cluster, void* udata)
{
	as_error_reset(err);
	as_metrics_file_exporter* mw = udata;

	if (!mw) {
		return AEROSPIKE_OK;
	}

	as_status status = AEROSPIKE_OK;

	if (mw->legacy_active && mw->file) {
		as_metrics_snapshot* snapshot = NULL;
		status = as_metrics_snapshot_create(err, cluster, mw->labels, mw->cpu, &snapshot);

		if (status == AEROSPIKE_OK) {
			status = as_metrics_file_export(&mw->base, err, snapshot);
			as_metrics_snapshot_destroy(snapshot);
		}
	}

	as_metrics_file_exporter_destroy(&mw->base);
	return status;
}
