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
#include <aerospike/as_metrics.h>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_stats.h>
#include <aerospike/as_address.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_config_file.h>
#include <aerospike/as_event.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_metrics_writer.h>
#include <aerospike/as_node.h>
#include <aerospike/as_string_builder.h>
#include <aerospike/as_thread.h>
#include <aerospike/version.h>

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(__linux__)
#include <sys/sysinfo.h>
#include <unistd.h>
#endif

#if defined(__APPLE__)
#include <mach/mach.h>
#include <unistd.h>
#endif

#if defined(_MSC_VER)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <Psapi.h>
#endif

//---------------------------------
// Static Functions
//---------------------------------

static const as_metrics_policy*
as_metrics_policy_merge(aerospike* as, const as_metrics_policy* src, as_metrics_policy* mrg)
{
	if (!src) {
		as_config* config = aerospike_load_config(as);
		return &config->policies.metrics;
	}
	else if (as->config_bitmap) {
		uint8_t* bitmap = as->config_bitmap;
		as_config* config = aerospike_load_config(as);
		as_metrics_policy* cfg = &config->policies.metrics;

		mrg->labels = as_field_is_set(bitmap, AS_METRICS_LABELS)?
			cfg->labels : src->labels;
		mrg->latency_columns = as_field_is_set(bitmap, AS_METRICS_LATENCY_COLUMNS)?
			cfg->latency_columns : src->latency_columns;
		mrg->latency_shift = as_field_is_set(bitmap, AS_METRICS_LATENCY_SHIFT)?
			cfg->latency_shift : src->latency_shift;
		mrg->enable = as_field_is_set(bitmap, AS_METRICS_ENABLE)?
			cfg->enable : src->enable;

		mrg->metrics_listeners = src->metrics_listeners;
		mrg->exporters = src->exporters;
		as_strncpy(		mrg->report_dir, src->report_dir, sizeof(mrg->report_dir));
		mrg->report_size_limit = src->report_size_limit;
		mrg->interval = src->interval;
		mrg->latency_unit = src->latency_unit;
		mrg->operational_enabled = src->operational_enabled;
		mrg->usage_enabled = src->usage_enabled;
		return mrg;
	}
	else {
		return src;
	}
}

//---------------------------------
// Functions
//---------------------------------

as_status
aerospike_enable_metrics(aerospike* as, as_error* err, const as_metrics_policy* policy)
{
	as_cluster* cluster = as->cluster;

	pthread_mutex_lock(&cluster->metrics_lock);

	if (as->config_bitmap &&
		as_field_is_set(as->config_bitmap, AS_METRICS_ENABLE) &&
		!as->config.policies.metrics.enable) {
		pthread_mutex_unlock(&cluster->metrics_lock);
		return as_error_set_message(err, AEROSPIKE_METRICS_CONFLICT,
			"Metrics can not be enabled by this function when metrics is disabled by dynamic configuration");
	}

	as_metrics_policy merged;
	policy = as_metrics_policy_merge(as, policy, &merged);

	as_status status = as_cluster_enable_metrics(err, cluster, policy);

	pthread_mutex_unlock(&cluster->metrics_lock);
	return status;
}

as_status
aerospike_disable_metrics(aerospike* as, as_error* err)
{
	as_cluster* cluster = as->cluster;

	pthread_mutex_lock(&cluster->metrics_lock);

	if (cluster->metrics_enabled && as->config_bitmap &&
		as_field_is_set(as->config_bitmap, AS_METRICS_ENABLE) &&
		as->config.policies.metrics.enable) {
		pthread_mutex_unlock(&cluster->metrics_lock);
		return as_error_set_message(err, AEROSPIKE_METRICS_CONFLICT,
			"Metrics can not be disabled by this function when metrics is enabled by dynamic configuration");
	}

	as_status status = as_cluster_disable_metrics(err, cluster);
	pthread_mutex_unlock(&cluster->metrics_lock);
	return status;
}

void
as_metrics_policy_init(as_metrics_policy* policy)
{
	policy->labels = NULL;
	policy->report_size_limit = 0;
	as_strncpy(policy->report_dir, ".", sizeof(policy->report_dir));
	policy->interval = 30;
	policy->latency_columns = 7;
	policy->latency_shift = 1;
	policy->latency_unit = AS_METRICS_LATENCY_MILLISECONDS;
	policy->operational_enabled = false;
	policy->usage_enabled = false;
	policy->metrics_listeners.enable_listener = NULL;
	policy->metrics_listeners.snapshot_listener = NULL;
	policy->metrics_listeners.node_close_listener = NULL;
	policy->metrics_listeners.disable_listener = NULL;
	policy->metrics_listeners.udata = NULL;
	policy->enable = false;
	policy->exporters = NULL;
}

void
as_metrics_policy_destroy(as_metrics_policy* policy)
{
	as_metrics_policy_destroy_labels(policy);

	if (policy->exporters) {
		// Exporter objects are owned by the application.
		as_vector_destroy(policy->exporters);
		policy->exporters = NULL;
	}
}

void
as_metrics_labels_destroy(as_vector* labels)
{
	if (labels) {
		for (uint32_t i = 0; i < labels->size; i++) {
			as_metrics_label* label = as_vector_get(labels, i);
			cf_free(label->name);
			cf_free(label->value);
		}
		as_vector_destroy(labels);
	}
}

void
as_metrics_policy_destroy_labels(as_metrics_policy* policy)
{
	as_metrics_labels_destroy(policy->labels);
	policy->labels = NULL;
}

void
as_metrics_policy_set_labels(as_metrics_policy* policy, as_vector* labels)
{
	as_vector* old = policy->labels;
	policy->labels = labels;
	as_metrics_labels_destroy(old);
}

as_vector*
as_metrics_labels_copy(as_vector* labels)
{
	as_vector* list = NULL;

	if (labels) {
		list = as_vector_create(sizeof(as_metrics_label), labels->size);

		for (uint32_t i = 0; i < labels->size; i++) {
			as_metrics_label* label = as_vector_get(labels, i);

			as_metrics_label tmp;
			tmp.name = cf_strdup(label->name);
			tmp.value = cf_strdup(label->value);

			as_vector_append(list, &tmp);
		}
	}
	return list;
}

bool
as_metrics_labels_equal(as_vector* labels1, as_vector* labels2)
{
	if (labels1 == NULL) {
		return labels2 == NULL;
	}
	else if (labels2 == NULL) {
		return false;
	}

	if (labels1->size != labels2->size) {
		return false;
	}

	for (uint32_t i = 0; i < labels1->size; i++) {
		as_metrics_label* label1 = as_vector_get(labels1, i);
		as_metrics_label* label2 = as_vector_get(labels2, i);

		if (strcmp(label1->name, label2->name) != 0) {
			return false;
		}

		if (strcmp(label1->value, label2->value) != 0) {
			return false;
		}
	}
	return true;
}

void
as_metrics_policy_copy_labels(as_metrics_policy* policy, as_vector* labels)
{
	as_vector* list = as_metrics_labels_copy(labels);
	as_metrics_policy_set_labels(policy, list);
}

void
as_metrics_policy_add_label(as_metrics_policy* policy, const char* name, const char* value)
{
	if (!policy->labels) {
		policy->labels = as_vector_create(sizeof(as_metrics_label), 8);
	}

	as_metrics_label label;
	label.name = cf_strdup(name);
	label.value = cf_strdup(value);

	as_vector_append(policy->labels, &label);
}

//---------------------------------
// Snapshot and export runtime
//---------------------------------

#define AS_METRICS_EXPORTER_MAX_FAILURES 3
#define AS_METRICS_EXPORTER_SUSPEND_CYCLES 3

extern char* aerospike_client_language;

typedef struct as_metrics_exporter_slot_s {
	as_metrics_exporter* exporter;
	uint32_t consecutive_failures;
	uint32_t suspend_remaining;
	bool owned;
} as_metrics_exporter_slot;

#if defined(_MSC_VER)
struct as_metrics_cpu_state_s {
	FILETIME prev_process_times_kernel;
	FILETIME prev_system_times_kernel;
	FILETIME prev_process_times_user;
	FILETIME prev_system_times_user;
	HANDLE process;
	DWORD pid;
};
#endif

typedef struct as_metrics_runtime_s as_metrics_runtime;

struct as_metrics_runtime_s {
	as_cluster* cluster;
	as_vector* slots;
	as_vector* departed;
	as_vector* labels;
	as_metrics_cpu_state* cpu;
	pthread_t thread;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool thread_running;
	bool thread_started;
};

static as_status as_metrics_runtime_export(as_cluster* cluster, as_metrics_runtime* rt, bool force, as_error* err);
static void as_metrics_runtime_free(as_metrics_runtime* rt);

#if defined(__linux__)
static as_status
as_metrics_read_cpu_mem(as_error* err, as_metrics_cpu_state* state, uint32_t* cpu_usage, uint64_t* mem)
{
	(void)state;
	FILE* proc_stat = fopen("/proc/self/stat", "r");

	if (!proc_stat) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating memory and CPU usage");
	}

	uint64_t utime, stime;
	long long unsigned int starttime;
	int64_t rss;
	int matched = fscanf(proc_stat,
		"%*d %*s %*s %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu %*d %*d %*d %*d %*d %*d %llu %*lu %ld",
		&utime, &stime, &starttime, &rss);

	fclose(proc_stat);

	if (matched == 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating memory and CPU usage");
	}

	// rss is resident pages, not VmSize. cluster.memory.bytes is that RSS in bytes.
	long page_size = sysconf(_SC_PAGE_SIZE);

	if (rss < 0 || page_size <= 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating memory usage");
	}

	double mem_bytes = (double)rss * (double)page_size;
	float u_time_sec = utime / sysconf(_SC_CLK_TCK);
	float s_time_sec = stime / sysconf(_SC_CLK_TCK);
	float start_time_sec = starttime / sysconf(_SC_CLK_TCK);

	struct sysinfo info;

	if (sysinfo(&info) != 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating CPU usage");
	}

	double cpu_usage_d = (u_time_sec + s_time_sec) / (info.uptime - start_time_sec) * 100;
	cpu_usage_d = cpu_usage_d + 0.5 - (cpu_usage_d < 0);
	*cpu_usage = (uint32_t)cpu_usage_d;
	*mem = (uint64_t)mem_bytes;
	return AEROSPIKE_OK;
}
#elif defined(__APPLE__)
static double
as_metrics_process_mem_usage(void)
{
	struct task_basic_info t_info;
	mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count)) {
		return -1.0;
	}
	return t_info.resident_size;
}

static double
as_metrics_process_cpu_load(void)
{
	pid_t pid = getpid();
	as_string_builder sb;
	as_string_builder_inita(&sb, 128, false);
	as_string_builder_append(&sb, "ps -p ");
	as_string_builder_append_int(&sb, pid);
	as_string_builder_append(&sb, " -o %cpu");

	FILE* file = popen(sb.data, "r");

	if (!file) {
		return -1.0;
	}

	char cpu_holder[5];
	char cpu_percent[6];

	if (!fgets(cpu_holder, sizeof(cpu_holder), file)) {
		pclose(file);
		return 0.0;
	}

	if (!fgets(cpu_percent, sizeof(cpu_percent), file)) {
		pclose(file);
		return 0.0;
	}

	pclose(file);
	return atof(cpu_percent);
}

static as_status
as_metrics_read_cpu_mem(as_error* err, as_metrics_cpu_state* state, uint32_t* cpu_usage, uint64_t* mem)
{
	(void)state;
	double cpu_usage_d = as_metrics_process_cpu_load();
	double mem_d = as_metrics_process_mem_usage();

	if (cpu_usage_d < 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating CPU usage");
	}

	if (mem_d < 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating memory usage");
	}

	*cpu_usage = (uint32_t)(cpu_usage_d + 0.5);
	*mem = (uint64_t)(mem_d + 0.5);
	return AEROSPIKE_OK;
}
#elif defined(_MSC_VER)
static ULONGLONG
as_metrics_filetime_difference(FILETIME* prev_kernel, FILETIME* prev_user, FILETIME* cur_kernel, FILETIME* cur_user)
{
	LARGE_INTEGER prev_kernel_time;
	prev_kernel_time.LowPart = prev_kernel->dwLowDateTime;
	prev_kernel_time.HighPart = prev_kernel->dwHighDateTime;

	LARGE_INTEGER prev_user_time;
	prev_user_time.LowPart = prev_user->dwLowDateTime;
	prev_user_time.HighPart = prev_user->dwHighDateTime;

	LARGE_INTEGER cur_kernel_time;
	cur_kernel_time.LowPart = cur_kernel->dwLowDateTime;
	cur_kernel_time.HighPart = cur_kernel->dwHighDateTime;

	LARGE_INTEGER cur_user_time;
	cur_user_time.LowPart = cur_user->dwLowDateTime;
	cur_user_time.HighPart = cur_user->dwHighDateTime;

	return (cur_kernel_time.QuadPart - prev_kernel_time.QuadPart) +
		(cur_user_time.QuadPart - prev_user_time.QuadPart);
}

static as_status
as_metrics_read_cpu_mem(as_error* err, as_metrics_cpu_state* state, uint32_t* cpu_usage, uint64_t* mem)
{
	if (!state || !state->process) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating CPU usage");
	}

	FILETIME dummy;
	FILETIME process_times_kernel, process_times_user, system_times_kernel, system_times_user;

	if (GetProcessTimes(state->process, &dummy, &dummy, &process_times_kernel, &process_times_user) == 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating CPU usage");
	}

	if (GetSystemTimes(0, &system_times_kernel, &system_times_user) == 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating CPU usage");
	}

	ULONGLONG proc = as_metrics_filetime_difference(&state->prev_process_times_kernel, &state->prev_process_times_user,
		&process_times_kernel, &process_times_user);
	ULONGLONG system = as_metrics_filetime_difference(&state->prev_system_times_kernel, &state->prev_system_times_user,
		&system_times_kernel, &system_times_user);
	double usage = 0.0;

	if (system != 0) {
		usage = 100.0 * (proc / (double)system);
	}

	state->prev_process_times_kernel = process_times_kernel;
	state->prev_process_times_user = process_times_user;
	state->prev_system_times_kernel = system_times_kernel;
	state->prev_system_times_user = system_times_user;
	usage = usage + 0.5 - (usage < 0);
	*cpu_usage = (uint32_t)usage;

	PROCESS_MEMORY_COUNTERS mem_counter;
	GetProcessMemoryInfo(GetCurrentProcess(), &mem_counter, sizeof(mem_counter));
	*mem = (uint64_t)mem_counter.WorkingSetSize;
	return AEROSPIKE_OK;
}
#else
static as_status
as_metrics_read_cpu_mem(as_error* err, as_metrics_cpu_state* state, uint32_t* cpu_usage, uint64_t* mem)
{
	(void)state;
	(void)cpu_usage;
	(void)mem;
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error calculating memory and CPU usage");
}
#endif

as_metrics_cpu_state*
as_metrics_cpu_state_create(void)
{
#if defined(_MSC_VER)
	as_metrics_cpu_state* state = cf_calloc(1, sizeof(as_metrics_cpu_state));
	state->pid = GetCurrentProcessId();
	state->process = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, state->pid);

	if (state->process != NULL) {
		FILETIME dummy;
		GetProcessTimes(state->process, &dummy, &dummy, &state->prev_process_times_kernel, &state->prev_process_times_user);
		GetSystemTimes(0, &state->prev_system_times_kernel, &state->prev_system_times_user);
	}
	return state;
#else
	// Linux and macOS read CPU and memory directly. They do not keep samples between snapshots.
	return NULL;
#endif
}

void
as_metrics_cpu_state_destroy(as_metrics_cpu_state* state)
{
	if (!state) {
		return;
	}
#if defined(_MSC_VER)
	if (state->process) {
		CloseHandle(state->process);
	}
#endif
	cf_free(state);
}

static char*
as_metrics_strdup_or_empty(const char* value)
{
	return cf_strdup(value ? value : "");
}

static void
as_metrics_conn_from_stats(as_metrics_conn_snapshot* dst, const as_conn_stats* src)
{
	dst->in_use = src->in_use;
	dst->in_pool = src->in_pool;
	dst->opened = src->opened;
	dst->closed = src->closed;
	dst->recovered = src->recovered;
	dst->aborted = src->aborted;
}

static struct tm*
as_metrics_localtime(const time_t* now, struct tm* out)
{
#if defined(_MSC_VER)
	return localtime_s(out, now) == 0 ? out : NULL;
#else
	return localtime_r(now, out);
#endif
}

void
as_metrics_timestamp(char* str, size_t str_size)
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

void
as_metrics_node_snapshot_destroy(as_metrics_node_snapshot* snapshot)
{
	if (!snapshot) {
		return;
	}

	cf_free(snapshot->name);
	cf_free(snapshot->address);

	if (snapshot->namespaces) {
		for (uint32_t i = 0; i < snapshot->namespace_count; i++) {
			as_metrics_namespace_snapshot* ns = &snapshot->namespaces[i];
			cf_free(ns->name);

			for (uint8_t j = 0; j < AS_LATENCY_TYPE_MAX; j++) {
				cf_free(ns->latencies[j].buckets);
			}
		}
		cf_free(snapshot->namespaces);
	}
	cf_free(snapshot);
}

void
as_metrics_node_snapshot_create(as_node* node, as_metrics_node_snapshot** snapshot)
{
	as_metrics_node_snapshot* snap = cf_calloc(1, sizeof(as_metrics_node_snapshot));
	snap->name = as_metrics_strdup_or_empty(node->name);

	as_address* address = as_node_get_address(node);
	struct sockaddr* addr = (struct sockaddr*)&address->addr;
	char address_name[AS_IP_ADDRESS_SIZE];
	as_address_short_name(addr, address_name, sizeof(address_name));
	snap->address = as_metrics_strdup_or_empty(address_name);
	snap->port = as_address_port(addr);

	as_conn_stats sync_stats;
	as_conn_stats async_stats;
	as_conn_stats_init(&sync_stats);
	as_conn_stats_init(&async_stats);

	uint32_t max = node->cluster->conn_pools_per_node;

	for (uint32_t i = 0; i < max; i++) {
		as_conn_pool* pool = &node->sync_conn_pools[i];

		pthread_mutex_lock(&pool->lock);
		uint32_t in_pool = as_queue_size(&pool->queue);
		uint32_t total = pool->queue.total;
		pthread_mutex_unlock(&pool->lock);

		sync_stats.in_pool += in_pool;
		sync_stats.in_use += total - in_pool;
	}
	sync_stats.opened = as_node_get_sync_conns_opened(node);
	sync_stats.closed = as_node_get_sync_conns_closed(node);
	sync_stats.recovered = as_node_get_sync_conns_recovered(node);
	sync_stats.aborted = as_node_get_sync_conns_aborted(node);
	as_metrics_conn_from_stats(&snap->sync, &sync_stats);

	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_conn_stats_sum(&async_stats, &node->async_conn_pools[i]);
	}
	as_metrics_conn_from_stats(&snap->async, &async_stats);

	if (node->cluster->metrics_operational_enabled) {
		snap->conn_open_failures = as_load_uint32(&node->conn_open_failures);
		snap->conn_tls_handshake_failures = as_load_uint32(&node->conn_tls_handshake_failures);
		snap->conn_auth_failures = as_load_uint32(&node->conn_auth_failures);
	}

	uint8_t ns_max = 0;

	if (node->cluster->metrics_enabled && node->cluster->metrics_operational_enabled) {
		ns_max = node->metrics_size;
	}

	if (ns_max > 0) {
		snap->namespaces = cf_calloc(ns_max, sizeof(as_metrics_namespace_snapshot));
	}

	for (uint8_t i = 0; i < ns_max; i++) {
		as_ns_metrics* metrics = node->metrics[i];

		if (!metrics) {
			continue;
		}

		as_metrics_namespace_snapshot* ns = &snap->namespaces[snap->namespace_count];
		ns->name = as_metrics_strdup_or_empty(metrics->ns);
		ns->errors = as_node_get_error_count(metrics);
		ns->timeouts = as_node_get_timeout_count(metrics);
		ns->key_busy = as_node_get_key_busy_count(metrics);
		ns->bytes_in = as_node_get_bytes_in(metrics);
		ns->bytes_out = as_node_get_bytes_out(metrics);

		for (uint8_t j = 0; j < AS_LATENCY_TYPE_MAX; j++) {
			ns->latencies[j].type = j;
			as_latency* latency = metrics->latency[j];

			if (!latency) {
				continue;
			}

			latency = as_latency_reserve(latency);
			uint8_t size = latency->size;

			if (size > 0) {
				uint64_t* buckets = cf_malloc(sizeof(uint64_t) * size);

				for (uint8_t k = 0; k < size; k++) {
					buckets[k] = as_latency_get_bucket(latency, k);
				}
				ns->latencies[j].buckets = buckets;
				ns->latencies[j].bucket_count = size;
			}
			as_latency_release(latency);
		}
		snap->namespace_count++;
	}

	*snapshot = snap;
}

void
as_metrics_snapshot_destroy(as_metrics_snapshot* snapshot)
{
	if (!snapshot) {
		return;
	}

	cf_free(snapshot->cluster_name);
	cf_free(snapshot->client_type);
	cf_free(snapshot->client_version);
	cf_free(snapshot->app_id);

	if (snapshot->labels) {
		for (uint32_t i = 0; i < snapshot->label_count; i++) {
			cf_free(snapshot->labels[i].name);
			cf_free(snapshot->labels[i].value);
		}
		cf_free(snapshot->labels);
	}

	cf_free(snapshot->event_loops);

	if (snapshot->nodes) {
		for (uint32_t i = 0; i < snapshot->nodes_count; i++) {
			as_metrics_node_snapshot_destroy(snapshot->nodes[i]);
		}
		cf_free(snapshot->nodes);
	}

	if (snapshot->nodes_departed) {
		for (uint32_t i = 0; i < snapshot->nodes_departed_count; i++) {
			as_metrics_node_snapshot_destroy(snapshot->nodes_departed[i]);
		}
		cf_free(snapshot->nodes_departed);
	}
	cf_free(snapshot);
}

as_status
as_metrics_snapshot_create(
	as_error* err, as_cluster* cluster, as_vector* labels, as_metrics_cpu_state* cpu,
	as_metrics_snapshot** snapshot
	)
{
	*snapshot = NULL;
	bool operational = cluster->metrics_enabled && cluster->metrics_operational_enabled;
	uint32_t cpu_usage = 0;
	uint64_t mem = 0;

	if (operational) {
		as_status status = as_metrics_read_cpu_mem(err, cpu, &cpu_usage, &mem);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	as_metrics_snapshot* snap = cf_calloc(1, sizeof(as_metrics_snapshot));
	as_metrics_timestamp(snap->timestamp, sizeof(snap->timestamp));
	snap->metrics_enabled = cluster->metrics_enabled;
	snap->operational_metrics_enabled = operational;
	snap->usage_metrics_enabled = cluster->metrics_enabled && cluster->metrics_usage_enabled;
	snap->cluster_name = as_metrics_strdup_or_empty(cluster->cluster_name);
	snap->client_type = as_metrics_strdup_or_empty(aerospike_client_language);
	snap->client_version = as_metrics_strdup_or_empty(aerospike_client_version);
	snap->app_id = as_metrics_strdup_or_empty(cluster->app_id);
	snap->cpu = cpu_usage;
	snap->mem = mem;
	snap->invalid_node_count = cluster->invalid_node_count;
	snap->recover_queue_size = as_cluster_recover_queue_size(cluster);
	snap->delay_queue_timeout_count = as_cluster_get_delay_queue_timeout_count(cluster);
	snap->command_count = as_cluster_get_command_count(cluster);
	snap->retry_count = as_cluster_get_retry_count(cluster);
	snap->latency_unit = cluster->metrics_latency_unit;
	snap->latency_columns = cluster->metrics_latency_columns;
	snap->latency_shift = cluster->metrics_latency_shift;

	if (labels && labels->size > 0) {
		snap->labels = cf_calloc(labels->size, sizeof(as_metrics_label));
		snap->label_count = labels->size;

		for (uint32_t i = 0; i < labels->size; i++) {
			as_metrics_label* label = as_vector_get(labels, i);
			snap->labels[i].name = as_metrics_strdup_or_empty(label->name);
			snap->labels[i].value = as_metrics_strdup_or_empty(label->value);
		}
	}

	if (as_event_loop_size > 0) {
		snap->event_loops = cf_calloc(as_event_loop_size, sizeof(as_metrics_event_loop_snapshot));
		snap->event_loop_count = as_event_loop_size;

		for (uint32_t i = 0; i < as_event_loop_size; i++) {
			as_event_loop* loop = &as_event_loops[i];
			snap->event_loops[i].process_size = as_event_loop_get_process_size(loop);
			snap->event_loops[i].queue_size = as_event_loop_get_queue_size(loop);
		}
	}

	as_nodes* nodes = as_nodes_reserve(cluster);
	snap->nodes_count = nodes->size;

	if (nodes->size > 0) {
		snap->nodes = cf_calloc(nodes->size, sizeof(as_metrics_node_snapshot*));
	}

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_metrics_node_snapshot* node_snap = NULL;
		as_metrics_node_snapshot_create(nodes->array[i], &node_snap);
		snap->nodes[i] = node_snap;
	}
	as_nodes_release(nodes);
	*snapshot = snap;
	return AEROSPIKE_OK;
}

as_status
aerospike_get_metrics_snapshot(aerospike* as, as_error* err, as_metrics_snapshot** snapshot)
{
	as_error_reset(err);

	if (!as || !as->cluster) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Cluster not initialized");
	}

	as_cluster* cluster = as->cluster;
	pthread_mutex_lock(&cluster->metrics_lock);

	as_metrics_runtime* rt = cluster->metrics_runtime;
	as_vector* labels = NULL;
	as_metrics_cpu_state* cpu = NULL;

	if (rt) {
		labels = rt->labels;
		cpu = rt->cpu;
	}
	else {
		as_config* config = aerospike_load_config(as);
		labels = config->policies.metrics.labels;
	}

	as_status status = as_metrics_snapshot_create(err, cluster, labels, cpu, snapshot);
	pthread_mutex_unlock(&cluster->metrics_lock);
	return status;
}

static void
as_metrics_snapshot_take_departed(as_metrics_runtime* rt, as_metrics_snapshot* snap)
{
	uint32_t n = rt->departed->size;

	if (n == 0) {
		return;
	}

	snap->nodes_departed = cf_malloc(sizeof(as_metrics_node_snapshot*) * n);

	for (uint32_t i = 0; i < n; i++) {
		snap->nodes_departed[i] = as_vector_get_ptr(rt->departed, i);
	}
	snap->nodes_departed_count = n;
	as_vector_clear(rt->departed);
}

static void
as_metrics_export_slots(as_metrics_runtime* rt, const as_metrics_snapshot* snap, bool force)
{
	for (uint32_t i = 0; i < rt->slots->size; i++) {
		as_metrics_exporter_slot* slot = as_vector_get(rt->slots, i);

		if (!force && slot->suspend_remaining > 0) {
			slot->suspend_remaining--;
			continue;
		}

		as_error err;
		as_error_reset(&err);
		as_status status = AEROSPIKE_ERR_CLIENT;

		if (slot->exporter && slot->exporter->export_fn) {
			status = slot->exporter->export_fn(slot->exporter, &err, snap);
		}
		else {
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT, "Metrics exporter is missing an export function");
		}

		if (status != AEROSPIKE_OK) {
			as_log_warn("Metrics exporter error: %s %s", as_error_string(status), err.message);
			slot->consecutive_failures++;

			if (slot->consecutive_failures >= AS_METRICS_EXPORTER_MAX_FAILURES) {
				slot->suspend_remaining = AS_METRICS_EXPORTER_SUSPEND_CYCLES;
				slot->consecutive_failures = 0;
				as_log_warn("Metrics exporter suspended after %u consecutive failures",
					AS_METRICS_EXPORTER_MAX_FAILURES);
			}
		}
		else {
			slot->consecutive_failures = 0;
			slot->suspend_remaining = 0;
		}
	}
}

static as_status
as_metrics_runtime_export(as_cluster* cluster, as_metrics_runtime* rt, bool force, as_error* err)
{
	as_metrics_snapshot* snap = NULL;
	as_status status = as_metrics_snapshot_create(err, cluster, rt->labels, rt->cpu, &snap);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_metrics_snapshot_take_departed(rt, snap);
	as_metrics_export_slots(rt, snap, force);
	as_metrics_snapshot_destroy(snap);
	return AEROSPIKE_OK;
}

static void
as_metrics_runtime_notify(as_cluster* cluster)
{
	pthread_mutex_lock(&cluster->metrics_lock);

	as_metrics_runtime* rt = cluster->metrics_runtime;

	if (!cluster->metrics_enabled || !rt) {
		pthread_mutex_unlock(&cluster->metrics_lock);
		return;
	}

	as_error err;

	if (rt->slots->size > 0) {
		as_status status = as_metrics_runtime_export(cluster, rt, false, &err);

		if (status != AEROSPIKE_OK) {
			as_log_warn("Metrics error: %s %s", as_error_string(status), err.message);
		}
	}

	if (cluster->metrics_listeners.snapshot_listener) {
		as_error_reset(&err);
		as_status status = cluster->metrics_listeners.snapshot_listener(
			&err, cluster, cluster->metrics_listeners.udata);

		if (status != AEROSPIKE_OK) {
			as_log_warn("Metrics error: %s %s", as_error_string(status), err.message);
		}
	}
	pthread_mutex_unlock(&cluster->metrics_lock);
}

static void*
as_metrics_thread(void* udata)
{
	as_metrics_runtime* rt = udata;
	as_cluster* cluster = rt->cluster;
	as_thread_set_name("metrics");

	pthread_mutex_lock(&rt->lock);

	while (rt->thread_running) {
		uint32_t interval = cluster->metrics_interval;
		uint32_t tend_ms = cluster->tend_interval;
		uint32_t interval_ms = interval * tend_ms;

		if (interval == 0 || tend_ms == 0) {
			interval_ms = 30000;
		}

		struct timespec delta;
		struct timespec abstime;
		cf_clock_set_timespec_ms((int)interval_ms, &delta);
		cf_clock_current_add(&delta, &abstime);

		int rc = pthread_cond_timedwait(&rt->cond, &rt->lock, &abstime);

		if (!rt->thread_running) {
			break;
		}

		if (rc == ETIMEDOUT) {
			pthread_mutex_unlock(&rt->lock);
			as_metrics_runtime_notify(cluster);
			pthread_mutex_lock(&rt->lock);
		}
	}

	pthread_mutex_unlock(&rt->lock);
	return NULL;
}

static void
as_metrics_runtime_free(as_metrics_runtime* rt)
{
	if (!rt) {
		return;
	}

	if (rt->slots) {
		for (uint32_t i = 0; i < rt->slots->size; i++) {
			as_metrics_exporter_slot* slot = as_vector_get(rt->slots, i);

			if (slot->owned && slot->exporter) {
				as_metrics_file_exporter_destroy(slot->exporter);
			}
		}
		as_vector_destroy(rt->slots);
	}

	if (rt->departed) {
		for (uint32_t i = 0; i < rt->departed->size; i++) {
			as_metrics_node_snapshot_destroy(as_vector_get_ptr(rt->departed, i));
		}
		as_vector_destroy(rt->departed);
	}

	as_metrics_labels_destroy(rt->labels);
	as_metrics_cpu_state_destroy(rt->cpu);
	pthread_cond_destroy(&rt->cond);
	pthread_mutex_destroy(&rt->lock);
	cf_free(rt);
}

static bool
as_metrics_listeners_defined(const as_metrics_listeners* listeners)
{
	return listeners->enable_listener && listeners->snapshot_listener &&
		listeners->node_close_listener && listeners->disable_listener && listeners->udata;
}

as_status
as_metrics_runtime_enable(as_error* err, as_cluster* cluster, const as_metrics_policy* policy)
{
	bool custom_listener = policy->metrics_listeners.enable_listener != NULL;

	if (custom_listener && !as_metrics_listeners_defined(&policy->metrics_listeners)) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "All metrics listeners and udata must be defined");
	}

	if (cluster->metrics_enabled || cluster->metrics_runtime) {
		as_status status = as_metrics_runtime_disable(err, cluster);

		if (status != AEROSPIKE_OK) {
			as_log_warn("Metrics disable error: %s %s", as_error_string(status), err->message);
			as_error_reset(err);
		}
	}

	cluster->metrics_interval = policy->interval;
	cluster->metrics_latency_columns = policy->latency_columns;
	cluster->metrics_latency_shift = policy->latency_shift;
	cluster->metrics_latency_unit = policy->latency_unit;
	cluster->metrics_operational_enabled = policy->operational_enabled;
	cluster->metrics_usage_enabled = policy->usage_enabled;
	cluster->metrics_listeners = policy->metrics_listeners;

	as_metrics_runtime* rt = cf_calloc(1, sizeof(as_metrics_runtime));
	rt->cluster = cluster;
	rt->slots = as_vector_create(sizeof(as_metrics_exporter_slot), 2);
	rt->departed = as_vector_create(sizeof(as_metrics_node_snapshot*), 4);
	rt->labels = as_metrics_labels_copy(policy->labels);
	rt->cpu = as_metrics_cpu_state_create();
	pthread_mutex_init(&rt->lock, NULL);
	pthread_cond_init(&rt->cond, NULL);

	if (policy->exporters) {
		for (uint32_t i = 0; i < policy->exporters->size; i++) {
			as_metrics_exporter_slot slot;
			memset(&slot, 0, sizeof(slot));
			slot.exporter = as_vector_get_ptr(policy->exporters, i);
			slot.owned = false;
			as_vector_append(rt->slots, &slot);
		}
	}

	if (rt->slots->size == 0 && !custom_listener && policy->report_dir[0] != '\0') {
		as_metrics_exporter* file_exporter = NULL;
		as_status status = as_metrics_file_exporter_create(err, policy, &file_exporter);

		if (status != AEROSPIKE_OK) {
			as_metrics_runtime_free(rt);
			return status;
		}

		as_metrics_exporter_slot slot;
		memset(&slot, 0, sizeof(slot));
		slot.exporter = file_exporter;
		slot.owned = true;
		as_vector_append(rt->slots, &slot);

		// Open now so enable fails when the log cannot be created.
		status = as_metrics_file_exporter_open(err, file_exporter);

		if (status != AEROSPIKE_OK) {
			as_metrics_runtime_free(rt);
			return status;
		}
	}

	cluster->metrics_runtime = rt;

	as_nodes* nodes = as_nodes_reserve(cluster);

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node_enable_metrics(nodes->array[i], policy);
	}
	as_nodes_release(nodes);

	if (custom_listener) {
		as_status status = cluster->metrics_listeners.enable_listener(err, cluster->metrics_listeners.udata);

		if (status != AEROSPIKE_OK) {
			as_metrics_runtime_free(rt);
			cluster->metrics_runtime = NULL;
			memset(&cluster->metrics_listeners, 0, sizeof(cluster->metrics_listeners));
			return status;
		}
	}

	cluster->metrics_enabled = true;

	if (rt->slots->size > 0 || cluster->metrics_listeners.snapshot_listener) {
		rt->thread_running = true;

		if (pthread_create(&rt->thread, NULL, as_metrics_thread, rt) != 0) {
			rt->thread_running = false;
			cluster->metrics_enabled = false;
			as_status status = as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Failed to create metrics thread: %s", strerror(errno));

			if (cluster->metrics_listeners.disable_listener) {
				as_error listener_err;
				cluster->metrics_listeners.disable_listener(
					&listener_err, cluster, cluster->metrics_listeners.udata);
			}
			as_metrics_runtime_free(rt);
			cluster->metrics_runtime = NULL;
			memset(&cluster->metrics_listeners, 0, sizeof(cluster->metrics_listeners));
			return status;
		}
		rt->thread_started = true;
	}

	return AEROSPIKE_OK;
}

as_status
as_metrics_runtime_disable(as_error* err, as_cluster* cluster)
{
	as_metrics_runtime* rt = cluster->metrics_runtime;
	bool was_enabled = cluster->metrics_enabled;
	cluster->metrics_enabled = false;

	if (rt && rt->thread_started) {
		pthread_mutex_lock(&rt->lock);
		rt->thread_running = false;
		pthread_cond_signal(&rt->cond);
		pthread_mutex_unlock(&rt->lock);

		pthread_mutex_unlock(&cluster->metrics_lock);
		pthread_join(rt->thread, NULL);
		rt->thread_started = false;
		pthread_mutex_lock(&cluster->metrics_lock);
	}

	as_error_reset(err);
	as_status status = AEROSPIKE_OK;
	as_error export_err;
	as_error_reset(&export_err);

	if (rt && rt->slots && rt->slots->size > 0) {
		status = as_metrics_runtime_export(cluster, rt, true, &export_err);
	}

	if (was_enabled && cluster->metrics_listeners.disable_listener) {
		as_error listener_err;
		as_error_reset(&listener_err);
		as_status listener_status = cluster->metrics_listeners.disable_listener(
			&listener_err, cluster, cluster->metrics_listeners.udata);

		if (status == AEROSPIKE_OK) {
			status = listener_status;
			export_err = listener_err;
		}
	}

	if (status != AEROSPIKE_OK) {
		*err = export_err;
	}

	if (rt) {
		as_metrics_runtime_free(rt);
		cluster->metrics_runtime = NULL;
	}
	memset(&cluster->metrics_listeners, 0, sizeof(cluster->metrics_listeners));
	return status;
}

as_status
as_metrics_runtime_node_close(as_error* err, as_cluster* cluster, as_node* node)
{
	as_metrics_runtime* rt = cluster->metrics_runtime;

	if (rt && rt->slots && rt->slots->size > 0) {
		as_metrics_node_snapshot* snap = NULL;
		as_metrics_node_snapshot_create(node, &snap);
		as_vector_append(rt->departed, &snap);
	}

	if (cluster->metrics_listeners.node_close_listener) {
		return cluster->metrics_listeners.node_close_listener(
			err, node, cluster->metrics_listeners.udata);
	}
	return AEROSPIKE_OK;
}
