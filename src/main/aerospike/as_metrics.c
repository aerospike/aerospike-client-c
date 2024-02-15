/*
 * Copyright 2008-2024 Aerospike, Inc.
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

#include <aerospike/as_atomic.h>
#include <aerospike/as_metrics.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_event.h>
#include <aerospike/as_node.h>
#include <aerospike/aerospike_stats.h>
#include <time.h>

#define LOG(_fmt, ...) { printf(_fmt "\n", ##__VA_ARGS__); fflush(stdout); }

//---------------------------------
// Globals
//---------------------------------
extern uint32_t as_event_loop_capacity;

//---------------------------------
// Static Functions
//---------------------------------

static void
timestamp_to_string_filename(char* str, size_t str_size)
{
	time_t now = time(NULL);
	struct tm * local = localtime(&now);
	snprintf(str, str_size,
		"%4d%02d%02d%02d%02d%02d",
		1900 + local->tm_year, local->tm_mon + 1, local->tm_mday,
		local->tm_hour, local->tm_min, local->tm_sec);
}

static void
timestamp_to_string(char* str, size_t str_size)
{
	time_t now = time(NULL);
	struct tm* local = localtime(&now);
	snprintf(str, str_size,
		"%4d-%02d-%02d %02d:%02d:%02d",
		1900 + local->tm_year, local->tm_mon + 1, local->tm_mday,
		local->tm_hour, local->tm_min, local->tm_sec);
}

static as_status
as_metrics_open_writer(as_metrics_writer* mw, as_error* err);

static as_status
as_metrics_write_line(as_metrics_writer* mw, const char* data, as_error* err)
{
	int written = fprintf(mw->file, "%s", data);
	if (written <= 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Failed to write metrics data: %d,%s", written, mw->report_dir);
	}
	mw->size += written;

	if (mw->max_size > 0 && mw->size >= mw->max_size) {
		uint32_t result = fclose(mw->file);

		if (result != 0) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"File stream did not close successfully: %s", mw->report_dir);
		}
		return as_metrics_open_writer(mw, err);
	}

	return AEROSPIKE_OK;
}

static void*
as_metrics_writer_init_udata()
{
	as_metrics_writer* mw = (as_metrics_writer*)cf_malloc(sizeof(as_metrics_writer));
	mw->file = NULL;
	mw->max_size = 0;
	mw->latency_columns = 0;
	mw->latency_shift = 0;
	mw->enable = false;
#ifdef _MSC_VER
	mw->pid = GetCurrentProcessId();
	mw->process = OpenProcess(PROCESS_QUERY_INFORMATION, false, mw->pid);
	FILETIME dummy;
	if (mw->process != NULL)
	{
		GetProcessTimes(mw->process, &dummy, &dummy, &mw->prev_process_times_kernel, &mw->prev_process_times_user);
		GetSystemTimes(0, &mw->prev_system_times_kernel, &mw->prev_system_times_user);
	}
#endif
	
	return mw;
}

static inline char separator()
{
#ifdef _MSC_VER
	return '\\';
#else
	return '/';
#endif
}

static as_status
as_metrics_open_writer(as_metrics_writer* mw, as_error* err)
{
	as_error_reset(err);
	char now_file_str[128];
	timestamp_to_string_filename(now_file_str, sizeof(now_file_str));
	
	as_string_builder file_name;
	as_string_builder_inita(&file_name, 256, false);
	as_string_builder_append(&file_name, mw->report_dir);
	char last_char = mw->report_dir[(strlen(mw->report_dir) - 1)];
	if (last_char != '/' && last_char != '\\') {
		as_string_builder_append_char(&file_name, separator());
	}
	as_string_builder_append(&file_name, "metrics-");
	as_string_builder_append(&file_name, now_file_str);
	as_string_builder_append(&file_name, ".log");
	mw->file = fopen(file_name.data, "w");

	if (!mw->file) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Failed to open file: %s", mw->report_dir);
	}

	mw->size = 0;
	char now_str[128];
	timestamp_to_string(now_str, sizeof(now_str));
	
	char data[512];
	int rv = snprintf(data, sizeof(data), "%s header(1) cluster[name,cpu,mem,invalidNodeCount,tranCount,retryCount,delayQueueTimeoutCount,eventloop[],node[]] eventloop[processSize,queueSize] node[name,address:port,syncConn,asyncConn,errors,timeouts,latency[]] conn[inUse,inPool,opened,closed] latency(%u,%u)[type[l1,l2,l3...]]\n",
		now_str, mw->latency_columns, mw->latency_shift);
	if (rv <= 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Failed to construct metrics header: %d,%s", rv, file_name.data);
	}

	return as_metrics_write_line(mw, data, err);
}

static void
as_metrics_get_node_sync_conn_stats(const struct as_node_s* node, struct as_conn_stats_s* sync)
{
	uint32_t max = node->cluster->conn_pools_per_node;

	// Sync connection summary.
	for (uint32_t i = 0; i < max; i++) {
		as_conn_pool* pool = &node->sync_conn_pools[i];

		pthread_mutex_lock(&pool->lock);
		uint32_t in_pool = as_queue_size(&pool->queue);
		uint32_t total = pool->queue.total;
		pthread_mutex_unlock(&pool->lock);

		sync->in_pool += in_pool;
		sync->in_use += total - in_pool;
	}
	sync->opened = node->sync_conns_opened;
	sync->closed = node->sync_conns_closed;
}

static void
as_metrics_get_node_async_conn_stats(const struct as_node_s* node, struct as_conn_stats_s* async)
{
	// Async connection summary.
	if (as_event_loop_capacity > 0) {
		for (uint32_t i = 0; i < as_event_loop_size; i++) {
			// Regular async.
			as_conn_stats_sum(async, &node->async_conn_pools[i]);
		}
	}
}

static void
as_metrics_write_conn(as_metrics_writer* mw, as_string_builder* sb, const struct as_conn_stats_s* stats)
{
	as_string_builder_append_uint(sb, stats->in_use);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->in_pool);
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->opened); // Cumulative. Not reset on each interval.
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint(sb, stats->closed); // Cumulative. Not reset on each interval.
}

static void
as_metrics_write_node(as_metrics_writer* mw, as_string_builder* sb, struct as_node_s* node)
{
	as_string_builder_append_char(sb, '[');
	as_string_builder_append(sb, node->name);
	as_string_builder_append_char(sb, ',');

	as_string_builder_append(sb, as_node_get_address_string(node));
	as_string_builder_append_char(sb, ',');

	struct as_conn_stats_s sync;
	struct as_conn_stats_s async;
	as_conn_stats_init(&sync);
	as_conn_stats_init(&async);
	as_metrics_get_node_sync_conn_stats(node, &sync);
	as_metrics_write_conn(mw, sb, &sync);
	as_string_builder_append_char(sb, ',');
	as_metrics_get_node_async_conn_stats(node, &async);
	as_metrics_write_conn(mw, sb, &async);
	as_string_builder_append_char(sb, ',');

	as_string_builder_append_uint64(sb, as_node_get_error_count(node));
	as_string_builder_append_char(sb, ',');
	as_string_builder_append_uint64(sb, as_node_get_timeout_count(node));
	as_string_builder_append(sb, ",[");

	as_node_metrics* node_metrics = node->metrics;
	uint32_t max = AS_LATENCY_TYPE_NONE;

	for (uint32_t i = 0; i < max; i++) {
		if (i > 0) {
			as_string_builder_append_char(sb, ',');
		}
		as_string_builder_append(sb, as_latency_type_to_string(i));
		as_string_builder_append_char(sb, '[');

		as_latency_buckets* buckets = &node_metrics->latency[i];
		uint32_t bucket_max = buckets->latency_columns;

		for (uint32_t j = 0; j < bucket_max; j++) {
			if (j > 0) {
				as_string_builder_append_char(sb, ',');
			}
			as_string_builder_append_uint64(sb, as_metrics_get_bucket(buckets, j));
		}
		as_string_builder_append_char(sb, ']');
	}
	as_string_builder_append(sb, "]]");
}

#if defined(__linux__)

#include <unistd.h>
#include <sys/sysinfo.h>

static as_status
as_metrics_proc_stat_mem_cpu(as_error* err, double* vm_usage, double* resident_set, double* cpu_usage)
{
	*vm_usage = 0.0;
	*resident_set = 0.0;

	FILE* proc_stat = fopen("/proc/self/stat", "r");
	if (!proc_stat) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Error calculating memory and CPU usage");
	}

	// dummies
	int dummy_d;
	char dummy_c;
	unsigned int dummy_u;
	long unsigned int dummy_lu;
	long int dummy_ld;

	// the fields we want
	uint64_t utime, stime;
	long long unsigned int starttime;
	uint64_t vsize;
	int64_t rss;

	int matched = fscanf(proc_stat, "%d %s %c %d %d %d %d %d %u %lu %lu %lu %lu %lu %lu %ld %ld %ld %ld %ld %ld %llu %lu %ld",
		&dummy_d, &dummy_c, &dummy_c, &dummy_d, &dummy_d, &dummy_d, &dummy_d, &dummy_d, &dummy_u, &dummy_lu, &dummy_lu, &dummy_lu, &dummy_lu,
		&utime, &stime, &dummy_ld, &dummy_ld, &dummy_ld, &dummy_ld, &dummy_ld, &dummy_ld, &starttime, &vsize, &rss);

	if (matched == 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Error calculating memory and CPU usage");
	}

	int result = fclose(proc_stat);

	if (result != 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Error closing /proc/self/stat");
	}

	int64_t page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
	*vm_usage = vsize / 1024.0;
	*resident_set = rss * page_size_kb;

	float u_time_sec = utime / sysconf(_SC_CLK_TCK);
	float s_time_sec = stime / sysconf(_SC_CLK_TCK);
	float start_time_sec = starttime / sysconf(_SC_CLK_TCK);

	struct sysinfo info;
	int success = sysinfo(&info);
	if (success != 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Error calculating CPU usage");
	}

	*cpu_usage = (u_time_sec + s_time_sec) / (info.uptime - start_time_sec) * 100;

	return AEROSPIKE_OK;
}

static as_status
as_metrics_process_cpu_load_mem_usage(as_error* err, uint32_t* cpu_usage, uint32_t* mem)
{
	double resident_set = 0.0;
	double mem_d = 0.0;
	double cpu_usage_d = 0.0;
	as_status result = as_metrics_proc_stat_mem_cpu(err, &mem_d, &resident_set, &cpu_usage_d);
	if (result != AEROSPIKE_OK) {
		return result;
	}

	LOG("cpu %f", cpu_usage_d);
	cpu_usage_d = cpu_usage_d + 0.5 - (cpu_usage_d < 0);
	mem_d = mem_d + 0.5 - (mem_d < 0);
	*cpu_usage = (uint32_t)cpu_usage_d;
	*mem = (uint32_t)mem_d;

	return AEROSPIKE_OK;
}
#endif

#if defined(_MSC_VER)
#include <Psapi.h>

static ULONGLONG 
as_metrics_filetime_difference(FILETIME* prev_kernel, FILETIME* prev_user, FILETIME* cur_kernel, FILETIME* cur_user) {
	LARGE_INTEGER a1, a2;
	a1.LowPart = prev_kernel->dwLowDateTime;
	a1.HighPart = prev_kernel->dwHighDateTime;
	a2.LowPart = prev_user->dwLowDateTime;
	a2.HighPart = prev_user->dwHighDateTime;

	LARGE_INTEGER b1, b2;
	b1.LowPart = cur_kernel->dwLowDateTime;
	b1.HighPart = cur_kernel->dwHighDateTime;
	b2.LowPart = cur_user->dwLowDateTime;
	b2.HighPart = cur_user->dwHighDateTime;

	//a1 and b1 - contains kernel times
	//a2 and b2 - contains user times
	return (b1.QuadPart - a1.QuadPart) + (b2.QuadPart - a2.QuadPart);
}

static double
as_metrics_process_cpu_load(as_metrics_writer* mw)
{
	if (mw->process == NULL) {
		return -1;
	}

	FILETIME dummy;
	FILETIME process_times_kernel, process_times_user, system_times_kernel, system_times_user;
	
	if (GetProcessTimes(mw->process, &dummy, &dummy, &process_times_kernel, &process_times_user) == 0) {
		return -1;
	}
	if (GetSystemTimes(0, &system_times_kernel, &system_times_user) == 0) {
		return -1;
	}

	// Get diffrence latest - previous times.
	ULONGLONG proc = as_metrics_filetime_difference(&mw->prev_process_times_kernel, &mw->prev_process_times_user,
		&process_times_kernel, &process_times_user);
	ULONGLONG system = as_metrics_filetime_difference(&mw->prev_system_times_kernel, &mw->prev_system_times_user,
		&system_times_kernel, &system_times_user);
	double usage = 0.0;

	// Calcualte percentage.
	if (system != 0) {
		usage = 100.0 * (proc / (double)system);
	}

	// Assign latest times to previous times for the next round of calculation.
	mw->prev_process_times_kernel = process_times_kernel;
	mw->prev_process_times_user = process_times_user;
	mw->prev_system_times_kernel = system_times_kernel;
	mw->prev_system_times_user = system_times_user;

	return usage;
}

static uint32_t
as_metrics_process_mem_usage()
{
	PROCESS_MEMORY_COUNTERS memCounter;
	BOOL result = GetProcessMemoryInfo(GetCurrentProcess(),
		&memCounter,
		sizeof(memCounter));

	return (uint32_t)memCounter.WorkingSetSize;
}

static as_status
as_metrics_process_cpu_load_mem_usage(as_error* err, as_metrics_writer* mw, uint32_t* cpu_usage, uint32_t* mem)
{
	double cpu_usage_d = as_metrics_process_cpu_load(mw);
	if (cpu_usage_d < 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Error calculating CPU usage");
	}
	cpu_usage_d = cpu_usage_d + 0.5 - (cpu_usage_d < 0);
	*cpu_usage = (uint32_t)cpu_usage_d;
	*mem = as_metrics_process_mem_usage();

	return AEROSPIKE_OK;
}

#endif

#if defined(__APPLE__)
#include<mach/mach.h>
#include<unistd.h>
#include<sys/syscall.h>

static double
as_metrics_process_mem_usage()
{
	struct task_basic_info t_info;
	mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

	if (KERN_SUCCESS != task_info(mach_task_self(),
		TASK_BASIC_INFO, (task_info_t)&t_info,
		&t_info_count))
	{
		return -1;
	}

	return t_info.virtual_size;
}

static double
as_metrics_process_cpu_load()
{
	pid_t pid = getpid();

	as_string_builder sb;
	as_string_builder_inita(&sb, 128, false);
	as_string_builder_append(&sb, "ps -p ");
	as_string_builder_append_int(&sb, pid);
	as_string_builder_append(&sb, " -o %CPU");

	FILE* file = popen(sb.data, "r");
	
	if (!file) {
		return -1.0;
	}
	
	char cpu_holder[5];
	char cpu_percent[6];
	
	if (!fgets(cpu_holder, sizeof(cpu_holder), file)) {
		pclose(file);
		return -1.0;
	}
	
	if (!fgets(cpu_percent, sizeof(cpu_percent), file)) {
		pclose(file);
		return -1.0;
	}
	
	pclose(file);
	return atof(cpu_percent);
}

static as_status
as_metrics_process_cpu_load_mem_usage(as_error* err, uint32_t* cpu_usage, uint32_t* mem)
{
	double cpu_usage_d = as_metrics_process_cpu_load();
	double mem_d = as_metrics_process_mem_usage();

	if (cpu_usage_d < 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Error calculating CPU usage");
	}

	if (mem_d < 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Error calculating memory usage");
	}

	cpu_usage_d = cpu_usage_d + 0.5 - (cpu_usage_d < 0);
	mem_d = mem_d + 0.5 - (mem_d < 0);
	*cpu_usage = (uint32_t)cpu_usage_d;
	*mem = (uint32_t)mem_d;

	return AEROSPIKE_OK;
}
#endif

static as_status
as_metrics_write_cluster(as_error* err, as_metrics_writer* mw, struct as_cluster_s* cluster) 
{
	char* cluster_name = cluster->cluster_name;
	
	if (cluster_name == NULL) {
		cluster_name = "";
	}

	uint32_t cpu_load = 0;
	uint32_t mem = 0;
#ifdef _MSC_VER
	as_status result = as_metrics_process_cpu_load_mem_usage(err, mw, &cpu_load, &mem);
#else
	as_status result = as_metrics_process_cpu_load_mem_usage(err, &cpu_load, &mem);
#endif
	if (result != AEROSPIKE_OK) {
		return result;
	}

	char now_str[128];
	timestamp_to_string(now_str, sizeof(now_str));
	as_string_builder sb;
	as_string_builder_inita(&sb, 16384, true);
	as_string_builder_append(&sb, now_str);
	as_string_builder_append(&sb, " cluster[");
	as_string_builder_append(&sb, cluster_name);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_int(&sb, cpu_load);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_int(&sb, mem);
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint(&sb, cluster->invalid_node_count); // Cumulative. Not reset on each interval.
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint64(&sb, as_cluster_get_tran_count(cluster));  // Cumulative. Not reset on each interval.
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint64(&sb, cluster->retry_count); // Cumulative. Not reset on each interval.
	as_string_builder_append_char(&sb, ',');
	as_string_builder_append_uint64(&sb, cluster->delay_queue_timeout_count); // Cumulative. Not reset on each interval.
	as_string_builder_append(&sb, ",[");

	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_loop* loop = &as_event_loops[i];
		if (i > 0) {
			as_string_builder_append_char(&sb, ',');
		}
		as_string_builder_append_char(&sb, '[');
		as_string_builder_append_int(&sb, as_event_loop_get_process_size(loop));
		as_string_builder_append_char(&sb, ',');
		as_string_builder_append_uint(&sb, as_event_loop_get_queue_size(loop));
		as_string_builder_append_char(&sb, ']');
	}
	as_string_builder_append(&sb, "],[");

	as_nodes* nodes = as_nodes_reserve(cluster);
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		as_node_reserve(node);
		
		if (i > 0) {
			as_string_builder_append_char(&sb, ',');
		}
		as_metrics_write_node(mw, &sb, node);

		as_node_release(node);
	}
	as_string_builder_append(&sb, "]]");

	as_nodes_release(nodes);
	as_string_builder_append_newline(&sb);
	as_status status = as_metrics_write_line(mw, sb.data, err);
	as_string_builder_destroy(&sb);
	return status;
}

static void
as_metrics_writer_destroy(as_metrics_writer* mw)
{
	fclose(mw->file);
	cf_free(mw);
}

static void
as_metrics_writer_destroy_node_metrics(as_node* node)
{
	if (node->metrics != NULL) {
		uint32_t max_latency_type = AS_LATENCY_TYPE_NONE;
		for (uint32_t i = 0; i < max_latency_type; i++) {
			cf_free(node->metrics->latency[i].buckets);
		}
		cf_free(node->metrics->latency);
		cf_free(node->metrics);
		node->metrics = NULL;
	}
}

static void
as_metrics_writer_destroy_nodes(as_cluster* cluster)
{
	// Free node memory
	as_nodes* nodes = as_nodes_reserve(cluster);
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_metrics_writer_destroy_node_metrics(nodes->array[i]);
	}
	as_nodes_release(nodes);
}

static as_status
as_metrics_writer_enable(as_error* err, const struct as_policy_metrics_s* policy, void* udata)
{
	as_error_reset(err);
	if (policy->report_size_limit != 0 && policy->report_size_limit < MIN_FILE_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Metrics policy report_size_limit %d must be at least %d", policy->report_size_limit, MIN_FILE_SIZE);
	}

	// create file directory
	as_metrics_writer* mw = udata;
	mw->max_size = policy->report_size_limit;
	mw->latency_columns = policy->latency_columns;
	mw->latency_shift = policy->latency_shift;
	as_strncpy(mw->report_dir, policy->report_dir, sizeof(mw->report_dir));

	as_status status = as_metrics_open_writer(mw, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	mw->enable = true;
	return AEROSPIKE_OK;
}

static as_status
as_metrics_writer_snapshot(as_error* err, struct as_cluster_s* cluster, void* udata)
{
	as_error_reset(err);
	as_metrics_writer* mw = udata;

	if (mw->enable && mw->file != NULL) {
		as_status status = as_metrics_write_cluster(err, mw, cluster);
		if (status != AEROSPIKE_OK) {
			as_metrics_writer_destroy_nodes(cluster);
			as_metrics_writer_destroy(mw);
			return status;
		}
		uint32_t result = fflush(mw->file);
		if (result != 0) {
			as_metrics_writer_destroy_nodes(cluster);
			as_metrics_writer_destroy(mw);
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"File stream did not flush successfully: %s", mw->report_dir);
		}
	}

	return AEROSPIKE_OK;
}

static as_status
as_metrics_writer_disable(as_error* err, struct as_cluster_s* cluster, void* udata)
{
	// write cluster into to file, disable
	as_error_reset(err);
	as_metrics_writer* mw = udata;
	if (mw != NULL) {
		if (mw->enable && mw->file != NULL) {
			as_status status = as_metrics_write_cluster(err, mw, cluster);

			if (status != AEROSPIKE_OK) {
				as_metrics_writer_destroy_nodes(cluster);
				as_metrics_writer_destroy(mw);
				return status;
			}
		}
		as_metrics_writer_destroy_nodes(cluster);
		as_metrics_writer_destroy(mw);
	}

	return AEROSPIKE_OK;
}

static as_status
as_metrics_writer_node_close(as_error* err, struct as_node_s* node, void* udata)
{
	// write node info to file
	as_error_reset(err);
	as_metrics_writer* mw = udata;

	if (mw->enable && mw->file != NULL) {
		char now_str[128];
		timestamp_to_string(now_str, sizeof(now_str));
		as_string_builder sb;
		as_string_builder_inita(&sb, 16384, true);
		as_string_builder_append(&sb, now_str);
		as_node_reserve(node);
		as_metrics_write_node(mw, &sb, node);
		as_string_builder_append_newline(&sb);
		as_status status = as_metrics_write_line(mw, sb.data, err);

		as_metrics_writer_destroy_node_metrics(node);
		as_node_release(node);
		as_string_builder_destroy(&sb);

		return status;
	}

	return AEROSPIKE_OK;
}

//---------------------------------
// Functions
//---------------------------------

as_status
aerospike_enable_metrics(aerospike* as, as_error* err, struct as_policy_metrics_s* policy)
{
	return as_cluster_enable_metrics(err, as->cluster, policy);
}

as_status
aerospike_disable_metrics(aerospike* as, as_error* err)
{
	return as_cluster_disable_metrics(err, as->cluster);
}

void
as_metrics_policy_init(as_policy_metrics* policy, const char* report_dir)
{
	policy->report_size_limit = 0;
	as_strncpy(policy->report_dir, report_dir, sizeof(policy->report_dir));
	policy->interval = 30;
	policy->latency_columns = 7;
	policy->latency_shift = 1;
	policy->metrics_listeners.enable_listener = as_metrics_writer_enable;
	policy->metrics_listeners.disable_listener = as_metrics_writer_disable;
	policy->metrics_listeners.node_close_listener = as_metrics_writer_node_close;
	policy->metrics_listeners.snapshot_listener = as_metrics_writer_snapshot;
	policy->metrics_listeners.udata = as_metrics_writer_init_udata();
}

char*
as_latency_type_to_string(as_latency_type type)
{
	switch (type) {
	case AS_LATENCY_TYPE_CONN:
		return "conn"; 
		break;
	case AS_LATENCY_TYPE_WRITE:
		return "write";
		break;
	case AS_LATENCY_TYPE_READ:
		return "read";
		break;
	case AS_LATENCY_TYPE_BATCH:
		return "batch";
		break;
	case AS_LATENCY_TYPE_QUERY:
		return "query";
		break;
	case AS_LATENCY_TYPE_NONE:
		return "none";
		break;
	default:
		return "none";
		break;
	}
}

void
as_metrics_latency_buckets_init(as_latency_buckets* latency_buckets, uint32_t latency_columns, uint32_t latency_shift)
{
	latency_buckets->latency_columns = latency_columns;
	latency_buckets->latency_shift = latency_shift;
	latency_buckets->buckets = cf_malloc(sizeof(uint64_t) * latency_columns);
	for (uint32_t i = 0; i < latency_columns; i++) {
		as_store_uint64(&latency_buckets->buckets[i], 0);
	}
}

uint64_t
as_metrics_get_bucket(as_latency_buckets* buckets, uint32_t i)
{
	return as_load_uint64(&buckets->buckets[i]);
}

void
as_metrics_latency_buckets_add(as_latency_buckets* latency_buckets, uint64_t elapsed)
{
	uint32_t index = as_metrics_get_index(latency_buckets, elapsed);
	as_incr_uint64(&latency_buckets->buckets[index]);
}

uint32_t 
as_metrics_get_index(as_latency_buckets* latency_buckets, uint64_t elapsed_nanos)
{
	// Convert nanoseconds to milliseconds.
	uint64_t elapsed = elapsed_nanos / NS_TO_MS;

	// Round up elapsed to nearest millisecond.
	if ((elapsed_nanos - (elapsed * NS_TO_MS)) > 0) {
		elapsed++;
	}

	uint32_t last_bucket = latency_buckets->latency_columns - 1;
	uint64_t limit = 1;

	for (uint32_t i = 0; i < last_bucket; i++) {
		if (elapsed <= limit) {
			return i;
		}
		limit <<= latency_buckets->latency_shift;
	}
	return last_bucket;
}

as_node_metrics*
as_node_metrics_init(uint32_t latency_columns, uint32_t latency_shift)
{
	as_node_metrics* node_metrics = (as_node_metrics*)cf_malloc(sizeof(as_node_metrics));
	uint32_t max_latency_type = AS_LATENCY_TYPE_NONE;
	node_metrics->latency = (as_latency_buckets*)cf_malloc(sizeof(as_latency_buckets) * max_latency_type);
	for (uint32_t i = 0; i < max_latency_type; i++) {
		as_metrics_latency_buckets_init(&node_metrics->latency[i], latency_columns, latency_shift);
	}

	return node_metrics;
}

void
as_metrics_add_latency(as_node_metrics* node_metrics, as_latency_type latency_type, uint64_t elapsed)
{
	as_metrics_latency_buckets_add(&node_metrics->latency[latency_type], elapsed);
}
