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

//---------------------------------
// Globals
//---------------------------------
extern uint32_t as_event_loop_capacity;

//---------------------------------
// Functions
//---------------------------------

const char* 
utc_time_str(time_t t)
{
	static char buf[UTC_STR_LEN + 1];
	struct tm* utc = gmtime(&t);
	snprintf(buf, sizeof(buf),
		"%4d-%02d-%02dT%02d:%02d:%02dZ",
		1900 + utc->tm_year, utc->tm_mon + 1, utc->tm_mday,
		utc->tm_hour, utc->tm_min, utc->tm_sec);
	return buf;
}


void
as_metrics_policy_init(as_policy_metrics* policy)
{
	policy->report_size_limit = 0;
	policy->interval = 30;
	policy->latency_columns = 7;
	policy->latency_shift = 1;
}

char*
as_latency_type_to_string(as_latency_type type)
{
	switch (type)
	{
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
as_metrics_latency_buckets_init(as_latency_buckets* latency_buckets, int32_t latency_columns, int32_t latency_shift)
{
	latency_buckets->latency_columns = latency_columns;
	latency_buckets->latency_shift = latency_shift;
	latency_buckets->buckets = cf_malloc(sizeof(uint64_t) * latency_columns);
}

uint64_t
as_metrics_get_bucket(as_latency_buckets* buckets, uint32_t i)
{
	return as_load_uint64(&buckets[i]);
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
	if ((elapsed_nanos - (elapsed * NS_TO_MS)) > 0)
	{
		elapsed++;
	}

	uint32_t last_bucket = latency_buckets->latency_columns - 1;
	uint64_t limit = 1;

	for (uint32_t i = 0; i < last_bucket; i++)
	{
		if (elapsed <= limit)
		{
			return i;
		}
		limit <<= latency_buckets->latency_shift;
	}

	return last_bucket;
}

void
as_node_metrics_init(as_node_metrics* node_metrics, const as_policy_metrics* policy)
{
	uint32_t max_latency_type = AS_LATENCY_TYPE_NONE;
	node_metrics->latency = cf_malloc(sizeof(as_latency_buckets) * max_latency_type);
	for (uint32_t i = 0; i < max_latency_type; i++)
	{
		as_metrics_latency_buckets_init(&node_metrics->latency[i], policy->latency_columns, policy->latency_shift);
	}
}

void
as_metrics_add_latency(as_node_metrics* node_metrics, as_latency_type latency_type, uint64_t elapsed)
{
	as_metrics_latency_buckets_add(&node_metrics->latency[latency_type], elapsed);
}

as_status
as_metrics_writer_enable(as_error* err, const struct as_policy_metrics_s* policy)
{
	if (policy->report_size_limit != 0 && policy->report_size_limit < MIN_FILE_SIZE)
	{
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, 
			"Metrics policy report_size_limit %d must be at least %d", policy->report_size_limit, MIN_FILE_SIZE);
	}

	// create file directory
	as_metrics_writer* mw = policy->udata;
	mw->max_size = policy->report_size_limit;
	mw->latency_columns = policy->latency_columns;
	mw->latency_shift = policy->latency_shift;
	mw->report_directory = policy->report_directory;

	as_status status = as_metrics_open_writer(mw, err);
	if (status != AEROSPIKE_OK)
	{
		return status;
	}

	mw->enable = true;
	return AEROSPIKE_OK;
}

as_status
as_metrics_open_writer(as_metrics_writer* mw, as_error* err)
{
	const char* now = utc_time_str(time(NULL));
	as_string_builder file_name;
	as_string_builder_inita(&file_name, 50, true);
	as_string_builder_append(&file_name, mw->report_directory);
	as_string_builder_append(&file_name, "metrics-");
	as_string_builder_append(&file_name, now);
	as_string_builder_append(&file_name, ".log");
	mw->file = fopen(file_name.data, "w");

	if (!mw->file)
	{
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Failed to open file: %s", mw->report_directory);
	}

	mw->size = 0;
	
	as_string_builder_inita(mw->sb, 2048, true);
	as_string_builder_append(mw->sb, now);
	as_string_builder_append(mw->sb, " header(1)");
	as_string_builder_append(mw->sb, " cluster[name,cpu,mem,invalidNodeCount,tranCount,retryCount,delayQueueTimeoutCount,eventloop[],node[]]");
	as_string_builder_append(mw->sb, " eventloop[processSize,queueSize]");
	as_string_builder_append(mw->sb, " node[name,address,port,syncConn,asyncConn,errors,timeouts,latency[]]");
	as_string_builder_append(mw->sb, " conn[inUse,inPool,opened,closed]");
	as_string_builder_append(mw->sb, " latency(");
	as_string_builder_append_int(mw->sb, mw->latency_columns);
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_int(mw->sb, mw->latency_shift);
	as_string_builder_append_char(mw->sb, ')');
	as_string_builder_append(mw->sb, "[type[l1,l2,l3...]]");
	as_status status = as_metrics_write_line(mw, err);

	if (status != AEROSPIKE_OK)
	{
		return status;
	}

	return AEROSPIKE_OK;
}

as_status
as_metrics_writer_snapshot(as_error* err, struct as_cluster_s* cluster, void* udata)
{
	as_metrics_writer* mw = udata;
	if (mw->enable && mw->file != NULL)
	{
		as_status status = as_metrics_write_cluster(err, mw, cluster);
		if (status != AEROSPIKE_OK)
		{
			return status;
		}
	}
	return AEROSPIKE_OK;
}

as_status
as_metrics_writer_node_close(as_error* err, struct as_node_s* node, void* udata)
{
	// write node info to file
	as_metrics_writer* mw = udata;
	if (mw->enable && mw->file != NULL)
	{
		as_string_builder_append(mw->sb, utc_time_str(time(NULL)));
		as_metrics_write_node(mw, node);
		as_status status = as_metrics_write_line(mw, err);
		if (status != AEROSPIKE_OK)
		{
			return status;
		}
	}
	return AEROSPIKE_OK;
}

as_status
as_metrics_writer_disable(as_error* err, struct as_cluster_s* cluster, void* udata)
{
	// write cluster into to file, disable
	as_metrics_writer* mw = udata;
	if (mw->enable && mw->file != NULL)
	{
		as_status status = as_metrics_write_cluster(err, mw, cluster);
		if (status != AEROSPIKE_OK)
		{
			return status;
		}
		uint32_t result = fclose(mw->file);
		mw->file = NULL;
		mw->enable = false;
		if (result != 0)
		{
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"File stream did not close successfully: %s", mw->report_directory);
		}
	}

	return AEROSPIKE_OK;
}

void
as_metrics_listeners_init(as_metrics_listeners* listeners)
{
	listeners->enable_callback = as_metrics_writer_enable;
	listeners->disable_callback = as_metrics_writer_disable;
	listeners->node_close_callback = as_metrics_writer_node_close;
	listeners->snapshot_callback = as_metrics_writer_snapshot;
}

as_status
as_metrics_write_cluster(as_error* err, as_metrics_writer* mw, struct as_cluster_s* cluster) {
	char* cluster_name = cluster->cluster_name;
	if (cluster_name == NULL)
	{
		cluster_name = "";
	}

	uint32_t cpu_load = 0;
	uint32_t mem = 0;
	as_metrics_process_cpu_load_mem_usage(&cpu_load, &mem);

	as_string_builder_append(mw->sb, utc_time_str(time(NULL)));
	as_string_builder_append(mw->sb, " cluster[");
	as_string_builder_append(mw->sb, cluster_name);
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_int(mw->sb, cpu_load);
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_int(mw->sb, mem);
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint(mw->sb, cluster->invalid_node_count); // Cumulative. Not reset on each interval.
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint64(mw->sb, as_cluster_get_tran_count(cluster));  // Cumulative. Not reset on each interval.
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint64(mw->sb, cluster->retry_count); // Cumulative. Not reset on each interval.
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint64(mw->sb, cluster->delay_queue_timeout_count); // Cumulative. Not reset on each interval.
	as_string_builder_append(mw->sb, ",[");

	for (uint32_t i = 0; i < as_event_loop_size; i++)
	{
		as_event_loop* loop = &as_event_loops[i];
		if (i > 0) {
			as_string_builder_append_char(mw->sb, ',');
		}
		as_string_builder_append_char(mw->sb, '[');
		as_string_builder_append_int(mw->sb, as_event_loop_get_process_size(loop));
		as_string_builder_append_char(mw->sb, ',');
		as_string_builder_append_uint(mw->sb, as_event_loop_get_queue_size(loop));
		as_string_builder_append_char(mw->sb, ']');
	}
	as_string_builder_append(mw->sb, "],[");

	as_nodes* nodes = as_nodes_reserve(cluster);
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		as_node_reserve(node);
		if (i > 0) {
			as_string_builder_append_char(mw->sb, ',');
		}
		as_metrics_write_node(mw, node);

		as_node_release(node);
	}
	as_string_builder_append(mw->sb, "]]");

	as_nodes_release(nodes);
	as_status status = as_metrics_write_line(mw, err);
	if (status != AEROSPIKE_OK)
	{
		return status;
	}

	return AEROSPIKE_OK;
}

void
as_metrics_write_node(as_metrics_writer* mw, struct as_node_s* node)
{
	as_string_builder_append_char(mw->sb, '[');
	as_string_builder_append(mw->sb, node->name);
	as_string_builder_append_char(mw->sb, ',');

	as_string_builder_append(mw->sb, as_node_get_address_string(node));
	as_string_builder_append_char(mw->sb, ',');

	struct as_conn_stats_s sync;
	struct as_conn_stats_s async;
	as_sum_init(&sync);
	as_sum_init(&async);
	as_metrics_get_node_sync_conn_stats(node, &sync);
	as_metrics_write_conn(mw, &sync);
	as_string_builder_append_char(mw->sb, ',');
	as_metrics_get_node_async_conn_stats(node, &async);
	as_metrics_write_conn(mw, &async);
	as_string_builder_append_char(mw->sb, ',');

	as_string_builder_append_uint64(mw->sb, as_node_get_error_count(node));
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint64(mw->sb, as_node_get_timeout_count(node));
	as_string_builder_append(mw->sb, ",[");

	as_node_metrics* node_metrics = node->metrics;
	uint32_t max = AS_LATENCY_TYPE_NONE;


	for (uint32_t i = 0; i < max; i++) {
		if (i > 0) {
			as_string_builder_append_char(mw->sb, ',');
		}
		as_string_builder_append(mw->sb, as_latency_type_to_string(i));
		as_string_builder_append_char(mw->sb, '[');

		as_latency_buckets* buckets = &node_metrics->latency[i];
		uint32_t bucket_max = buckets->latency_columns;

		for (uint32_t j = 0; j < bucket_max; j++) {
			if (j > 0) {
				as_string_builder_append_char(mw->sb, ',');
			}
			as_string_builder_append_uint64(mw->sb, as_metrics_get_bucket(buckets, i));
		}
		as_string_builder_append_char(mw->sb, ']');
	}
	as_string_builder_append(mw->sb, "]]");
}

void
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

void
as_metrics_get_node_async_conn_stats(const struct as_node_s* node, struct as_conn_stats_s* async)
{
	// Async connection summary.
	if (as_event_loop_capacity > 0) {
		for (uint32_t i = 0; i < as_event_loop_size; i++) {
			// Regular async.
			as_sum_no_lock(&node->async_conn_pools[i], async);
		}
	}
}

void
as_metrics_write_conn(as_metrics_writer* mw, const struct as_conn_stats_s* stats)
{
	as_string_builder_append_uint(mw->sb, stats->in_use);
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint(mw->sb, stats->in_pool);
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint(mw->sb, stats->opened); // Cumulative. Not reset on each interval.
	as_string_builder_append_char(mw->sb, ',');
	as_string_builder_append_uint(mw->sb, stats->closed); // Cumulative. Not reset on each interval.
}

as_status
as_metrics_write_line(as_metrics_writer* mw, as_error* err)
{
	as_string_builder_append_newline(mw->sb);
	fprintf(mw->file, mw->sb->data);
	mw->size += mw->sb->length;

	if (mw->max_size > 0 && mw->size >= mw->max_size)
	{
		uint32_t result = fclose(mw->file);
		if (result != 0)
		{
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"File stream did not close successfully: %s", mw->report_directory);
		}
		as_status status = as_metrics_open_writer(mw, err);
		if (status != AEROSPIKE_OK)
		{
			return status;
		}
	}

	return AEROSPIKE_OK;
}

#if defined(__linux__)
void
as_metrics_process_cpu_load_mem_usage(uint32_t* cpu_usage, uint32_t* mem)
{
	double resident_set, mem_d, cpu_usage_d;
	as_metrics_proc_stat_mem_cpu(&mem_d, &resident_set, &cpu_usage_d);
	cpu_usage_d = cpu_usage_d + 0.5 - (cpu_usage_d < 0);
	mem_d = mem_d + 0.5 - (mem_d < 0);
	*cpu_usage = (uint32_t)cpu_usage_d;
	*mem = (uint32_t)mem_d;
}

void 
as_metrics_proc_stat_mem_cpu(double* vm_usage, double* resident_set, double* cpu_usage)
{
	using std::ios_base;
	using std::ifstream;
	using std::string;

	vm_usage = 0.0;
	resident_set = 0.0;

	ifstream stat_stream("/proc/self/stat", ios_base::in);

	// dummy vars for leading entries in stat that we don't care about
	string pid, comm, state, ppid, pgrp, session, tty_nr;
	string tpgid, flags, minflt, cminflt, majflt, cmajflt;
	string cutime, cstime, priority, nice;
	string O, itrealvalue;

	// the fields we want
	uint64_t utime, stime, starttime;
	uint64_t vsize;
	int64_t rss;

	stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
		>> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
		>> utime >> stime >> cutime >> cstime >> priority >> nice
		>> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

	stat_stream.close();

	int64_t page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
	*vm_usage = vsize / 1024.0;
	*resident_set = rss * page_size_kb;

	uint64_t u_time_sec = utime / sysconf(_SC_CLK_TCK);
	uint64_t s_time_sec = stime / sysconf(_SC_CLK_TCK);
	uint64_t start_time_sec = starttime / sysconf(_SC_CLK_TCK);

	*cpu_usage = (u_time_sec + s_time_sec) / (cf_get_seconds() - start_time_sec);
}
#endif

#if defined(_MSC_VER)
#include <Windows.h>
#include <Psapi.h>

void
as_metrics_process_cpu_load_mem_usage(uint32_t* cpu_usage, uint32_t* mem)
{
	double cpu_usage_d = as_metrics_process_cpu_load();
	cpu_usage_d = cpu_usage_d + 0.5 - (cpu_usage_d < 0);
	*cpu_usage = (uint32_t)cpu_usage_d;
	*mem = as_metrics_process_mem_usage();
}

static double 
as_metrics_calculate_cpu_load(uint64_t idleTicks, uint64_t totalTicks)
{
	static uint64_t _previousTotalTicks = 0;
	static uint64_t _previousIdleTicks = 0;

	uint64_t totalTicksSinceLastTime = totalTicks - _previousTotalTicks;
	uint64_t idleTicksSinceLastTime = idleTicks - _previousIdleTicks;

	double ret = 1.0f - ((totalTicksSinceLastTime > 0) ? ((double)idleTicksSinceLastTime) / totalTicksSinceLastTime : 0);

	_previousTotalTicks = totalTicks;
	_previousIdleTicks = idleTicks;
	return ret;
}

static uint64_t
as_metrics_file_time_to_uint_64(const FILETIME ft)
{ 
	return (((uint64_t)(ft.dwHighDateTime)) << 32) | ((uint64_t)ft.dwLowDateTime);
}

// Returns 1.0f for "CPU fully pinned", 0.0f for "CPU idle", or somewhere in between
// You'll need to call this at regular intervals, since it measures the load between
// the previous call and the current one.  Returns -1.0 on error.
double
as_metrics_process_cpu_load()
{
	FILETIME idleTime, kernelTime, userTime;
	return GetSystemTimes(&idleTime, &kernelTime, &userTime) ? 
		as_metrics_calculate_cpu_load(as_metrics_file_time_to_uint_64(idleTime), as_metrics_file_time_to_uint_64(kernelTime) + as_metrics_file_time_to_uint_64(userTime)) * 100: -1.0f;
}

uint32_t
as_metrics_process_mem_usage()
{
	PROCESS_MEMORY_COUNTERS memCounter;
	BOOL result = GetProcessMemoryInfo(GetCurrentProcess(),
		&memCounter,
		sizeof(memCounter));

	return (uint32_t)memCounter.WorkingSetSize;
}

#endif

#if defined(__APPLE__)
#include<mach/mach.h>
#include<unistd.h>
#include<sys/syscall.h>

void
as_metrics_process_cpu_load_mem_usage(uint32_t* cpu_usage, uint32_t* mem)
{
	double cpu_usage_d = as_metrics_process_cpu_load();
	double mem_d = as_metrics_process_mem_usage();
	cpu_usage_d = cpu_usage_d + 0.5 - (cpu_usage_d < 0);
	mem_d = mem_d + 0.5 - (mem_d < 0);
	*cpu_usage = (uint32_t)cpu_usage_d;
	*mem = (uint32_t)mem_d;
}

double
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

double
as_metrics_process_cpu_load()
{
	pid_t pid = getpid();
	as_string_builder sb;
	as_string_builder_inita(&sb, 20, true);
	as_string_builder_append(&sb, "ps -p ");
	as_string_builder_append(&sb, pid);
	as_string_builder_append(&sb, " -o %CPU");
	FILE* result = popen(sb.data);
	char[5] cpu_holder;
	char[6] cpu_percent;
	fgets(result, 4, cpu_holder); // %CPU placeholder
	fgets(result, 5, cpu_percent);
	pclose(result);

	return atof(cpu_percent);
}
#endif