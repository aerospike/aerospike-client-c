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
#include <aerospike/as_node.h>
#include <aerospike/aerospike_stats.h>

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

void 
as_metrics_writer_enable(const struct as_policy_metrics_s* policy)
{
	if (policy->report_size_limit != 0 && policy->report_size_limit < MIN_FILE_SIZE)
	{
		// error
	}

	// create file directory
	as_metrics_writer* mw = policy->udata;
	mw->file = fopen(policy->report_directory, "w");
	mw->max_size = policy->report_size_limit;
	mw->latency_columns = policy->latency_columns;
	mw->latency_shift = policy->latency_shift;
	mw->size = 0;

	as_string_builder_inita(mw->sb, 25, true);
	as_string_builder_append(&mw->sb, utc_time_str(time(NULL)));
	as_string_builder_append(&mw->sb, " header(1)");
	as_string_builder_append(&mw->sb, " cluster[name,cpu,mem,invalidNodeCount,tranCount,retryCount,delayQueueTimeoutCount,eventloop[],node[]]");
	as_string_builder_append(&mw->sb, " eventloop[processSize,queueSize]");
	as_string_builder_append(&mw->sb, " node[name,address,port,syncConn,asyncConn,errors,timeouts,latency[]]");
	as_string_builder_append(&mw->sb, " conn[inUse,inPool,opened,closed]");
	as_string_builder_append(&mw->sb, " latency(");
	as_string_builder_append(&mw->sb, mw->latency_columns);
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, mw->latency_shift);
	as_string_builder_append(&mw->sb, ')');
	as_string_builder_append(&mw->sb, "[type[l1,l2,l3...]]");
	as_metrics_write_line(mw);

	mw->enable = true;
}

void
as_metrics_writer_snapshot(const struct as_cluster_s* cluster, void* udata)
{
	as_metrics_writer* mw = udata;
	if (mw->enable && mw->file != NULL)
	{
		as_metrics_write_cluster(mw, cluster);
	}
}

void
as_metrics_writer_node_close(const struct as_node_s* node, void* udata)
{
	// write node info to file
	as_metrics_writer* mw = udata;
	if (mw->enable && mw->file != NULL)
	{
		as_string_builder_append(&mw->sb, utc_time_str(time(NULL)));
		as_metrics_write_node(&mw->sb, node);
		as_metrics_write_line(mw);
	}
}

void
as_metrics_writer_disable(const struct as_cluster_s* cluster, void* udata)
{
	// write cluster into to file, disable
	as_metrics_writer* mw = udata;
	if (mw->enable && mw->file != NULL)
	{
		as_metrics_write_cluster(mw, cluster);
		fclose(mw->file);
		mw->file = NULL;
		mw->enable = false;
	}
}

void
as_metrics_listeners_init(as_metrics_listeners* listeners)
{
	listeners->enable_callback = as_metrics_writer_enable;
	listeners->disable_callback = as_metrics_writer_disable;
	listeners->node_close_callback = as_metrics_writer_node_close;
	listeners->snapshot_callback = as_metrics_writer_snapshot;
}

void
as_metrics_write_cluster(as_metrics_writer* mw, const struct as_cluster_s* cluster) {
	char* cluster_name = cluster->cluster_name;
	if (cluster_name == NULL)
	{
		cluster_name = "";
	}

	double* cpu_load;
	double* mem;
	as_metrics_process_cpu_load_mem_usage(cpu_load, mem);
	as_cluster_stats* stats;
	aerospike_cluster_stats(cluster, stats);

	as_string_builder_append(&mw->sb, utc_time_str(time(NULL)));
	as_string_builder_append(&mw->sb, " cluster[");
	as_string_builder_append(&mw->sb, cluster_name);
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, (int)cpu_load);
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, mem);
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, cluster->invalid_node_count); // Cumulative. Not reset on each interval.
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, as_cluster_get_tran_count(cluster));  // Cumulative. Not reset on each interval.
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, cluster->retry_count); // Cumulative. Not reset on each interval.
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, cluster->delay_queue_timeout_count); // Cumulative. Not reset on each interval.
	as_string_builder_append(&mw->sb, ",[");

	as_event_loop_stats* event_loops = stats->event_loops;
	for (uint32_t i = 0; i < stats->event_loops_size; i++)
	{
		as_event_loop_stats* loop = &event_loops[i];
		if (i > 0) {
			as_string_builder_append(&mw->sb, ',');
		}
		as_string_builder_append(&mw->sb, '[');
		as_string_builder_append(&mw->sb, loop->process_size);
		as_string_builder_append(&mw->sb, ',');
		as_string_builder_append(&mw->sb, loop->queue_size);
		as_string_builder_append(&mw->sb, ']');
	}
	as_string_builder_append(&mw->sb, '],[');

	as_node_stats* nodes = stats->nodes;
	for (uint32_t i = 0; i < stats->nodes_size; i++) {
		as_node_stats* node = &stats->nodes[i];
		if (i > 0) {
			as_string_builder_append(&mw->sb, ",");
		}
		as_metrics_write_node(&mw->sb, node);
	}
	as_string_builder_append(&mw->sb, "]]");

	as_metrics_write_line(mw);
}

void
as_metrics_write_node(as_metrics_writer* mw, struct as_node_stats_s* node_stats)
{
	as_node* node = node_stats->node;
	as_string_builder_append(&mw->sb, '[');
	as_string_builder_append(&mw->sb, node->name);
	as_string_builder_append(&mw->sb, ',');

	//as_host* host = node-> TODO: how to get host from node? it is in node_info

	//as_string_builder_append(&mw->sb, host->name);
	//as_string_builder_append(&mw->sb, ',');
	//as_string_builder_append(&mw->sb, host->port);
	//as_string_builder_append(&mw->sb, ',');

	as_metrics_write_conn(&mw->sb, &node_stats->sync);
	as_string_builder_append(&mw->sb, ',');
	as_metrics_write_conn(&mw->sb, &node_stats->async);
	as_string_builder_append(&mw->sb, ',');

	as_string_builder_append(&mw->sb, node->error_count);
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, node->timeout_count);
	as_string_builder_append(&mw->sb, ',[');

	as_node_metrics* node_metrics = node->metrics;
	uint32_t max = AS_LATENCY_TYPE_NONE;


	for (uint32_t i = 0; i < max; i++) {
		if (i > 0) {
			as_string_builder_append(&mw->sb, ",");
		}
		as_string_builder_append(&mw->sb, as_latency_type_to_string(i));
		as_string_builder_append(&mw->sb, '[');

		as_latency_buckets* buckets = &node_metrics->latency[i];
		uint32_t bucket_max = buckets->latency_columns;

		for (uint32_t j = 0; j < bucket_max; j++) {
			if (j > 0) {
				as_string_builder_append(&mw->sb, ',');
			}
			as_string_builder_append(&mw->sb, as_metrics_get_bucket(&buckets, i));
		}
		as_string_builder_append(&mw->sb, ']');
	}
	as_string_builder_append(&mw->sb, ']]');
}

void
as_metrics_write_conn(as_metrics_writer* mw, struct as_conn_stats_s* conn_stats)
{
	as_string_builder_append(&mw->sb, conn_stats->in_use);
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, conn_stats->in_pool);
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, conn_stats->opened); // Cumulative. Not reset on each interval.
	as_string_builder_append(&mw->sb, ',');
	as_string_builder_append(&mw->sb, conn_stats->closed); // Cumulative. Not reset on each interval.
}

void
as_metrics_write_line(as_metrics_writer* mw)
{
	as_string_builder_append_newline(&mw->sb);
	fprintf(mw->file, &mw->sb->data);
	mw->size += mw->sb->length;

	if (mw->max_size > 0 && mw->size >= mw->max_size)
	{
		// write new file?
	}
}

#if defined(__linux__)
void
as_metrics_process_cpu_load_mem_usage(double* cpu_usage, double* mem)
{
	double resident_set;
	as_metrics_proc_stat_mem_cpu(mem, resident_set, cpu_usage);
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
	vm_usage = vsize / 1024.0;
	resident_set = rss * page_size_kb;

	uint64_t u_time_sec = utime / sysconf(_SC_CLK_TCK);
	uint64_t s_time_sec = stime / sysconf(_SC_CLK_TCK);
	uint64_t start_time_sec = starttime / sysconf(_SC_CLK_TCK);

	cpu_usage = (u_time_sec + s_time_sec) / (cf_get_seconds() - start_time_sec);
}
#endif

#if defined(_MSC_VER)
#include <Windows.h>

void
as_metrics_process_cpu_load_mem_usage(double* cpu_usage, double* mem)
{
	*cpu_usage = as_metrics_process_cpu_load();
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

double
as_metrics_process_mem_usage()
{
	MEMORYSTATUSEX statex;

	statex.dwLength = sizeof(statex);

	GlobalMemoryStatusEx(&statex);
	return statex.ullTotalVirtual - statex.ullAvailVirtual;
}

#endif

#if defined(__APPLE__)
#include<mach/mach.h>
#include<unistd.h>
#include<sys/syscall.h>

void
as_metrics_process_cpu_load_mem_usage(double* cpu_usage, double* mem)
{
	*cpu_usage = as_metrics_process_cpu_load();
	*mem = as_metrics_process_mem_usage();
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