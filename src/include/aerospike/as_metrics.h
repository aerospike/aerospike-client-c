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

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <aerospike/as_error.h>
#include <aerospike/as_string_builder.h>

#if !defined(_MSC_VER)
#include <netinet/in.h>
#include <sys/uio.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

#define NS_TO_MS 1000000
#define MIN_FILE_SIZE 1000000
#define UTC_STR_LEN 72

typedef uint8_t as_latency_type;

#define AS_LATENCY_TYPE_CONN 0
#define AS_LATENCY_TYPE_WRITE 1
#define AS_LATENCY_TYPE_READ 2
#define AS_LATENCY_TYPE_BATCH 3
#define AS_LATENCY_TYPE_QUERY 4
#define AS_LATENCY_TYPE_NONE 5

//---------------------------------
// Types
//---------------------------------

/**
 * Latency buckets for a transaction group.
 * Latency bucket counts are cumulative and not reset on each metrics snapshot interval
 */
typedef struct as_latency_buckets_s {
	int32_t latency_shift;
	int32_t latency_columns;
	uint64_t* buckets;
} as_latency_buckets;

struct as_metrics_listeners_s;

/**
* Metrics Policy
*/
typedef struct as_policy_metrics_s {
	const char* report_directory; // where the metrics file is output

	int64_t report_size_limit; // default 0

	int32_t interval; // default 30

	int32_t latency_columns; // default 7

	int32_t latency_shift; // default 1

	struct as_metrics_listeners_s* metrics_listeners;

	void* udata;
} as_policy_metrics;

struct as_cluster_s;
struct as_node_s;

/**
 * Callbacks for metrics listener operations
 */
typedef as_status (*as_metrics_enable_callback)(as_error* err, const struct as_policy_metrics_s* policy);

typedef void (*as_metrics_snapshot_callback)(const struct as_cluster_s* cluster, void* udata);

typedef void (*as_metrics_node_close_callback)(const struct as_node_s* node, void* udata);

typedef as_status (*as_metrics_disable_callback)(as_error* err, const struct as_cluster_s* cluster, void* udata);

/**
 * Struct to hold required callbacks
 */
typedef struct as_metrics_listeners_s {
	as_metrics_enable_callback enable_callback;
	as_metrics_snapshot_callback snapshot_callback;
	as_metrics_node_close_callback node_close_callback;
	as_metrics_disable_callback disable_callback;
} as_metrics_listeners;

/**
 * Node metrics latency bucket struct
 */
typedef struct as_node_metrics_s {
	as_latency_buckets* latency;
} as_node_metrics;

/**
 * Implementation of metrics_listeners
 */
typedef struct as_metrics_writer_s {
	FILE* file;

	as_string_builder* sb;

	bool enable;

	uint64_t max_size;

	uint64_t size;

	int32_t latency_columns;

	int32_t latency_shift;

	const char* report_directory;
} as_metrics_writer;

/**
 * Format time into UTC string
 */
const char* 
utc_time_str(time_t t);

/**
 * Initalize metrics policy
 */
void
as_metrics_policy_init(as_policy_metrics* policy);

/**
 * Convert latency_type to string version for printing to the output file
 */
char*
as_latency_type_to_string(as_latency_type type);

/**
 * Initalize latency bucket struct
 */
void
as_metrics_latency_buckets_init(as_latency_buckets* latency_buckets, int32_t latency_columns, int32_t latency_shift);

/**
 * Return cumulative count of a bucket.
 */
uint64_t
as_metrics_get_bucket(as_latency_buckets* buckets, uint32_t i);

/**
 * Increment count of bucket corresponding to the elapsed time in nanoseconds.
 */
void
as_metrics_latency_buckets_add(as_latency_buckets* latency_buckets, uint64_t elapsed);

/**
 * Determine which index of bucket the elapsed time belongs in
 */
uint32_t
as_metrics_get_index(as_latency_buckets* latency_buckets, uint64_t elapsed_nanos);

/**
 * Initalize node metrics struct
 */
void
as_node_metrics_init(as_node_metrics* node_metrics, const as_policy_metrics* policy);

/**
 * Add latency to corresponding bucket type
 */
void
as_metrics_add_latency(as_node_metrics* node_metrics, as_latency_type latency_type, uint64_t elapsed);

/**
 * Initalize metrics listener struct
 */
void
as_metrics_listeners_init(as_metrics_listeners* listeners);

/**
 * Calculate CPU and memory usage
 */
void
as_metrics_process_cpu_load_mem_usage(double* cpu_usage, double* mem);

struct as_cluster_s;
/**
 * Write cluster information to the metrics output file
 */
void
as_metrics_write_cluster(as_metrics_writer* mw, const struct as_cluster_s* cluster);

struct as_node_stats_s;
/**
 * Write node information to the metrics output file
 */
void
as_metrics_write_node(as_metrics_writer* mw, struct as_node_stats_s* node_stats);

/**
 * Write connection information to the metrics output file
 */
void
as_metrics_write_conn(as_metrics_writer* mw, struct as_conn_stats_s* conn_stats);

/**
 * Write line to the metrics output file
 */
void
as_metrics_write_line(as_metrics_writer* mw);

#if defined(__linux__)
/**
 * Gets memory and CPU usage information from proc/stat
 */
void
as_metrics_proc_stat_mem_cpu(double* vm_usage, double* resident_set, double* cpu_usage);
#endif

#if defined(_MSC_VER)

/**
 * Helper method that calculates CPU usage using ticks
 */
static double
as_metrics_calculate_cpu_load(uint64_t idleTicks, uint64_t totalTicks);

/**
 * Helper method for converting file time to uint64_t
 */
static uint64_t
as_metrics_file_time_to_uint_64(const FILETIME ft);

/**
 * Gets CPU usage using GetSystemTimes()
 */
double
as_metrics_process_cpu_load();

/**
 * Gets memory usage using GlobalMemoryStatusEx()
 */
double
as_metrics_process_mem_usage();
#endif

#if defined(__APPLE__)
/**
 * Gets memory usage using task_info
 */
double
as_metrics_process_mem_usage();

/**
 * Gets cpu usage using ps -p
 */
double
as_metrics_process_cpu_load();
#endif

#ifdef __cplusplus
} // end extern "C"
#endif