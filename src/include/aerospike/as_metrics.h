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
	uint32_t latency_shift;
	uint32_t latency_columns;
	uint64_t* buckets;
} as_latency_buckets;

struct as_policy_metrics_s;
struct as_node_s;
struct as_cluster_s;

/**
 * Callbacks for metrics listener operations
 */
typedef as_status(*as_metrics_enable_listener)(as_error* err, const struct as_policy_metrics_s* policy, void* udata);

typedef as_status(*as_metrics_snapshot_listener)(as_error* err, struct as_cluster_s* cluster, void* udata);

typedef as_status(*as_metrics_node_close_listener)(as_error* err, struct as_node_s* node, void* udata);

typedef as_status(*as_metrics_disable_listener)(as_error* err, struct as_cluster_s* cluster, void* udata);

/**
 * Struct to hold required callbacks
 */
typedef struct as_metrics_listeners_s {
	as_metrics_enable_listener enable_listener;
	as_metrics_snapshot_listener snapshot_listener;
	as_metrics_node_close_listener node_close_listener;
	as_metrics_disable_listener disable_listener;
	void* udata;
} as_metrics_listeners;

/**
* Metrics Policy
*/
typedef struct as_policy_metrics_s {
	const char* report_directory; // where the metrics file is output

	uint64_t report_size_limit; // default 0

	uint32_t interval; // default 30

	uint32_t latency_columns; // default 7

	uint32_t latency_shift; // default 1

	as_metrics_listeners metrics_listeners;
} as_policy_metrics;

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

	uint32_t latency_columns;

	uint32_t latency_shift;

	const char* report_directory;
} as_metrics_writer;

/**
 * Initalize metrics policy
 */
AS_EXTERN void
as_metrics_policy_init(as_policy_metrics* policy);

static inline void
as_metrics_set_listeners(
	as_policy_metrics* policy, as_metrics_enable_listener enable, 
	as_metrics_disable_listener disable, as_metrics_node_close_listener node_close,
	as_metrics_snapshot_listener snapshot
	)
{
	policy->metrics_listeners.enable_listener = enable;
	policy->metrics_listeners.disable_listener = disable;
	policy->metrics_listeners.node_close_listener = node_close;
	policy->metrics_listeners.snapshot_listener = snapshot;
}

/**
 * Convert latency_type to string version for printing to the output file
 */
char*
as_latency_type_to_string(as_latency_type type);

/**
 * Initalize latency bucket struct
 */
void
as_metrics_latency_buckets_init(as_latency_buckets* latency_buckets, uint32_t latency_columns, uint32_t latency_shift);

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
as_node_metrics*
as_node_metrics_init(uint32_t latency_columns, uint32_t latency_shift);

/**
 * Add latency to corresponding bucket type
 */
void
as_metrics_add_latency(as_node_metrics* node_metrics, as_latency_type latency_type, uint64_t elapsed);

/**
 * Calculate CPU and memory usage
 */
void
as_metrics_process_cpu_load_mem_usage(uint32_t* cpu_usage, uint32_t* mem);

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
 * Gets memory usage using GetProcessMemoryInfo()
 */
uint32_t
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