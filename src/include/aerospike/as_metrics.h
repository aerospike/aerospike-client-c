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
#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_string.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

#define NS_TO_MS 1000000

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

struct as_policy_metrics_s;
struct as_node_s;
struct as_cluster_s;

/**
 * Callbacks for metrics listener operations.
 */
typedef as_status(*as_metrics_enable_listener)(as_error* err, void* udata);

typedef as_status(*as_metrics_snapshot_listener)(as_error* err, struct as_cluster_s* cluster, void* udata);

typedef as_status(*as_metrics_node_close_listener)(as_error* err, struct as_node_s* node, void* udata);

typedef as_status(*as_metrics_disable_listener)(as_error* err, struct as_cluster_s* cluster, void* udata);

/**
 * Metrics listener callbacks.
 */
typedef struct as_metrics_listeners_s {
	/**
	 * Periodic extended metrics has been enabled for the given cluster.
	 */
	as_metrics_enable_listener enable_listener;

	/**
	 * A metrics snapshot has been requested for the given cluster.
	 */
	as_metrics_snapshot_listener snapshot_listener;

	/**
	 * A node is being dropped from the cluster.
	 */
	as_metrics_node_close_listener node_close_listener;

	/**
	 * Periodic extended metrics has been disabled for the given cluster.
	 */
	as_metrics_disable_listener disable_listener;

	/**
	 * User defined data.
	 */
	void* udata;
} as_metrics_listeners;

/**
 * Client periodic metrics configuration.
 */
typedef struct as_policy_metrics_s {
	/**
	 * Listeners that handles metrics notification events. The default listener implementation
	 * writes the metrics snapshot to a file which will later be read and forwarded to
	 * OpenTelemetry by a separate offline application.
	 * <p>
	 * The listener could be overridden to send the metrics snapshot directly to OpenTelemetry.
	 */
	as_metrics_listeners metrics_listeners;
	
	/**
	 * Directory path to write metrics log files for listeners that write logs.
	 *
	 * Default: <current directory>
	 */
	char report_dir[256];

	/**
	 * Metrics file size soft limit in bytes for listeners that write logs.
	 *
	 * When report_size_limit is reached or exceeded, the current metrics file is closed and a new
	 * metrics file is created with a new timestamp. If report_size_limit is zero, the metrics file
	 * size is unbounded and the file will only be closed when aerospike_disable_metrics() or
	 * aerospike_close() is called.
	 *
	 * Default: 0
	 */
	uint64_t report_size_limit;

	/**
	 * Number of cluster tend iterations between metrics notification events. One tend iteration
	 * is defined as as_config.tender_interval (default 1 second) plus the time to tend all
	 * nodes.
	 *
	 * Default: 30
	 */
	uint32_t interval;

	/**
	 * Number of elapsed time range buckets in latency histograms.
	 *
	 * Default: 7
	 */
	uint32_t latency_columns;

	/**
	 * Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
	 * are in milliseconds. The first 2 buckets are "<=1ms" and ">1ms". Examples:
	 * 
	 * ~~~~~~~~~~{.c}
	 * // latencyColumns=7 latencyShift=1
	 * <=1ms >1ms >2ms >4ms >8ms >16ms >32ms
	 *
	 * // latencyColumns=5 latencyShift=3
	 * <=1ms >1ms >8ms >64ms >512ms
	 * ~~~~~~~~~~
	 *
	 * Default: 1
	 */
	uint32_t latency_shift;
} as_metrics_policy;

/**
 * Latency buckets for a transaction group.
 * Latency bucket counts are cumulative and not reset on each metrics snapshot interval
 */
typedef struct as_latency_buckets_s {
	uint64_t* buckets;
	uint32_t latency_shift;
	uint32_t latency_columns;
} as_latency_buckets;

/**
 * Node metrics latency bucket struct
 */
typedef struct as_node_metrics_s {
	as_latency_buckets* latency;
} as_node_metrics;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initalize metrics policy
 */
AS_EXTERN void
as_metrics_policy_init(as_metrics_policy* policy);

/**
 * Initalize metrics policy
 */
static inline void
as_metrics_policy_set_report_dir(as_metrics_policy* policy, const char* report_dir)
{
	as_strncpy(policy->report_dir, report_dir, sizeof(policy->report_dir));
}

static inline void
as_metrics_policy_set_listeners(
	as_metrics_policy* policy, as_metrics_enable_listener enable,
	as_metrics_disable_listener disable, as_metrics_node_close_listener node_close,
	as_metrics_snapshot_listener snapshot, void* udata
	)
{
	policy->metrics_listeners.enable_listener = enable;
	policy->metrics_listeners.disable_listener = disable;
	policy->metrics_listeners.node_close_listener = node_close;
	policy->metrics_listeners.snapshot_listener = snapshot;
	policy->metrics_listeners.udata = udata;
}

/**
 * Enable extended periodic cluster and node latency metrics.
 */
AS_EXTERN as_status
aerospike_enable_metrics(aerospike* as, as_error* err, as_metrics_policy* policy);

/**
 * Disable extended periodic cluster and node latency metrics.
 */
AS_EXTERN as_status
aerospike_disable_metrics(aerospike* as, as_error* err);

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

#ifdef __cplusplus
} // end extern "C"
#endif
