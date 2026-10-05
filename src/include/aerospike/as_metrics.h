/*
 * Copyright 2008-2025 Aerospike, Inc.
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

#include <aerospike/as_error.h>
#include <aerospike/as_latency.h>
#include <aerospike/as_vector.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

struct as_node_s;
struct as_cluster_s;
struct aerospike_s;

/**
 * @deprecated
 * Callbacks for the four-callback metrics listener. Prefer as_metrics_exporter.
 * The listener remains until the next major release.
 *
 * Callbacks for metrics listener operations.
 */
typedef as_status(*as_metrics_enable_listener)(as_error* err, void* udata);

typedef as_status(*as_metrics_snapshot_listener)(as_error* err, struct as_cluster_s* cluster, void* udata);

typedef as_status(*as_metrics_node_close_listener)(as_error* err, struct as_node_s* node, void* udata);

typedef as_status(*as_metrics_disable_listener)(as_error* err, struct as_cluster_s* cluster, void* udata);

/**
 * @deprecated
 * Metrics listener callbacks. Prefer registering an as_metrics_exporter.
 * Retained until the next major release. Exporters do not receive these callbacks.
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
 * Metrics label that is applied when exporting metrics.
 */
typedef struct {
	char* name;
	char* value;
} as_metrics_label;

struct as_metrics_exporter_s;

/**
 * Connection counts copied into a metrics snapshot.
 * `recovered` and `aborted` are legacy extensions of this client.
 */
typedef struct as_metrics_conn_snapshot_s {
	uint32_t in_use;
	uint32_t in_pool;
	uint32_t opened;
	uint32_t closed;
	uint32_t recovered;
	uint32_t aborted;
} as_metrics_conn_snapshot;

/**
 * One latency histogram copied into a metrics snapshot.
 * Bucket totals are cumulative since metrics were enabled.
 */
typedef struct as_metrics_latency_snapshot_s {
	/**
	 * AS_LATENCY_TYPE_* value.
	 */
	uint8_t type;

	uint8_t bucket_count;

	/**
	 * Heap copy of bucket totals. Owned by the snapshot.
	 */
	uint64_t* buckets;
} as_metrics_latency_snapshot;

/**
 * Per-namespace counters and histograms copied into a metrics snapshot.
 */
typedef struct as_metrics_namespace_snapshot_s {
	char* name;
	uint64_t errors;
	uint64_t timeouts;
	uint64_t key_busy;
	uint64_t bytes_in;
	uint64_t bytes_out;
	as_metrics_latency_snapshot latencies[AS_LATENCY_TYPE_MAX];
} as_metrics_namespace_snapshot;

/**
 * Per-node metrics copied into a metrics snapshot.
 * Sync and async pools are both reported. This client keeps separate async pools.
 */
typedef struct as_metrics_node_snapshot_s {
	char* name;
	char* address;
	uint16_t port;

	/**
	 * Synchronous connection pool. This is the shared connection series.
	 */
	as_metrics_conn_snapshot sync;

	/**
	 * Asynchronous connection pool. Legacy extension.
	 */
	as_metrics_conn_snapshot async;

	as_metrics_namespace_snapshot* namespaces;
	uint32_t namespace_count;
} as_metrics_node_snapshot;

/**
 * Event-loop gauges. Legacy extension. Empty when async event loops are not in use.
 */
typedef struct as_metrics_event_loop_snapshot_s {
	int process_size;
	uint32_t queue_size;
} as_metrics_event_loop_snapshot;

/**
 * Point-in-time metrics snapshot passed to each exporter.
 * Valid only for the duration of the export call. Copy fields that must be retained.
 * Counters and histogram buckets are cumulative. Gauges are the values at build time.
 *
 * This established client records the existing extended profile (latency, errors, bytes)
 * whenever metrics are enabled. `usage_metrics_enabled` is false; there is no usage catalog.
 */
typedef struct as_metrics_snapshot_s {
	/**
	 * Local timestamp, "YYYY-MM-DD HH:MM:SS". Same clock as the learn-metrics log.
	 */
	char timestamp[128];

	bool metrics_enabled;

	/**
	 * True when latency, error, and byte counters in this snapshot were collected.
	 */
	bool operational_metrics_enabled;

	/**
	 * Always false on this client. Usage counters are not part of the established-client scope.
	 */
	bool usage_metrics_enabled;

	char* cluster_name;
	char* client_type;
	char* client_version;
	char* app_id;

	as_metrics_label* labels;
	uint32_t label_count;

	uint32_t node_count;
	uint32_t recover_queue_size;
	uint32_t invalid_node_count;
	uint64_t delay_queue_timeout_count;
	uint64_t command_count;
	uint64_t retry_count;

	/**
	 * Process CPU percent and resident set size in bytes, written to the
	 * learn-metrics log. RSS is the memory the process is using, not its
	 * virtual address space. Stored as uint64_t because this client is
	 * 64-bit only and RSS can exceed 4 GB.
	 */
	uint32_t cpu;
	uint64_t mem;

	as_metrics_event_loop_snapshot* event_loops;
	uint32_t event_loop_count;

	as_metrics_node_snapshot** nodes;
	uint32_t nodes_count;

	/**
	 * Final samples for nodes removed since the previous export. Often empty.
	 * Replaces on_node_close for exporters.
	 */
	as_metrics_node_snapshot** nodes_departed;
	uint32_t nodes_departed_count;

	uint8_t latency_columns;
	uint8_t latency_shift;
} as_metrics_snapshot;

/**
 * Export one snapshot. The snapshot is valid only for this call.
 * Return AEROSPIKE_OK on success. The client isolates a failure to this exporter.
 */
typedef as_status (*as_metrics_export_fn)(
	struct as_metrics_exporter_s* exporter, as_error* err, const as_metrics_snapshot* snapshot);

/**
 * Exporter base. Embed this as the first field of a concrete exporter so the
 * pointer can be cast to as_metrics_exporter*. The exporter owns its init and cleanup.
 * The application destroys exporters it registers. The client does not.
 */
typedef struct as_metrics_exporter_s {
	as_metrics_export_fn export_fn;
} as_metrics_exporter;

/**
 * @private
 * Windows CPU sampling state. Opaque outside the metrics implementation.
 */
typedef struct as_metrics_cpu_state_s as_metrics_cpu_state;

/**
 * Client periodic metrics configuration.
 */
typedef struct as_metrics_policy_s {
	/**
	 * @deprecated
	 * Four-callback listener. Prefer exporters. When listeners are set, a non-empty
	 * report_dir does not also install the file exporter (existing listener behavior).
	 * Retained until the next major release.
	 */
	as_metrics_listeners metrics_listeners;

	/**
	 * List of name/value labels that is applied when exporting metrics.
	 * Do not set directly. Use multiple as_metrics_add_label() calls to add labels.
	 *
	 * Default: NULL
	 */
	as_vector* labels;

	/**
	 * Directory path for the built-in learn-metrics file exporter.
	 *
	 * When this is non-empty, no exporters have been added, and deprecated listeners
	 * are not set, aerospike_enable_metrics() installs the file exporter.
	 * An empty string installs nothing. Collection can still run with no exporter.
	 *
	 * Default: . (current directory)
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
	 * How often the metrics thread exports, measured in cluster tend intervals.
	 * The thread sleeps interval * as_config.tender_interval milliseconds
	 * (default 30 * 1000). Export does not run on the tend thread.
	 *
	 * Default: 30
	 */
	uint32_t interval;

	/**
	 * Number of elapsed time range buckets in latency histograms.
	 *
	 * Default: 7
	 */
	uint8_t latency_columns;

	/**
	 * Power of 2 multiple between each range bucket in latency histograms starting at column 3. The bucket units
	 * are in milliseconds. The first 2 buckets are "<=1ms" and ">1ms". Examples:
	 * 
	 * @code
	 * // latencyColumns=7 latencyShift=1
	 * <=1ms >1ms >2ms >4ms >8ms >16ms >32ms
	 *
	 * // latencyColumns=5 latencyShift=3
	 * <=1ms >1ms >8ms >64ms >512ms
	 * @endcode
	 *
	 * Default: 1
	 */
	uint8_t latency_shift;

	/**
	 * @private
	 * Should metrics be started as part of dynamic configuration. If aerospike_enable_metrics()
	 * is called, metrics will automaticallly be enabled and this field is ignored.
	 * For internal use only.
	 */
	bool enable;

	/**
	 * Exporters that receive each metrics snapshot. Append with
	 * as_metrics_policy_add_exporter(). The application owns these exporters.
	 *
	 * When this list is empty, listeners are not set, and report_dir is non-empty,
	 * enable installs the built-in file exporter and destroys that instance on disable.
	 *
	 * Default: NULL
	 */
	as_vector* exporters;
} as_metrics_policy;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initalize metrics policy.
 */
AS_EXTERN void
as_metrics_policy_init(as_metrics_policy* policy);

/**
 * Destroy metrics policy.
 */
AS_EXTERN void
as_metrics_policy_destroy(as_metrics_policy* policy);

/**
 * Destroy metrics policy labels.
 */
AS_EXTERN void
as_metrics_policy_destroy_labels(as_metrics_policy* policy);

/**
 * Add label that will be applied when exporting metrics.
 *
 * @code
 * as_metrics_policy mp;
 * as_metrics_policy_init(&mp);
 * as_metrics_policy_add_label(&mp, "region", "us-west");
 * as_metrics_policy_add_label(&mp, "zone", "usw1-az3");
 * @endcode
 */
AS_EXTERN void
as_metrics_policy_add_label(as_metrics_policy* policy, const char* name, const char* value);

/**
 * Copy all metrics labels. Previous labels will be destroyed.
 */
AS_EXTERN void
as_metrics_policy_copy_labels(as_metrics_policy* policy, as_vector* labels);

/**
 * Set all metrics labels. Previous labels will be destroyed.
 */
AS_EXTERN void
as_metrics_policy_set_labels(as_metrics_policy* policy, as_vector* labels);

/**
 * Transfer ownership of heap allocated app_id to metrics.
 * app_id must be heap allocated.  For internal use only.
 */
AS_EXTERN void
as_metrics_policy_assign_app_id(as_metrics_policy* policy, char* app_id);

/**
 * Set output directory path for metrics files.
 */
static inline void
as_metrics_policy_set_report_dir(as_metrics_policy* policy, const char* report_dir)
{
	as_strncpy(policy->report_dir, report_dir, sizeof(policy->report_dir));
}

/**
 * Append an exporter. The application owns `exporter` and destroys it after
 * metrics are disabled. The snapshot passed to export_fn is valid only for that call.
 */
static inline void
as_metrics_policy_add_exporter(as_metrics_policy* policy, as_metrics_exporter* exporter)
{
	if (!policy->exporters) {
		policy->exporters = as_vector_create(sizeof(as_metrics_exporter*), 2);
	}

	as_vector_append(policy->exporters, &exporter);
}

/**
 * @deprecated
 * Set the four-callback metrics listener. Prefer as_metrics_policy_add_exporter().
 * Retained until the next major release.
 */
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

AS_EXTERN as_vector*
as_metrics_labels_copy(as_vector* labels);

AS_EXTERN bool
as_metrics_labels_equal(as_vector* labels1, as_vector* labels2);

AS_EXTERN void
as_metrics_labels_destroy(as_vector* labels);

/**
 * @private
 * Create CPU sampling state for snapshot builds.
 */
AS_EXTERN as_metrics_cpu_state*
as_metrics_cpu_state_create(void);

/**
 * @private
 * Destroy CPU sampling state.
 */
AS_EXTERN void
as_metrics_cpu_state_destroy(as_metrics_cpu_state* state);

/**
 * @private
 * Copy current cluster metrics into a snapshot. Does not include nodes_departed;
 * the caller attaches those. `labels` may be NULL. `cpu` may be NULL (cpu and mem stay 0).
 */
AS_EXTERN as_status
as_metrics_snapshot_create(
	as_error* err, struct as_cluster_s* cluster, as_vector* labels, as_metrics_cpu_state* cpu,
	as_metrics_snapshot** snapshot
	);

/**
 * @private
 * Release a snapshot and every heap field it owns, including nodes_departed.
 */
AS_EXTERN void
as_metrics_snapshot_destroy(as_metrics_snapshot* snapshot);

/**
 * @private
 * Copy one node's metrics. Used for nodes_departed and by the deprecated file listener.
 */
AS_EXTERN as_status
as_metrics_node_snapshot_create(as_error* err, struct as_node_s* node, as_metrics_node_snapshot** snapshot);

/**
 * @private
 * Release a node snapshot.
 */
AS_EXTERN void
as_metrics_node_snapshot_destroy(as_metrics_node_snapshot* snapshot);

/**
 * @private
 * Install exporters, keep deprecated listeners, and start the metrics thread.
 * Called with cluster->metrics_lock held.
 */
AS_EXTERN as_status
as_metrics_runtime_enable(as_error* err, struct as_cluster_s* cluster, const as_metrics_policy* policy);

/**
 * @private
 * Stop the metrics thread, push a final snapshot, and release client-owned exporters.
 * Called with cluster->metrics_lock held. May drop and reacquire that lock to join the thread.
 */
AS_EXTERN as_status
as_metrics_runtime_disable(as_error* err, struct as_cluster_s* cluster);

/**
 * @private
 * Record a departing node for the next snapshot and invoke the deprecated node-close listener.
 * Called with cluster->metrics_lock held.
 */
AS_EXTERN as_status
as_metrics_runtime_node_close(as_error* err, struct as_cluster_s* cluster, struct as_node_s* node);

/**
 * Enable extended periodic cluster and node latency metrics.
 */
AS_EXTERN as_status
aerospike_enable_metrics(struct aerospike_s* as, as_error* err, const as_metrics_policy* policy);

/**
 * Disable extended periodic cluster and node latency metrics.
 */
AS_EXTERN as_status
aerospike_disable_metrics(struct aerospike_s* as, as_error* err);

#ifdef __cplusplus
} // end extern "C"
#endif
