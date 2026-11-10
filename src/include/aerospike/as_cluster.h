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
#pragma once

#include <aerospike/as_atomic.h>
#include <aerospike/as_config.h>
#include <aerospike/as_file.h>
#include <aerospike/as_metrics.h>
#include <aerospike/as_node.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_queue_mt.h>
#include <aerospike/as_thread_pool.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * @private
 * Reference counted array of server node pointers.
 */
typedef struct as_nodes_s {
	/**
	 * @private
	 * Reference count of node array.
	 */
	uint32_t ref_count;
	
	/**
	 * @private
	 * Length of node array.
	 */
	uint32_t size;
	
	/**
	 * @private
	 * Server node array.
	 */
	as_node* array[];
} as_nodes;

/**
 * @private
 * Cluster state for an event loop.
 */
typedef struct as_event_state_s {
	/**
	 * Cluster's pending command count for this event loop.
	 */
	int pending;
	
	/**
	 * Is cluster closed for this event loop.
	 */
	bool closed;
} as_event_state;

/**
 * @private
 * Immutable, reference counted seed list published by the seed manager.
 */
typedef struct as_seed_snapshot_s as_seed_snapshot;

/**
 * @private
 * Seed discovery runtime: owns the published seed snapshots and schedules seed provider refreshes.
 */
typedef struct as_seed_manager_s as_seed_manager;

/**
 * @private
 * Seed manager counters, for diagnostics and tests.
 */
typedef struct as_seed_manager_stats_s {
	/**
	 * Tends counted after initialization. Passed to the provider as tend_iteration.
	 */
	uint64_t tend_iteration;

	/**
	 * Provider refresh calls, including the initialization call.
	 */
	uint32_t refreshes;

	/**
	 * Refreshes after initialization that kept the previous seeds because the provider failed,
	 * returned invalid output or the merge failed.
	 */
	uint32_t failures;

	/**
	 * Tend mode: consecutive refreshes over tend_refresh_deadline_ms. Thread mode: refreshes over
	 * refresh_timeout_ms, counted only; refreshes never stop.
	 */
	uint32_t overruns;

	/**
	 * Urgent requests that woke the sleeping refresh thread.
	 */
	uint32_t urgent_wakes;

	/**
	 * Snapshots published, including the initialization snapshot.
	 */
	uint32_t publications;

	/**
	 * True while the discovery thread exists, including a thread left finishing a refresh after
	 * stop.
	 */
	bool thread_running;

	/**
	 * True while a provider call runs on the discovery thread.
	 */
	bool in_refresh;

	/**
	 * True when three consecutive tend-mode overruns stopped refreshes and restored the
	 * initialization seeds.
	 */
	bool tripped;
} as_seed_manager_stats;

/**
 * @private
 * Reference counted release function definition.
 */
typedef void (*as_release_fn) (void* value);

/**
 * @private
 * Reference counted data to be garbage collected.
 */
typedef struct as_gc_item_s {
	/**
	 * @private
	 * Reference counted data to be garbage collected.
	 */
	void* data;
	
	/**
	 * @private
	 * Release function.
	 */
	as_release_fn release_fn;
} as_gc_item;

/**
 * Cluster of server nodes.
 */
typedef struct as_cluster_s {
	/**
	 * @private
	 * Active nodes in cluster.
	 */
	as_nodes* nodes;
	
	/**
	 * @private
	 * Hints for best node for a partition.
	 */
	as_partition_tables partition_tables;

	/**
	 * @private
	 * Garbage collector.
	 */
	as_vector* /* <as_gc_item> */ gc;
	
	/**
	 * @private
	 * Shared memory implementation of cluster.
	 */
	struct as_shm_info_s* shm_info;
	
	/**
	 * @private
	 * User name in UTF-8 encoded bytes.
	 */
	char* user;
	
	/**
	 * @private
	 * Password in clear text.
	 */
	char* password;

	/**
	 * @private
	 * Password in hashed format.
	 */
	char* password_hash;

	/**
	 * @private
	 * Expected cluster name for all nodes.  May be null.
	 */
	char* cluster_name;
	
	/**
	 * Application identifier.  May be null.
	 */
	char* app_id;

	/**
	 * Cluster event function that will be called when nodes are added/removed from the cluster.
	 */
	as_cluster_event_callback event_callback;

	/**
	 * Cluster event user data that will be passed back to event_callback.
	 */
	void* event_callback_udata;

	/**
	 * Cluster state for all event loops.
	 */
	as_event_state* event_state;

	/**
	 * @private
	 * Seed snapshots and seed provider refresh scheduling.
	 */
	as_seed_manager* seed_manager;

	/**
	 * @private
	 * A IP translation table is used in cases where different clients use different server
	 * IP addresses.  This may be necessary when using clients from both inside and outside
	 * a local area network.  Default is no translation.
	 *
	 * The key is the IP address returned from friend info requests to other servers.  The
	 * value is the real IP address used to connect to the server.
	 *
	 * NULL when a translator is registered: ip_map is then ignored, configured seeds included.
	 */
	as_vector* /* <as_addr_map> */ ip_map;

	/**
	 * @private
	 * Shallow copy of the translator registered in the config. The name and udata stay
	 * owned by the config, which outlives the cluster and cannot change while connected.
	 * Only valid when translator_registered is true; otherwise ip_map is the translator.
	 */
	as_address_translator translator;

	/**
	 * @private
	 * Behavior when the translator has no mapping.
	 */
	as_translation_miss_policy translation_on_miss;

	/**
	 * @private
	 * Client-wide counter passed to the translator's on_cluster_view_changed callback.
	 * Incremented in the tend thread.
	 */
	uint32_t translation_view_generation;

	/**
	 * @private
	 * True when a registered translator has a translate callback.
	 */
	bool translator_registered;

	/**
	 * @private
	 * True when the registered translator wants cluster view change notifications.
	 */
	bool translation_view_callback;

	/**
	 * @private
	 * TLS parameters
	 */
	as_tls_context* tls_ctx;
	
	/**
	 * @private
	 * Pool of threads used to query server nodes in parallel for batch, scan and query.
	 */
	as_thread_pool thread_pool;
		
	/**
	 * @private
	 * Cluster tend thread.
	 */
	pthread_t tend_thread;
	
	/**
	 * @private
	 * Lock for metrics operations.
	 */
	pthread_mutex_t metrics_lock;

	/**
	 * @private
	 * Lock for the tend thread to wait on with the tend interval as timeout.
	 * Normally locked, resulting in waiting a full interval between
	 * tend iterations.  Upon cluster shutdown, unlocked by the main
	 * thread, allowing a fast termination of the tend thread.
	 */
	pthread_mutex_t tend_lock;
	
	/**
	 * @private
	 * Tend thread identifier to be used with tend_lock.
	 */
	pthread_cond_t tend_cond;

	/**
	 * @private
	 * Queue of sockets that have experienced read timeouts.  This queue is to be
	 * processed by the tend thread.
	 */
	as_queue_mt recover_queue;

	/**
	 * @private
	 * Maximum socket idle to validate connections in commands.
	 */
	uint64_t max_socket_idle_ns_tran;

	/**
	 * @private
	 * Maximum socket idle to trim peak connections to min connections.
	 */
	uint64_t max_socket_idle_ns_trim;

	/**
	 * @private
	 * Rack ids
	 */
	as_vector* rack_ids;

	/**
	 * @private
	 * Max errors per node per error_rate_window.
	 */
	uint32_t max_error_rate;

	/**
	 * @private
	 * Number of tend iterations defining window for max_error_rate.
	 */
	uint32_t error_rate_window;

	/**
	 * @private
	 * Milliseconds between cluster tends.
	 */
	uint32_t tend_interval;

	/**
	 * @private
	 * Cluster tend counter.
	 */
	uint32_t tend_count;

	/**
	 * @private
	 * Minimum sync connections per node.
	 */
	uint32_t min_conns_per_node;

	/**
	 * @private
	 * Maximum sync connections per node.
	 */
	uint32_t max_conns_per_node;
	
	/**
	 * @private
	 * Minimum async connections per node.
	 */
	uint32_t async_min_conns_per_node;

	/**
	 * @private
	 * Maximum async (non-pipeline) connections per node.
	 */
	uint32_t async_max_conns_per_node;

	/**
	 * @private
	 * Maximum pipeline connections per node.
	 */
	uint32_t pipe_max_conns_per_node;
	
	/**
	 * @private
	 * Number of synchronous connection pools used for each node.
	 */
	uint32_t conn_pools_per_node;

	/**
	 * @private
	 * Cluster tend info command timeout in milliseconds.
	 */
	uint32_t conn_timeout_ms;

	/**
	 * @private
	 * Node login timeout in milliseconds.
	 */
	uint32_t login_timeout_ms;

	/**
	 * @private
	 * Random node index counter.
	 */
	uint32_t node_index;

	/**
	 * @private
	 * AS_POLICY_REPLICA_ANY index counter.
	 */
	uint32_t replica_index_any;

	/**
	 * @private
	 * AS_POLICY_REPLICA_RANDOM index counter.
	 */
	uint32_t replica_index_random;

	/**
	 * @private
	 * Count of add node failures in the most recent cluster tend iteration.
	 */
	uint32_t invalid_node_count;

	/**
	 * @private
	 * Assign tend thread to this specific CPU ID.
	 */
	int tend_thread_cpu;

	/**
	 * @private
	 * Authentication mode.
	 */
	as_auth_mode auth_mode;

	/**
	 * @private
	 * Total number of data partitions used by cluster.
	 */
	uint16_t n_partitions;
	
	/**
	 * @private
	 * If alternate services info commands should be used.
	 */
	bool use_services_alternate;
	
	/**
	 * @private
	 * Request server rack ids.
	 */
	bool rack_aware;

	/**
	 * @private
	 * Is authentication enabled
	 */
	bool auth_enabled;

	/**
	 * @private
	 * Does cluster support partition queries.
	 */
	bool has_partition_query;

	/**
	 * @private
	 * Do all cluster nodes support query order-by.
	 */
	bool has_order_by;

	/**
	 * @private
	 * Fail on cluster init if seed node and all peers are not reachable.
	 */
	bool fail_if_not_connected;

	/**
	 * @private
	 * True if the cluster supports query operations projection.
	 */
	bool has_query_ops_projection_ext;

	/**
	 * @private
	 * Should continue to tend cluster.
	 */
	volatile bool valid;

	/**
	 * @private
	 * Is metrics colleciton enabled.
	 */
	bool metrics_enabled;

	/**
	 * @private
	 * Command-path operational metrics. See as_metrics_policy.operational_enabled.
	 */
	bool metrics_operational_enabled;

	/**
	 * @private
	 * Client-wide usage counters. Off unless the metrics policy sets usage_enabled.
	 */
	bool metrics_usage_enabled;

	/**
	 * @private
	 * A learn-metrics file writer is installed. Timing follows operational
	 * metrics: commands are not timed while operational metrics are off.
	 */
	bool metrics_file_writer;

	/**
	 * @private
	 * Histogram bucket unit. This is set using as_metrics_policy.
	 */
	as_metrics_latency_unit metrics_latency_unit;

	/**
	 * @private
	 * Number of elapsed time range buckets in latency histograms. This is set using as_policy_metrics.
	 */
	uint8_t metrics_latency_columns;

	/**
	 * @private
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
	 * This is set using as_policy_metrics.
	 */
	uint8_t metrics_latency_shift;

	/**
	 * @private
	 * v3 command latency. One sample per command for total elapsed time
	 * including retries, across nodes. Allocated only while operational
	 * metrics are enabled.
	 */
	as_latency* command_latency[AS_LATENCY_TYPE_MAX];

	/**
	 * @private
	 * Operational sampler modulus. >= 1. Read with as_load_uint32().
	 */
	uint32_t metrics_sampler_range;

	/**
	 * @private
	 * Operational sampler threshold, 0 through range. 0 records no command-path
	 * operational metrics. Read with as_load_uint32().
	 */
	uint32_t metrics_sampler_threshold;

	/**
	 * @private
	 * Milliseconds between metrics exports. Set from as_metrics_policy.export_interval,
	 * or from interval * as_config.tender_interval when export_interval is empty.
	 */
	uint64_t metrics_interval;

	/**
	 * @private
	 * Deprecated four-callback metrics listener. Exporters are preferred.
	 * This is set using as_policy_metrics.
	 */
	as_metrics_listeners metrics_listeners;

	/**
	 * @private
	 * Metrics export runtime (thread and exporters). NULL when metrics are off.
	 * Departed-node samples live on the cluster so they survive disable and do
	 * not require an exporter.
	 */
	struct as_metrics_runtime_s* metrics_runtime;

	/**
	 * @private
	 * Final samples for nodes that left, one entry per node name. Kept until a
	 * successful export. On-demand snapshots copy this list and do not clear it.
	 * Rejoin and repeat-departure behavior: see as_metrics_snapshot.nodes_departed.
	 */
	as_vector* metrics_nodes_departed;

	/**
	 * @private
	 * Command retry count. There can be multiple retries for a single command.
	 * The value is cumulative and not reset per metrics interval.
	 */
	uint64_t retry_count;

	/**
	 * @private
	 * Command count. The value is cumulative and not reset per metrics interval.
	 */
	uint64_t command_count;

	/**
	 * @private
	 * feature.api.blocking. Cumulative public synchronous API calls.
	 */
	uint64_t metrics_api_blocking;

	/**
	 * @private
	 * feature.api.deferred. Cumulative public asynchronous API calls.
	 */
	uint64_t metrics_api_deferred;

	/**
	 * @private
	 * feature.api.background. Cumulative public background scan and query calls.
	 */
	uint64_t metrics_api_background;

	/**
	 * @private
	 * Delay queue timeout count. The value is cumulative and not reset per metrics interval.
	 */
	uint64_t delay_queue_timeout_count;

	/**
	 * @private
	 * cluster.validation.open.failure. Pre-node connect failures, including TLS and auth.
	 * Recorded only when operational metrics are enabled.
	 */
	uint32_t metrics_validation_open_failures;

	/**
	 * @private
	 * cluster.validation.tls.handshake.failure. Also included in metrics_validation_open_failures.
	 * Recorded only when operational metrics are enabled.
	 */
	uint32_t metrics_validation_tls_handshake_failures;

	/**
	 * @private
	 * cluster.validation.auth.failure. Also included in metrics_validation_open_failures.
	 * Recorded only when operational metrics are enabled.
	 */
	uint32_t metrics_validation_auth_failures;

	/**
	 * @private
	 * Aerospike back pointer.
	 */
	struct aerospike_s* as;

	/**
	 * @private
	 * Dynamic configuration file status.
	 */
	as_file_status config_file_status;

	/**
	 * @private
	 * Milliseconds between dynamic configuration check for file modifications.
	 */
	uint32_t config_interval;

} as_cluster;

struct aerospike_s;

//---------------------------------
// Functions
//---------------------------------

/**
 * Create and initialize cluster.
 */
as_status
as_cluster_create(struct aerospike_s* as, as_error* err);

/**
 * Close all connections and release memory associated with cluster.
 */
void
as_cluster_destroy(as_cluster* cluster);

/**
 * Is cluster connected to any server nodes.
 */
bool
as_cluster_is_connected(as_cluster* cluster);

/**
 * Get all node names in cluster.
 */
void
as_cluster_get_node_names(as_cluster* cluster, int* n_nodes, char** node_names);

/**
 * Reserve reference counted access to cluster nodes.
 */
static inline as_nodes*
as_nodes_reserve(as_cluster* cluster)
{
	as_nodes* nodes = (as_nodes*)as_load_ptr((void* const*)&cluster->nodes);
	as_incr_uint32(&nodes->ref_count);
	return nodes;
}

/**
 * Release each individual node and free nodes struct.
 */
AS_EXTERN void
as_nodes_destroy(as_nodes* nodes);

/**
 * Release reference counted access to cluster nodes.
 */
static inline void
as_nodes_release(as_nodes* nodes)
{
	if (as_aaf_uint32_rls(&nodes->ref_count, -1) == 0) {
		as_fence_acq();
		as_nodes_destroy(nodes);
	}
}

/**
 * @private
 * Translate an advertised peer or service endpoint into the host to connect to. Uses the
 * registered translator, or the static map over ip_map when none is registered, then applies the
 * translation miss policy. On AEROSPIKE_OK out is a copy owned by the caller, with a copy of
 * tls_name: translation never changes the TLS identity. A strict miss returns
 * AEROSPIKE_ERR_TRANSLATION_MISS. Performs no I/O.
 */
as_status
as_cluster_translate_address(
	as_cluster* cluster, as_error* err, const char* node_name, const char* tls_name,
	const char* host, uint16_t port, as_endpoint_source source, as_host* out
	);

/**
 * @private
 * Source list that servers advertise endpoints from, according to use_services_alternate.
 */
static inline as_endpoint_source
as_cluster_endpoint_source(const as_cluster* cluster)
{
	return cluster->use_services_alternate ? AS_ENDPOINT_SOURCE_ALTERNATE : AS_ENDPOINT_SOURCE_STANDARD;
}

/**
 * Reserve nodes. Return error if cluster is empty.
 */
AS_EXTERN as_status
as_cluster_reserve_all_nodes(as_cluster* cluster, as_error* err, as_nodes** nodes);

/**
 * Release nodes.
 */
static inline void
as_cluster_release_all_nodes(as_nodes* nodes)
{
	as_nodes_release(nodes);
}

/**
 * Verify cluster contains nodes and return node count.
 */
as_status
as_cluster_validate_size(as_cluster* cluster, as_error* err, uint32_t* size);

/**
 * Add seed to cluster. The seed list is replaced, not modified, so a reseed already in progress
 * keeps the list it started with. A later seed provider refresh may replace the added seed
 * according to as_config.discovery.merge_policy. An empty tls_name is the same as NULL; with TLS
 * enabled a NULL tls_name gets the configured-seed default.
 */
AS_EXTERN void
as_cluster_add_seed(as_cluster* cluster, const char* hostname, const char* tls_name, uint16_t port);

/**
 * Remove seed from cluster. The seed list is replaced, not modified, so a reseed already in
 * progress keeps the list it started with. A later seed provider refresh may add the seed again.
 */
AS_EXTERN void
as_cluster_remove_seed(as_cluster* cluster, const char* hostname, uint16_t port);

/**
 * @private
 * Tracks the discovery-thread provider call of one config, which can outlive the seed manager
 * that started it, and the provider destroys handed over while it runs.
 */
typedef struct as_seed_inflight_s as_seed_inflight;

/**
 * @private
 * Release a reference on the call tracker. Accepts NULL.
 */
void
as_seed_inflight_release(as_seed_inflight* inflight);

/**
 * @private
 * When a discovery-thread call with this udata is running, hand destroy(udata) over to that
 * thread, which runs it after the call returns, and return true. Otherwise return false and the
 * caller destroys udata. Accepts a NULL tracker.
 */
bool
as_seed_inflight_defer_destroy(as_seed_inflight* inflight, void (*destroy)(void*), void* udata);

/**
 * @private
 * Wait at most timeout_ms for a running discovery-thread call to return. Returns
 * AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, naming the provider, when it is still running. Accepts a
 * NULL tracker.
 */
as_status
as_seed_inflight_wait(as_seed_inflight* inflight, uint32_t timeout_ms, as_error* err);

/**
 * @private
 * Wait at most refresh_timeout_ms for a refresh left running by an earlier seed manager of the
 * same config, then validate the discovery configuration and create a seed manager over copies
 * of config->hosts, the cluster name and the provider name. Calls the provider's
 * needs_periodic_refresh and supports_deadline; does not call refresh. A manager that will run a
 * discovery thread takes a reference on config->discovery.inflight.
 *
 * Returns AEROSPIKE_ERR_SEED_DISCOVERY_FAILED when the earlier refresh is still running,
 * AEROSPIKE_ERR_PARAM for an invalid discovery setting.
 */
AS_EXTERN as_status
as_seed_manager_create(as_error* err, as_config* config, as_seed_manager** manager);

/**
 * @private
 * Obtain and publish the initialization seeds. A binding provider, or a periodic provider in
 * thread execution mode, is refreshed on the started discovery thread, and this call waits for
 * its first result for at most refresh_timeout_ms; otherwise the provider is refreshed once on
 * the calling thread.
 *
 * Returns AEROSPIKE_ERR_SEED_DISCOVERY_FAILED when the provider failed and no configured seed is
 * left.
 */
AS_EXTERN as_status
as_seed_manager_start(as_seed_manager* manager, as_error* err);

/**
 * @private
 * Mark connect complete. Until then, stop waits only for the rest of the connect budget, which
 * started with the initial refresh.
 */
AS_EXTERN void
as_seed_manager_connected(as_seed_manager* manager);

/**
 * @private
 * Reserve the current seed snapshot. Never calls the provider and never waits for a refresh.
 * Release with as_seed_snapshot_release().
 */
AS_EXTERN as_seed_snapshot*
as_seed_manager_reserve(as_seed_manager* manager);

/**
 * @private
 * Hosts (as_host elements) of a reserved snapshot. Read-only, valid until the snapshot is released.
 */
AS_EXTERN const as_vector*
as_seed_snapshot_hosts(const as_seed_snapshot* snapshot);

/**
 * @private
 * True when the legacy ip_map applies to the snapshot host at index: configured seeds, node
 * addresses and as_cluster_add_seed() hosts. Provider-only hosts are used as returned.
 */
AS_EXTERN bool
as_seed_snapshot_uses_ip_map(const as_seed_snapshot* snapshot, uint32_t index);

/**
 * @private
 * Release a reserved seed snapshot.
 */
AS_EXTERN void
as_seed_snapshot_release(as_seed_snapshot* snapshot);

/**
 * @private
 * Wake a sleeping refresh thread so that it refreshes now, at most once per refresh_interval_ms.
 * Returns immediately. Does nothing when a refresh is in progress or no refresh thread exists.
 */
AS_EXTERN void
as_seed_manager_request_urgent(as_seed_manager* manager);

/**
 * @private
 * Count one tend after initialization and, in tend execution mode, refresh the provider on its
 * cadence. Called by the tend thread after the reseed attempt of that tend.
 */
AS_EXTERN void
as_seed_manager_tend(as_seed_manager* manager);

/**
 * @private
 * Publish a copy of the current seeds with the given hosts (as_host elements) appended. Hosts
 * already present (same name and port) are skipped.
 */
AS_EXTERN void
as_seed_manager_add_hosts(as_seed_manager* manager, const as_vector* hosts);

/**
 * @private
 * Publish a copy of the current seeds without any host of the given name and port.
 */
AS_EXTERN void
as_seed_manager_remove_host(as_seed_manager* manager, const char* name, uint16_t port);

/**
 * @private
 * Stop the discovery thread. An idle thread is joined. A refresh in progress is waited for at most
 * refresh_timeout_ms, or the rest of the connect budget before as_seed_manager_connected(); after
 * that the thread is detached and finishes the call on its own. No provider call starts after
 * this returns. Seeds stay readable and modifiable. Idempotent.
 */
AS_EXTERN void
as_seed_manager_stop(as_seed_manager* manager);

/**
 * @private
 * Stop the seed manager and release the caller's reference. A thread still finishing a refresh
 * releases the manager when it exits.
 */
AS_EXTERN void
as_seed_manager_destroy(as_seed_manager* manager);

/**
 * @private
 * Copy the seed manager counters.
 */
AS_EXTERN void
as_seed_manager_get_stats(as_seed_manager* manager, as_seed_manager_stats* stats);

/**
 * @private
 * Change user and password that is used to authenticate with cluster servers.
 */
void
as_cluster_change_password(as_cluster* cluster, const char* user, const char* password, const char* password_hash);

/**
 * @private
 * Return the legacy ip_map translation of hostname, or hostname when unmapped.
 */
AS_EXTERN const char*
as_cluster_get_alternate_host(as_cluster* cluster, const char* hostname);

/**
 * @private
 * Get random node in the cluster.
 * as_nodes_release() must be called when done with node.
 */
AS_EXTERN as_node*
as_node_get_random(as_cluster* cluster);

/**
 * @private
 * Get node given node name.
 * as_nodes_release() must be called when done with node.
 */
AS_EXTERN as_node*
as_node_get_by_name(as_cluster* cluster, const char* name);

/**
 * @private
 * Get mapped node given partition and replica.  This function does not reserve the node.
 * The caller must reserve the node for future use.
 */
as_node*
as_partition_reg_get_node(
	as_cluster* cluster, const char* ns, as_partition* p, as_node* prev_node,
	as_policy_replica replica, uint8_t replica_size, uint8_t* replica_index
	);

struct as_partition_shm_s;

/**
 * @private
 * Get mapped node given partition and replica.  The function does not reserve the node.
 * The caller must reserve the node for future use.
 */
as_node*
as_partition_shm_get_node(
	as_cluster* cluster, const char* ns, struct as_partition_shm_s* partition,
	as_node* prev_node, as_policy_replica replica, uint8_t replica_size, uint8_t* replica_index
	);

/**
 * @private
 * Enable the collection of metrics
 */
as_status
as_cluster_enable_metrics(as_error* err, as_cluster* cluster, const as_metrics_policy* policy);

/**
 * @private
 * Disable the collection of metrics
 */
as_status
as_cluster_disable_metrics(as_error* err, as_cluster* cluster);

/**
 * @private
 * Allocate or reset cluster command latency histograms when operational
 * metrics are on. Release them when operational metrics are off.
 */
void
as_cluster_enable_command_latency(as_cluster* cluster, const as_metrics_policy* policy);

/**
 * @private
 * Release cluster command latency histograms. In-flight samples keep the
 * histogram alive until they finish.
 */
void
as_cluster_disable_command_latency(as_cluster* cluster);

/**
 * @private
 * Record one v3 command latency sample. elapsed_nanos includes retries.
 * No-op when operational command histograms are not allocated.
 */
void
as_cluster_add_command_latency(as_cluster* cluster, as_latency_type latency_type, uint64_t elapsed_nanos);

/**
 * @private
 * True when this draw should record command-path operational metrics.
 * One call is one decision. Draw before the retry loop and reuse the result.
 */
bool
as_cluster_sample_operational(as_cluster* cluster);

/**
 * @private
 * Draw the sampler once for this user call and increment command count when
 * it is sampled. Returns the sampler decision, so node commands and retries of
 * that same call can reuse it. See as_cluster_sample_operational().
 */
static inline bool
as_cluster_sample_command(as_cluster* cluster)
{
	if (!as_cluster_sample_operational(cluster)) {
		return false;
	}

	as_incr_uint64(&cluster->command_count);
	return true;
}

/**
 * @deprecated
 * Return command count. The value is cumulative and not reset per metrics interval.
 * This function is left for backwards compatibility. Use as_cluster_get_command_count() instead.
 */
static inline uint64_t
as_cluster_get_tran_count(const as_cluster* cluster)
{
	return as_load_uint64(&cluster->command_count);
}

/**
 * @private
 * Return command count. The value is cumulative and not reset per metrics interval.
 */
static inline uint64_t
as_cluster_get_command_count(const as_cluster* cluster)
{
	return as_load_uint64(&cluster->command_count);
}

/**
 * @private
 * Increment async delay queue timeout count.
 */
static inline void
as_cluster_add_retry(as_cluster* cluster)
{
	as_incr_uint64(&cluster->retry_count);
}

/**
 * @private
 * Add command retry count. There can be multiple retries for a single command.
 */
static inline void
as_cluster_add_retries(as_cluster* cluster, uint32_t count)
{
	as_faa_uint64(&cluster->retry_count, count);
}

/**
 * @private
 * Return command retry count. The value is cumulative and not reset per metrics interval.
 */
static inline uint64_t
as_cluster_get_retry_count(const as_cluster* cluster)
{
	return as_load_uint64(&cluster->retry_count);
}

/**
 * @private
 * Increment async delay queue timeout count.
 */
static inline void
as_cluster_add_delay_queue_timeout(as_cluster* cluster)
{
	as_incr_uint64(&cluster->delay_queue_timeout_count);
}

/**
 * @private
 * Record cluster.validation.open.failure when operational metrics are enabled.
 */
static inline void
as_cluster_add_validation_open_failure(as_cluster* cluster)
{
	if (cluster->metrics_operational_enabled) {
		as_incr_uint32(&cluster->metrics_validation_open_failures);
	}
}

/**
 * @private
 * Record cluster.validation.tls.handshake.failure. Also counts as open.failure.
 */
static inline void
as_cluster_add_validation_tls_handshake_failure(as_cluster* cluster)
{
	if (cluster->metrics_operational_enabled) {
		as_incr_uint32(&cluster->metrics_validation_tls_handshake_failures);
		as_incr_uint32(&cluster->metrics_validation_open_failures);
	}
}

/**
 * @private
 * Record cluster.validation.auth.failure. Also counts as open.failure.
 */
static inline void
as_cluster_add_validation_auth_failure(as_cluster* cluster)
{
	if (cluster->metrics_operational_enabled) {
		as_incr_uint32(&cluster->metrics_validation_auth_failures);
		as_incr_uint32(&cluster->metrics_validation_open_failures);
	}
}

/**
 * @private
 * Return async delay queue timeout count.
 */
static inline uint64_t
as_cluster_get_delay_queue_timeout_count(const as_cluster* cluster)
{
	return as_load_uint64(&cluster->delay_queue_timeout_count);
}

/**
 * @private
 * Get a count of the number of sockets currently waiting for timeout recovery.
 * This is determined directly from the queue itself.
 */
static inline uint32_t
as_cluster_recover_queue_size(as_cluster* cluster)
{
	return as_queue_mt_size(&cluster->recover_queue);
}

/**
 * @private
 * Get mapped node given partition and replica.  This function does not reserve the node.
 * The caller must reserve the node for future use.
 */
static inline as_node*
as_partition_get_node(
	as_cluster* cluster, const char* ns, void* partition, as_node* prev_node,
	as_policy_replica replica, uint8_t replica_size, uint8_t* replica_index
	)
{
	if (cluster->shm_info) {
		return as_partition_shm_get_node(cluster, ns, (struct as_partition_shm_s*)partition,
			prev_node, replica, replica_size, replica_index);
	}
	else {
		return as_partition_reg_get_node(cluster, ns, (as_partition*)partition, prev_node,
			replica, replica_size, replica_index);
	}
}

/**
 * @private
 * Increment node's error count.
 */
static inline void
as_node_incr_error_rate(as_node* node)
{
	as_incr_uint32(&node->error_rate);
}

/**
 * @private
 * Close connection and increment node's error count.
 */
static inline void
as_node_close_conn_error(as_node* node, as_socket* sock, as_conn_pool* pool)
{
	as_node_close_connection(node, sock, pool);
	as_node_incr_error_rate(node);
}

/**
 * @private
 * Put connection in pool and increment node's error count.
 */
static inline void
as_node_put_conn_error(as_node* node, as_socket* sock)
{
	as_node_put_connection(node, sock);
	as_node_incr_error_rate(node);
}

static inline as_vector*
as_rack_ids_load(as_vector** rack_ids)
{
	return (as_vector*)as_load_ptr((void* const*)rack_ids);
}

#ifdef __cplusplus
} // end extern "C"
#endif
