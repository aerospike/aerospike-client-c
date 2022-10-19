/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <aerospike/as_node.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_thread_pool.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

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
	 * Nodes to be garbage collected.
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
	 * Initial seed hosts specified by user.
	 */
	as_vector* /* <as_host> */ seeds;

	/**
	 * @private
	 * A IP translation table is used in cases where different clients use different server
	 * IP addresses.  This may be necessary when using clients from both inside and outside
	 * a local area network.  Default is no translation.
	 *
	 * The key is the IP address returned from friend info requests to other servers.  The
	 * value is the real IP address used to connect to the server.
	 */
	as_vector* /* <as_addr_map> */ ip_map;

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
	 * Lock for adding/removing seeds.
	 */
	pthread_mutex_t seed_lock;

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
	 * Maximum socket idle to validate connections in transactions.
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
	int* rack_ids;

	/**
	 * @private
	 * Rack ids size
	 */
	uint32_t rack_ids_size;

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
	 * Initial connection timeout in milliseconds.
	 */
	uint32_t conn_timeout_ms;

	/**
	 * @private
	 * Node login timeout in milliseconds.
	 */
	uint32_t login_timeout_ms;

	/**
	 * @private
	 * Random node index.
	 */
	uint32_t node_index;

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
	 * If "services-alternate" should be used instead of "services"
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
	 * Fail on cluster init if seed node and all peers are not reachable.
	 */
	bool fail_if_not_connected;

	/**
	 * @private
	 * Should continue to tend cluster.
	 */
	volatile bool valid;
} as_cluster;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Create and initialize cluster.
 */
as_status
as_cluster_create(as_config* config, as_error* err, as_cluster** cluster);

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
 * Release reference counted access to cluster nodes.
 */
static inline void
as_nodes_release(as_nodes* nodes)
{
	if (as_aaf_uint32_rls(&nodes->ref_count, -1) == 0) {
		cf_free(nodes);
	}
}

/**
 * Reserve nodes and all sub nodes.
 */
as_status
as_cluster_reserve_all_nodes(as_cluster* cluster, as_error* err, as_nodes** nodes);

/**
 * Release nodes and all sub nodes.
 */
void
as_cluster_release_all_nodes(as_nodes* nodes);

/**
 * Verify cluster contains nodes and return node count.
 */
as_status
as_cluster_validate_size(as_cluster* cluster, as_error* err, uint32_t* size);

/**
 * Add seed to cluster.
 */
AS_EXTERN void
as_cluster_add_seed(as_cluster* cluster, const char* hostname, const char* tls_name, uint16_t port);

/**
 * Remove seed from cluster.
 */
AS_EXTERN void
as_cluster_remove_seed(as_cluster* cluster, const char* hostname, uint16_t port);

/**
 * @private
 * Change user and password that is used to authenticate with cluster servers.
 */
void
as_cluster_change_password(as_cluster* cluster, const char* user, const char* password, const char* password_hash);

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
	as_policy_replica replica, bool use_master
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
	as_node* prev_node, as_policy_replica replica, bool use_master
	);

/**
 * @private
 * Get mapped node given partition and replica.  This function does not reserve the node.
 * The caller must reserve the node for future use.
 */
static inline as_node*
as_partition_get_node(
	as_cluster* cluster, const char* ns, void* partition, as_node* prev_node,
	as_policy_replica replica, bool master
	)
{
	if (cluster->shm_info) {
		return as_partition_shm_get_node(cluster, ns, (struct as_partition_shm_s*)partition,
										 prev_node, replica, master);
	}
	else {
		return as_partition_reg_get_node(cluster, ns, (as_partition*)partition,
										 prev_node, replica, master);
	}
}

/**
 * @private
 * Increment node's error count.
 */
static inline void
as_node_incr_error_count(as_node* node)
{
	if (node->cluster->max_error_rate > 0) {
		as_incr_uint32(&node->error_count);
	}
}

/**
 * @private
 * Reset node's error count.
 */
static inline void
as_node_reset_error_count(as_node* node)
{
	as_store_uint32(&node->error_count, 0);
}

/**
 * @private
 * Get node's error count.
 */
static inline uint32_t
as_node_get_error_count(as_node* node)
{
	return as_load_uint32(&node->error_count);
}

/**
 * @private
 * Validate node's error count.
 */
static inline bool
as_node_valid_error_count(as_node* node)
{
	uint32_t max = node->cluster->max_error_rate;
	return max == 0 || max >= as_load_uint32(&node->error_count);
}

/**
 * @private
 * Close connection and increment node's error count.
 */
static inline void
as_node_close_conn_error(as_node* node, as_socket* sock, as_conn_pool* pool)
{
	as_node_close_connection(node, sock, pool);
	as_node_incr_error_count(node);
}

/**
 * @private
 * Put connection in pool and increment node's error count.
 */
static inline void
as_node_put_conn_error(as_node* node, as_socket* sock)
{
	as_node_put_connection(node, sock);
	as_node_incr_error_count(node);
}

#ifdef __cplusplus
} // end extern "C"
#endif
