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

#include <aerospike/as_atomic.h>
#include <aerospike/as_config.h>
#include <aerospike/as_conn_pool.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_latency.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_queue.h>
#include <aerospike/as_vector.h>

#if !defined(_MSC_VER)
#include <netinet/in.h>
#include <sys/uio.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
	
/******************************************************************************
 * MACROS
 *****************************************************************************/

/**
 * Maximum size (including NULL byte) of a hostname.
 */
#define AS_HOSTNAME_SIZE 256

/**
 * Maximum size of node name
 */
#define AS_NODE_NAME_SIZE 20

// Leave this is in for backwards compatibility.
#define AS_NODE_NAME_MAX_SIZE AS_NODE_NAME_SIZE

#define AS_FEATURES_PARTITION_SCAN (1 << 0)
#define AS_FEATURES_QUERY_SHOW (1 << 1)
#define AS_FEATURES_BATCH_ANY (1 << 2)
#define AS_FEATURES_PARTITION_QUERY (1 << 3)

#define AS_ADDRESS4_MAX 4
#define AS_ADDRESS6_MAX 8

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Socket address information.
 */
typedef struct as_address_s {
	/**
	 * Socket IP address.
	 */
	struct sockaddr_storage addr;
	
	/**
	 * Socket IP address string representation including port.
	 */
	char name[AS_IP_ADDRESS_SIZE];
	
} as_address;

/**
 * @private
 * Rack.
 */
typedef struct as_rack_s {
	/**
	 * Namespace
	 */
	char ns[AS_MAX_NAMESPACE_SIZE];

	/**
	 * Rack ID
	 */
	int rack_id;

} as_rack;

/**
 * @private
 * Racks.
 */
typedef struct as_racks_s {
	/**
	 * Reference count of racks array.
	 */
	uint32_t ref_count;

	/**
	 * Rack ID when all namespaces use same rack.
	 */
	int rack_id;

	/**
	 * Length of racks array.
	 */
	uint32_t size;

	/**
	 * Pad to 8 byte boundary.
	 */
	uint32_t pad;

	/**
	 * Racks array.
	 */
	as_rack racks[];

} as_racks;

/**
 * @private
 * Session info.
 */
typedef struct as_session_s {
	/**
	 * Reference count of session.
	 */
	uint32_t ref_count;

	/**
	 * Session token length.
	 */
	uint32_t token_length;

	/**
	 * Session expiration for this node.
	 */
	uint64_t expiration;

	/**
	 * Session token for this node.
	 */
	uint8_t token[];

} as_session;

/**
 * @private
 * Async connection pool.
 */
typedef struct as_async_conn_pool_s {
	/**
	 * Async connection queue.
	 */
	as_queue queue;

	/**
	 * Min connections allowed for this pool.
	 */
	uint32_t min_size;

	/**
	 * Max connections allowed for this pool.
	 */
	uint32_t limit;

	/**
	 * Total async connections opened.
	 */
	uint32_t opened;

	/**
	 * Total async connections closed.
	 */
	uint32_t closed;

} as_async_conn_pool;

/**
 * Node metrics latency bucket struct
 */
typedef struct as_node_metrics_s {
	as_latency_buckets* latency;
} as_node_metrics;

struct as_cluster_s;

/**
 * Server node representation.
 */
typedef struct as_node_s {
	/**
	 * Reference count of node.
	 */
	uint32_t ref_count;
	
	/**
	 * Reference count of node in partition maps.
	 */
	uint32_t partition_ref_count;

	/**
	 * Server's generation count for partition management.
	 */
	uint32_t partition_generation;
	
	/**
	 * Features supported by server.  Stored in bitmap.
	 */
	uint32_t features;

	/**
	 * TLS certificate name (needed for TLS only, NULL otherwise).
	 */
	char* tls_name;
	
	/**
	 * The name of the node.
	 */
	char name[AS_NODE_NAME_SIZE];
	
	/**
	 * Primary address index into addresses array.
	 */
	uint32_t address_index;
		
	/**
	 * Number of IPv4 addresses.
	 */
	uint32_t address4_size;

	/**
	 * Number of IPv6 addresses.
	 */
	uint32_t address6_size;

	/**
	 * Array of IP addresses. Not thread-safe.
	 */
	as_address* addresses;
	
	/**
	 * Optional hostname. Not thread-safe.
	 */
	char* hostname;

	/**
	 * Cluster from which this node resides.
	 */
	struct as_cluster_s* cluster;
	
	/**
	 * Pools of current, cached sockets.
	 */
	as_conn_pool* sync_conn_pools;
	
	/**
	 * Array of connection pools used in async commands.  There is one pool per node/event loop.
	 * Only used by event loop threads. Not thread-safe.
	 */
	as_async_conn_pool* async_conn_pools;
	
	/**
	 * Pool of connections used in pipelined async commands.  Also not thread-safe.
	 */
	as_async_conn_pool* pipe_conn_pools;

	/**
	 * Authentication session.
	 */
	as_session* session;

	/**
	 * Racks data.
	 */
	as_racks* racks;

	/**
	 * Node metrics 
	 */
	as_node_metrics* metrics;

	/**
	 * Socket used exclusively for cluster tend thread info requests.
	 */
	as_socket info_socket;

	/**
	 * Command error count since node was initialized. If the error is retryable, multiple errors per
	 * command may occur.
	 */
	uint64_t error_count;

	/**
	 * Command timeout count since node was initialized. If the timeout is retryable (ie socketTimeout),
	 * multiple timeouts per command may occur.
	 */
	uint64_t timeout_count;

	/**
	 * Connection queue iterator.  Not atomic by design.
	 */
	uint32_t conn_iter;

	/**
	 * Total sync connections opened.
	 */
	uint32_t sync_conns_opened;

	/**
	 * Total sync connections closed.
	 */
	uint32_t sync_conns_closed;

	/**
	 * Error count for this node's error_rate_window.
	 */
	uint32_t error_rate;
	
	/**
	 * Server's generation count for peers.
	 */
	uint32_t peers_generation;

	/**
	 * Number of peers returned by server node.
	 */
	uint32_t peers_count;

	/**
	 * Server's generation count for partition rebalancing.
	 */
	uint32_t rebalance_generation;

	/**
	 * Number of other nodes that consider this node a member of the cluster.
	 */
	uint32_t friends;
	
	/**
	 * Number of consecutive info request failures.
	 */
	uint32_t failures;

	/**
	 * Shared memory node array index.
	 */
	uint32_t index;
	
	/**
	 * Should user login to avoid session expiration.
	 */
	uint8_t perform_login;

	/**
	 * Is node currently active.
	 */
	uint8_t active;
	
	/**
	 * Did partition change in current cluster tend.
	 */
	bool partition_changed;

	/**
	 * Did rebalance generation change in current cluster tend.
	 */
	bool rebalance_changed;

} as_node;

/**
 * @private
 * Node discovery information.
 */
typedef struct as_node_info_s {
	/**
	 * Node name.
	 */
	char name[AS_NODE_NAME_SIZE];

	/**
	 * Features supported by server.  Stored in bitmap.
	 */
	uint32_t features;

	/**
	 * Host.
	 */
	as_host host;

	/**
	 * Validated socket.
	 */
	as_socket socket;

	/**
	 * Socket address.
	 */
	struct sockaddr_storage addr;

	/**
	 * Authentication session.
	 */
	as_session* session;

} as_node_info;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * @private
 * Create new cluster node.
 */
as_node*
as_node_create(struct as_cluster_s* cluster, as_node_info* node_info);

/**
 * @private
 * Close all connections in pool and free resources.
 */
AS_EXTERN void
as_node_destroy(as_node* node);

/**
 * @private
 * Destroy node metrics.
 */
void
as_node_destroy_metrics(as_node* node);

/**
 * @private
 * Create configured minimum number of connections.
 */
void
as_node_create_min_connections(as_node* node);

/**
 * @private
 * Check if node is active from a command thread.
 */
static inline bool
as_node_is_active(const as_node* node)
{
	return (bool)as_load_uint8_acq(&node->active);
}

/**
 * @private
 * Set node to inactive.
 */
static inline void
as_node_deactivate(as_node* node)
{
	// Make volatile write so changes are reflected in other threads.
	as_store_uint8_rls(&node->active, false);
}

/**
 * @private
 * Read volatile node.
 */
static inline as_node*
as_node_load(as_node** node)
{
	return (as_node*)as_load_ptr((void* const*)node);
}

/**
 * @private
 * Reserve existing cluster node.
 */
static inline void
as_node_reserve(as_node* node)
{
	as_incr_uint32(&node->ref_count);
}

/**
 * @private
 * Set volatile node.
 */
static inline void
as_node_store(as_node** trg, as_node* src)
{
	as_store_ptr_rls((void**)trg, src);
}

/**
 * @private
 * Release existing cluster node.
 */
static inline void
as_node_release(as_node* node)
{
	if (as_aaf_uint32_rls(&node->ref_count, -1) == 0) {
		as_fence_acq();
		as_node_destroy(node);
	}
}

/**
 * @private
 * Release node on next cluster tend iteration.
 */
void
as_node_release_delayed(as_node* node);

/**
 * @private
 * Add socket address to node addresses.
 */
void
as_node_add_address(as_node* node, struct sockaddr* addr);

/**
 * @private
 * Set hostname.
 */
void
as_node_set_hostname(as_node* node, const char* hostname);

/**
 * Get primary socket address.
 */
static inline as_address*
as_node_get_address(as_node* node)
{
	return &node->addresses[node->address_index];
}

/**
 * Get socket address as a string.
 */
static inline const char*
as_node_get_address_string(as_node* node)
{
	return node->addresses[node->address_index].name;
}

/**
 * @private
 * Attempt to authenticate given current cluster's user and password.
 */
as_status
as_node_authenticate_connection(struct as_cluster_s* cluster, uint64_t deadline_ms);

/**
 * @private
 * Get a connection to the given node from pool and validate.  Return 0 on success.
 */
as_status
as_node_get_connection(as_error* err, as_node* node, uint32_t socket_timeout, uint64_t deadline_ms, as_socket* sock);

/**
 * @private
 * Close a node's connection and update node/pool statistics.
 */
static inline void
as_node_close_connection(as_node* node, as_socket* sock, as_conn_pool* pool)
{
	as_socket_close(sock);
	as_incr_uint32(&node->sync_conns_closed);
	as_conn_pool_decr(pool);
}

/**
 * @private
 * Close a node's connection and update node statistics.
 */
static inline void
as_node_close_socket(as_node* node, as_socket* sock)
{
	as_socket_close(sock);
	as_incr_uint32(&node->sync_conns_closed);
}

/**
 * @private
 * Put connection back into pool.
 */
static inline void
as_node_put_connection(as_node* node, as_socket* sock)
{
	// Save pool.
	as_conn_pool* pool = sock->pool;

	// Update last used timestamp.
	sock->last_used = cf_getns();

	// Put into pool.
	if (! as_conn_pool_push_head(pool, sock)) {
		as_node_close_connection(node, sock, pool);
	}
}

/**
 * @private
 * Balance sync connections.
 */
void
as_node_balance_connections(as_node* node);

/**
 * @private
 * Are hosts equal.
 */
static inline bool
as_host_equals(as_host* h1, as_host* h2)
{
	return strcmp(h1->name, h2->name) == 0 && h1->port == h2->port;
}

/**
 * @private
 * Destroy node_info contents.
 */
static inline void
as_node_info_destroy(as_node_info* node_info)
{
	as_socket_close(&node_info->socket);
	cf_free(node_info->session);
}

/**
 * @private
 * Tell tend thread to perform another node login.
 */
void
as_node_signal_login(as_node* node);

/**
 * @private
 * Does node contain rack.
 */
bool
as_node_has_rack(as_node* node, const char* ns, int rack_id);

/**
 * @private
 * Record latency of type latency_type for node
 */
void
as_node_add_latency(as_node* node, as_latency_type latency_type, uint64_t elapsed);

struct as_metrics_policy_s;

/**
 * @private
 * Enable metrics at the node level
 */
void
as_node_enable_metrics(as_node* node, const struct as_metrics_policy_s* policy);

/**
 * Return command error count. The value is cumulative and not reset per metrics interval.
 */
static inline uint64_t
as_node_get_error_count(as_node* node)
{
	return as_load_uint64(&node->error_count);
}

/**
 * Increment command error count. If the error is retryable, multiple errors per
 * command may occur.
 */
static inline void
as_node_add_error(as_node* node)
{
	as_incr_uint64(&node->error_count);
}

/**
 * Return command timeout count. The value is cumulative and not reset per metrics interval.
 */
static inline uint64_t
as_node_get_timeout_count(as_node* node)
{
	return as_load_uint64(&node->timeout_count);
}

/**
 * Increment command timeout count. If the timeout is retryable (ie socketTimeout),
 * multiple timeouts per command may occur.
 */
static inline void
as_node_add_timeout(as_node* node)
{
	as_incr_uint64(&node->timeout_count);
}

/**
 * @private
 * Volatile read session pointer.
 */
static inline as_session*
as_session_load(as_session** session)
{
	return (as_session*)as_load_ptr((void* const*)session);
}

/**
 * @private
 * Release existing session.
 */
static inline void
as_session_release(as_session* session)
{
	if (as_aaf_uint32_rls(&session->ref_count, -1) == 0) {
		cf_free(session);
	}
}

#ifdef __cplusplus
} // end extern "C"
#endif
