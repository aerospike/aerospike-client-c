/*
 * Copyright 2008-2019 Aerospike, Inc.
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

#define AS_FEATURES_GEO            (1 << 0)
#define AS_FEATURES_TRUNCATE_NS    (1 << 1)
#define AS_FEATURES_BIT_OP         (1 << 2)
#define AS_FEATURES_PIPELINING     (1 << 4)
#define AS_FEATURES_PEERS          (1 << 5)
#define AS_FEATURES_REPLICAS       (1 << 6)
#define AS_FEATURES_CLUSTER_STABLE (1 << 7)
#define AS_FEATURES_LUT_NOW        (1 << 8)

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
 * Host address alias information.
 */
typedef struct as_alias_s {
	/**
	 * @private
	 * Hostname or IP address string representation.
	 */
	char name[AS_HOSTNAME_SIZE];
	
	/**
	 * @private
	 * Socket IP port.
	 */
	uint16_t port;
	
} as_alias;

/**
 * @private
 * Rack.
 */
typedef struct as_rack_s {
	/**
	 * @private
	 * Namespace
	 */
	char ns[AS_MAX_NAMESPACE_SIZE];

	/**
	 * @private
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
	 * @private
	 * Reference count of racks array.
	 */
	uint32_t ref_count;

	/**
	 * @private
	 * Rack ID when all namespaces use same rack.
	 */
	int rack_id;

	/**
	 * @private
	 * Length of racks array.
	 */
	uint32_t size;

	/**
	 * @private
	 * Pad to 8 byte boundary.
	 */
	uint32_t pad;

	/**
	 * @private
	 * Racks array.
	 */
	as_rack racks[];

} as_racks;

struct as_cluster_s;

/**
 * Server node representation.
 */
typedef struct as_node_s {
	/**
	 * @private
	 * Reference count of node.
	 */
	uint32_t ref_count;
	
	/**
	 * @private
	 * Server's generation count for partition management.
	 */
	uint32_t partition_generation;
	
	/**
	 * @private
	 * TLS certificate name (needed for TLS only, NULL otherwise).
	 */
	char* tls_name;
	
	/**
	 * The name of the node.
	 */
	char name[AS_NODE_NAME_SIZE];
	
	/**
	 * @private
	 * Primary address index into addresses array.
	 */
	uint32_t address_index;
		
	/**
	 * @private
	 * Number of IPv4 addresses.
	 */
	uint32_t address4_size;

	/**
	 * @private
	 * Number of IPv6 addresses.
	 */
	uint32_t address6_size;

	/**
	 * @private
	 * Array of IP addresses. Not thread-safe.
	 */
	as_address* addresses;
	
	/**
	 * @private
	 * Array of hostnames aliases. Not thread-safe.
	 */
	as_vector /* <as_alias> */ aliases;

	/**
	 * @private
	 * Cluster from which this node resides.
	 */
	struct as_cluster_s* cluster;
	
	/**
	 * @private
	 * Pools of current, cached sockets.
	 */
	as_conn_pool* sync_conn_pools;
	
	/**
	 * @private
	 * Array of connection pools used in async commands.  There is one pool per node/event loop.
	 * Only used by event loop threads. Not thread-safe.
	 */
	as_queue* async_conn_pools;
	
	/**
	 * @private
	 * Pool of connections used in pipelined async commands.  Also not thread-safe.
	 */
	as_queue* pipe_conn_pools;

	/**
	 * @private
	 * Racks data.
	 */
	as_racks* racks;

	/**
	 * @private
	 * Socket used exclusively for cluster tend thread info requests.
	 */
	as_socket info_socket;
		
	/**
	 * @private
	 * Session expiration for this node.
	 */
	uint64_t session_expiration;

	/**
	 * @private
	 * Session token for this node.
	 */
	uint8_t* session_token;

	/**
	 * @private
	 * Session token length.
	 */
	uint32_t session_token_length;

	/**
	 * @private
	 * Features supported by server.  Stored in bitmap.
	 */
	uint32_t features;

	/**
	 * @private
	 * Connection queue iterator.  Not atomic by design.
	 */
	uint32_t conn_iter;

	/**
	 * @private
	 * Server's generation count for peers.
	 */
	uint32_t peers_generation;

	/**
	 * @private
	 * Number of peers returned by server node.
	 */
	uint32_t peers_count;

	/**
	 * @private
	 * Server's generation count for partition rebalancing.
	 */
	uint32_t rebalance_generation;

	/**
	 * @private
	 * Number of other nodes that consider this node a member of the cluster.
	 */
	uint32_t friends;
	
	/**
	 * @private
	 * Number of consecutive info request failures.
	 */
	uint32_t failures;

	/**
	 * @private
	 * Shared memory node array index.
	 */
	uint32_t index;
	
	/**
	 * @private
	 * Should user login to avoid session expiration.
	 */
	uint8_t perform_login;

	/**
	 * @private
	 * Is node currently active.
	 */
	uint8_t active;
	
	/**
	 * @private
	 * Did partition change in current cluster tend.
	 */
	bool partition_changed;

	/**
	 * @private
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
	 * Session expiration.
	 */
	uint64_t session_expiration;

	/**
	 * Session token.
	 */
	uint8_t* session_token;

	/**
	 * Session token length.
	 */
	uint32_t session_token_length;

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
 * Set node to inactive.
 */
static inline void
as_node_deactivate(as_node* node)
{
	// Make volatile write so changes are reflected in other threads.
	as_store_uint8(&node->active, false);
}

/**
 * @private
 * Reserve existing cluster node.
 */
static inline void
as_node_reserve(as_node* node)
{
	//as_fence_acquire();
	as_incr_uint32(&node->ref_count);
}

/**
 * @private
 * Release existing cluster node.
 */
static inline void
as_node_release(as_node* node)
{
	//as_fence_release();
	if (as_aaf_uint32(&node->ref_count, -1) == 0) {
		as_node_destroy(node);
	}
}

/**
 * @private
 * Add socket address to node addresses.
 */
void
as_node_add_address(as_node* node, struct sockaddr* addr);

/**
 * @private
 * Add hostname to node aliases.
 */
void
as_node_add_alias(as_node* node, const char* hostname, uint16_t port);

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
 * Close a node's connection and do not put back into pool.
 */
static inline void
as_node_close_connection(as_socket* sock, as_conn_pool* pool) {
	as_socket_close(sock);
	as_conn_pool_decr(pool);
}

/**
 * @private
 * Put connection back into pool.
 */
static inline void
as_node_put_connection(as_socket* sock)
{
	// Save pool.
	as_conn_pool* pool = sock->pool;

	// Update last used timestamp.
	sock->last_used = cf_getns();

	// Put into pool.
	if (! as_conn_pool_push_head(pool, sock)) {
		as_node_close_connection(sock, pool);
	}
}

/**
 * @private
 * Close idle sync connections.
 */
void
as_node_close_idle_connections(as_node* node);

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
	cf_free(node_info->session_token);
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
as_node_has_rack(struct as_cluster_s* cluster, as_node* node, const char* ns, int rack_id);

#ifdef __cplusplus
} // end extern "C"
#endif
