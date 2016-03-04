/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_event.h>
#include <aerospike/as_queue.h>
#include <aerospike/as_vector.h>
#include <citrusleaf/cf_queue.h>
#include <netinet/in.h>
#include <sys/uio.h>

#ifdef __cplusplus
extern "C" {
#endif

// Concurrency kit needs to be under extern "C" when compiling C++.
#include <aerospike/ck/ck_pr.h>
	
/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Maximum size (including NULL byte) of a hostname.
 */
#define AS_HOSTNAME_SIZE 256

/**
 *	Maximum size of node name
 */
#define AS_NODE_NAME_SIZE 20

// Leave this is in for backwards compatibility.
#define AS_NODE_NAME_MAX_SIZE AS_NODE_NAME_SIZE

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Socket address information.
 */
typedef struct as_address_s {
	/**
	 *	Socket IP address.
	 */
	struct sockaddr_in addr;
	
	/**
	 *	Socket IP address string representation (xxx.xxx.xxx.xxx).
	 */
	char name[INET_ADDRSTRLEN];
} as_address;
	
struct as_cluster_s;

/**
 *	Server node representation.
 */
typedef struct as_node_s {
	/**
	 *	@private
	 *  Reference count of node.
	 */
	uint32_t ref_count;
	
	/**
	 *	@private
	 *	Server's generation count for partition management.
	 */
	uint32_t partition_generation;
	
	/**
	 *	The name of the node.
	 */
	char name[AS_NODE_NAME_SIZE];
	
	/**
	 *	@private
	 *	Primary host address index into addresses array.
	 */
	uint32_t address_index;
	
	/**
	 *	@private
	 *	Vector of sockaddr_in which the host is currently known by.
	 *	Only used by tend thread. Not thread-safe.
	 */
	as_vector /* <as_address> */ addresses;
	
	/**
	 *	@private
	 *	Vector of aliases which the host is currently known by.
	 *	Only used by tend thread. Not thread-safe.
	 */
	as_vector /* <as_host> */ aliases;

	struct as_cluster_s* cluster;
	
	/**
	 *	@private
	 *	Pool of current, cached FDs.
	 */
	cf_queue* conn_q;
	
	/**
	 *	@private
	 *	Array of connection pools used in async commands.  There is one pool per node/event loop.
	 *	Only used by event loop threads. Not thread-safe.
	 */
	as_queue* async_conn_qs;
	
	/**
	 * 	@private
	 * 	Pool of connections used in pipelined async commands.  Also not thread-safe.
	 */
	as_queue* pipe_conn_qs;

	/**
	 *	@private
	 *	Socket used exclusively for cluster tend thread info requests.
	 */
	int info_fd;
		
	/**
	 *	@private
	 *	Number of other nodes that consider this node a member of the cluster.
	 */
	uint32_t conn_count;

	/**
	 *	@private
	 *	Number of other nodes that consider this node a member of the cluster.
	 */
	uint32_t friends;
	
	/**
	 *	@private
	 *	Number of consecutive info request failures.
	 */
	uint32_t failures;

	/**
	 *	@private
	 *	Shared memory node array index.
	 */
	uint32_t index;
	
	/**
	 *	@private
	 *	Is node currently active.
	 */
	uint8_t active;
	
	/**
	 *	@private
	 *	Does node support batch-index protocol?
	 */
	uint8_t has_batch_index;
	
	/**
	 *	@private
	 *	Does node support replicas-all info protocol?
	 */
	uint8_t has_replicas_all;
	
	/**
	 *	@private
	 *	Does node support floating point type?
	 */
	uint8_t has_double;
	
	/**
	 *	@private
	 *	Does node support geospatial queries?
	 */
	uint8_t has_geo;
	
} as_node;

/**
 *	@private
 *	Node discovery information.
 */
typedef struct as_node_info_s {
	/**
	 *	@private
	 *	Node name.
	 */
	char name[AS_NODE_NAME_SIZE];

	/**
	 *	@private
	 *	Validated socket.
	 */
	int fd;

	/**
	 *	@private
	 *	Does node support batch-index protocol?
	 */
	uint8_t has_batch_index;
	
	/**
	 *	@private
	 *	Does node support replicas-all info protocol?
	 */
	uint8_t has_replicas_all;
	
	/**
	 *	@private
	 *	Does node support floating point type?
	 */
	uint8_t has_double;
	
	/**
	 *	@private
	 *	Does node support geospatial queries?
	 */
	uint8_t has_geo;
	
} as_node_info;

/**
 *	@private
 *	Friend host address information.
 */
typedef struct as_host_s {
	/**
	 *	@private
	 *	Hostname or IP address string representation (xxx.xxx.xxx.xxx).
	 */
	char name[AS_HOSTNAME_SIZE];
	
	/**
	 *	@private
	 *	Socket IP port.
	 */
	in_port_t port;
	
} as_host;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 *	@private
 *	Create new cluster node.
 */
as_node*
as_node_create(struct as_cluster_s* cluster, as_host* host, struct sockaddr_in* addr, as_node_info* node_info);

/**
 *	@private
 *	Close all connections in pool and free resources.
 */
void
as_node_destroy(as_node* node);

/**
 *	@private
 *	Set node to inactive.
 */
static inline void
as_node_deactivate(as_node* node)
{
	// Make volatile write so changes are reflected in other threads.
	ck_pr_store_8(&node->active, false);
}

/**
 *	@private
 *	Reserve existing cluster node.
 */
static inline void
as_node_reserve(as_node* node)
{
	//ck_pr_fence_acquire();
	ck_pr_inc_32(&node->ref_count);
}

/**
 *	@private
 *	Release existing cluster node.
 */
static inline void
as_node_release(as_node* node)
{
	//ck_pr_fence_release();
	
	bool destroy;
	ck_pr_dec_32_zero(&node->ref_count, &destroy);
	
	if (destroy) {
		as_node_destroy(node);
	}
}

/**
 *	@private
 *	Add socket address to node addresses.
 */
void
as_node_add_address(as_node* node, as_host* host, struct sockaddr_in* addr);

/**
 *	@private
 *	Get socket address and name.
 */
static inline struct sockaddr_in*
as_node_get_address(as_node* node)
{
	as_address* address = (as_address *)as_vector_get(&node->addresses, node->address_index);
	return &address->addr;
}

/**
 *	Get socket address and name.
 */
static inline as_address*
as_node_get_address_full(as_node* node)
{
	return (as_address *)as_vector_get(&node->addresses, node->address_index);
}

/**
 *	@private
 *	Get a connection to the given node from pool and validate.  Return 0 on success.
 */
as_status
as_node_get_connection(as_error* err, as_node* node, uint64_t deadline_ms, int* fd);

/**
 *	@private
 *	Close a node's connection and do not put back into pool.
 */
static inline void
as_node_close_connection(as_node* node, int fd) {
	close(fd);
	ck_pr_dec_32(&node->conn_count);
}

/**
 *	@private
 *	Put connection back into pool.
 */
static inline void
as_node_put_connection(as_node* node, int fd)
{
	if (cf_queue_push(node->conn_q, &fd) != CF_QUEUE_OK) {
		as_node_close_connection(node, fd);
	}
}

/**
 *	@private
 *	Are hosts equal.
 */
static inline bool
as_host_equals(as_host* h1, as_host* h2)
{
	return strcmp(h1->name, h2->name) == 0 && h1->port == h2->port;
}

#ifdef __cplusplus
} // end extern "C"
#endif
