/*
 * Copyright 2008-2014 Aerospike, Inc.
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

#include <aerospike/as_vector.h>
#include <citrusleaf/cf_queue.h>
#include <netinet/in.h>
#include "ck_pr.h"

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Maximum size of node name
 */
#define AS_NODE_NAME_MAX_SIZE 20

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
	char name[AS_NODE_NAME_MAX_SIZE];
	
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
	
	struct as_cluster_s* cluster;
	
	/**
	 *	@private
	 *	Pool of current, cached FDs.
	 */
	cf_queue* conn_q;
	
	/**
	 *	@private
	 *	Socket used exclusively for cluster tend thread info requests.
	 */
	int info_fd;
	
	/**
	 *	@private
	 *	FDs for async command execution. Not currently used.
	 */
	// cf_queue* conn_q_asyncfd;
	
	/**
	 *	@private
	 *	Asynchronous work queue. Not currently used.
	 */
	// cf_queue* asyncwork_q;
	
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
} as_node;

/**
 *	@private
 *	Friend host address information.
 */
typedef struct as_friend_s {
	/**
	 *	@private
	 *	Socket IP address string representation (xxx.xxx.xxx.xxx).
	 */
	char name[INET_ADDRSTRLEN];
	
	/**
	 *	@private
	 *	Socket IP address.
	 */
	in_addr_t addr;
	
	/**
	 *	@private
	 *	Socket IP port.
	 */
	in_port_t port;
} as_friend;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 *	@private
 *	Create new cluster node.
 */
as_node*
as_node_create(struct as_cluster_s* cluster, const char* name, struct sockaddr_in* addr);

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
as_node_add_address(as_node* node, struct sockaddr_in* addr);

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
int
as_node_get_connection(as_node* node, int* fd);

/**
 *	@private
 *	Put connection back into pool.
 */
void
as_node_put_connection(as_node* node, int fd);
