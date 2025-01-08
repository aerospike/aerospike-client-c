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
#include <aerospike/as_node.h>
#include <aerospike/as_partition.h>
#include <citrusleaf/cf_queue.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * @private
 * Shared memory representation of node. 424 bytes.
 */
typedef struct as_node_shm_s {
	/**
	 * Node name.
	 */
	char name[AS_NODE_NAME_SIZE];
		
	/**
	 * Lightweight node read/write lock.
	 */
	as_swlock lock;
	
	/**
	 * Socket address.
	 */
	struct sockaddr_storage addr;

	/**
	 * TLS certificate name (needed for TLS only).
	 */
	char tls_name[AS_HOSTNAME_SIZE];
	
	/**
	 * Features supported by server.  Stored in bitmap.
	 */
	uint32_t features;

	/**
	 * Server's generation count for partition rebalancing.
	 */
	uint32_t rebalance_generation;

	/**
	 * Rack ID.
	 */
	int rack_id;

	/**
	 * Is node currently active.
	 */
	uint8_t active;
	
	/**
	 * Pad to 8 byte boundary.
	 */
	char pad[3];
} as_node_shm;

/**
 * @private
 * Shared memory representation of map of namespace data partitions to nodes. 16 bytes.
 */
typedef struct as_partition_shm_s {
	/**
	 * Node offsets array.
	 */
	uint32_t nodes[AS_MAX_REPLICATION_FACTOR];

	/**
	 * Current regime for strong consistency mode.
	 */
	uint32_t regime;
} as_partition_shm;

/**
 * @private
 * Shared memory representation of map of namespace to data partitions. 40 bytes + partitions size.
 */
typedef struct as_partition_table_shm_s {
	/**
	 * Namespace name.
	 */
	char ns[AS_MAX_NAMESPACE_SIZE];

	/**
	 * Replication factor.
	 */
	uint8_t replica_size;

	/**
	 * Is namespace running in strong consistency mode.
	 */
	uint8_t sc_mode;

	/**
	 * Pad to 8 byte boundary.
	 */
	char pad[6];

	/**
	 * Array of partitions for a given namespace.
	 */
	as_partition_shm partitions[];
} as_partition_table_shm;

/**
 * @private
 * Shared memory cluster map. The map contains fixed arrays of nodes and partition tables.
 * Each partition table contains a fixed array of partitions.  The shared memory segment will be 
 * sized on startup and never change afterwards.  If the max nodes or max namespaces are reached, 
 * the tender client will ignore additional nodes/namespaces and log an error message that the
 * corresponding array is full.
 */
typedef struct as_cluster_shm_s {
	/**
	 * Last time cluster was tended in milliseconds since epoch.
	 */
	uint64_t timestamp;

	/**
	 * Cluster tend owner process id.
	 */
	uint32_t owner_pid;
	
	/**
	 * Current size of nodes array.
	 */
	uint32_t nodes_size;
	
	/**
	 * Maximum size of nodes array.
	 */
	uint32_t nodes_capacity;
	
	/**
	 * Nodes generation count.  Incremented whenever a node is added or removed from cluster.
	 */
	uint32_t nodes_gen;
	
	/**
	 * Total number of data partitions used by cluster.
	 */
	uint32_t n_partitions;

	/**
	 * Current size of partition tables array.
	 */
	uint32_t partition_tables_size;
	
	/**
	 * Maximum size of partition tables array.
	 */
	uint32_t partition_tables_capacity;

	/**
	 * Cluster offset to partition tables at the end of this structure.
	 */
	uint32_t partition_tables_offset;
	
	/**
	 * Bytes required to hold one partition_table.
	 */
	uint32_t partition_table_byte_size;

	/**
	 * Spin lock for taking over from a dead cluster tender.
	 */
	as_spinlock take_over_lock;
	
	/**
	 * Shared memory master mutex lock.  Used to determine cluster tend owner.
	 */
	uint8_t lock;
	
	/**
	 * Has shared memory been fully initialized and populated.
	 */
	uint8_t ready;
	
	/**
	 * Pad to 4 byte boundary.
	 */
	char pad[2];

	/**
	 * Cluster rebalance generation count.
	 */
	uint32_t rebalance_gen;

	/*
	 * Dynamically allocated node array.
	 */
	as_node_shm nodes[];
	
	// This is where the dynamically allocated partition tables are located.
} as_cluster_shm;

/**
 * @private
 * Local data related to shared memory implementation.
 */
typedef struct as_shm_info_s {
	/**
	 * Pointer to cluster shared memory.
	 */
	as_cluster_shm* cluster_shm;
	
	/**
	 * Array of pointers to local nodes.
	 * Array index offsets are synchronized with shared memory node offsets.
	 */
	as_node** local_nodes;
	
	/**
	 * Shared memory identifier.
	 */
#if !defined(_MSC_VER)
	int shm_id;
#else
	HANDLE shm_id;
#endif

	/**
	 * Take over shared memory cluster tending if the cluster hasn't been tended by this
	 * millisecond threshold.
	 */
	uint32_t takeover_threshold_ms;
	
	/**
	 * Is this process responsible for performing cluster tending.
	 */
	volatile bool is_tend_master;
} as_shm_info;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * @private
 * Create shared memory implementation of cluster.
 */
as_status
as_shm_create(struct as_cluster_s* cluster, as_error* err, as_config* config);

/**
 * @private
 * Destroy shared memory components.
 */
void
as_shm_destroy(struct as_cluster_s* cluster);

/**
 * @private
 * Add nodes to shared memory.
 */
void
as_shm_add_nodes(struct as_cluster_s* cluster, as_vector* /* <as_node*> */ nodes_to_add);

/**
 * @private
 * Remove nodes from shared memory.
 */
void
as_shm_remove_nodes(struct as_cluster_s* cluster, as_vector* /* <as_node*> */ nodes_to_remove);

/**
 * @private
 * Update shared memory node racks.
 */
void
as_shm_node_replace_racks(as_cluster_shm* cluster_shm, as_node* node, as_racks* racks);

/**
 * @private
 * Find partition table for namespace in shared memory.
 */
AS_EXTERN as_partition_table_shm*
as_shm_find_partition_table(as_cluster_shm* cluster_shm, const char* ns);

/**
 * @private
 * Update shared memory partition tables for given namespace.
 */
void
as_shm_update_partitions(
	as_shm_info* shm_info, const char* ns, char* bitmap_b64, int64_t len, as_node* node,
	uint8_t replica_size, uint8_t replica_index, uint32_t regime
	);

/**
 * @private
 * Get shared memory partition tables array.
 */
static inline as_partition_table_shm*
as_shm_get_partition_tables(as_cluster_shm* cluster_shm)
{
	return (as_partition_table_shm*) ((char*)cluster_shm + cluster_shm->partition_tables_offset);
}

/**
 * @private
 * Get partition table identified by index.
 */
static inline as_partition_table_shm*
as_shm_get_partition_table(
	as_cluster_shm* cluster_shm, as_partition_table_shm* tables, uint32_t index
	)
{
	return (as_partition_table_shm*) ((char*)tables + (cluster_shm->partition_table_byte_size * index));
}

/**
 * @private
 * Get next partition table in array.
 */
static inline as_partition_table_shm*
as_shm_next_partition_table(as_cluster_shm* cluster_shm, as_partition_table_shm* table)
{
	return (as_partition_table_shm*) ((char*)table + cluster_shm->partition_table_byte_size);
}

#ifdef __cplusplus
} // end extern "C"
#endif
