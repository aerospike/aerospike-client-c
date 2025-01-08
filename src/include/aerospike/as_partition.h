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
#include <aerospike/as_std.h>
#include <aerospike/as_status.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * MACROS
 *****************************************************************************/

/**
 * If the server removes then adds namespaces, the client may contain more
 * than the server max of 32.
 */
#define AS_MAX_NAMESPACES 128

/**
 * Maximum namespace size including null byte.  Effective maximum length is 31.
 */
#define AS_MAX_NAMESPACE_SIZE 32

/**
 * Maximum number of stored replicas in partition map.
 */
#define AS_MAX_REPLICATION_FACTOR 3

/******************************************************************************
 * TYPES
 *****************************************************************************/
struct as_node_s;
struct as_cluster_s;
struct as_error_s;
struct as_key_s;

/**
 * @private
 * Map of namespace data partitions to nodes.
 */
typedef struct as_partition_s {
	struct as_node_s* nodes[AS_MAX_REPLICATION_FACTOR];
	uint32_t regime;
} as_partition;

/**
 * @private
 * Map of namespace to data partitions.
 */
typedef struct as_partition_table_s {
	char ns[AS_MAX_NAMESPACE_SIZE];
	uint32_t size;
	uint8_t replica_size;  // replication-factor on server.
	bool sc_mode;
	char pad[2];
	as_partition partitions[];
} as_partition_table;

/**
 * @private
 * Array of partition table pointers.
 */
typedef struct as_partition_tables_s {
	as_partition_table* tables[AS_MAX_NAMESPACES];
	uint32_t size;
} as_partition_tables;

/**
 * @private
 * Partition info.
 */
typedef struct as_partition_info_s {
	const char* ns;
	void* partition;  // as_partition or as_shm_partition.
	uint32_t partition_id;
	uint8_t replica_size;
	bool sc_mode;
} as_partition_info;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * @private
 * Destroy partition tables.
 */
void
as_partition_tables_destroy(as_partition_tables* tables);

/**
 * @private
 * Get partition table given namespace.
 */
AS_EXTERN as_partition_table*
as_partition_tables_get(as_partition_tables* tables, const char* ns);
	
/**
 * @private
 * Return partition ID given digest.
 */
static inline uint32_t
as_partition_getid(const uint8_t* digest, uint32_t n_partitions)
{
	return (*(uint16_t*)digest) & (n_partitions - 1);
}

/**
 * @private
 * Initialize partition info given key.  If this function succeeds and not using shared memory,
 * as_partition_tables_release() must be called when done with partition.
 */
as_status
as_partition_info_init(
	as_partition_info* pi, struct as_cluster_s* cluster, struct as_error_s* err,
	const struct as_key_s* key
	);

/**
 * @private
 * Log all partition maps in the cluster.
 */
void
as_partition_tables_dump(struct as_cluster_s* cluster);

#ifdef __cplusplus
} // end extern "C"
#endif
