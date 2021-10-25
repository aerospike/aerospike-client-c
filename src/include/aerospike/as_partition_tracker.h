/*
 * Copyright 2008-2021 Aerospike, Inc.
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

#include <aerospike/as_std.h>
#include <aerospike/as_key.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_log_macros.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/
struct as_node_s;
struct as_cluster_s;
struct as_policy_scan_s;
struct as_error_s;

/**
 * @private
 * List of partitions assigned to a node.
 */
typedef struct as_node_partitions_s {
	struct as_node_s* node;
	as_vector parts_full;
	as_vector parts_partial;
	uint64_t record_count;
	uint64_t record_max;
	uint32_t parts_requested;
	uint32_t parts_received;
} as_node_partitions;

/**
 * @private
 * Scan partition tracker.
 */
typedef struct as_partition_tracker_s {
	as_partitions_status* parts_all;
	uint32_t node_capacity;
	struct as_node_s* node_filter;
	as_vector node_parts;
	as_vector* errors;
	uint64_t max_records;
	uint32_t parts_capacity;
	uint32_t sleep_between_retries;
	uint32_t socket_timeout;
	uint32_t total_timeout;
	uint32_t max_retries;
	uint32_t iteration;
	uint64_t deadline;
} as_partition_tracker;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

void
as_partition_tracker_init_nodes(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const struct as_policy_scan_s* policy,
	as_scan* scan, uint32_t cluster_size
	);

void
as_partition_tracker_init_node(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const struct as_policy_scan_s* policy,
	as_scan* scan, struct as_node_s* node
	);

as_status
as_partition_tracker_init_filter(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const struct as_policy_scan_s* policy,
	as_scan* scan, uint32_t cluster_size, as_partition_filter* pf, struct as_error_s* err
	);

as_status
as_partition_tracker_assign(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const char* ns, struct as_error_s* err
	);

static inline void
as_partition_tracker_part_done(as_cluster* cluster, as_partition_tracker* pt, as_node_partitions* np, as_digest* digest)
{
	as_partitions_status* ps = pt->parts_all;
	uint32_t part_id = 0;

	if (digest->init) {
		part_id = as_partition_getid(digest->value, cluster->n_partitions);
		ps->parts[part_id].digest = *digest;
	}
	np->record_count++;
	np->parts_received++;
}

static inline uint32_t
as_partition_tracker_set_digest(
	as_partition_tracker* pt, as_node_partitions* np, as_digest* digest, uint32_t n_partitions
	)
{
	uint32_t part_id = 0;
	
	if (digest->init) {
		as_partitions_status* ps = pt->parts_all;

		part_id = as_partition_getid(digest->value, n_partitions);
		if (part_id < ps->part_begin || part_id > (ps->part_begin + ps->part_count)) {
			as_log_error("as_partition_tracker_set_digest: invalid part_id:%d begin:%d count:%d.\n",
						part_id, ps->part_begin, ps->part_count);
		} else {
			ps->parts[part_id - ps->part_begin].digest = *digest;
			np->record_count++;
		}
	} else {
		as_log_error("as_partition_tracker_set_digest: digest is not initialized.\n");
	}
	return part_id;
}

static inline uint16_t
as_partition_tracker_get_id(as_vector* list, uint32_t index)
{
	return *(uint16_t*)as_vector_get(list, index);
}

static inline as_partition_status*
as_partition_tracker_get_status(as_partition_tracker* pt, as_vector* list, uint32_t index)
{
	uint16_t part_id = *(uint16_t*)as_vector_get(list, index);
	as_partitions_status* ps = pt->parts_all;
	return &ps->parts[part_id - ps->part_begin];
}

as_status
as_partition_tracker_is_complete(as_partition_tracker* pt, struct as_error_s* err);

bool
as_partition_tracker_should_retry(as_partition_tracker* pt, as_status status);

void
as_partition_tracker_destroy(as_partition_tracker* pt);

#ifdef __cplusplus
} // end extern "C"
#endif
