/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_partition.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_vector.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/
struct as_node_s;
struct as_cluster_s;
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
	uint32_t parts_unavailable;
	bool retry;
} as_node_partitions;

/**
 * @private
 * Scan/Query partition tracker.
 */
typedef struct as_partition_tracker_s {
	pthread_mutex_t lock;
	as_partitions_status* parts_all;
	uint32_t node_capacity;
	struct as_node_s* node_filter;
	as_vector node_parts;
	as_vector* errors;
	uint64_t max_records;
	uint64_t record_count;
	uint64_t deadline;
	as_policy_replica replica;
	uint32_t parts_capacity;
	uint32_t sleep_between_retries;
	uint32_t socket_timeout;
	uint32_t total_timeout;
	uint32_t max_retries;
	uint32_t iteration;
	bool check_max;
} as_partition_tracker;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

void
as_partition_tracker_init_nodes(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const as_policy_base* policy,
	uint64_t max_records, as_policy_replica replica, as_partitions_status** parts_all,
	bool paginate, uint32_t cluster_size
	);

void
as_partition_tracker_init_node(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const as_policy_base* policy,
	uint64_t max_records, as_policy_replica replica, as_partitions_status** parts_all, bool paginate, struct as_node_s* node
	);

as_status
as_partition_tracker_init_filter(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const as_policy_base* policy,
	uint64_t max_records, as_policy_replica replica, as_partitions_status** parts_all,
	bool paginate, uint32_t cluster_size, as_partition_filter* pf, struct as_error_s* err
	);

as_status
as_partition_tracker_assign(
	as_partition_tracker* pt, struct as_cluster_s* cluster, const char* ns, struct as_error_s* err
	);

void
as_partition_tracker_part_unavailable(
	as_partition_tracker* pt, as_node_partitions* np, uint32_t part_id
	);

static inline void
as_partition_tracker_set_digest(
	as_partition_tracker* pt, as_node_partitions* np, as_digest* digest, uint32_t n_partitions
	)
{
	uint32_t part_id = as_partition_getid(digest->value, n_partitions);
	as_partitions_status* ps = pt->parts_all;
	ps->parts[part_id - ps->part_begin].digest = *digest;
	np->record_count++;
}

static inline void
as_partition_tracker_set_last(
	as_partition_tracker* pt, as_node_partitions* np, as_digest* digest, uint64_t bval,
	uint32_t n_partitions
	)
{
	uint32_t part_id = as_partition_getid(digest->value, n_partitions);
	as_partitions_status* ps = pt->parts_all;
	as_partition_status* p = &ps->parts[part_id - ps->part_begin];
	p->digest = *digest;
	p->bval = bval;
	np->record_count++;
}

static inline bool
as_partition_tracker_reached_max_records_sync(as_partition_tracker* pt, as_node_partitions* np)
{
	// Sync scan/query runs in multiple threads, so atomics are required.
	if (pt && pt->check_max && (as_aaf_uint64(&pt->record_count, 1) > pt->max_records)) {
		// Record was returned, but would exceed max_records. Discard record
		// and mark node for retry on next scan/query page.
		np->retry = true;
		return true;
	}
	return false;
}

static inline bool
as_partition_tracker_reached_max_records_async(as_partition_tracker* pt, as_node_partitions* np)
{
	// Async scan/query runs in a single event loop thread, so atomics are not necessary.
	if (pt && pt->check_max && (++pt->record_count > pt->max_records)) {
		// Record was returned, but would exceed max_records. Discard record
		// and mark node for retry on next scan/query page.
		np->retry = true;
		return true;
	}
	return false;
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
as_partition_tracker_is_complete(
	as_partition_tracker* pt, struct as_cluster_s* cluster, struct as_error_s* err
	);

bool
as_partition_tracker_should_retry(
	as_partition_tracker* pt, as_node_partitions* np, as_status status
	);

void
as_partition_tracker_destroy(as_partition_tracker* pt);

static inline void
as_partition_error(as_partitions_status* parts_all)
{
	// Mark all partitions for retry on fatal errors.
	if (parts_all) {
		parts_all->retry = true;
	}
}

#ifdef __cplusplus
} // end extern "C"
#endif
