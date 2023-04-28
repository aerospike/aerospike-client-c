/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#include <aerospike/as_atomic.h>
#include <aerospike/as_key.h>
#include <citrusleaf/alloc.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

struct as_node_s;

/**
 * Status of a single partition.
 */
typedef struct as_partition_status_s {
	uint16_t part_id;
	uint8_t replica_index;
	bool retry;
	as_digest digest;
	uint64_t bval;
	struct as_node_s* node;
} as_partition_status;

/**
 * Status of all partitions after scan/query has ended.
 */
typedef struct as_partitions_status_s {
	uint32_t ref_count;
	uint16_t part_begin;
	uint16_t part_count;
	bool done;
	bool retry;
	char pad[6];
	as_partition_status parts[];
} as_partitions_status;

/**
 * Partition filter.
 */
typedef struct as_partition_filter_s {
	uint16_t begin;
	uint16_t count;
	as_digest digest;
	as_partitions_status* parts_all;
} as_partition_filter;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Read all partitions.
 *
 * @param pf			Partition filter.
 */
static inline void
as_partition_filter_set_all(as_partition_filter* pf)
{
	pf->begin = 0;
	pf->count = 4096;
	pf->digest.init = false;
	pf->parts_all = NULL;
}

/**
 * Filter by partition id.
 *
 * @param pf			Partition filter.
 * @param part_id		Partition id (0 - 4095).
 */
static inline void
as_partition_filter_set_id(as_partition_filter* pf, uint32_t part_id)
{
	pf->begin = part_id;
	pf->count = 1;
	pf->digest.init = false;
	pf->parts_all = NULL;
}

/**
 * Return records after key's digest in a single partition containing the digest.
 * Note that digest order is not the same as user key order. This function only
 * works for scan or query without a where clause.
 *
 * @param pf			Partition filter.
 * @param digest		Return records after this key's digest.
 */
static inline void
as_partition_filter_set_after(as_partition_filter* pf, as_digest* digest)
{
	pf->begin = 0;
	pf->count = 1;
	pf->digest = *digest;
	pf->parts_all = NULL;
}

/**
 * Filter by partition range.
 *
 * @param pf			Partition filter.
 * @param begin			Start partition id (0 - 4095).
 * @param count			Number of partitions.
 */
static inline void
as_partition_filter_set_range(as_partition_filter* pf, uint32_t begin, uint32_t count)
{
	pf->begin = begin;
	pf->count = count;
	pf->digest.init = false;
	pf->parts_all = NULL;
}

/**
 * Filter by status of all partitions obtained from a previous scan/query that was terminated
 * before reading all records.
 *
 * @param pf			Partition filter.
 * @param parts_all		Completion status of all partitions.
 */
static inline void
as_partition_filter_set_partitions(as_partition_filter* pf, as_partitions_status* parts_all)
{
	pf->begin = parts_all->part_begin;
	pf->count = parts_all->part_count;
	pf->digest.init = false;
	pf->parts_all = parts_all;
}

/**
 * Reserve status of all partitions.
 */
static inline as_partitions_status*
as_partitions_status_reserve(as_partitions_status* parts_all)
{
	as_partitions_status* pa = (as_partitions_status*)as_load_ptr((void* const*)&parts_all);
	as_incr_uint32(&pa->ref_count);
	return pa;
}

/**
 * Release status of all partitions.
 */
static inline void
as_partitions_status_release(as_partitions_status* parts_all)
{
	if (as_aaf_uint32_rls(&parts_all->ref_count, -1) == 0) {
		cf_free(parts_all);
	}
}

#ifdef __cplusplus
} // end extern "C"
#endif
