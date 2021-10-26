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
#include <aerospike/as_partition_tracker.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_string_builder.h>

/******************************************************************************
 * Static Functions
 *****************************************************************************/

as_partitions_status*
parts_create(uint16_t part_begin, uint16_t part_count, const as_digest* digest)
{
	as_partitions_status* parts_all = cf_malloc(sizeof(as_partitions_status) +
											   (sizeof(as_partition_status) * part_count));
	memset(parts_all, 0, sizeof(as_partitions_status) +
											   (sizeof(as_partition_status) * part_count));
	parts_all->ref_count = 1;
	parts_all->part_begin = part_begin;
	parts_all->part_count = part_count;
	parts_all->done = false;

	for (uint16_t i = 0; i < part_count; i++) {
		as_partition_status* ps = &parts_all->parts[i];
		ps->part_id = part_begin + i;
		ps->done = false;
		ps->digest.init = false;
	}

	if (digest && digest->init) {
		parts_all->parts[0].digest = *digest;
	}
	return parts_all;
}

static void
tracker_init(
	as_partition_tracker* pt, const as_policy_scan* policy, as_scan* scan, uint16_t part_begin,
	uint16_t part_count, const as_digest* digest
	)
{
	if (! scan->parts_all) {
		// Initial scan.
		pt->parts_all = parts_create(part_begin, part_count, digest);

		if (scan->paginate) {
			// Save parts_all in as_scan, so it can be reused in next scan page.
			scan->parts_all = as_partitions_status_reserve(pt->parts_all);
		}
	}
	else {
		// Scan instance contains partitions from previous scan.
		// Reset partition status.
		as_partitions_status* parts_all = as_partitions_status_reserve(scan->parts_all);

		for (uint16_t i = 0; i < parts_all->part_count; i++) {
			parts_all->parts[i].done = false;
		}
		pt->parts_all = parts_all;
	}

	as_vector_init(&pt->node_parts, sizeof(as_node_partitions), pt->node_capacity);
	pt->errors = NULL;
	pt->max_records = policy->max_records;

	const as_policy_base* pb = &policy->base;
	pt->sleep_between_retries = pb->sleep_between_retries;
	pt->socket_timeout = pb->socket_timeout;
	pt->total_timeout = pb->total_timeout;
	pt->max_retries = pb->max_retries;

	if (pt->total_timeout > 0) {
		pt->deadline = cf_getms() + pt->total_timeout;

		if (pt->socket_timeout == 0 || pt->socket_timeout > pt->total_timeout) {
			pt->socket_timeout = pt->total_timeout;
		}
	}
	else {
		pt->deadline = 0;
	}
	pt->iteration = 1;
}

static as_node_partitions*
find_node_partitions(as_vector* list, as_node* node)
{
	for (uint32_t i = 0; i < list->size; i++) {
		as_node_partitions* np = as_vector_get(list, i);

		// Use pointer equality for performance.
		if (np->node == node) {
			return np;
		}
	}
	return NULL;
}

static void
assign_partition(as_partition_tracker* pt, as_partition_status* ps, as_node* node)
{
	as_node_partitions* np = find_node_partitions(&pt->node_parts, node);

	if (!np) {
		// If the partition map is in a transitional state, multiple
		// as_node_partitions instances (each with different partitions)
		// may be created for a single node.
		np = as_vector_reserve(&pt->node_parts);
		as_node_reserve(node);
		np->node = node;
		as_vector_init(&np->parts_full, sizeof(uint16_t), pt->parts_capacity);
		as_vector_init(&np->parts_partial, sizeof(uint16_t), pt->parts_capacity);
	}

	if (ps->digest.init) {
		as_vector_append(&np->parts_partial, &ps->part_id);
	}
	else {
		as_vector_append(&np->parts_full, &ps->part_id);
	}
	np->parts_requested++;
}

static void
release_np(as_node_partitions* np)
{
	as_vector_destroy(&np->parts_full);
	as_vector_destroy(&np->parts_partial);
	as_node_release(np->node);
}

static void
release_node_partitions(as_vector* list)
{
	for (uint32_t i = 0; i < list->size; i++) {
		as_node_partitions* np = as_vector_get(list, i);
		release_np(np);
	}
}

/******************************************************************************
 * Functions
 *****************************************************************************/

void
as_partition_tracker_init_nodes(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_scan* policy, as_scan* scan,
	uint32_t cluster_size
	)
{
	pt->node_filter = NULL;
	pt->node_capacity = cluster_size;

	// Create initial partition capacity for each node as average + 25%.
	uint32_t ppn = cluster->n_partitions / cluster_size;
	ppn += ppn >> 2;
	pt->parts_capacity = ppn;
	tracker_init(pt, policy, scan, 0, cluster->n_partitions, NULL);
}

void
as_partition_tracker_init_node(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_scan* policy, as_scan* scan,
	as_node* node
	)
{
	pt->node_filter = node;
	pt->node_capacity = 1;
	pt->parts_capacity = cluster->n_partitions;
	tracker_init(pt, policy, scan, 0, cluster->n_partitions, NULL);
}

as_status
as_partition_tracker_init_filter(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_scan* policy, as_scan* scan,
	uint32_t cluster_size, as_partition_filter* pf, as_error* err
	)
{
	if (pf->digest.init) {
		pf->begin = (uint16_t)as_partition_getid(pf->digest.value, cluster->n_partitions);
	}

	if (pf->begin >= cluster->n_partitions) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Invalid partition begin %u. Valid range: 0-%u", pf->begin, cluster->n_partitions - 1);
	}

	if (pf->count == 0) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid partition count %u", pf->count);
	}

	if (pf->begin + pf->count > cluster->n_partitions) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid partition range (%u,%u)",
			pf->begin, pf->count);
	}

	if (pf->parts_all && ! scan->parts_all) {
		as_scan_set_partitions(scan, pf->parts_all);
	}

	pt->node_filter = NULL;
	pt->node_capacity = cluster_size;
	pt->parts_capacity = pf->count;
	tracker_init(pt, policy, scan, pf->begin, pf->count, &pf->digest);
	return AEROSPIKE_OK;
}

as_status
as_partition_tracker_assign(
	as_partition_tracker* pt, as_cluster* cluster, const char* ns, as_error* err
	)
{
    //printf("Round %u\n", pt->iteration);

	if (!cluster->shm_info) {
		as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, ns);

		if (! table) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid namespace: %s", ns);
		}

		as_partitions_status* parts_all = pt->parts_all;

		for (uint16_t i = 0; i < parts_all->part_count; i++) {
			as_partition_status* ps = &parts_all->parts[i];

			if (!ps->done) {
				as_node* node = table->partitions[ps->part_id].master;

				if (! node) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %u", ps->part_id);
				}

				// Use node name to check for single node equality because
				// partition map may be in transitional state between
				// the old and new node with the same name.
				if (pt->node_filter && strcmp(pt->node_filter->name, node->name) != 0) {
					continue;
				}

				assign_partition(pt, ps, node);
			}
		}
	}
	else {
		as_cluster_shm* cluster_shm = cluster->shm_info->cluster_shm;
		as_partition_table_shm* table = as_shm_find_partition_table(cluster_shm, ns);

		if (! table) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid namespace: %s", ns);
		}

		as_node** local_nodes = cluster->shm_info->local_nodes;
		as_partitions_status* parts_all = pt->parts_all;

		for (uint16_t i = 0; i < parts_all->part_count; i++) {
			as_partition_status* ps = &parts_all->parts[i];

			if (!ps->done) {
				uint32_t master = as_load_uint32(&table->partitions[ps->part_id].master);

				// node index zero indicates unset.
				if (master == 0) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %u", ps->part_id);
				}

				as_node* node = (as_node*)as_load_ptr(&local_nodes[master-1]);

				if (! node) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %u", ps->part_id);
				}

				// Use node name to check for single node equality because
				// partition map may be in transitional state between
				// the old and new node with the same name.
				if (pt->node_filter && strcmp(pt->node_filter->name, node->name) != 0) {
					continue;
				}

				assign_partition(pt, ps, node);
			}
		}
	}

	if (pt->max_records > 0) {
		// Distribute max_records across nodes.
		uint32_t node_size = pt->node_parts.size;

		if (pt->max_records < node_size) {
			// Only include nodes that have at least 1 record requested.
			node_size = (uint32_t)pt->max_records;

			// Delete unused node partitions.
			for (uint32_t i = node_size; i < pt->node_parts.size; i++) {
				as_node_partitions* np = as_vector_get(&pt->node_parts, i);
				release_np(np);
			}

			// Reset list size.
			pt->node_parts.size = node_size;
		}

		uint64_t max = pt->max_records / node_size;
		uint32_t rem = (uint32_t)(pt->max_records - (max * node_size));

		for (uint32_t i = 0; i < node_size; i++) {
			as_node_partitions* np = as_vector_get(&pt->node_parts, i);
			np->record_max = i < rem ? max + 1 : max;
		}
	}
	return AEROSPIKE_OK;
}

as_status
as_partition_tracker_is_complete(as_partition_tracker* pt, as_error* err)
{
	uint64_t record_count = 0;
	uint32_t parts_requested = 0;
	uint32_t parts_received = 0;
	as_vector* list = &pt->node_parts;

	for (uint32_t i = 0; i < list->size; i++) {
		as_node_partitions* np = as_vector_get(list, i);
		record_count += np->record_count;
		parts_requested += np->parts_requested;
		parts_received += np->parts_received;
		//printf("Node %s partsFull=%u partsPartial=%u partsReceived=%u recordsRequested=%llu recordsReceived=%llu\n",
		//	as_node_get_address_string(np->node), np->parts_full.size, np->parts_partial.size,
		//	np->parts_received, np->record_max, np->record_count);
	}

	if (parts_received >= parts_requested) {
		if (pt->max_records == 0 || record_count == 0) {
			pt->parts_all->done = true;
		}
		return AEROSPIKE_OK;
	}

	if (pt->max_records > 0 && record_count >= pt->max_records) {
		return AEROSPIKE_OK;
	}

	// Check if limits have been reached.
	if (pt->iteration > pt->max_retries) {
		as_error_set_message(err, AEROSPIKE_ERR_MAX_RETRIES_EXCEEDED, "");

		as_string_builder sb;
		as_string_builder_assign(&sb, sizeof(err->message), err->message);
		as_string_builder_append(&sb, "Max retries exceeded: ");
		as_string_builder_append_uint(&sb, pt->max_retries);

		if (pt->errors) {
			as_string_builder_append_newline(&sb);
			as_string_builder_append(&sb, "sub-errors:");

			uint32_t max = pt->errors->size;

			for (uint32_t i = 0; i < max; i++) {
				as_status st = *(as_status*)as_vector_get(pt->errors, i);
				as_string_builder_append_newline(&sb);
				as_string_builder_append_int(&sb, st);
				as_string_builder_append_char(&sb, ' ');
				as_string_builder_append(&sb, as_error_string(st));
			}
		}
		return err->code;
	}

	if (pt->deadline > 0) {
		// Check for total timeout.
		int64_t remaining = pt->deadline - cf_getms() - pt->sleep_between_retries;

		if (remaining <= 0) {
			return as_error_update(err, AEROSPIKE_ERR_TIMEOUT,
				"timeout: iterations=%u", pt->iteration);
		}

		if (remaining < pt->total_timeout) {
			pt->total_timeout = (uint32_t)remaining;

			if (pt->socket_timeout > pt->total_timeout) {
				pt->socket_timeout = pt->total_timeout;
			}
		}
	}

	// Prepare for next iteration.
	if (pt->max_records > 0) {
		pt->max_records -= record_count;
	}
	release_node_partitions(&pt->node_parts);
	as_vector_clear(&pt->node_parts);
	pt->iteration++;
	return AEROSPIKE_ERR_CLIENT;
}

bool
as_partition_tracker_should_retry(as_partition_tracker* pt, as_status status)
{
	switch (status) {
	case AEROSPIKE_ERR_CONNECTION:
	case AEROSPIKE_ERR_ASYNC_CONNECTION:
	case AEROSPIKE_ERR_TIMEOUT:
	case AEROSPIKE_ERR_CLUSTER: // partition not available
		if (!pt->errors) {
			pt->errors = as_vector_create(sizeof(as_status), 10);
		}
		as_vector_append(pt->errors, &status);
		return true;

	default:
		return false;
	}
}

void
as_partition_tracker_destroy(as_partition_tracker* pt)
{
	release_node_partitions(&pt->node_parts);
	as_vector_destroy(&pt->node_parts);
	as_partitions_status_release(pt->parts_all);

	if (pt->errors) {
		as_vector_destroy(pt->errors);
		pt->errors = NULL;
	}
}
