/*
 * Copyright 2008-2020 Aerospike, Inc.
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

/******************************************************************************
 * Static Functions
 *****************************************************************************/

static void
tracker_init(as_partition_tracker* pt, const as_policy_base* policy, const as_digest* digest)
{
	pt->parts_all = cf_malloc(sizeof(as_partition_status) * pt->part_count);

	for (uint32_t i = 0; i < pt->part_count; i++) {
		as_partition_status* ps = &pt->parts_all[i];
		ps->part_id = pt->part_begin + i;
		ps->done = false;
		ps->digest.init = false;
	}

	if (digest && digest->init) {
		pt->parts_all[0].digest = *digest;
	}

	as_vector_init(&pt->node_parts, sizeof(as_node_partitions), pt->node_capacity);
	pt->parts_requested = 0;
	pt->sleep_between_retries = policy->sleep_between_retries;
	pt->socket_timeout = policy->socket_timeout;
	pt->total_timeout = policy->total_timeout;
	pt->max_retries = policy->max_retries;

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
	pt->parts_requested++;
}

static void
release_node_partitions(as_vector* list)
{
	for (uint32_t i = 0; i < list->size; i++) {
		as_node_partitions* np = as_vector_get(list, i);
		as_vector_destroy(&np->parts_full);
		as_vector_destroy(&np->parts_partial);
		as_node_release(np->node);
	}
}

/******************************************************************************
 * Functions
 *****************************************************************************/

void
as_partition_tracker_init_nodes(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_base* policy,
	uint32_t cluster_size
	)
{
	pt->part_begin = 0;
	pt->part_count = cluster->n_partitions;
	pt->node_filter = NULL;
	pt->node_capacity = cluster_size;

	// Create initial partition capacity for each node as average + 25%.
	uint32_t ppn = pt->part_count / cluster_size;
	ppn += ppn >> 2;
	pt->parts_capacity = ppn;
	tracker_init(pt, policy, NULL);
}

void
as_partition_tracker_init_node(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_base* policy, as_node* node
	)
{
	pt->part_begin = 0;
	pt->part_count = cluster->n_partitions;
	pt->node_filter = node;
	pt->node_capacity = 1;
	pt->parts_capacity = pt->part_count;
	tracker_init(pt, policy, NULL);
}

as_status
as_partition_tracker_init_filter(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_base* policy, uint32_t cluster_size,
	as_partition_filter* pf, as_error* err
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

	pt->part_begin = pf->begin;
	pt->part_count = pf->count;
	pt->node_filter = NULL;
	pt->node_capacity = cluster_size;
	pt->parts_capacity = pt->part_count;
	tracker_init(pt, policy, &pf->digest);
	return AEROSPIKE_OK;
}

as_status
as_partition_tracker_assign(
	as_partition_tracker* pt, as_cluster* cluster, const char* ns, as_error* err
	)
{
    //printf("Round %u\n", pt->iteration);
	pt->parts_requested = 0;

	if (!cluster->shm_info) {
		as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, ns);

		if (! table) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid namespace: %s", ns);
		}

		for (uint32_t i = 0; i < pt->part_count; i++) {
			as_partition_status* ps = &pt->parts_all[i];

			if (!ps->done) {
				as_node* node = table->partitions[ps->part_id].master;

				if (! node) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %s", ps->part_id);
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

		for (uint32_t i = 0; i < pt->part_count; i++) {
			as_partition_status* ps = &pt->parts_all[i];

			if (!ps->done) {
				uint32_t master = as_load_uint32(&table->partitions[ps->part_id].master);

				// node index zero indicates unset.
				if (master == 0) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %s", ps->part_id);
				}

				as_node* node = (as_node*)as_load_ptr(&local_nodes[master-1]);

				if (! node) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %s", ps->part_id);
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
	return AEROSPIKE_OK;
}

as_status
as_partition_tracker_is_complete(as_partition_tracker* pt, as_error* err)
{
	uint32_t parts_received = 0;
	as_vector* list = &pt->node_parts;

	for (uint32_t i = 0; i < list->size; i++) {
		as_node_partitions* np = as_vector_get(list, i);
		parts_received += np->parts_received;
		//printf("Node %s partsFull=%u partsPartial=%u partsReceived=%u\n",
		//	as_node_get_address_string(np->node), np->parts_full.size, np->parts_partial.size,
		//	np->parts_received);
	}

	if (parts_received >= pt->parts_requested) {
		return AEROSPIKE_OK;
	}

	// Check if limits have been reached.
	if (pt->iteration > pt->max_retries) {
		return as_error_update(err, AEROSPIKE_ERR_MAX_RETRIES_EXCEEDED, "Max retries exceeded: %u",
							   pt->max_retries);
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
	release_node_partitions(&pt->node_parts);
	as_vector_clear(&pt->node_parts);
	pt->parts_capacity = pt->parts_requested - parts_received;
	pt->iteration++;
	return AEROSPIKE_ERR_CLIENT;
}

bool
as_partition_tracker_should_retry(as_status status)
{
	switch (status) {
	case AEROSPIKE_ERR_CONNECTION:
	case AEROSPIKE_ERR_ASYNC_CONNECTION:
	case AEROSPIKE_ERR_TIMEOUT:
	case AEROSPIKE_ERR_CLUSTER: // partition not available
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
	cf_free(pt->parts_all);
}
