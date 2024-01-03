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
#include <aerospike/as_partition_tracker.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_string_builder.h>

//---------------------------------
// Types
//---------------------------------

typedef struct {
	char node_address[AS_IP_ADDRESS_SIZE];
	as_status status;
	uint32_t iteration;
	uint32_t part_id;
} query_error;

//---------------------------------
// Static Functions
//---------------------------------

static as_partitions_status*
parts_create(uint16_t part_begin, uint16_t part_count, const as_digest* digest)
{
	as_partitions_status* parts_all = cf_malloc(sizeof(as_partitions_status) +
											   (sizeof(as_partition_status) * part_count));

	parts_all->ref_count = 1;
	parts_all->part_begin = part_begin;
	parts_all->part_count = part_count;
	parts_all->done = false;
	parts_all->retry = true;

	for (uint16_t i = 0; i < part_count; i++) {
		as_partition_status* ps = &parts_all->parts[i];
		ps->part_id = part_begin + i;
		// as_replica_index_init_read() could be called here to support AS_POLICY_REPLICA_ANY,
		// but AS_POLICY_REPLICA_ANY does not make sense in the scan/query context since
		// partitions are already well distributed across nodes.  Therefore, treat
		// AS_POLICY_REPLICA_ANY like AS_POLICY_REPLICA_SEQUENCE and start at the primary node.
		// ps->replica_index = as_replica_index_init_read(replica);
		ps->replica_index = 0;
		ps->retry = true;
		ps->digest.init = false;
		ps->bval = 0;
		ps->node = NULL;
	}

	if (digest && digest->init) {
		parts_all->parts[0].digest = *digest;
	}
	return parts_all;
}

static void
tracker_init(
	as_partition_tracker* pt, const as_policy_base* policy, as_partitions_status** pp_resume,
	uint64_t max_records, as_policy_replica replica, bool paginate, uint16_t part_begin,
	uint16_t part_count, const as_digest* digest
	)
{
	as_partitions_status* resume = *pp_resume;

	if (! resume) {
		// Initial scan.
		pt->parts_all = parts_create(part_begin, part_count, digest);

		if (paginate) {
			// Save parts_all, so it can be reused in next page.
			*pp_resume = as_partitions_status_reserve(pt->parts_all);
		}
	}
	else {
		// Instance contains partitions from previous scan/query.
		pt->parts_all = as_partitions_status_reserve(resume);

		// Retry all partitions when max_records not specified.
		if (max_records == 0) {
			pt->parts_all->retry = true;
		}

		// Reset replica sequence and last node used.
		for (uint16_t i = 0; i < part_count; i++) {
			as_partition_status* ps = &pt->parts_all->parts[i];
			// ps->replica_index = as_replica_index_init_read(replica);
			ps->replica_index = 0;
			ps->node = NULL;
		}
	}

	pthread_mutex_init(&pt->lock, NULL);

	as_vector_init(&pt->node_parts, sizeof(as_node_partitions), pt->node_capacity);
	pt->errors = NULL;
	pt->max_records = max_records;
	pt->record_count = 0;
	pt->check_max = false;
	pt->replica = replica;

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
}

static void
add_error(as_partition_tracker* pt, as_node* node, as_status status, uint32_t part_id)
{
	query_error e;
	as_strncpy(e.node_address, as_node_get_address_string(node), sizeof(e.node_address));
	e.status = status;
	e.iteration = pt->iteration;
	e.part_id = part_id;
	
	// Multiple scan/query threads may call this function, so error
	// list must be modified under lock.
	pthread_mutex_lock(&pt->lock);
	if (!pt->errors) {
		pt->errors = as_vector_create(sizeof(query_error), 32);
	}
	as_vector_append(pt->errors, &e);
	pthread_mutex_unlock(&pt->lock);
}

static void
mark_retry_on_error(as_partition_tracker* pt, as_node_partitions* np)
{
	as_vector* list = &np->parts_full;

	for (uint32_t i = 0; i < list->size; i++) {
		as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
		ps->retry = true;
		ps->replica_index++;
	}

	list = &np->parts_partial;

	for (uint32_t i = 0; i < list->size; i++) {
		as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
		ps->retry = true;
		ps->replica_index++;
	}
}

static void
mark_retry(as_partition_tracker* pt, as_node_partitions* np)
{
	as_vector* list = &np->parts_full;

	for (uint32_t i = 0; i < list->size; i++) {
		as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
		ps->retry = true;
	}

	list = &np->parts_partial;

	for (uint32_t i = 0; i < list->size; i++) {
		as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
		ps->retry = true;
	}
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

//---------------------------------
// Functions
//---------------------------------

void
as_partition_tracker_init_nodes(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_base* policy,
	uint64_t max_records, as_policy_replica replica, as_partitions_status** parts_all,
	bool paginate, uint32_t cluster_size
	)
{
	pt->node_filter = NULL;
	pt->node_capacity = cluster_size;

	// Create initial partition capacity for each node as average + 25%.
	uint32_t ppn = cluster->n_partitions / cluster_size;
	ppn += ppn >> 2;
	pt->parts_capacity = ppn;
	tracker_init(pt, policy, parts_all, max_records, replica, paginate, 0, cluster->n_partitions, NULL);
}

void
as_partition_tracker_init_node(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_base* policy,
	uint64_t max_records, as_policy_replica replica, as_partitions_status** parts_all, bool paginate, as_node* node
	)
{
	pt->node_filter = node;
	pt->node_capacity = 1;
	pt->parts_capacity = cluster->n_partitions;
	tracker_init(pt, policy, parts_all, max_records, replica, paginate, 0, cluster->n_partitions, NULL);
}

as_status
as_partition_tracker_init_filter(
	as_partition_tracker* pt, as_cluster* cluster, const as_policy_base* policy,
	uint64_t max_records, as_policy_replica replica, as_partitions_status** parts_all,
	bool paginate, uint32_t cluster_size, as_partition_filter* pf, as_error* err
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

	pt->node_filter = NULL;
	pt->node_capacity = cluster_size;
	pt->parts_capacity = pf->count;
	tracker_init(pt, policy, parts_all, max_records, replica, paginate, pf->begin, pf->count, &pf->digest);
	return AEROSPIKE_OK;
}

as_status
as_partition_tracker_assign(
	as_partition_tracker* pt, as_cluster* cluster, const char* ns, as_error* err
	)
{
	//printf("Round %u\n", pt->iteration);

	as_partitions_status* parts_all = pt->parts_all;
	bool retry = parts_all->retry && pt->iteration == 1;

	if (!cluster->shm_info) {
		as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, ns);

		if (! table) {
			return as_error_update(err, AEROSPIKE_ERR_NAMESPACE_NOT_FOUND, "Invalid namespace: %s",
				ns);
		}

		for (uint16_t i = 0; i < parts_all->part_count; i++) {
			as_partition_status* ps = &parts_all->parts[i];

			if (retry || ps->retry) {
				as_partition* part = &table->partitions[ps->part_id];

				as_node* node = as_partition_get_node(cluster, ns, part, ps->node, pt->replica,
													  table->replica_size, &ps->replica_index);

				if (! node) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %u", ps->part_id);
				}

				ps->node = node;
				ps->retry = false;

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
			return as_error_update(err, AEROSPIKE_ERR_NAMESPACE_NOT_FOUND, "Invalid namespace: %s",
				ns);
		}

		for (uint16_t i = 0; i < parts_all->part_count; i++) {
			as_partition_status* ps = &parts_all->parts[i];

			if (retry || ps->retry) {
				as_partition_shm* part = &table->partitions[ps->part_id];

				as_node* node = as_partition_get_node(cluster, ns, part, ps->node, pt->replica,
													  table->replica_size, &ps->replica_index);

				if (! node) {
					return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
										   "Node not found for partition %u", ps->part_id);
				}

				ps->node = node;
				ps->retry = false;

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

	uint32_t node_size = pt->node_parts.size;

	if (node_size == 0) {
		return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE, "No nodes were assigned");
	}

	// Set global retry to true because scan/query may terminate early and all partitions
	// will need to be retried if the as_partitions_status instance is reused in a new query.
	// Global retry will be set to false if the scan/query completes normally and max_records
	// is specified.
	parts_all->retry = true;
	pt->record_count = 0;
	pt->check_max = false;

	if (pt->max_records > 0) {
		if (pt->max_records >= node_size) {
			// Distribute max_records across nodes.
			uint64_t max = pt->max_records / node_size;
			uint32_t rem = (uint32_t)(pt->max_records - (max * node_size));

			for (uint32_t i = 0; i < node_size; i++) {
				as_node_partitions* np = as_vector_get(&pt->node_parts, i);
				np->record_max = i < rem ? max + 1 : max;
			}
		}
		else {
			// If max_records < node_size, the scan/query could consistently return 0 records even
			// when some records are still available in nodes that were not included in the
			// max_record distribution. Therefore, ensure each node receives at least one max record
			// allocation and filter out excess records when receiving records from the server.
			for (uint32_t i = 0; i < node_size; i++) {
				as_node_partitions* np = as_vector_get(&pt->node_parts, i);
				np->record_max = 1;
			}

			// Track records returned for this iteration.
			pt->check_max = true;
		}
	}
	return AEROSPIKE_OK;
}

void
as_partition_tracker_part_unavailable(
	as_partition_tracker* pt, as_node_partitions* np, uint32_t part_id
	)
{
	as_partitions_status* ps = pt->parts_all;
	as_partition_status* p = &ps->parts[part_id - ps->part_begin];
	p->retry = true;
	p->replica_index++;
	np->parts_unavailable++;
	add_error(pt, np->node, AEROSPIKE_ERR_CLUSTER, part_id);
}

as_status
as_partition_tracker_is_complete(as_partition_tracker* pt, as_cluster* cluster, as_error* err)
{
	as_vector* list = &pt->node_parts;
	uint64_t record_count = 0;
	uint32_t parts_unavailable = 0;

	for (uint32_t i = 0; i < list->size; i++) {
		as_node_partitions* np = as_vector_get(list, i);
		record_count += np->record_count;
		parts_unavailable += np->parts_unavailable;

		//printf("Node %s partsFull=%u partsPartial=%u partsUnavailable=%u recordsRequested=%llu recordsReceived=%llu\n",
		//	as_node_get_address_string(np->node), np->parts_full.size, np->parts_partial.size,
		//	np->parts_unavailable, np->record_max, np->record_count);
	}

	if (parts_unavailable == 0) {
		if (pt->max_records == 0) {
			pt->parts_all->done = true;
		}
		else if (pt->iteration > 1) {
			// If errors occurred on a node, only that node's partitions are retried in the
			// next iteration. If that node finally succeeds, the other original nodes still
			// need to be retried if parts_all is reused in the next scan/query command.
			// Force retry on all node partitions.
			pt->parts_all->retry = true;
			pt->parts_all->done = false;
		}
		else {
			// Set global retry to false because only specific node partitions
			// should be retried.
			pt->parts_all->retry = false;

			if (cluster->has_partition_query) {
				// Server version >= 6.0 will return all records for each node up to
				// that node's max. If node's record count reached max, there still
				// may be records available for that node.
				bool is_done = true;

				for (uint32_t i = 0; i < list->size; i++) {
					as_node_partitions* np = as_vector_get(list, i);

					if (np->retry || np->record_count >= np->record_max) {
						mark_retry(pt, np);
						is_done = false;
					}
				}
				pt->parts_all->done = is_done;
			}
			else {
				// Servers version < 6.0 can return less records than max and still
				// have more records for each node, so the node is only done if no
				// records were retrieved for that node.
				for (uint32_t i = 0; i < list->size; i++) {
					as_node_partitions* np = as_vector_get(list, i);

					if (np->retry || np->record_count > 0) {
						mark_retry(pt, np);
					}
				}
				pt->parts_all->done = record_count == 0;
			}
		}
		return AEROSPIKE_OK;
	}

	if (pt->max_records > 0 && record_count >= pt->max_records) {
		return AEROSPIKE_OK;
	}

	// Check if limits have been reached.
	if (pt->iteration > pt->max_retries) {
		// Return last sub-error code received.
		uint32_t max = pt->errors->size;
		query_error* last_error = as_vector_get(pt->errors, max - 1);
		as_error_set_message(err, last_error->status, "");

		// Include all sub-errors in error message.
		as_string_builder sb;
		as_string_builder_assign(&sb, sizeof(err->message), err->message);
		as_string_builder_append(&sb, as_error_string(err->code));
		as_string_builder_append_newline(&sb);
		as_string_builder_append(&sb, "sub-errors:");

		for (uint32_t i = 0; i < max; i++) {
			query_error* qe = as_vector_get(pt->errors, i);
			as_string_builder_append_newline(&sb);
			as_string_builder_append_int(&sb, qe->status);
			as_string_builder_append_char(&sb, ',');
			as_string_builder_append_uint(&sb, qe->iteration);
			as_string_builder_append_char(&sb, ',');
			as_string_builder_append(&sb, qe->node_address);
			as_string_builder_append_char(&sb, ',');
			
			if (qe->status == AEROSPIKE_ERR_CLUSTER) {
				as_string_builder_append(&sb, "Partition ");
				as_string_builder_append_uint(&sb, qe->part_id);
				as_string_builder_append(&sb, " unavailable");
			}
			else {
				as_string_builder_append(&sb, as_error_string(qe->status));
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
as_partition_tracker_should_retry(
	as_partition_tracker* pt, as_node_partitions* np, as_status status
	)
{
	switch (status) {
	case AEROSPIKE_ERR_CLUSTER:
	case AEROSPIKE_ERR_INVALID_NODE:
	case AEROSPIKE_ERR_CONNECTION:
	case AEROSPIKE_ERR_ASYNC_CONNECTION:
	case AEROSPIKE_ERR_TIMEOUT:
	case AEROSPIKE_ERR_INDEX_NOT_FOUND:
	case AEROSPIKE_ERR_INDEX_NOT_READABLE:
		add_error(pt, np->node, status, 0);
		mark_retry_on_error(pt, np);
		np->parts_unavailable = np->parts_full.size + np->parts_partial.size;
		return true;

	default:
		return false;
	}
}

void
as_partition_tracker_destroy(as_partition_tracker* pt)
{
	if (pt->errors) {
		as_vector_destroy(pt->errors);
		pt->errors = NULL;
	}

	release_node_partitions(&pt->node_parts);
	as_vector_destroy(&pt->node_parts);
	as_partitions_status_release(pt->parts_all);
	pthread_mutex_destroy(&pt->lock);
}
