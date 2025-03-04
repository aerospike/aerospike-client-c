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
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_cpu.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_string.h>
#include <aerospike/as_thread.h>
#include <citrusleaf/cf_b64.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_clock.h>
#include <errno.h>
#include <string.h>
#include <signal.h>

#if !defined(_MSC_VER)
#include <sys/shm.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <process.h>
#define getpid _getpid
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/sysctl.h>
#endif

/******************************************************************************
 * DECLARATIONS
 ******************************************************************************/

as_status
as_cluster_init(as_cluster* cluster, as_error* err);

void
as_cluster_add_seeds(as_cluster* cluster);

as_status
as_cluster_tend(as_cluster* cluster, as_error* err, bool is_init);

as_status
as_node_ensure_login_shm(as_error* err, as_node* node);

as_status
as_node_refresh_racks(as_cluster* cluster, as_error* err, as_node* node);

void
as_cluster_add_nodes_copy(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_add);

void
as_cluster_remove_nodes_copy(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_remove);

void
as_cluster_manage(as_cluster* cluster);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

// Note on why shared memory robust mutex locks were not used:
//
// Shared memory robust mutex locks do not work properly on some supported platforms.
// For example, Centos 6.5 will allow multiple contenders to get the same lock when EOWNERDEAD
// condition is triggered.  Also, robust mutex locks are not supported at all on Mac OS X.
// Therefore, use custom locking system which works on all platforms.

/*
static void
as_shm_dump_partition_table(as_partition_table_shm* table, uint32_t n_partitions)
{
	printf("Namespace: %s\n", table->ns);

	for (uint32_t i = 0; i < n_partitions; i++) {
		as_partition_shm* p = &table->partitions[i];
		printf("%d %d\n", i, p->master);
	}
}

static void
as_shm_dump_partition_tables(as_cluster_shm* cluster_shm)
{
	as_partition_table_shm* table = as_shm_get_partition_tables(cluster_shm);
	uint32_t max = cluster_shm->partition_tables_size;

	for (uint32_t i = 0; i < max; i++) {
		as_shm_dump_partition_table(table, cluster_shm->n_partitions);
		table = as_shm_next_partition_table(cluster_shm, table);
	}
}
*/

#if !defined(_MSC_VER)
static size_t
as_shm_get_max_size(void)
{
#ifdef __linux__
	char* fn = "/proc/sys/kernel/shmmax";
	size_t shm_max;
	FILE *f = fopen(fn, "r");
	
	if (!f) {
		as_log_error("Failed to open file: %s", fn);
		return 0;
	}
	
	if (fscanf(f, "%zu", &shm_max) != 1) {
		as_log_error("Failed to read shmmax from file: %s", fn);
		fclose(f);
		return 0;
	}
	fclose(f);
	return shm_max;
#else
	size_t shm_max;
	size_t len = sizeof(size_t);
	sysctlbyname("kern.sysv.shmmax", &shm_max, &len, NULL, 0);
	return shm_max;
#endif
}
#endif

static int
as_shm_find_node_index(as_cluster_shm* cluster_shm, const char* name)
{
	for (uint32_t i = 0; i < cluster_shm->nodes_size; i++) {
		as_node_shm* node = &cluster_shm->nodes[i];
		
		if (strcmp(node->name, name) == 0) {
			return i;
		}
	}
	return -1;
}

void
as_shm_add_nodes(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_add)
{
	// This function is called by shared memory master tending thread.
	as_shm_info* shm_info = cluster->shm_info;
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;

	// Update shared memory and local nodes.
	for (uint32_t i = 0; i < nodes_to_add->size; i++) {
		as_node* node_to_add = as_vector_get_ptr(nodes_to_add, i);
		as_address* address = as_node_get_address(node_to_add);
		int node_index = as_shm_find_node_index(cluster_shm, node_to_add->name);
		
		if (node_index >= 0) {
			// Node already exists.  Activate node.
			as_node_shm* node_shm = &cluster_shm->nodes[node_index];
			
			// Update shared memory node in write lock.
			as_swlock_write_lock(&node_shm->lock);
			memcpy(&node_shm->addr, &address->addr, sizeof(struct sockaddr_storage));
			if (node_to_add->tls_name) {
				strcpy(node_shm->tls_name, node_to_add->tls_name);
			}
			else {
				node_shm->tls_name[0] = 0;
			}
			node_shm->features = node_to_add->features;
			node_shm->active = true;
			as_swlock_write_unlock(&node_shm->lock);
			
			// Set shared memory node array index.
			// Only referenced by shared memory tending thread, so volatile write not necessary.
			node_to_add->index = node_index;
		}
		else {
			// Add new node and activate.
			if (cluster_shm->nodes_size < cluster_shm->nodes_capacity) {
				as_node_shm* node_shm = &cluster_shm->nodes[cluster_shm->nodes_size];
				
				// Update shared memory node in write lock.
				as_swlock_write_lock(&node_shm->lock);
				memcpy(node_shm->name, node_to_add->name, AS_NODE_NAME_SIZE);
				memcpy(&node_shm->addr, &address->addr, sizeof(struct sockaddr_storage));
				if (node_to_add->tls_name) {
					strcpy(node_shm->tls_name, node_to_add->tls_name);
				}
				else {
					node_shm->tls_name[0] = 0;
				}
				node_shm->features = node_to_add->features;
				node_shm->active = true;
				as_swlock_write_unlock(&node_shm->lock);
				
				// Set shared memory node array index.
				// Only referenced by shared memory tending thread, so volatile write not necessary.
				node_to_add->index = cluster_shm->nodes_size;

				// Increment node array size.
				as_incr_uint32_rls(&cluster_shm->nodes_size);
			}
			else {
				// There are no more node slots available in shared memory.
				as_log_error("Failed to add node %s %s. Shared memory capacity exceeeded: %d",
					node_to_add->name, address->name, cluster_shm->nodes_capacity);
			}
		}
		as_node_store(&shm_info->local_nodes[node_to_add->index], node_to_add);
	}
	as_incr_uint32(&cluster_shm->nodes_gen);
}

void
as_shm_remove_nodes(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_remove)
{
	// This function is called by shared memory master tending thread.
	as_shm_info* shm_info = cluster->shm_info;
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	
	for (uint32_t i = 0; i < nodes_to_remove->size; i++) {
		as_node* node_to_remove = as_vector_get_ptr(nodes_to_remove, i);
		as_node_shm* node_shm = &cluster_shm->nodes[node_to_remove->index];
		
		// Update shared memory node in write lock.
		as_swlock_write_lock(&node_shm->lock);
		node_shm->active = false;
		as_swlock_write_unlock(&node_shm->lock);

		// Set local node pointer to null, but do not decrement cluster_shm->nodes_size
		// because nodes are stored in a fixed array.
		// TODO: Could decrement nodes_size when index is the last node in the array.
		as_node_store(&shm_info->local_nodes[node_to_remove->index], 0);
	}
	as_incr_uint32(&cluster_shm->nodes_gen);
}

static void
as_shm_ensure_login_node(as_error* err, as_node* node)
{
	as_status status = as_node_ensure_login_shm(err, node);

	if (status != AEROSPIKE_OK) {
		as_log_error("Failed to retrieve session token in shared memory prole tender: %d %s",
					 err->code, err->message);
	}
}

static void
as_shm_ensure_login(as_cluster* cluster, as_error* err)
{
	as_shm_info* shm_info = cluster->shm_info;
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	as_node_shm* nodes_shm = cluster_shm->nodes;
	uint32_t max = as_load_uint32_acq(&cluster_shm->nodes_size);

	for (uint32_t i = 0; i < max; i++) {
		as_node_shm* node_shm = &nodes_shm[i];

		as_swlock_read_lock(&node_shm->lock);
		uint8_t active = node_shm->active;
		as_swlock_read_unlock(&node_shm->lock);

		if (active) {
			as_node* node = shm_info->local_nodes[i];

			if (node) {
				as_shm_ensure_login_node(err, node);
			}
		}
	}
}

static void
as_shm_reset_nodes(as_cluster* cluster)
{
	// Synchronize shared memory nodes with local nodes.
	as_shm_info* shm_info = cluster->shm_info;
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	
	as_node_shm* nodes_shm = cluster_shm->nodes;
	as_node_shm node_tmp;
	uint32_t max = as_load_uint32(&cluster_shm->nodes_size);
	
	as_vector nodes_to_add;
	as_vector_inita(&nodes_to_add, sizeof(as_node*), max);
	
	as_vector nodes_to_remove;
	as_vector_inita(&nodes_to_remove, sizeof(as_node*), max);

	for (uint32_t i = 0; i < max; i++) {
		as_node_shm* node_shm = &nodes_shm[i];
		as_node* node = shm_info->local_nodes[i];

		// Make copy of shared memory node under a read lock.
		as_swlock_read_lock(&node_shm->lock);
		memcpy(&node_tmp, node_shm, sizeof(as_node_shm));
		as_swlock_read_unlock(&node_shm->lock);
		
		if (node_tmp.active) {
			if (! node) {
				as_node_info node_info;
				strcpy(node_info.name, node_tmp.name);
				as_socket_init(&node_info.socket);
				node_info.features = node_tmp.features;
				node_info.host.name = NULL;
				node_info.host.tls_name = node_tmp.tls_name;
				node_info.host.port = 0;
				as_address_copy_storage((struct sockaddr*)&node_tmp.addr, &node_info.addr);
				node_info.session = NULL;
				node = as_node_create(cluster, &node_info);
				as_node_create_min_connections(node);
				node->index = i;

				if (cluster->auth_enabled) {
					// Retrieve session token.
					as_error err;
					node->perform_login = 1;
					as_shm_ensure_login_node(&err, node);
				}
				as_vector_append(&nodes_to_add, &node);
				as_node_store(&shm_info->local_nodes[i], node);
			}
			node->rebalance_generation = node_tmp.rebalance_generation;
		}
		else {
			if (node) {
				as_node_deactivate(node);
				as_vector_append(&nodes_to_remove, &node);
				as_node_store(&shm_info->local_nodes[i], 0);
			}
		}
	}

	// Remove nodes in a batch.
	if (nodes_to_remove.size > 0) {
		as_cluster_remove_nodes_copy(cluster, &nodes_to_remove);
	}
	
	// Add nodes in a batch.
	if (nodes_to_add.size > 0) {
		as_cluster_add_nodes_copy(cluster, &nodes_to_add);
	}
	
	as_vector_destroy(&nodes_to_add);
	as_vector_destroy(&nodes_to_remove);
}

static as_status
as_shm_reset_racks_node(as_cluster* cluster, as_error* err, as_node* node)
{
	uint64_t deadline_ms = as_socket_deadline(cluster->conn_timeout_ms);
	as_status status = as_node_get_connection(err, node, 0, deadline_ms, &node->info_socket);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_node_refresh_racks(cluster, err, node);

	if (status != AEROSPIKE_OK) {
		as_node_close_socket(node, &node->info_socket);
		return status;
	}

	as_node_put_connection(node, &node->info_socket);
	return status;
}

static void
as_shm_reset_racks(
	as_cluster* cluster, as_shm_info* shm_info, as_cluster_shm* cluster_shm, as_error* err
	)
{
	// Per namespace racks not stored in shared memory.
	// Retrieve racks from server on prole tender.
	as_node_shm* nodes_shm = cluster_shm->nodes;
	uint32_t max = as_load_uint32(&cluster_shm->nodes_size);
	int rack_id;
	uint8_t active;

	for (uint32_t i = 0; i < max; i++) {
		as_node_shm* node_shm = &nodes_shm[i];

		as_swlock_read_lock(&node_shm->lock);
		rack_id = node_shm->rack_id;
		active = node_shm->active;
		as_swlock_read_unlock(&node_shm->lock);

		// Retrieve racks only when different rack ids per namespace (rack_id == -1).
		if (rack_id == -1 && active) {
			as_node* node = shm_info->local_nodes[i];

			if (node) {
				as_status status = as_shm_reset_racks_node(cluster, err, node);

				if (status != AEROSPIKE_OK) {
					as_log_error("Node %s shm rack refresh failed: %s %s",
								node->name, as_error_string(status), err->message);
				}
			}
		}
	}
}

void
as_shm_node_replace_racks(as_cluster_shm* cluster_shm, as_node* node, as_racks* racks)
{
	as_node_shm* node_shm = &cluster_shm->nodes[node->index];
	int rack_id = (racks->size == 0)? racks->rack_id : -1;

	// Update shared memory node in write lock.
	as_swlock_write_lock(&node_shm->lock);
	node_shm->rebalance_generation = node->rebalance_generation;
	node_shm->rack_id = rack_id;
	as_swlock_write_unlock(&node_shm->lock);
}

as_partition_table_shm*
as_shm_find_partition_table(as_cluster_shm* cluster_shm, const char* ns)
{
	as_partition_table_shm* table = as_shm_get_partition_tables(cluster_shm);
	uint32_t max = cluster_shm->partition_tables_size;
	
	for (uint32_t i = 0; i < max; i++) {
		if (strcmp(table->ns, ns) == 0) {
			return table;
		}
		table = as_shm_next_partition_table(cluster_shm, table);
	}
	return 0;
}

static as_partition_table_shm*
as_shm_add_partition_table(
	as_cluster_shm* cluster_shm, const char* ns, uint8_t replica_size, bool sc_mode
	)
{
	if (cluster_shm->partition_tables_size >= cluster_shm->partition_tables_capacity) {
		// There are no more partition table slots available in shared memory.
		as_log_error(
			"Failed to add partition table namespace %s. Shared memory capacity exceeeded: %u",
			ns, cluster_shm->partition_tables_capacity);
		return NULL;
	}
	
	as_partition_table_shm* tables = as_shm_get_partition_tables(cluster_shm);
	as_partition_table_shm* table = as_shm_get_partition_table(cluster_shm, tables,
		cluster_shm->partition_tables_size);
	as_strncpy(table->ns, ns, AS_MAX_NAMESPACE_SIZE);
	table->replica_size = replica_size;
	table->sc_mode = sc_mode;
	
	// Increment partition tables array size.
	as_incr_uint32(&cluster_shm->partition_tables_size);
	return table;
}

static void
as_shm_force_replicas_refresh(as_shm_info* shm_info, uint32_t node_index)
{
	// node_index starts at one (zero indicates unset).
	as_node* node = shm_info->local_nodes[node_index-1];
	
	if (node) {
		node->partition_generation = (uint32_t)-1;
	}
}

static void
as_shm_decode_and_update(
	as_shm_info* shm_info, char* bitmap_b64, int64_t len, as_partition_table_shm* table,
	uint32_t node_index, uint8_t replica_index, uint32_t regime
	)
{
	// Size allows for padding - is actual size rounded up to multiple of 3.
	uint8_t* bitmap = (uint8_t*)alloca(cf_b64_decoded_buf_size((uint32_t)len));
	
	// For now - for speed - trust validity of encoded characters.
	cf_b64_decode(bitmap_b64, (uint32_t)len, bitmap, NULL);
	
	// Expand the bitmap.
	uint32_t max = shm_info->cluster_shm->n_partitions;

	for (uint32_t i = 0; i < max; i++) {
		if ((bitmap[i >> 3] & (0x80 >> (i & 7))) != 0) {
			// This node claims ownership of partition.
			as_partition_shm* p = &table->partitions[i];

			if (regime >= as_load_uint32(&p->regime)) {
				if (regime > p->regime) {
					as_store_uint32(&p->regime, regime);
				}

				uint32_t node_index_old = p->nodes[replica_index];

				if (node_index != node_index_old) {
					// node index starts at one (zero indicates unset).
					if (node_index_old) {
						as_shm_force_replicas_refresh(shm_info, node_index_old);
					}
					as_store_uint32_rls(&p->nodes[replica_index], node_index);
				}
			}
		}
	}
}

void
as_shm_update_partitions(
	as_shm_info* shm_info, const char* ns, char* bitmap_b64, int64_t len, as_node* node,
	uint8_t replica_size, uint8_t replica_index, uint32_t regime
	)
{
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	as_partition_table_shm* table = as_shm_find_partition_table(cluster_shm, ns);
	
	if (! table) {
		table = as_shm_add_partition_table(cluster_shm, ns, replica_size, regime != 0);
	}
	
	if (table) {
		as_shm_decode_and_update(shm_info, bitmap_b64, len, table, node->index + 1, replica_index,
			regime);
	}
}

static as_node*
as_shm_get_replica_master(as_partition_shm* p, as_node** local_nodes)
{
	uint32_t node_index = as_load_uint32_acq(&p->nodes[0]);

	// node_index starts at one (zero indicates unset).
	if (node_index) {
		as_node* node = as_node_load(&local_nodes[node_index-1]);

		if (node && as_node_is_active(node)) {
			return node;
		}
	}
	// When master only specified, should never get random nodes.
	return NULL;
}

static as_node*
as_shm_get_replica_sequence(
	as_node** local_nodes, as_partition_shm* p, uint8_t replica_size, uint8_t* replica_index
	)
{
	for (uint8_t i = 0; i < replica_size; i++) {
		uint8_t index = (*replica_index) % replica_size;
		uint32_t node_index = as_load_uint32_acq(&p->nodes[index]);

		// node_index starts at one (zero indicates unset).
		if (node_index) {
			as_node* node = as_node_load(&local_nodes[node_index-1]);

			if (node && as_node_is_active(node)) {
				return node;
			}
		}
		(*replica_index)++;
	}
	return NULL;
}

static as_node*
as_shm_get_replica_rack(
	as_cluster* cluster, as_node** local_nodes, const char* ns, as_partition_shm* p,
	as_node* prev_node, uint8_t replica_size, uint8_t* replica_index
	)
{
	as_node_shm* nodes_shm = cluster->shm_info->cluster_shm->nodes;
	as_node* fallback1 = NULL;
	as_node* fallback2 = NULL;
	uint32_t replica_max = replica_size;
	uint32_t seq1 = 0;
	uint32_t seq2 = 0;
	uint32_t rack_max = cluster->rack_ids_size;

	for (uint32_t i = 0; i < rack_max; i++) {
		int search_id = cluster->rack_ids[i];
		uint32_t seq = (*replica_index);

		for (uint32_t j = 0; j < replica_max; j++, seq++) {
			uint32_t index = seq % replica_max;
			uint32_t node_index = as_load_uint32_acq(&p->nodes[index]);

			// node_index starts at one (zero indicates unset).
			if (! node_index) {
				continue;
			}
			node_index--;

			as_node_shm* node_shm = &nodes_shm[node_index];
			int rack_id;
			uint8_t active;

			as_swlock_read_lock(&node_shm->lock);
			rack_id = node_shm->rack_id;
			active = node_shm->active;
			as_swlock_read_unlock(&node_shm->lock);

			if (! active) {
				continue;
			}

			as_node* node = as_node_load(&local_nodes[node_index]);

			// Avoid retrying on node where command failed even if node is the
			// only one on the same rack. The contents of prev_node may have
			// already been destroyed, so just use pointer comparison and never
			// examine the contents of prev_node!
			if (node == prev_node) {
				// Previous node is the least desirable fallback.
				if (! fallback2) {
					fallback2 = node;
					seq2 = index;
				}
				continue;
			}

			// Rack ids may be different per namespace. A rack id of -1 indicates all ids are
			// stored on the local node because there is not enough node shared memory to cover
			// this case. Check rack id on node's shared memory first.
			if (rack_id == search_id || (rack_id == -1 && as_node_has_rack(node, ns, search_id))) {
				// Found node on same rack.
				return node;
			}

			// Node meets all criteria except not on same rack.
			if (! fallback1) {
				fallback1 = node;
				seq1 = index;
			}
		}
	}

	// Return node on a different rack if it exists.
	if (fallback1) {
		*replica_index = (uint8_t)seq1;
		return fallback1;
	}

	// Return previous node if it still exists.
	if (fallback2) {
		*replica_index = (uint8_t)seq2;
		return fallback2;
	}
	return NULL;
}

as_node*
as_partition_shm_get_node(
	as_cluster* cluster, const char* ns, as_partition_shm* p, as_node* prev_node,
	as_policy_replica replica, uint8_t replica_size, uint8_t* replica_index
	)
{
	as_node** local_nodes = cluster->shm_info->local_nodes;

	switch (replica) {
		case AS_POLICY_REPLICA_MASTER:
			return as_shm_get_replica_master(p, local_nodes);

		default:
		case AS_POLICY_REPLICA_ANY:
		case AS_POLICY_REPLICA_SEQUENCE:
			return as_shm_get_replica_sequence(local_nodes, p, replica_size, replica_index);

		case AS_POLICY_REPLICA_PREFER_RACK:
			return as_shm_get_replica_rack(cluster, local_nodes, ns, p, prev_node, replica_size,
				replica_index);
	}
}

static void
as_shm_reset_rebalance_gen(as_shm_info* shm_info, as_cluster_shm* cluster_shm)
{
	// Copy shared memory node rebalance generation to local nodes.
	as_node_shm* nodes_shm = cluster_shm->nodes;
	uint32_t max = as_load_uint32(&cluster_shm->nodes_size);
	uint32_t gen;

	for (uint32_t i = 0; i < max; i++) {
		as_node_shm* node_shm = &nodes_shm[i];

		as_swlock_read_lock(&node_shm->lock);
		gen = node_shm->rebalance_generation;
		as_swlock_read_unlock(&node_shm->lock);

		as_node* node = shm_info->local_nodes[i];

		if (node) {
			node->rebalance_generation = gen;
		}
	}
}

static void
as_shm_takeover_cluster(
	as_cluster* cluster, as_shm_info* shm_info, as_cluster_shm* cluster_shm, uint32_t pid
	)
{
	as_log_info("Take over shared memory cluster: %u", pid);
	as_store_uint32(&cluster_shm->owner_pid, pid);
	shm_info->is_tend_master = true;

	if (cluster->rack_aware) {
		as_shm_reset_rebalance_gen(shm_info, cluster_shm);
	}
}

static bool
as_process_exists(uint32_t pid)
{
#if !defined(_MSC_VER)
	return kill(pid, 0) == 0;
#else
	HANDLE process = OpenProcess(SYNCHRONIZE, FALSE, pid);

	if (process == NULL) {
		return false;
	}
	CloseHandle(process);
	return true;
#endif
}

static void*
as_shm_tender(void* userdata)
{
	// Shared memory cluster tender.
	as_thread_set_name("shmtend");

	as_cluster* cluster = userdata;

	if (cluster->tend_thread_cpu >= 0) {
		if (as_cpu_assign_thread(pthread_self(), cluster->tend_thread_cpu) != 0) {
			as_log_warn("Failed to assign tend thread to cpu %d", cluster->tend_thread_cpu);
		}
	}

	as_shm_info* shm_info = cluster->shm_info;
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	uint64_t threshold = shm_info->takeover_threshold_ms;
	uint64_t limit = 0;
	uint32_t pid = getpid();
	uint32_t nodes_gen = 0;
	uint32_t rebalance_gen = 0;

	struct timespec delta;
	cf_clock_set_timespec_ms(cluster->tend_interval, &delta);
	
	struct timespec abstime;
	
	as_status status;
	as_error err;
	
	pthread_mutex_lock(&cluster->tend_lock);
	
	while (cluster->valid) {
		if (shm_info->is_tend_master) {
			// Tend shared memory cluster.
			status = as_cluster_tend(cluster, &err, false);
			as_store_uint64(&cluster_shm->timestamp, cf_getms());
			
			if (status != AEROSPIKE_OK) {
				as_log_warn("Tend error: %s %s", as_error_string(status), err.message);
			}
		}
		else {
			// Follow shared memory cluster.
			// Check if tend owner has released lock.
			if (as_cas_uint8(&cluster_shm->lock, 0, 1)) {
				as_shm_takeover_cluster(cluster, shm_info, cluster_shm, pid);
				continue;
			}
			
			// Check if tend owner died without releasing lock.
			uint64_t now = cf_getms();
			if (now >= limit) {
				uint64_t ts = as_load_uint64(&cluster_shm->timestamp);
				
				// Check if cluster hasn't been tended within threshold.
				if (now - ts >= threshold) {
					uint32_t owner_pid = as_load_uint32(&cluster_shm->owner_pid);
					
					// Check if owner process id is invalid or does not exist.
					if (owner_pid == 0 || !as_process_exists(owner_pid)) {
						// Cluster should be taken over, but this must be done under lock.
						as_spinlock_lock(&cluster_shm->take_over_lock);
						
						// Reload timestamp, just in case another process just modified it.
						ts = as_load_uint64(&cluster_shm->timestamp);
						
						// Check if cluster hasn't been tended within threshold.
						if (now - ts >= threshold) {
							// Take over cluster tending.
							// Update timestamp, so other processes will not try to take over.
							as_store_uint64(&cluster_shm->timestamp, now);
							as_store_uint8(&cluster_shm->lock, 1);
							as_spinlock_unlock(&cluster_shm->take_over_lock);
							as_shm_takeover_cluster(cluster, shm_info, cluster_shm, pid);
							continue;
						}
						as_spinlock_unlock(&cluster_shm->take_over_lock);
					}
				}
				limit = ts + threshold;
			}
			
			// Synchronize local cluster with shared memory cluster.
			uint32_t gen = as_load_uint32(&cluster_shm->nodes_gen);
			
			if (nodes_gen != gen) {
				nodes_gen = gen;
				as_shm_reset_nodes(cluster);
			}

			if (cluster->rack_aware) {
				// Synchronize racks
				gen = as_load_uint32(&cluster_shm->rebalance_gen);

				if (rebalance_gen != gen) {
					as_shm_reset_racks(cluster, shm_info, cluster_shm, &err);
					rebalance_gen = gen;
				}
			}

			if (cluster->auth_enabled) {
				as_shm_ensure_login(cluster, &err);
			}

			as_cluster_manage(cluster);
		}

		// Convert tend interval into absolute timeout.
		cf_clock_current_add(&delta, &abstime);
		
		// Sleep for tend interval and exit early if cluster destroy is signaled.
		pthread_cond_timedwait(&cluster->tend_cond, &cluster->tend_lock, &abstime);
	}
	pthread_mutex_unlock(&cluster->tend_lock);
	
	if (shm_info->is_tend_master) {
		shm_info->is_tend_master = false;
		as_store_uint8_rls(&cluster_shm->lock, 0);
	}
	return 0;
}

static void
as_shm_wait_till_ready(as_cluster* cluster, as_cluster_shm* cluster_shm, uint32_t pid)
{
	// Wait till cluster is initialized or connection timeout is reached.
	uint32_t interval_ms = 200;  // 200 milliseconds
	uint64_t limit = cf_getms() + 10000;    // 10 second timeout.
	
	do {
		as_sleep(interval_ms);
		
		if (as_load_uint8_acq(&cluster_shm->ready)) {
			as_log_info("Follow cluster initialized: %u", pid);
			return;
		}
	} while (cf_getms() < limit);
	as_log_warn("Follow cluster initialize timed out: %u", pid);
}

as_status
as_shm_create(as_cluster* cluster, as_error* err, as_config* config)
{
	// In order to calculate total shared memory size, n_partitions needs to be initialized
	// before cluster init.  This would require every client process to query for n_partitions
	// even before seeds have been validated.
	// Hard code value for now.
	cluster->n_partitions = 4096;
	
	uint32_t size = sizeof(as_cluster_shm) + (sizeof(as_node_shm) * config->shm_max_nodes) +
		((sizeof(as_partition_table_shm) + (sizeof(as_partition_shm) * cluster->n_partitions)) *
		config->shm_max_namespaces);
	
	uint32_t pid = getpid();

#if !defined(_MSC_VER)
	// Create shared memory segment.  Only one process will succeed.
	int id = shmget(config->shm_key, size, IPC_CREAT | IPC_EXCL | 0666);

	if (id >= 0) {
		// Exclusive shared memory lock succeeded. shmget docs say shared memory create initializes
		// memory to zero, so memset is not necessary.
		// memset(cluster_shm, 0, size);
		as_log_info("Create shared memory cluster: %u", pid);
	}
	else if (errno == EEXIST) {
		// Some other process has created shared memory.  Use that shared memory.
		id = shmget(config->shm_key, size, IPC_CREAT | 0666);
		
		if (id < 0) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Shared memory get failed: %s pid: %u", strerror(errno), pid);
		}
	}
	else if (errno == ENOMEM) {
		// OS shared memory max exceeded.
		size_t max = as_shm_get_max_size();

#ifdef __linux__
		const char* increase_msg = "You can increase shared memory size by: sysctl -w kernel.shmmax=<new_size>";
#else
		const char* increase_msg = "You can increase shared memory size by: sysctl -w kern.sysv.shmmax=<new_size>";
#endif
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Shared memory max %zu has been exceeded with latest shared memory request of size %zu. %s",
			max, size, increase_msg);
	}
	else {
		// Exclusive shared memory lock failed.
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Shared memory get failed: %s pid: %u",
			strerror(errno), pid);
	}

	// Attach to shared memory.
	as_cluster_shm* cluster_shm = shmat(id, NULL, 0);

	if (cluster_shm == (void*)-1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error attaching to shared memory: %s pid: %u",
			strerror(errno), pid);
		// Try removing the shared memory - it will fail if any other process is still attached.
		shmctl(id, IPC_RMID, 0);
		return err->code;
	}
#else // _MSC_VER
	char name[256];
	HANDLE id;
	DWORD code;
	int i;

	for (i = 0; i < 2; i++) {
		// Try global shared memory namespace first.  This will fail with 
		// ERROR_ACCESS_DENIED if the process is not run with administrator
		// privileges.  If fail, try local shared memory namespace instead.
		const char* prefix = (i == 0) ? "Global" : "Local";
		sprintf(name, "%s\\Aerospike%x", prefix, config->shm_key);
		id = CreateFileMappingA(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, size, name);
		code = GetLastError();

		if (id && id != INVALID_HANDLE_VALUE) {
			if (code == 0) {
				as_log_info("Create shared memory cluster: %s pid: %u", name, pid);
				break;
			}
			else if (code == ERROR_ALREADY_EXISTS) {
				as_log_info("Follow shared memory cluster: %s pid: %u", name, pid);
				// Handle should be handle of file that was already created.
				// There is no need to reopen.
				// id = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, name);
				break;
			}
		}
	}

	if (i >= 2) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Shared memory create/get failed: %s pid: %u code: %d", name, pid, code);
	}

	as_cluster_shm* cluster_shm = MapViewOfFile(id, FILE_MAP_ALL_ACCESS, 0, 0, size);

	if (cluster_shm == NULL) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error attaching to shared memory: %d pid: %u", GetLastError(), pid);
		CloseHandle(id);
		return err->code;
	}
#endif

	// Initialize local data.
	as_shm_info* shm_info = cf_malloc(sizeof(as_shm_info));
	shm_info->local_nodes = cf_calloc(config->shm_max_nodes, sizeof(as_node*));
	shm_info->cluster_shm = cluster_shm;
	shm_info->shm_id = id;
	shm_info->takeover_threshold_ms = config->shm_takeover_threshold_sec * 1000;
	shm_info->is_tend_master = as_cas_uint8(&cluster_shm->lock, 0, 1);
	cluster->shm_info = shm_info;

	if (shm_info->is_tend_master) {
		as_log_info("Take over shared memory cluster: %u", pid);
		as_store_uint64(&cluster_shm->timestamp, cf_getms());
		as_store_uint32(&cluster_shm->owner_pid, pid);

		uint32_t pt_offset = sizeof(as_cluster_shm) + (sizeof(as_node_shm) * config->shm_max_nodes);
		uint32_t pt_size = sizeof(as_partition_table_shm) + (sizeof(as_partition_shm) * cluster->n_partitions);

		// Ensure shared memory cluster is fully initialized.
		if (as_load_uint8_acq(&cluster_shm->ready)) {
			as_log_info("Cluster already initialized: %u", pid);

			// Validate that the already initialized shared memory has the expected offset and size.
			if (! (cluster_shm->partition_tables_capacity == config->shm_max_namespaces &&
				cluster_shm->partition_tables_offset == pt_offset &&
				cluster_shm->partition_table_byte_size == pt_size)) {

				as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Existing shared memory size is not compatible with new configuration. "
					"Stop client processes and ensure shared memory is removed before "
					"attempting new configuration: %u,%u,%u vs %u,%u,%u",
					cluster_shm->partition_tables_capacity,
					cluster_shm->partition_tables_offset,
					cluster_shm->partition_table_byte_size,
					config->shm_max_namespaces, pt_offset, pt_size);

				as_store_uint8_rls(&cluster_shm->lock, 0);
				as_shm_destroy(cluster);
				return err->code;
			}

			// Copy shared memory nodes to local nodes.
			as_shm_reset_nodes(cluster);
			as_cluster_add_seeds(cluster);
		}
		else {
			as_log_info("Initialize cluster: %u", pid);
			cluster_shm->n_partitions = cluster->n_partitions;
			cluster_shm->nodes_capacity = config->shm_max_nodes;
			cluster_shm->partition_tables_capacity = config->shm_max_namespaces;
			cluster_shm->partition_tables_offset = pt_offset;
			cluster_shm->partition_table_byte_size = pt_size;

			as_status status = as_cluster_init(cluster, err);
			
			if (status != AEROSPIKE_OK) {
				as_store_uint8_rls(&cluster_shm->lock, 0);
				as_shm_destroy(cluster);
				return status;
			}
			as_store_uint8_rls(&cluster_shm->ready, 1);
		}
	}
	else {
		as_log_info("Follow shared memory cluster: %u", pid);

		// Prole should wait until master has fully initialized shared memory.
		if (! as_load_uint8_acq(&cluster_shm->ready)) {
			as_shm_wait_till_ready(cluster, cluster_shm, pid);
		}

		// Copy shared memory nodes to local nodes.
		as_shm_reset_nodes(cluster);
		as_cluster_add_seeds(cluster);
	}
	cluster->valid = true;
	
	// Run tending thread which handles both master and prole tending.
	pthread_attr_t attr;
	pthread_attr_init(&attr);

	if (cluster->tend_thread_cpu >= 0) {
		as_cpu_assign_thread_attr(&attr, cluster->tend_thread_cpu);
	}

	if (pthread_create(&cluster->tend_thread, &attr, as_shm_tender, cluster) != 0) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to create tend thread: %s pid: %u",
						strerror(errno), pid);
		pthread_attr_destroy(&attr);
		as_shm_destroy(cluster);
		return err->code;
	}
	pthread_attr_destroy(&attr);
	return AEROSPIKE_OK;
}

void
as_shm_destroy(as_cluster* cluster)
{
	as_shm_info* shm_info = cluster->shm_info;
	
	if (!shm_info) {
		return;
	}

#if !defined(_MSC_VER)
	// Detach shared memory.
	shmdt(shm_info->cluster_shm);

	// Determine how many processes are still attached to shared memory.
	struct shmid_ds shm_stat;
	int rv = shmctl(shm_info->shm_id, IPC_STAT, &shm_stat);

	// If no more processes are attached, remove shared memory.
	if (rv == 0 && shm_stat.shm_nattch == 0) {
		uint32_t pid = getpid();
		as_log_info("Remove shared memory segment: %u", pid);
		shmctl(shm_info->shm_id, IPC_RMID, 0);
	}
#else
	if (!UnmapViewOfFile(shm_info->cluster_shm)) {
		as_log_error("Failed to detach from shared memory");
	}
	CloseHandle(shm_info->shm_id);
#endif

	// Release memory.
	cf_free(shm_info->local_nodes);
	cf_free(shm_info);
	cluster->shm_info = 0;
}
