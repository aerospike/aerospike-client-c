/*
 * Copyright 2008-2017 Aerospike, Inc.
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
#include <aerospike/as_log_macros.h>
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_string.h>
#include <citrusleaf/cf_b64.h>
#include <citrusleaf/cf_types.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_clock.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/shm.h>

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

/******************************************************************************
 * DECLARATIONS
 ******************************************************************************/

as_status
as_cluster_init(as_cluster* cluster, as_error* err, bool fail_if_not_connected);

void
as_cluster_add_seeds(as_cluster* cluster);

as_status
as_cluster_tend(as_cluster* cluster, as_error* err, bool enable_seed_warnings);

void
as_cluster_add_nodes_copy(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_add);

void
as_cluster_remove_nodes_copy(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_remove);

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

static size_t
as_shm_get_max_size()
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
			ck_swlock_write_lock(&node_shm->lock);
			memcpy(&node_shm->addr, &address->addr, sizeof(struct sockaddr_storage));
			if (node_to_add->tls_name) {
				strcpy(node_shm->tls_name, node_to_add->tls_name);
			}
			else {
				node_shm->tls_name[0] = 0;
			}
			node_shm->features = node_to_add->features;
			node_shm->active = true;
			ck_swlock_write_unlock(&node_shm->lock);
			
			// Set shared memory node array index.
			// Only referenced by shared memory tending thread, so volatile write not necessary.
			node_to_add->index = node_index;
		}
		else {
			// Add new node and activate.
			if (cluster_shm->nodes_size < cluster_shm->nodes_capacity) {
				as_node_shm* node_shm = &cluster_shm->nodes[cluster_shm->nodes_size];
				
				// Update shared memory node in write lock.
				ck_swlock_write_lock(&node_shm->lock);
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
				ck_swlock_write_unlock(&node_shm->lock);
				
				// Set shared memory node array index.
				// Only referenced by shared memory tending thread, so volatile write not necessary.
				node_to_add->index = cluster_shm->nodes_size;

				// Increment node array size.
				ck_pr_inc_32(&cluster_shm->nodes_size);
			}
			else {
				// There are no more node slots available in shared memory.
				as_log_error("Failed to add node %s %s. Shared memory capacity exceeeded: %d",
					node_to_add->name, address->name, cluster_shm->nodes_capacity);
			}
		}
		ck_pr_store_ptr(&shm_info->local_nodes[node_to_add->index], node_to_add);
	}
	ck_pr_inc_32(&cluster_shm->nodes_gen);
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
		ck_swlock_write_lock(&node_shm->lock);
		node_shm->active = false;
		ck_swlock_write_unlock(&node_shm->lock);

		ck_pr_store_ptr(&shm_info->local_nodes[node_to_remove->index], 0);
	}
	ck_pr_inc_32(&cluster_shm->nodes_gen);
}

static void
as_shm_reset_nodes(as_cluster* cluster)
{
	// Synchronize shared memory nodes with local nodes.
	as_shm_info* shm_info = cluster->shm_info;
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	
	as_node_shm* nodes_shm = cluster_shm->nodes;
	as_node_shm node_tmp;
	uint32_t max = ck_pr_load_32(&cluster_shm->nodes_size);
	
	as_vector nodes_to_add;
	as_vector_inita(&nodes_to_add, sizeof(as_node*), max);
	
	as_vector nodes_to_remove;
	as_vector_inita(&nodes_to_remove, sizeof(as_node*), max);

	for (uint32_t i = 0; i < max; i++) {
		as_node_shm* node_shm = &nodes_shm[i];
		as_node* node = shm_info->local_nodes[i];

		// Make copy of shared memory node under a read lock.
		ck_swlock_read_lock(&node_shm->lock);
		memcpy(&node_tmp, node_shm, sizeof(as_node_shm));
		ck_swlock_read_unlock(&node_shm->lock);
		
		if (node_tmp.active) {
			if (! node) {
				as_node_info node_info;
				strcpy(node_info.name, node_tmp.name);
				as_socket_init(&node_info.socket);
				node_info.features = node_tmp.features;
				node = as_node_create(cluster, NULL, node_tmp.tls_name, 0, false, (struct sockaddr*)&node_tmp.addr, &node_info);
				node->index = i;
				as_vector_append(&nodes_to_add, &node);
				ck_pr_store_ptr(&shm_info->local_nodes[i], node);
			}
		}
		else {
			if (node) {
				as_node_deactivate(node);
				as_vector_append(&nodes_to_remove, &node);
				ck_pr_store_ptr(&shm_info->local_nodes[i], 0);
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
as_shm_add_partition_table(as_cluster_shm* cluster_shm, const char* ns)
{
	if (cluster_shm->partition_tables_size >= cluster_shm->partition_tables_capacity) {
		// There are no more partition table slots available in shared memory.
		as_log_error("Failed to add partition table namespace %s. Shared memory capacity exceeeded: %d",
				 ns, cluster_shm->partition_tables_capacity);
		return 0;
	}
	
	as_partition_table_shm* tables = as_shm_get_partition_tables(cluster_shm);
	as_partition_table_shm* table = as_shm_get_partition_table(cluster_shm, tables, cluster_shm->partition_tables_size);
	as_strncpy(table->ns, ns, AS_MAX_NAMESPACE_SIZE);
	
	// Increment partition tables array size.
	ck_pr_inc_32(&cluster_shm->partition_tables_size);
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
as_shm_partition_update(as_shm_info* shm_info, as_partition_shm* p, uint32_t node_index, bool master, bool owns)
{
	// node_index starts at one (zero indicates unset).
	if (master) {
		if (node_index == p->master) {
			if (! owns) {
				ck_pr_store_32(&p->master, 0);
			}
		}
		else {
			if (owns) {
				if (p->master) {
					as_shm_force_replicas_refresh(shm_info, p->master);
				}
				ck_pr_store_32(&p->master, node_index);
			}
		}
	}
	else {
		if (node_index == p->prole) {
			if (! owns) {
				ck_pr_store_32(&p->prole, 0);
			}
		}
		else {
			if (owns) {
				if (p->prole) {
					as_shm_force_replicas_refresh(shm_info, p->prole);
				}
				ck_pr_store_32(&p->prole, node_index);
			}
		}
	}
}

static void
as_shm_decode_and_update(as_shm_info* shm_info, char* bitmap_b64, int64_t len, as_partition_table_shm* table, uint32_t node_index, bool master)
{
	// Size allows for padding - is actual size rounded up to multiple of 3.
	uint8_t* bitmap = (uint8_t*)alloca(cf_b64_decoded_buf_size((uint32_t)len));
	
	// For now - for speed - trust validity of encoded characters.
	cf_b64_decode(bitmap_b64, (uint32_t)len, bitmap, NULL);
	
	// Expand the bitmap.
	uint32_t max = shm_info->cluster_shm->n_partitions;
	
	for (uint32_t i = 0; i < max; i++) {
		bool owns = ((bitmap[i >> 3] & (0x80 >> (i & 7))) != 0);
		as_shm_partition_update(shm_info, &table->partitions[i], node_index, master, owns);
	}
}

void
as_shm_update_partitions(as_shm_info* shm_info, const char* ns, char* bitmap_b64, int64_t len, as_node* node, bool master)
{
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	as_partition_table_shm* table = as_shm_find_partition_table(cluster_shm, ns);
	
	if (! table) {
		table = as_shm_add_partition_table(cluster_shm, ns);
	}
	
	if (table) {
		as_shm_decode_and_update(shm_info, bitmap_b64, len, table, node->index + 1, master);
	}
}

static inline as_node*
as_shm_reserve_node(as_cluster* cluster, as_node** local_nodes, uint32_t node_index)
{
	// node_index starts at one (zero indicates unset).
	if (node_index) {
		as_node* node = ck_pr_load_ptr(&local_nodes[node_index-1]);
		
		if (node && ck_pr_load_8(&node->active)) {
			as_node_reserve(node);
			return node;
		}
	}
	
	// as_log_debug("Choose random node for unmapped namespace/partition");
	return as_node_get_random(cluster);
}

static as_node*
as_shm_reserve_node_alternate(as_cluster* cluster, as_node** local_nodes, uint32_t chosen_index, uint32_t alternate_index)
{
	// index values start at one (zero indicates unset).
	as_node* chosen = ck_pr_load_ptr(&local_nodes[chosen_index-1]);
	
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (chosen && ck_pr_load_8(&chosen->active)) {
		as_node_reserve(chosen);
		return chosen;
	}
	return as_shm_reserve_node(cluster, local_nodes, alternate_index);
}

static uint32_t g_shm_randomizer = 0;

as_node*
as_shm_node_get(as_cluster* cluster, const char* ns, const uint8_t* digest, as_policy_replica replica, bool use_master)
{
	as_cluster_shm* cluster_shm = cluster->shm_info->cluster_shm;
	as_partition_table_shm* table = as_shm_find_partition_table(cluster_shm, ns);

	if (table) {
		uint32_t partition_id = as_partition_getid(digest, cluster_shm->n_partitions);
		as_partition_shm* p = &table->partitions[partition_id];
		return as_partition_shm_get_node(cluster, p, replica, use_master);
	}

	// as_log_debug("Choose random node for null partition table");
	return as_node_get_random(cluster);
}

as_node*
as_partition_shm_get_node(as_cluster* cluster, as_partition_shm* p, as_policy_replica replica, bool use_master)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	as_node** local_nodes = cluster->shm_info->local_nodes;
	uint32_t master = ck_pr_load_32(&p->master);

	if (replica == AS_POLICY_REPLICA_MASTER) {
		return as_shm_reserve_node(cluster, local_nodes, master);
	}

	uint32_t prole = ck_pr_load_32(&p->prole);

	if (! prole) {
		return as_shm_reserve_node(cluster, local_nodes, master);
	}

	if (! master) {
		return as_shm_reserve_node(cluster, local_nodes, prole);
	}

	if (replica == AS_POLICY_REPLICA_ANY) {
		// Alternate between master and prole for reads with global iterator.
		uint32_t r = ck_pr_faa_32(&g_shm_randomizer, 1);
		use_master = (r & 1);
	}

	// AS_POLICY_REPLICA_SEQUENCE uses the use_master preference without modification.
	if (use_master) {
		return as_shm_reserve_node_alternate(cluster, local_nodes, master, prole);
	}
	return as_shm_reserve_node_alternate(cluster, local_nodes, prole, master);
}

static void
as_shm_takeover_cluster(as_shm_info* shm_info, as_cluster_shm* cluster_shm, uint32_t pid)
{
	as_log_info("Take over shared memory cluster: %d", pid);
	ck_pr_store_32(&cluster_shm->owner_pid, pid);
	shm_info->is_tend_master = true;
}

static void*
as_shm_tender(void* userdata)
{
	// Shared memory cluster tender.
	as_cluster* cluster = userdata;
	as_shm_info* shm_info = cluster->shm_info;
	as_cluster_shm* cluster_shm = shm_info->cluster_shm;
	uint64_t threshold = shm_info->takeover_threshold_ms;
	uint64_t limit = 0;
	uint32_t pid = getpid();
	uint32_t nodes_gen = 0;
	
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
			ck_pr_store_64(&cluster_shm->timestamp, cf_getms());
			
			if (status != AEROSPIKE_OK) {
				as_log_warn("Tend error: %s %s", as_error_string(status), err.message);
			}
		}
		else {
			// Follow shared memory cluster.
			// Check if tend owner has released lock.
			if (ck_pr_cas_8(&cluster_shm->lock, 0, 1)) {
				as_shm_takeover_cluster(shm_info, cluster_shm, pid);
				continue;
			}
			
			// Check if tend owner died without releasing lock.
			uint64_t now = cf_getms();
			if (now >= limit) {
				uint64_t ts = ck_pr_load_64(&cluster_shm->timestamp);
				
				// Check if cluster hasn't been tended within threshold.
				if (now - ts >= threshold) {
					uint32_t owner_pid = ck_pr_load_32(&cluster_shm->owner_pid);
					
					// Check if owner process id is invalid or does not exist.
					if (owner_pid == 0 || kill(owner_pid, 0) != 0) {
						// Cluster should be taken over, but this must be done under lock.
						ck_spinlock_lock(&cluster_shm->take_over_lock);
						
						// Reload timestamp, just in case another process just modified it.
						ts = ck_pr_load_64(&cluster_shm->timestamp);
						
						// Check if cluster hasn't been tended within threshold.
						if (now - ts >= threshold) {
							// Take over cluster tending.
							// Update timestamp, so other processes will not try to take over.
							ck_pr_store_64(&cluster_shm->timestamp, now);
							ck_pr_store_8(&cluster_shm->lock, 1);
							ck_spinlock_unlock(&cluster_shm->take_over_lock);
							as_shm_takeover_cluster(shm_info, cluster_shm, pid);
							continue;
						}
						ck_spinlock_unlock(&cluster_shm->take_over_lock);
					}
				}
				limit = ts + threshold;
			}
			
			// Synchronize local cluster with shared memory cluster.
			uint32_t gen = ck_pr_load_32(&cluster_shm->nodes_gen);
			
			if (nodes_gen != gen) {
				nodes_gen = gen;
				as_shm_reset_nodes(cluster);
			}
		}

		// Convert tend interval into absolute timeout.
		cf_clock_current_add(&delta, &abstime);
		
		// Sleep for tend interval and exit early if cluster destroy is signaled.
		pthread_cond_timedwait(&cluster->tend_cond, &cluster->tend_lock, &abstime);
	}
	pthread_mutex_unlock(&cluster->tend_lock);
	
	if (shm_info->is_tend_master) {
		shm_info->is_tend_master = false;
		ck_pr_store_8(&cluster_shm->lock, 0);
	}
	return 0;
}

static void
as_shm_wait_till_ready(as_cluster* cluster, as_cluster_shm* cluster_shm, uint32_t pid)
{
	// Wait till cluster is initialized or connection timeout is reached.
	uint32_t interval_micros = 200 * 1000;  // 200 milliseconds
	uint64_t limit = cf_getms() + 10000;    // 10 second timeout.
	
	do {
		usleep(interval_micros);
		
		if (ck_pr_load_8(&cluster_shm->ready)) {
			as_log_info("Follow cluster initialized: %d", pid);
			return;
		}
	} while (cf_getms() < limit);
	as_log_warn("Follow cluster initialize timed out: %d", pid);
}

static void
as_shm_cleanup(int id, as_cluster_shm* cluster_shm)
{
	// Detach shared memory.
	if (cluster_shm) {
		shmdt(cluster_shm);
	}
	
	// Try removing the shared memory - it will fail if any other process is still attached.
	shmctl(id, IPC_RMID, 0);
}

as_status
as_shm_create(as_cluster* cluster, as_error* err, as_config* config)
{
	// In order to calculate total shared memory size, n_partitions needs to be initialized
	// before cluster init.  This would require every client process to query for n_partitions
	// even before seeds have been validated.
	// Hard code value for now.
	uint32_t n_partitions = 4096;
	
	uint32_t size = sizeof(as_cluster_shm) + (sizeof(as_node_shm) * config->shm_max_nodes) +
		((sizeof(as_partition_table_shm) + (sizeof(as_partition_shm) * n_partitions)) * config->shm_max_namespaces);
	
	uint32_t pid = getpid();

	// Create shared memory segment.  Only one process will succeed.
	int id = shmget(config->shm_key, size, IPC_CREAT | IPC_EXCL | 0666);
	as_cluster_shm* cluster_shm = 0;

	if (id >= 0) {
		// Exclusive shared memory lock succeeded.
		// shmget docs say shared memory create initializes memory to zero, so memset is not necessary.
		// memset(cluster_shm, 0, size);
		as_log_info("Create shared memory cluster: %d", pid);
	}
	else if (errno == EEXIST) {
		// Some other process has created shared memory.  Use that shared memory.
		id = shmget(config->shm_key, size, IPC_CREAT | 0666);
		
		if (id < 0) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Shared memory get failed: %s pid: %d", strerror(errno), pid);
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
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Shared memory get failed: %s pid: %d", strerror(errno), pid);
	}

	// Attach to shared memory.
	cluster_shm = shmat(id, NULL, 0);

	if (cluster_shm == (void*)-1) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Error attaching to shared memory: %s pid: %d", strerror(errno), pid);
		as_shm_cleanup(id, 0);
		return err->code;
	}

	// Initialize local data.
	as_shm_info* shm_info = cf_malloc(sizeof(as_shm_info));
	shm_info->local_nodes = cf_calloc(config->shm_max_nodes, sizeof(as_node*));
	shm_info->cluster_shm = cluster_shm;
	shm_info->shm_id = id;
	shm_info->takeover_threshold_ms = config->shm_takeover_threshold_sec * 1000;
	shm_info->is_tend_master = ck_pr_cas_8(&cluster_shm->lock, 0, 1);
	cluster->shm_info = shm_info;

	if (shm_info->is_tend_master) {
		as_log_info("Take over shared memory cluster: %d", pid);
		ck_pr_fence_lock();
		cluster_shm->n_partitions = n_partitions;
		cluster_shm->nodes_capacity = config->shm_max_nodes;
		cluster_shm->partition_tables_capacity = config->shm_max_namespaces;
		cluster_shm->partition_tables_offset = sizeof(as_cluster_shm) + (sizeof(as_node_shm) * config->shm_max_nodes);
		cluster_shm->partition_table_byte_size = sizeof(as_partition_table_shm) + (sizeof(as_partition_shm) * n_partitions);
		cluster_shm->timestamp = cf_getms();

		ck_pr_store_32(&cluster_shm->owner_pid, pid);
		
		// Ensure shared memory cluster is fully initialized.
		if (ck_pr_load_8(&cluster_shm->ready)) {
			// Copy shared memory nodes to local nodes.
			as_log_info("Cluster already initialized: %d", pid);
			as_shm_reset_nodes(cluster);
			as_cluster_add_seeds(cluster);
		}
		else {
			as_log_info("Initialize cluster: %d", pid);
			as_status status = as_cluster_init(cluster, err, true);
			
			if (status != AEROSPIKE_OK) {
				ck_pr_store_8(&cluster_shm->lock, 0);
				as_shm_destroy(cluster);
				return status;
			}
			ck_pr_store_8(&cluster_shm->ready, 1);
		}
		ck_pr_fence_unlock();
	}
	else {
		as_log_info("Follow shared memory cluster: %d", pid);
		ck_pr_fence_lock();

		// Prole should wait until master has fully initialized shared memory.
		if (! ck_pr_load_8(&cluster_shm->ready)) {
			as_shm_wait_till_ready(cluster, cluster_shm, pid);
		}
		ck_pr_fence_unlock();

		// Copy shared memory nodes to local nodes.
		as_shm_reset_nodes(cluster);
		as_cluster_add_seeds(cluster);
	}
	cluster->valid = true;
	
	// Run tending thread which handles both master and prole tending.
	pthread_create(&cluster->tend_thread, 0, as_shm_tender, cluster);
	return AEROSPIKE_OK;
}

void
as_shm_destroy(as_cluster* cluster)
{
	as_shm_info* shm_info = cluster->shm_info;
	
	if (!shm_info) {
		return;
	}
	
	// Detach shared memory.
	shmdt(shm_info->cluster_shm);
	
	// Try removing the shared memory - it will fail if any other process is still attached.
	// Failure is normal behavior, so don't check return code.
	shmctl(shm_info->shm_id, IPC_RMID, 0);

	// Release memory.
	cf_free(shm_info->local_nodes);
	cf_free(shm_info);
	cluster->shm_info = 0;
}
