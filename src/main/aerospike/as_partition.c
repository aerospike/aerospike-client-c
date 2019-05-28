/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_partition.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_node.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_string.h>
#include <citrusleaf/cf_b64.h>
#include <stdlib.h>

/******************************************************************************
 * Functions
 *****************************************************************************/

/* Used for debugging only.
static void
as_partition_table_print(as_partition_table* table)
{
	for (uint32_t i = 0; i < table->size; i++) {
		as_partition* p = &table->partitions[i];
		
		if (p->master) {
			printf("%u %s\n", i, p->master->name);
		}
		else {
			printf("%u null\n", i);
		}
	}
}

void
as_partition_tables_print(as_partition_tables* tables)
{
	for (uint32_t i = 0; i < tables->size; i++) {
		as_partition_table* table = tables->array[i];
		
		printf("Namespace: %s\n", table->ns);
		as_partition_table_print(table);
	}
}
*/

static inline void
set_partition_tables(as_cluster* cluster, as_partition_tables* tables)
{
	// Volatile write used so other threads can see node changes.
	as_fence_store();
	as_store_ptr(&cluster->partition_tables, tables);
}

static inline void
set_node(as_node** trg, as_node* src)
{
	as_fence_store();
	as_store_ptr(trg, src);
}

static as_partition_table*
as_partition_table_create(const char* ns, uint32_t capacity, bool sc_mode)
{
	size_t len = sizeof(as_partition_table) + (sizeof(as_partition) * capacity);
	as_partition_table* table = cf_malloc(len);
	memset(table, 0, len);
	table->ref_count = 1;
	table->size = capacity;
	as_strncpy(table->ns, ns, AS_MAX_NAMESPACE_SIZE);
	table->sc_mode = sc_mode;
	return table;
}

void
as_partition_table_destroy(as_partition_table* table)
{
	for (uint32_t i = 0; i < table->size; i++) {
		as_partition* p = &table->partitions[i];
		
		if (p->master) {
			as_node_release(p->master);
		}
		
		if (p->prole) {
			as_node_release(p->prole);
		}
	}
	cf_free(table);
}

static inline void
as_partition_table_reserve(as_partition_table** table)
{
	as_partition_table* pt = (as_partition_table*)as_load_ptr(table);
	as_incr_uint32(&pt->ref_count);
}

static inline void
as_partition_table_release(as_partition_table* table)
{
	if (as_aaf_uint32(&table->ref_count, -1) == 0) {
		as_partition_table_destroy(table);
	}
}

as_partition_tables*
as_partition_tables_create(uint32_t capacity)
{
	size_t size = sizeof(as_partition_tables) + (sizeof(as_partition_table*) * capacity);
	as_partition_tables* tables = cf_malloc(size);
	memset(tables, 0, size);
	tables->ref_count = 1;
	tables->size = capacity;
	return tables;
}

void
as_partition_tables_destroy(as_partition_tables* tables)
{
	for (uint32_t i = 0; i < tables->size; i++) {
		as_partition_table_release(tables->array[i]);
	}
	cf_free(tables);
}

static inline as_partition_tables*
as_partition_tables_reserve(as_cluster* cluster)
{
	as_partition_tables* tables = (as_partition_tables*)as_load_ptr(&cluster->partition_tables);
	as_incr_uint32(&tables->ref_count);
	return tables;
}

static inline as_node*
try_master(as_cluster* cluster, as_node* node)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (node && as_load_uint8(&node->active)) {
		return node;
	}
	// When master only specified, should never get random nodes.
	return NULL;
}

static inline as_node*
try_node(as_cluster* cluster, as_node* node)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (node && as_load_uint8(&node->active)) {
		return node;
	}
	return NULL;
}

static as_node*
try_node_alternate(as_cluster* cluster, as_node* chosen, as_node* alternate)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (as_load_uint8(&chosen->active)) {
		return chosen;
	}
	return try_node(cluster, alternate);
}

static as_node*
get_sequence_node(as_cluster* cluster, as_partition* p, bool use_master)
{
	as_node* master = (as_node*)as_load_ptr(&p->master);
	as_node* prole = (as_node*)as_load_ptr(&p->prole);

	if (! prole) {
		return try_node(cluster, master);
	}

	if (! master) {
		return try_node(cluster, prole);
	}
	
	if (use_master) {
		return try_node_alternate(cluster, master, prole);
	}
	return try_node_alternate(cluster, prole, master);
}

static inline bool
try_rack_node(as_cluster* cluster, const char* ns, as_node* node)
{
	if (node && as_load_uint8(&node->active) &&
		as_node_has_rack(cluster, node, ns, cluster->rack_id)) {
		return true;
	}
	return false;
}

static as_node*
prefer_rack_node(as_cluster* cluster, const char* ns, as_partition* p, bool use_master)
{
	as_node* master;
	as_node* prole;

	if (use_master) {
		master = (as_node*)as_load_ptr(&p->master);

		if (try_rack_node(cluster, ns, master)) {
			return master;
		}

		prole = (as_node*)as_load_ptr(&p->prole);

		if (try_rack_node(cluster, ns, prole)) {
			return prole;
		}
	}
	else {
		prole = (as_node*)as_load_ptr(&p->prole);

		if (try_rack_node(cluster, ns, prole)) {
			return prole;
		}

		master = (as_node*)as_load_ptr(&p->master);

		if (try_rack_node(cluster, ns, master)) {
			return master;
		}
	}

	// Default to sequence mode.
	if (! prole) {
		return try_node(cluster, master);
	}

	if (! master) {
		return try_node(cluster, prole);
	}

	if (use_master) {
		return try_node_alternate(cluster, master, prole);
	}
	return try_node_alternate(cluster, prole, master);
}

static uint32_t g_randomizer = 0;

as_node*
as_partition_reg_get_node(
	as_cluster* cluster, const char* ns, as_partition* p, as_policy_replica replica,
	bool use_master, bool is_retry
	)
{
	switch (replica) {
		case AS_POLICY_REPLICA_MASTER: {
			// Make volatile reference so changes to tend thread will be reflected in this thread.
			as_node* master = (as_node*)as_load_ptr(&p->master);
			return try_master(cluster, master);
		}

		case AS_POLICY_REPLICA_ANY: {
			// Alternate between master and prole for reads with global iterator.
			uint32_t r = as_faa_uint32(&g_randomizer, 1);
			use_master = (r & 1);
			return get_sequence_node(cluster, p, use_master);
		}

		default:
		case AS_POLICY_REPLICA_SEQUENCE: {
			return get_sequence_node(cluster, p, use_master);
		}

		case AS_POLICY_REPLICA_PREFER_RACK: {
			if (!is_retry) {
				return prefer_rack_node(cluster, ns, p, use_master);
			}
			else {
				return get_sequence_node(cluster, p, use_master);
			}
		}
	}
}

as_status
as_partition_info_init(as_partition_info* pi, as_cluster* cluster, as_error* err, const as_key* key)
{
	if (cluster->shm_info) {
		as_cluster_shm* cluster_shm = cluster->shm_info->cluster_shm;
		as_partition_table_shm* table = as_shm_find_partition_table(cluster_shm, key->ns);

		if (! table) {
			as_nodes* nodes = as_nodes_reserve(cluster);
			uint32_t n_nodes = nodes->size;
			as_nodes_release(nodes);

			if (n_nodes == 0) {
				return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Cluster is empty");
			}
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid namespace: %s", key->ns);
		}
		pi->ns = table->ns;
		pi->partition_id = as_partition_getid(key->digest.value, cluster_shm->n_partitions);
		pi->partition = &table->partitions[pi->partition_id];
		pi->sc_mode = table->sc_mode;
	}
	else {
		// Partition tables array size does not currently change after first cluster tend.
		// Also, there is a one second delayed garbage collection coupled with as_partition_tables_get()
		// being very fast.  Reference counting the tables array is not currently necessary, but do it
		// anyway in case the server starts supporting dynamic namespaces.
		//
		// Note: as_partition_tables must be released when done with partition.
		as_partition_tables* tables = as_partition_tables_reserve(cluster);
		as_partition_table* table = as_partition_tables_get(tables, key->ns);

		if (! table) {
			as_partition_tables_release(tables);
			as_nodes* nodes = as_nodes_reserve(cluster);
			uint32_t n_nodes = nodes->size;
			as_nodes_release(nodes);

			if (n_nodes == 0) {
				return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Cluster is empty");
			}
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid namespace: %s", key->ns);
		}
		pi->ns = table->ns;
		pi->partition_id = as_partition_getid(key->digest.value, cluster->n_partitions);
		pi->partition = &table->partitions[pi->partition_id];
		pi->sc_mode = table->sc_mode;
	}
	return AEROSPIKE_OK;
}

as_partition_table*
as_partition_tables_get(as_partition_tables* tables, const char* ns)
{
	as_partition_table* table;
	
	for (uint32_t i = 0; i < tables->size; i++) {
		table = tables->array[i];
		
		if (strcmp(table->ns, ns) == 0) {
			return table;
		}
	}
	return 0;
}

bool
as_partition_tables_find_node(as_partition_tables* tables, as_node* node)
{
	as_partition_table* table;
	as_partition* p;
	
	for (uint32_t i = 0; i < tables->size; i++) {
		table = tables->array[i];
		
		for (uint32_t j = 0; j < table->size; j++) {
			p = &table->partitions[j];
			
			// Use reference equality for performance.
			if (p->master == node || p->prole == node) {
				return true;
			}
		}
	}
	return false;
}

static inline void
force_replicas_refresh(as_node* node)
{
	node->partition_generation = (uint32_t)-1;
}

static as_partition_table*
as_partition_vector_get(as_vector* tables, const char* ns)
{
	as_partition_table* table;
	
	for (uint32_t i = 0; i < tables->size; i++) {
		table = as_vector_get_ptr(tables, i);
		
		if (strcmp(table->ns, ns) == 0) {
			return table;
		}
	}
	return 0;
}

static void
decode_and_update(
	char* bitmap_b64, uint32_t len, as_partition_table* table, as_node* node, bool master,
	uint32_t regime, bool* regime_error
	)
{
	// Size allows for padding - is actual size rounded up to multiple of 3.
	uint8_t* bitmap = (uint8_t*)alloca(cf_b64_decoded_buf_size(len));

	// For now - for speed - trust validity of encoded characters.
	cf_b64_decode(bitmap_b64, len, bitmap, NULL);

	// Expand the bitmap.
	for (uint32_t i = 0; i < table->size; i++) {
		if ((bitmap[i >> 3] & (0x80 >> (i & 7))) != 0) {
			// This node claims ownership of partition.
			// as_log_debug("Set partition %s:%s:%u:%s", master? "master" : "prole", table->ns, i,
			//				node->name);

			// Volatile reads are not necessary because the tend thread exclusively modifies
			// partition.  Volatile writes are used so other threads can view change.
			as_partition* p = &table->partitions[i];

			if (regime >= p->regime) {
				if (regime > p->regime) {
					p->regime = regime;
				}

				if (master) {
					if (node != p->master) {
						as_node* tmp = p->master;
						as_node_reserve(node);
						set_node(&p->master, node);

						if (tmp) {
							force_replicas_refresh(tmp);
							as_node_release(tmp);
						}
					}
				}
				else {
					if (node != p->prole) {
						as_node* tmp = p->prole;
						as_node_reserve(node);
						set_node(&p->prole, node);

						if (tmp) {
							force_replicas_refresh(tmp);
							as_node_release(tmp);
						}
					}
				}
			}
			else {
				if (!(*regime_error)) {
					as_log_info("%s regime(%u) < old regime(%u)",
								as_node_get_address_string(node), regime, p->regime);
					*regime_error = true;
				}
			}
		}
	}
}

static void
release_partition_tables(as_partition_tables* tables)
{
	as_partition_tables_release(tables);
}

static void
as_partition_tables_copy_add(
	as_cluster* cluster, as_partition_tables* tables_old,
	as_vector* /* <as_partition_table*> */ tables_to_add)
{
	// Increment reference count on old partition tables.
	for (uint32_t i = 0; i < tables_old->size; i++) {
		as_partition_table_reserve(&tables_old->array[i]);
	}

	// Create new tables array.
	as_partition_tables* tables_new =
		as_partition_tables_create(tables_old->size + tables_to_add->size);
	
	// Add existing tables.
	memcpy(tables_new->array, tables_old->array, sizeof(as_partition_table*) * tables_old->size);
	
	// Add new tables.
	memcpy(&tables_new->array[tables_old->size], tables_to_add->list,
		   sizeof(as_partition_table*) * tables_to_add->size);


	// Replace tables with copy.
	set_partition_tables(cluster, tables_new);
	
	// Put old tables on garbage collector stack.
	as_gc_item item;
	item.data = tables_old;
	item.release_fn = (as_release_fn)release_partition_tables;
	as_vector_append(cluster->gc, &item);
}

bool
as_partition_tables_update_all(as_cluster* cluster, as_node* node, char* buf, bool has_regime)
{
	// Use destructive parsing (ie modifying input buffer with null termination) for performance.
	// Receive format: replicas-all\t or replicas\t
	//              <ns1>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
	//              <ns2>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
	as_partition_tables* tables = cluster->partition_tables;
	uint32_t bitmap_size = (cluster->n_partitions + 7) / 8;
	long expected_len = (long)cf_b64_encoded_len(bitmap_size);

	char* p = buf;
	char* ns = p;
	char* begin = 0;
	int64_t len;
	uint32_t regime = 0;
	bool regime_error = false;

	// Add all tables at once to avoid copying entire array multiple times.
	as_vector tables_to_add;
	as_vector_inita(&tables_to_add, sizeof(as_partition_table*), 16);

	while (*p) {
		if (*p == ':') {
			// Parse namespace.
			*p = 0;
			len = p - ns;
			
			if (len <= 0 || len >= 32) {
				as_log_error("Partition update. Invalid partition namespace %s", ns);
				as_vector_destroy(&tables_to_add);
				return false;
			}
			begin = ++p;

			if (has_regime) {
				// Parse regime.
				while (*p) {
					if (*p == ',') {
						*p = 0;
						break;
					}
					p++;
				}
				regime = (uint32_t)strtoul(begin, NULL, 10);
				begin = ++p;
			}

			// Parse replica count.
			while (*p) {
				if (*p == ',') {
					*p = 0;
					break;
				}
				p++;
			}
			
			int replica_count = atoi(begin);
			
			// Parse master and one prole partition bitmaps.
			for (int i = 0; i < replica_count; i++) {
				begin = ++p;
				
				while (*p) {
					if (*p == ',' || *p == ';') {
						*p = 0;
						break;
					}
					p++;
				}
				int64_t len = p - begin;
				
				if (expected_len != len) {
					as_log_error(
						"Partition update. unexpected partition map encoded length %" PRId64 " for namespace %s",
						len, ns);
					as_vector_destroy(&tables_to_add);
					return false;
				}
				
				// Only handle first two levels.  Do not process other proles.
				// Level 0: master
				// Level 1: prole 1
				if (i < 2) {
					bool master = (i == 0);
					
					if (cluster->shm_info) {
						as_shm_update_partitions(cluster->shm_info, ns, begin, len, node, master,
												 regime);
					}
					else {
						as_partition_table* table = as_partition_tables_get(tables, ns);
						
						if (! table) {
							table = as_partition_vector_get(&tables_to_add, ns);
							
							if (! table) {
								table = as_partition_table_create(ns, cluster->n_partitions,
																  regime != 0);
								as_vector_append(&tables_to_add, &table);
							}
						}
						
						// Decode partition bitmap and update client's view.
						decode_and_update(begin, (uint32_t)len, table, node, master, regime,
										  &regime_error);
					}
				}
			}
			ns = ++p;
		}
		else {
			p++;
		}
	}
	
	if (tables_to_add.size > 0) {
		// Make shallow copy of map and add new tables.
		as_partition_tables_copy_add(cluster, tables, &tables_to_add);
	}
	as_vector_destroy(&tables_to_add);
	return true;
}
