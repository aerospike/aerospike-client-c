/*
 * Copyright 2008-2018 Aerospike, Inc.
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
as_partition_table_create(const char* ns, uint32_t capacity, bool cp_mode)
{
	size_t len = sizeof(as_partition_table) + (sizeof(as_partition) * capacity);
	as_partition_table* table = cf_malloc(len);
	memset(table, 0, len);
	as_strncpy(table->ns, ns, AS_MAX_NAMESPACE_SIZE);
	table->cp_mode = cp_mode;
	table->size = capacity;
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

static inline as_node*
reserve_master(as_cluster* cluster, as_node* node)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (node && as_load_uint8(&node->active)) {
		as_node_reserve(node);
		return node;
	}
	// When master only specified, should never get random nodes.
	return NULL;
}

static inline as_node*
reserve_node(as_cluster* cluster, as_node* node, bool cp_mode)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (node && as_load_uint8(&node->active)) {
		as_node_reserve(node);
		return node;
	}
	return cp_mode ? NULL : as_node_get_random(cluster);
}

static as_node*
reserve_node_alternate(as_cluster* cluster, as_node* chosen, as_node* alternate, bool cp_mode)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (as_load_uint8(&chosen->active)) {
		as_node_reserve(chosen);
		return chosen;
	}
	return reserve_node(cluster, alternate, cp_mode);
}

static uint32_t g_randomizer = 0;

as_node*
as_partition_get_node(as_cluster* cluster, as_partition* p, as_policy_replica replica, bool use_master, bool cp_mode)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	as_node* master = (as_node*)as_load_ptr(&p->master);

	if (replica == AS_POLICY_REPLICA_MASTER) {
		return reserve_master(cluster, master);
	}

	as_node* prole = (as_node*)as_load_ptr(&p->prole);

	if (! prole) {
		return reserve_node(cluster, master, cp_mode);
	}

	if (! master) {
		return reserve_node(cluster, prole, cp_mode);
	}

	if (replica == AS_POLICY_REPLICA_ANY) {
		// Alternate between master and prole for reads with global iterator.
		uint32_t r = as_faa_uint32(&g_randomizer, 1);
		use_master = (r & 1);
	}

	// AS_POLICY_REPLICA_SEQUENCE uses the use_master preference without modification.
	if (use_master) {
		return reserve_node_alternate(cluster, master, prole, cp_mode);
	}
	return reserve_node_alternate(cluster, prole, master, cp_mode);
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

static void
as_partition_update(as_partition* p, as_node* node, bool master, bool owns, uint32_t regime)
{
	// Volatile reads are not necessary because the tend thread exclusively modifies partition.
	// Volatile writes are used so other threads can view change.
	if (master) {
		if (node == p->master) {
			if (! owns) {
				set_node(&p->master, NULL);
				as_node_release(node);
			}
		}
		else {
			if (owns && regime >= p->regime) {
				as_node* tmp = p->master;
				as_node_reserve(node);
				set_node(&p->master, node);

				if (regime > p->regime) {
					p->regime = regime;
				}

				if (tmp) {
					force_replicas_refresh(tmp);
					as_node_release(tmp);
				}
			}
		}
	}
	else {
		if (node == p->prole) {
			if (! owns) {
				set_node(&p->prole, NULL);
				as_node_release(node);
			}
		}
		else {
			if (owns && regime >= p->regime) {
				as_node* tmp = p->prole;
				as_node_reserve(node);
				set_node(&p->prole, node);

				if (regime > p->regime) {
					p->regime = regime;
				}

				if (tmp) {
					force_replicas_refresh(tmp);
					as_node_release(tmp);
				}
			}
		}
	}
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
decode_and_update(char* bitmap_b64, uint32_t len, as_partition_table* table, as_node* node, bool master, uint32_t regime)
{
	// Size allows for padding - is actual size rounded up to multiple of 3.
	uint8_t* bitmap = (uint8_t*)alloca(cf_b64_decoded_buf_size(len));

	// For now - for speed - trust validity of encoded characters.
	cf_b64_decode(bitmap_b64, len, bitmap, NULL);

	// Expand the bitmap.
	for (uint32_t i = 0; i < table->size; i++) {
		bool owns = ((bitmap[i >> 3] & (0x80 >> (i & 7))) != 0);
		/*
		if (owns) {
			as_log_debug("Set partition %s:%s:%u:%s", master? "master" : "prole", table->ns, i, node->name);
		}
		*/
		as_partition_update(&table->partitions[i], node, master, owns, regime);
	}
}

static void
release_partition_tables(as_partition_tables* tables)
{
	as_partition_tables_release(tables);
}

static void
as_partition_tables_copy_add(as_cluster* cluster, as_partition_tables* tables_old, as_vector* /* <as_partition_table*> */ tables_to_add)
{
	// Create new tables array.
	as_partition_tables* tables_new = as_partition_tables_create(tables_old->size + tables_to_add->size);
	
	// Add existing tables.
	memcpy(tables_new->array, tables_old->array, sizeof(as_partition_table*) * tables_old->size);
	
	// Add new tables.
	memcpy(&tables_new->array[tables_old->size], tables_to_add->list, sizeof(as_partition_table*) * tables_to_add->size);
	
	// Replace tables with copy.
	set_partition_tables(cluster, tables_new);
	
	// Put old tables on garbage collector stack.
	as_gc_item item;
	item.data = tables_old;
	item.release_fn = (as_release_fn)release_partition_tables;
	as_vector_append(cluster->gc, &item);
}

bool
as_partition_tables_update(as_cluster* cluster, as_node* node, char* buf, bool master)
{
	// Use destructive parsing (ie modifying input buffer with null termination) for performance.
	as_partition_tables* tables = cluster->partition_tables;
	char* p = buf;
	char* ns = p;
	char* bitmap_b64 = 0;
	int64_t len;
	
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
			bitmap_b64 = ++p;
			
			// Parse partition bitmap.
			while (*p) {
				if (*p == ';' || *p == '\n') {
					*p = 0;
					break;
				}
				p++;
			}

			len = p - bitmap_b64;

			// Check encoded length.
			uint32_t bitmap_size = (cluster->n_partitions + 7) / 8;
			long expected_len = (long)cf_b64_encoded_len(bitmap_size);

			if (expected_len != len) {
				as_log_error("Partition update. unexpected partition map encoded length %" PRId64 " for namespace %s", len, ns);
				as_vector_destroy(&tables_to_add);
				return false;
			}

			if (cluster->shm_info) {
				as_shm_update_partitions(cluster->shm_info, ns, bitmap_b64, len, node, master, 0);
			}
			else {
				as_partition_table* table = as_partition_tables_get(tables, ns);
				
				if (! table) {
					table = as_partition_vector_get(&tables_to_add, ns);
					
					if (! table) {
						table = as_partition_table_create(ns, cluster->n_partitions, false);
						as_vector_append(&tables_to_add, &table);
					}
				}

				// Decode partition bitmap and update client's view.
				decode_and_update(bitmap_b64, (uint32_t)len, table, node, master, 0);
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

bool
as_partition_tables_update_all(as_cluster* cluster, as_node* node, char* buf, bool has_regime)
{
	// Use destructive parsing (ie modifying input buffer with null termination) for performance.
	// Receive format: replicas-all\t or replicas\t
	//                 <ns1>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
	//                 <ns2>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
	as_partition_tables* tables = cluster->partition_tables;
	uint32_t bitmap_size = (cluster->n_partitions + 7) / 8;
	long expected_len = (long)cf_b64_encoded_len(bitmap_size);

	char* p = buf;
	char* ns = p;
	char* begin = 0;
	int64_t len;
	uint32_t regime = 0;

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
					as_log_error("Partition update. unexpected partition map encoded length %" PRId64 " for namespace %s", len, ns);
					as_vector_destroy(&tables_to_add);
					return false;
				}
				
				// Only handle first two levels.  Do not process other proles.
				// Level 0: master
				// Level 1: prole 1
				if (i < 2) {
					bool master = (i == 0);
					
					if (cluster->shm_info) {
						as_shm_update_partitions(cluster->shm_info, ns, begin, len, node, master, regime);
					}
					else {
						as_partition_table* table = as_partition_tables_get(tables, ns);
						
						if (! table) {
							table = as_partition_vector_get(&tables_to_add, ns);
							
							if (! table) {
								table = as_partition_table_create(ns, cluster->n_partitions, regime != 0);
								as_vector_append(&tables_to_add, &table);
							}
						}
						
						// Decode partition bitmap and update client's view.
						decode_and_update(begin, (uint32_t)len, table, node, master, regime);
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
