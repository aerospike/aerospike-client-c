/*
 * Copyright 2008-2022 Aerospike, Inc.
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
		as_partition_table* table = tables->tables[i];
		
		printf("Namespace: %s\n", table->ns);
		as_partition_table_print(table);
	}
}
*/

static void
as_partition_reserve_node(as_node* node)
{
	// Use a separate reference count for nodes in partition maps.
	// Only call as_node_reserve() the first time a node is added to
	// the partition maps.
	//
	// partition_ref_count is only referenced in the tend thread
	// (except for cluster destroy), so the increment is non-atomic.
	if (node->partition_ref_count++ == 0) {
		as_node_reserve(node);
	}
}

static void
as_partition_release_node_delayed(as_node* node)
{
	// Only call as_node_release_delayed() when there are no more
	// references in the partition maps.
	//
	// partition_ref_count is only referenced in the tend thread
	// (except for cluster destroy), so the decrement is non-atomic.
	if (--node->partition_ref_count == 0) {
		// Delay node release to avoid race condition.
		// See as_node_release_delayed() comments.
		as_node_release_delayed(node);
	}
}

static void
as_partition_release_node_now(as_node* node)
{
	// Only call as_node_release() when there are no more references
	// in the partition maps.
	//
	// The only call stack to this function is:
	// as_cluster_destroy() -> as_partition_tables_destroy() ->
	// as_partition_table_destroy() -> as_partition_release_node_now().
	//
	// as_cluster_destroy() is called from outside the tend thread,
	// but it does stop the tend thread and wait for tending to
	// finish before calling this method.  Therefore, it should
	// be okay to use non-atomic decrement of partition_ref_count.
	//
	if (--node->partition_ref_count == 0) {
		as_node_release(node);
	}
}

static as_partition_table*
as_partition_table_create(const char* ns, uint32_t capacity, bool sc_mode)
{
	size_t len = sizeof(as_partition_table) + (sizeof(as_partition) * capacity);
	as_partition_table* table = cf_malloc(len);
	memset(table, 0, len);
	as_strncpy(table->ns, ns, AS_MAX_NAMESPACE_SIZE);
	table->size = capacity;
	table->sc_mode = sc_mode;
	return table;
}

static void
as_partition_table_destroy(as_partition_table* table)
{
	for (uint32_t i = 0; i < table->size; i++) {
		as_partition* p = &table->partitions[i];
		
		if (p->master) {
			as_partition_release_node_now(p->master);
		}
		
		if (p->prole) {
			as_partition_release_node_now(p->prole);
		}
	}
	cf_free(table);
}

void
as_partition_tables_destroy(as_partition_tables* tables)
{
	uint32_t max = as_load_uint32(&tables->size);

	for (uint32_t i = 0; i < max; i++) {
		as_partition_table_destroy(tables->tables[i]);
	}
}

static inline as_node*
try_master(as_cluster* cluster, as_node* node)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (node && as_node_is_active(node)) {
		return node;
	}
	// When master only specified, should never get random nodes.
	return NULL;
}

static inline as_node*
try_node(as_cluster* cluster, as_node* node)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (node && as_node_is_active(node)) {
		return node;
	}
	return NULL;
}

static as_node*
try_node_alternate(as_cluster* cluster, as_node* chosen, as_node* alternate)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (as_node_is_active(chosen)) {
		return chosen;
	}
	return try_node(cluster, alternate);
}

static as_node*
get_sequence_node(as_cluster* cluster, as_partition* p, bool use_master)
{
	as_node* master = as_node_load(&p->master);
	as_node* prole = as_node_load(&p->prole);

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

static as_node*
prefer_rack_node(
	as_cluster* cluster, const char* ns, as_partition* p, as_node* prev_node, bool use_master
	)
{
	as_node* nodes[2];

	if (use_master) {
		nodes[0] = as_node_load(&p->master);
		nodes[1] = as_node_load(&p->prole);
	}
	else {
		nodes[0] = as_node_load(&p->prole);
		nodes[1] = as_node_load(&p->master);
	}

	as_node* fallback1 = NULL;
	as_node* fallback2 = NULL;
	uint32_t max = cluster->rack_ids_size;

	for (uint32_t i = 0; i < max; i++) {
		int rack_id = cluster->rack_ids[i];

		for (uint32_t j = 0; j < 2; j++) {
			as_node* node = nodes[j];

			if (node) {
				// Avoid retrying on node where command failed even if node is the
				// only one on the same rack. The contents of prev_node may have
				// already been destroyed, so just use pointer comparison and never
				// examine the contents of prev_node!
				if (node != prev_node) {
					if (as_node_has_rack(node, ns, rack_id)) {
						if (as_node_is_active(node)) {
							return node;
						}
					}
					else if (!fallback1 && as_node_is_active(node)) {
						// Meets all criteria except not on same rack.
						fallback1 = node;
					}
				}
				else if (!fallback2 && as_node_is_active(node)) {
					// Previous node is the least desirable fallback.
					fallback2 = node;
				}
			}
		}
	}

	// Return node on a different rack if it exists.
	if (fallback1) {
		return fallback1;
	}

	// Return previous node if it still exists.
	if (fallback2) {
		return fallback2;
	}
	return NULL;
}

as_node*
as_partition_reg_get_node(
	as_cluster* cluster, const char* ns, as_partition* p, as_node* prev_node,
	as_policy_replica replica, bool use_master
	)
{
	switch (replica) {
		case AS_POLICY_REPLICA_MASTER: {
			// Make volatile reference so changes to tend thread will be reflected in this thread.
			as_node* master = as_node_load(&p->master);
			return try_master(cluster, master);
		}

		default:
		case AS_POLICY_REPLICA_ANY:
		case AS_POLICY_REPLICA_SEQUENCE: {
			return get_sequence_node(cluster, p, use_master);
		}

		case AS_POLICY_REPLICA_PREFER_RACK: {
			return prefer_rack_node(cluster, ns, p, prev_node, use_master);
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
		as_partition_table* table = as_partition_tables_get(&cluster->partition_tables, key->ns);

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
		pi->partition_id = as_partition_getid(key->digest.value, cluster->n_partitions);
		pi->partition = &table->partitions[pi->partition_id];
		pi->sc_mode = table->sc_mode;
	}
	return AEROSPIKE_OK;
}

as_partition_table*
as_partition_tables_get(as_partition_tables* tables, const char* ns)
{
	uint32_t max = as_load_uint32_acq(&tables->size);
	
	for (uint32_t i = 0; i < max; i++) {
		as_partition_table* table = tables->tables[i];

		if (strcmp(table->ns, ns) == 0) {
			return table;
		}
	}
	return NULL;
}

static inline void
force_replicas_refresh(as_node* node)
{
	node->partition_generation = (uint32_t)-1;
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
						as_partition_reserve_node(node);
						as_node_store(&p->master, node);

						if (tmp) {
							force_replicas_refresh(tmp);
							as_partition_release_node_delayed(tmp);
						}
					}
				}
				else {
					if (node != p->prole) {
						as_node* tmp = p->prole;
						as_partition_reserve_node(node);
						as_node_store(&p->prole, node);

						if (tmp) {
							force_replicas_refresh(tmp);
							as_partition_release_node_delayed(tmp);
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

bool
as_partition_tables_update_all(as_cluster* cluster, as_node* node, char* buf, bool has_regime)
{
	// Use destructive parsing (ie modifying input buffer with null termination) for performance.
	// Receive format: replicas-all\t or replicas\t
	//              <ns1>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;
	//              <ns2>:[regime],<count>,<base 64 encoded bitmap1>,<base 64 encoded bitmap2>...;\n
	as_partition_tables* tables = &cluster->partition_tables;
	uint32_t bitmap_size = (cluster->n_partitions + 7) / 8;
	long expected_len = (long)cf_b64_encoded_len(bitmap_size);

	char* p = buf;
	char* ns = p;
	char* begin = 0;
	int64_t len;
	uint32_t regime = 0;
	bool regime_error = false;

	while (*p) {
		if (*p == ':') {
			// Parse namespace.
			*p = 0;
			len = p - ns;
			
			if (len <= 0 || len >= 32) {
				as_log_error("Partition update. Invalid partition namespace %s", ns);
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
						bool create = !table;

						if (create) {
							if (tables->size >= AS_MAX_NAMESPACES) {
								as_log_error("Partition update. Max namespaces exceeded %u",
											 AS_MAX_NAMESPACES);
								return false;
							}

							table = as_partition_table_create(ns, cluster->n_partitions,
															  regime != 0);
						}
						
						// Decode partition bitmap and update client's view.
						decode_and_update(begin, (uint32_t)len, table, node, master, regime,
										  &regime_error);

						if (create) {
							tables->tables[tables->size] = table;
							as_store_uint32_rls(&tables->size, tables->size + 1);
						}
					}
				}
			}
			ns = ++p;
		}
		else {
			p++;
		}
	}
	return true;
}

void
as_partition_tables_dump(as_cluster* cluster)
{
	as_partition_tables* tables = &cluster->partition_tables;

	for (uint32_t i = 0; i < tables->size; i++) {
		as_partition_table* pt = tables->tables[i];

		as_log_info("Partitions %s,%s", pt->ns, pt->sc_mode? "true" : "false");

		for (uint32_t j = 0; j < pt->size; j++) {
			as_partition* p = &pt->partitions[j];
			as_node* master = as_node_load(&p->master);
			as_node* prole = as_node_load(&p->prole);
			const char* mstr = master ? as_node_get_address_string(master) : "null";
			const char* pstr = prole ? as_node_get_address_string(prole) : "null";

			as_log_info("%s[%u] %u,%s,%s", pt->ns, j, p->regime, mstr, pstr);
		}
	}
}
