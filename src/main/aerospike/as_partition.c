/******************************************************************************
 * Copyright 2008-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/
#include <aerospike/as_partition.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_string.h>
#include "citrusleaf/cf_log_internal.h"
#include "ck_pr.h"

/******************************************************************************
 *	Functions
 *****************************************************************************/

static inline void
set_partition_tables(as_cluster* cluster, as_partition_tables* tables)
{
	// Volatile write used so other threads can see node changes.
	ck_pr_fence_store();
	ck_pr_store_ptr(&cluster->partition_tables, tables);
}

static inline void
set_node(as_node** trg, as_node* src)
{
	ck_pr_fence_store();
	ck_pr_store_ptr(trg, src);
}

static as_partition_table*
as_partition_table_create(const char* ns, uint32_t capacity)
{
	size_t len = sizeof(as_partition_table) + (sizeof(as_partition) * capacity);
	as_partition_table* table = cf_malloc(len);
	memset(table, 0, len);
	as_strncpy(table->ns, ns, AS_MAX_NAMESPACE_SIZE);
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
reserve_node(as_cluster* cluster, as_node* node)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (node && ck_pr_load_8(&node->active)) {
		as_node_reserve(node);
		return node;
	}
#ifdef DEBUG_VERBOSE
	cf_debug("Choose random node for unmapped namespace/partition");
#endif
	return as_node_get_random(cluster);
}

static as_node*
reserve_node_alternate(as_cluster* cluster, as_node* chosen, as_node* alternate)
{
	// Make volatile reference so changes to tend thread will be reflected in this thread.
	if (ck_pr_load_8(&chosen->active)) {
		as_node_reserve(chosen);
		return chosen;
	}
	return reserve_node(cluster, alternate);
}

static uint32_t g_randomizer = 0;

as_node*
as_partition_table_get_node(as_cluster* cluster, as_partition_table* table, const cf_digest* d, bool write)
{
	if (table) {
		cl_partition_id partition_id = cl_partition_getid(cluster->n_partitions, d);
		as_partition* p = &table->partitions[partition_id];
		
		// Make volatile reference so changes to tend thread will be reflected in this thread.
		as_node* master = ck_pr_load_ptr(&p->master);
		
		if (write) {
			// Writes always go to master.
			return reserve_node(cluster, master);
		}
		
		as_node* prole = ck_pr_load_ptr(&p->prole);
			
		if (! prole) {
			return reserve_node(cluster, master);
		}
		
		if (! master) {
			return reserve_node(cluster, prole);
		}

		// Alternate between master and prole for reads.
		uint32_t r = ck_pr_faa_32(&g_randomizer, 1);
				
		if (r & 1) {
			return reserve_node_alternate(cluster, master, prole);
		}
		return reserve_node_alternate(cluster, prole, master);
	}
	
#ifdef DEBUG_VERBOSE
	cf_debug("Choose random node for null partition table");
#endif
	return as_node_get_random(cluster);
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

// TODO - MOVE B64 CODE TO COMMON.  ANDY PROMISED.
const uint8_t CF_BASE64_DECODE_ARRAY[] = {
	/*00*/ /*01*/ /*02*/ /*03*/ /*04*/ /*05*/ /*06*/ /*07*/   /*08*/ /*09*/ /*0A*/ /*0B*/ /*0C*/ /*0D*/ /*0E*/ /*0F*/
	/*00*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*10*/      0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*20*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,    62,     0,     0,     0,    63,
	/*30*/	   52,    53,    54,    55,    56,    57,    58,    59,      60,    61,     0,     0,     0,     0,     0,     0,
	/*40*/	    0,     0,     1,     2,     3,     4,     5,     6,       7,     8,     9,    10,    11,    12,    13,    14,
	/*50*/	   15,    16,    17,    18,    19,    20,    21,    22,      23,    24,    25,     0,     0,     0,     0,     0,
	/*60*/	    0,    26,    27,    28,    29,    30,    31,    32,      33,    34,    35,    36,    37,    38,    39,    40,
	/*70*/	   41,    42,    43,    44,    45,    46,    47,    48,      49,    50,    51,     0,     0,     0,     0,     0,
	/*80*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*90*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*A0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*B0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*C0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*D0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*E0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0,
	/*F0*/	    0,     0,     0,     0,     0,     0,     0,     0,       0,     0,     0,     0,     0,     0,     0,     0
};

#define B64DA CF_BASE64_DECODE_ARRAY

static void
b64_decode(const uint8_t* in, int len, uint8_t* out)
{
	int i = 0;
	int j = 0;
	
	while (i < len) {
		out[j + 0] = (B64DA[in[i + 0]] << 2) | (B64DA[in[i + 1]] >> 4);
		out[j + 1] = (B64DA[in[i + 1]] << 4) | (B64DA[in[i + 2]] >> 2);
		out[j + 2] = (B64DA[in[i + 2]] << 6) |  B64DA[in[i + 3]];
		
		i += 4;
		j += 3;
	}
}

static inline void
force_replicas_refresh(as_node* node)
{
	node->partition_generation = (uint32_t)-1;
}

static void
as_partition_update(as_partition* p, as_node* node, bool master, bool owns)
{
	// Volatile reads are not necessary because the tend thread exclusively modifies partition.
	// Volatile writes are used so other threads can view change.
	if (master) {
		if (node == p->master) {
			if (! owns) {
				set_node(&p->master, 0);
				as_node_release(node);
			}
		}
		else {
			if (owns) {
				as_node* tmp = p->master;
				as_node_reserve(node);
				set_node(&p->master, node);
				
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
				set_node(&p->prole, 0);
				as_node_release(node);
			}
		}
		else {
			if (owns) {
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
	uint8_t* bitmap;
	long len;
	
	// Add all tables at once to avoid copying entire array multiple times.
	as_vector tables_to_add;
	as_vector_inita(&tables_to_add, sizeof(as_partition_table*), 16);

	while (*p) {
		if (*p == ':') {
			// Parse namespace.
			*p = 0;
			len = p - ns;
			
			if (len <= 0 || len >= 32) {
				cf_error("Partition update. Invalid partition namespace %s", ns);
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
			
			if (p == bitmap_b64) {
				cf_error("Partition update. Empty partition id for namespace %s", ns);
				as_vector_destroy(&tables_to_add);
				return false;
			}
			
			len = p - bitmap_b64;
			
			as_partition_table* table = as_partition_tables_get(tables, ns);
			
			if (! table) {
				table = as_partition_vector_get(&tables_to_add, ns);
				
				if (! table) {
					table = as_partition_table_create(ns, cluster->n_partitions);
					as_vector_append(&tables_to_add, &table);
				}
			}
			
			// Decode partition bitmap.
			// Size allows for padding - is actual size rounded up to multiple of 3.
			bitmap = (uint8_t*)alloca((len / 4) * 3);
			b64_decode((const uint8_t*)bitmap_b64, (int)len, bitmap);
			
			// Expand the bitmap.
			for (uint32_t i = 0; i < table->size; i++) {
				bool owns = ((bitmap[i >> 3] & (0x80 >> (i & 7))) != 0);
				/*
				if (owns) {
					cf_debug("Set partition %s:%s:%u:%s", master? "master" : "prole", table->ns, i, node->name);
				}
				*/
				as_partition_update(&table->partitions[i], node, master, owns);
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
