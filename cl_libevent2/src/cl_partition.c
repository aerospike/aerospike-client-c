/*
 * A good, basic C client for the Aerospike protocol
 * Creates a library which is linkable into a variety of systems
 *
 * The cl_partitions section creates a simple in-memory database of where
 * all the partitions in the system can be located.
 *
 * Brian Bulkowski, 2009
 * All rights reserved
 */

#include <stdlib.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_base_types.h"
#include "citrusleaf/cf_log_internal.h"

#include "citrusleaf_event2/cl_cluster.h"
#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"



cl_partition_table*
cl_partition_table_create(ev2citrusleaf_cluster* asc, const char* ns)
{
	int n_partitions = (int)asc->n_partitions;
	size_t size = sizeof(cl_partition_table) +
			(sizeof(cl_partition) * n_partitions);
	cl_partition_table* pt = (cl_partition_table*)malloc(size);

	if (! pt) {
		return NULL;
	}

	memset((void*)pt, 0, size);
	strcpy(pt->ns, ns);

	pt->next = asc->partition_table_head;
	asc->partition_table_head = pt;

	for (int pid = 0; pid < n_partitions; pid++) {
		MUTEX_ALLOC(pt->partitions[pid].lock);
	}

	cf_atomic_int_incr(&g_cl_stats.partition_create);

	return pt;
}


void
cl_partition_table_destroy_all(ev2citrusleaf_cluster* asc)
{
	int n_partitions = (int)asc->n_partitions;
	cl_partition_table* pt = asc->partition_table_head;

	while (pt) {
		for (int pid = 0; pid < n_partitions; pid++) {
			cl_partition* p = &pt->partitions[pid];

			if (p->master) {
				cl_cluster_node_release(p->master, "PM-");
				p->master = NULL;
			}

			if (p->prole) {
				cl_cluster_node_release(p->prole, "PP-");
				p->prole = NULL;
			}

			MUTEX_FREE(p->lock);
		}

		void* next = pt->next;

		free(pt);
		cf_atomic_int_incr(&g_cl_stats.partition_destroy);

		pt = (cl_partition_table*)next;
	}
}


cl_partition_table*
cl_partition_table_get_by_ns(ev2citrusleaf_cluster* asc, const char* ns)
{
	cl_partition_table* pt = asc->partition_table_head;

	while (pt) {
		if (strcmp(ns, pt->ns) == 0) {
			return pt;
		}

		pt = pt->next;
	}

	return NULL;
}


bool
cl_partition_table_is_node_present(cl_cluster_node* node)
{
	ev2citrusleaf_cluster* asc = node->asc;
	int n_partitions = (int)asc->n_partitions;
	cl_partition_table* pt = asc->partition_table_head;

	while (pt) {
		for (int pid = 0; pid < n_partitions; pid++) {
			cl_partition* p = &pt->partitions[pid];

			MUTEX_LOCK(p->lock);

			// Assuming a legitimate node must be master of some partitions,
			// this is all we need to check.
			if (node == p->master) {
				MUTEX_UNLOCK(p->lock);
				return true;
			}

			MUTEX_UNLOCK(p->lock);
		}

		pt = pt->next;
	}

	// The node is master of no partitions - it's effectively gone from the
	// cluster. The node shouldn't be present as prole, but it's possible it's
	// not completely overwritten as prole yet, so just remove it here.

	pt = asc->partition_table_head;

	while (pt) {
		for (int pid = 0; pid < n_partitions; pid++) {
			cl_partition* p = &pt->partitions[pid];

			MUTEX_LOCK(p->lock);

			if (node == p->prole) {
				cl_cluster_node_release(node, "PP-");
				p->prole = NULL;
				pt->was_dumped = false;
			}

			MUTEX_UNLOCK(p->lock);
		}

		pt = pt->next;
	}

	return false;
}


void
cl_partition_table_update(cl_cluster_node* node, const char* ns, bool* masters,
		bool* proles)
{
	ev2citrusleaf_cluster* asc = node->asc;
	cl_partition_table* pt = cl_partition_table_get_by_ns(asc, ns);

	if (! pt) {
		pt = cl_partition_table_create(asc, ns);

		if (! pt) {
			return;
		}
	}

	int n_partitions = (int)asc->n_partitions;

	for (int pid = 0; pid < n_partitions; pid++) {
		cl_partition* p = &pt->partitions[pid];

		MUTEX_LOCK(p->lock);

		// Logic is simpler if we remove this node as master and prole first.
		// (Don't worry, these releases won't cause node destruction.)

		if (node == p->master) {
			cl_cluster_node_release(node, "PM-");
			p->master = NULL;
		}

		if (node == p->prole) {
			cl_cluster_node_release(node, "PP-");
			p->prole = NULL;
		}

		if (masters[pid]) {
			// This node is the new (or still) master for this partition.

			if (p->master) {
				cl_cluster_node_release(p->master, "PM-");
			}

			p->master = node;
			cl_cluster_node_reserve(node, "PM+");
		}
		else if (proles[pid]) {
			// This node is the new (or still) prole for this partition.

			if (p->prole) {
				cl_cluster_node_release(p->prole, "PP-");
			}

			p->prole = node;
			cl_cluster_node_reserve(node, "PP+");
		}

		MUTEX_UNLOCK(p->lock);
	}

	// Just assume something changed...
	pt->was_dumped = false;
}


static cf_atomic32 g_randomizer = 0;

cl_cluster_node*
cl_partition_table_get(ev2citrusleaf_cluster* asc, const char* ns,
		cl_partition_id pid, bool write)
{
	cl_partition_table* pt = cl_partition_table_get_by_ns(asc, ns);

	if (! pt) {
		return NULL;
	}

	cl_cluster_node* node;
	cl_partition* p = &pt->partitions[pid];

	MUTEX_LOCK(p->lock);

	if (write || asc->options.read_master_only || ! p->prole) {
		node = p->master;
	}
	else if (! p->master) {
		node = p->prole;
	}
	else {
		uint32_t master_throttle = cf_atomic32_get(p->master->throttle_pct);
		uint32_t prole_throttle = cf_atomic32_get(p->prole->throttle_pct);

		if (master_throttle == 0 && prole_throttle != 0) {
			node = p->master;
		}
		else if (prole_throttle == 0 && master_throttle != 0) {
			node = p->prole;
		}
		else {
			// Both throttling or both ok - roll the dice.
			uint32_t r = (uint32_t)cf_atomic32_incr(&g_randomizer);

			node = (r & 1) ? p->master : p->prole;
		}
	}

	if (node) {
		cl_cluster_node_reserve(node, "T+");
	}

	MUTEX_UNLOCK(p->lock);

	return node;
}


static inline const char*
safe_node_name(cl_cluster_node* node)
{
	return node ? (const char*)node->name : "";
}

void
cl_partition_table_dump(ev2citrusleaf_cluster* asc)
{
	if (! cf_debug_enabled()) {
		return;
	}

	cl_partition_table* pt = asc->partition_table_head;

	if (pt && pt->was_dumped) {
		return;
	}

	while (pt) {
		cf_debug("--- CLUSTER MAP for %s ---", pt->ns);

		for (int pid = 0; pid < asc->n_partitions; pid++) {
			cl_partition* p = &pt->partitions[pid];

			MUTEX_LOCK(p->lock);

			cf_debug("%4d: %s %s", pid, safe_node_name(p->master),
					safe_node_name(p->prole));

			MUTEX_UNLOCK(p->lock);
		}

		pt->was_dumped = true;
		pt = pt->next;
	}
}
