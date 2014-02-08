/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_log_internal.h"
#include "citrusleaf/cl_cluster.h"

#include "citrusleaf/cl_partition.h"


cl_partition_table*
cl_partition_table_create(cl_cluster* asc, const char* ns)
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
		pthread_mutex_init(&pt->partitions[pid].lock, 0);
	}

	return pt;
}


void
cl_partition_table_destroy_all(cl_cluster* asc)
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

			pthread_mutex_destroy(&p->lock);
		}

		cl_partition_table* next = pt->next;

		free(pt);
		pt = next;
	}
}


cl_partition_table*
cl_partition_table_get_by_ns(cl_cluster* asc, const char* ns)
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
cl_partition_table_is_node_present(cl_cluster* asc, cl_cluster_node* node)
{
	int n_partitions = (int)asc->n_partitions;
	cl_partition_table* pt = asc->partition_table_head;

	while (pt) {
		for (int pid = 0; pid < n_partitions; pid++) {
			cl_partition* p = &pt->partitions[pid];

			pthread_mutex_lock(&p->lock);

			// Assuming a legitimate node must be master of some partitions,
			// this is all we need to check.
			if (node == p->master) {
				pthread_mutex_unlock(&p->lock);
				return true;
			}

			pthread_mutex_unlock(&p->lock);
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

			pthread_mutex_lock(&p->lock);

			if (node == p->prole) {
				cl_cluster_node_release(node, "PP-");
				p->prole = NULL;
			}

			pthread_mutex_unlock(&p->lock);
		}

		pt = pt->next;
	}

	return false;
}


static inline void
force_replicas_refresh(cl_cluster_node* node)
{
	node->partition_generation = (uint32_t)-1;
}

void
cl_partition_table_update(cl_cluster* asc, cl_cluster_node* node,
		const char* ns, bool* masters, bool* proles)
{
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

		pthread_mutex_lock(&p->lock);

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
				// Replacing another master.
				force_replicas_refresh(p->master);
				cl_cluster_node_release(p->master, "PM-");
			}

			p->master = node;
			cl_cluster_node_reserve(node, "PM+");
		}
		else if (proles[pid]) {
			// This node is the new (or still) prole for this partition.

			if (p->prole) {
				// Replacing another prole.
				force_replicas_refresh(p->prole);
				cl_cluster_node_release(p->prole, "PP-");
			}

			p->prole = node;
			cl_cluster_node_reserve(node, "PP+");
		}

		pthread_mutex_unlock(&p->lock);
	}
}


static cf_atomic32 g_randomizer = 0;

cl_cluster_node*
cl_partition_table_get(cl_cluster* asc, const char* ns, cl_partition_id pid,
		bool write)
{
	cl_partition_table* pt = cl_partition_table_get_by_ns(asc, ns);

	if (! pt) {
		return NULL;
	}

	cl_cluster_node* node;
	cl_partition* p = &pt->partitions[pid];

	pthread_mutex_lock(&p->lock);

	if (write || ! p->prole) {
		node = p->master;
	}
	else if (! p->master) {
		node = p->prole;
	}
	else {
		// Roll the dice.
		uint32_t r = (uint32_t)cf_atomic32_incr(&g_randomizer);

		node = (r & 1) ? p->master : p->prole;
	}

	if (node) {
		cl_cluster_node_reserve(node, "T+");
	}

	pthread_mutex_unlock(&p->lock);

	return node;
}
