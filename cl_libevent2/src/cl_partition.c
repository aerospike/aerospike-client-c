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
#include "citrusleaf/cf_log_internal.h"

#include "citrusleaf_event2/cl_cluster.h"
#include "citrusleaf_event2/ev2citrusleaf.h"
#include "citrusleaf_event2/ev2citrusleaf-internal.h"


#define EXTRA_CHECKS 1

//
// When a node has been dunned, remove it from all partition tables.
// Better to have nothing than have a dunned node in the tables.
//


void
cl_partition_table_remove_node( ev2citrusleaf_cluster *asc, cl_cluster_node *node )
{
	cl_partition_table *pt = asc->partition_table_head;
	while(pt) {
		
		for (int i=0 ; i<asc->n_partitions ; i++) {
			
			cl_partition *p = &pt->partitions[i];
			
			MUTEX_LOCK(p->lock);

			if (p->write == node) {
				cl_cluster_node_release(node, "PW-");	
				p->write = 0;
			}
			
			for (int j=0;j<p->n_read;j++) {
				if (p->read[j] == node) {
					cl_cluster_node_release(node, "PR-");	

					if (j < MAX_REPLICA_COUNT-1)
						// overlapping copies must use memmove!
						memmove(&p->read[j], &p->read[j+1], 
							((MAX_REPLICA_COUNT - 1) - j) * sizeof(cl_cluster_node *) );
					p->n_read--;
					break;
				}
			}

			MUTEX_UNLOCK(p->lock);

		}
		
		pt = pt->next;
	}
	return;
}

cl_partition_table *
cl_partition_table_create(ev2citrusleaf_cluster *asc, const char *ns)
{
	cf_atomic_int_incr(&g_cl_stats.partition_create);

	cl_partition_table *pt = (cl_partition_table*)malloc( sizeof(cl_partition_table) + (sizeof(cl_partition) * asc->n_partitions) );
	if (!pt)	return(0);
	memset(pt, 0, sizeof(cl_partition_table) + (sizeof(cl_partition) * asc->n_partitions) );
	strcpy( pt->ns, ns );
	
	pt->next = asc->partition_table_head;
	asc->partition_table_head = pt;
	
	// initialize mutexes, can be used for other element fields that don't init to 0
	for (int i=0; i < asc->n_partitions ; i++) {
		MUTEX_ALLOC(pt->partitions[i].lock);
	}

	return(pt);
}

// when can we figure out that a namespace is no longer in a cluster?
// it would have to be a mark-and-sweep kind of thing, where we look and see
// there are no nodes anywhere

void
cl_partition_table_destroy(ev2citrusleaf_cluster *asc, cl_partition_table *pt)
{
	cf_atomic_int_incr(&g_cl_stats.partition_destroy);
	
	cl_partition_table **prev = &asc->partition_table_head;
	cl_partition_table *now = asc->partition_table_head;
	while ( now ) {
		if (now == pt) {
			*prev = pt->next;
			break;
		}
	}
#ifdef EXTRA_CHECKS	
	if (now == 0) {
		cf_warn("warning! passed in partition table %p not in list", pt);
		return;
	}
#endif

	// free mutexes, release reference counts
	// do not have to worry about locking the table mutex
	for (int i=0; i < asc->n_partitions ; i++) {
		cl_partition *p = &pt->partitions[i];
		if (p->write) {
			cl_cluster_node_release(p->write, "PW-");	
			p->write = 0;
		}
		for (int j=0;j<p->n_read;j++) {
			if (p->read[j]) {
				cl_cluster_node_release(p->read[j], "PR-");
				p->read[j] = 0;
			}
		}
		MUTEX_FREE(pt->partitions[i].lock);
	}

	free(pt);
}

void
cl_partition_table_destroy_all(ev2citrusleaf_cluster *asc)
{
	cl_partition_table *now = asc->partition_table_head;
	while (now ) {
		cf_atomic_int_incr(&g_cl_stats.partition_destroy);
		void *t = now->next;
		for (int i=0; i < asc->n_partitions ; i++) {
			cl_partition *p = &now->partitions[i];
			if (p->write) {
				cl_cluster_node_release(p->write, "PW-");	
				p->write = 0;
			}
			for (int j=0;j<p->n_read;j++) {
				if (p->read[j]) {
					cl_cluster_node_release(p->read[j], "PR-");
					p->read[j] = 0;
				}
			}
			MUTEX_FREE(p->lock);
		}
		free( now );
		now = t;
	}
	return;
}

cl_partition_table *
cl_partition_table_get_byns(ev2citrusleaf_cluster *asc, const char *ns)
{
	cl_partition_table *pt = asc->partition_table_head;
	while (pt) {
		if (strcmp(ns, pt->ns)==0) return(pt);
		pt = pt->next;
	}
	return(0);
}


void
cl_partition_table_set( ev2citrusleaf_cluster *asc, cl_cluster_node *node, const char *ns, cl_partition_id pid, bool write)
{
	cl_partition_table *pt = cl_partition_table_get_byns(asc, ns);
	if (!pt) {
		pt = cl_partition_table_create(asc, ns);
		if (!pt)	return; // could not add, quite the shame
	}
	
#ifdef EXTRA_CHECKS
	if (pid > asc->n_partitions) {
		cf_warn("internal error: partition table set got out of range partition id %d", pid);
		return;
	}
#endif	

	cl_partition *p = &pt->partitions[pid];
	
	MUTEX_LOCK(p->lock);

	if (write) {
		if (p->write)  cl_cluster_node_release(p->write, "PW-");
		p->write = node;
		if (node)  cl_cluster_node_reserve(node, "PW+");
	}
	else {
		for (int i=0;i < p->n_read ; i++) {
			if (p->read[i] == node) {
				MUTEX_UNLOCK(pt->partitions[pid].lock);
				return; // already in!
			}
		}
		if (MAX_REPLICA_COUNT == p->n_read) { // full, replace 0 for fun
//			cf_warn("read replica set full");
			if (p->read[0]) cl_cluster_node_release(p->read[0], "PR-");
			p->read[0] = node;
			if (node)  cl_cluster_node_reserve(node, "PR+");		
		}
		else {
			p->read[p->n_read] = node;
			if (node)  cl_cluster_node_reserve(node, "PR+");		
			p->n_read++;
		}
	}
	MUTEX_UNLOCK(pt->partitions[pid].lock);
		
	return;
}

static cf_atomic_int round_robin_counter = 0;

cl_cluster_node *
cl_partition_table_get( ev2citrusleaf_cluster *asc, const char *ns, cl_partition_id pid, bool write)
{
	cl_partition_table *pt = cl_partition_table_get_byns(asc,ns);
	if (!pt)	return(0);
	
	cl_cluster_node *node;

	MUTEX_LOCK(pt->partitions[pid].lock);

	if (write || asc->options.read_master_only) {
		node = pt->partitions[pid].write;
	}
	else {
		cl_partition *p = &pt->partitions[pid];
		if (p->n_read) {
			cf_atomic_int_incr(&round_robin_counter);
			uint32_t my_rr = cf_atomic_int_get(round_robin_counter);
			node = p->read[ my_rr % p->n_read ];
		} else {
			node = 0;
		}
	}
	if (node) cl_cluster_node_reserve(node, "T+");

	MUTEX_UNLOCK(pt->partitions[pid].lock);

	return(node);
}


