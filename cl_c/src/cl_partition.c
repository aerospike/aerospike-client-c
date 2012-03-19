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

#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h> // inet_ntop

#include <netdb.h> //gethostbyname_r

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"

// #define DEBUG 1
// #define DEBUG_VERBOSE 1
#define EXTRA_CHECKS 1

//
// When a node has been dunned, remove it from all partition tables.
// Better to have nothing than have a dunned node in the tables.
//


void
cl_partition_table_remove_node( cl_cluster *asc, cl_cluster_node *node )
{
#ifdef DEBUG
	fprintf(stderr, "remove node %s\n",node->name);
#endif	
	
	cl_partition_table *pt = asc->partition_table_head;
	while(pt) {
		
		for (int i=0;i<asc->n_partitions;i++) {
			cl_partition *p = &pt->partitions[i];
			if (p->write == node) p->write = 0;
			for (int j=0;j<p->n_read;j++) {
				if (p->read[j] == node) {
					if (j < MAX_REPLICA_COUNT-1)
						// overlapping copies must use memmove!
						memmove(&p->read[j], &p->read[j+1], 
							((MAX_REPLICA_COUNT - 1) - j) * sizeof(cl_cluster_node *) );
					p->n_read--;
					break;
				}
			}
		}
		
		pt = pt->next;
	}
	return;
}

cl_partition_table *
cl_partition_table_create(cl_cluster *asc, char *ns) 
{
#ifdef DEBUG
	fprintf(stderr, "partition table create: npartitions %d\n",asc->n_partitions);
#endif

	cl_partition_table *pt = malloc( sizeof(cl_partition_table) + (sizeof(cl_partition) * asc->n_partitions) );
	if (!pt)	return(0);
	memset(pt, 0, sizeof(cl_partition_table) + (sizeof(cl_partition) * asc->n_partitions) );
	strcpy( pt->ns, ns );
	
	pt->next = asc->partition_table_head;
	asc->partition_table_head = pt;

	return(pt);
}

// when can we figure out that a namespace is no longer in a cluster?
// it would have to be a mark-and-sweep kind of thing, where we look and see
// there are no nodes anywhere

void
cl_partition_table_destroy(cl_cluster *asc, cl_partition_table *pt)
{
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
		fprintf(stderr, "warning! passed in partition table %p not in list\n",pt);
		return;
	}
#endif

	free(pt);
}

void
cl_partition_table_destroy_all(cl_cluster *asc)
{
	cl_partition_table *now = asc->partition_table_head;
	while (now ) {
		void *t = now->next;
		free( now );
		now = t;
	}
	asc->partition_table_head = 0;
	return;
}

cl_partition_table *
cl_partition_table_get_byns(cl_cluster *asc, char *ns)
{
	cl_partition_table *pt = asc->partition_table_head;
	while (pt) {
		if (strcmp(ns, pt->ns)==0) return(pt);
		pt = pt->next;
	}
	return(0);
}


void
cl_partition_table_set( cl_cluster *asc, cl_cluster_node *node, char *ns, cl_partition_id pid, bool write)
{
#ifdef DEBUG
	fprintf(stderr, "partition-table-set: ns %s partition %d node %s write %d\n",ns,pid,node->name,(int)write);
#endif

	//
	cl_partition_table *pt = cl_partition_table_get_byns(asc, ns);
	if (!pt) {
		pt = cl_partition_table_create(asc, ns);
		if (!pt)	return; // could not add, quite the shame
	}
	
#ifdef EXTRA_CHECKS
	if (pid > asc->n_partitions) {
		fprintf(stderr, "internal error: partition table set got out of range partition id %d\n",pid);
		return;
	}
#endif	
	
	if (write)
		pt->partitions[pid].write = node;
	else {
		cl_partition *p = &pt->partitions[pid];
		for (int i=0;i < p->n_read ; i++) {
			if (p->read[i] == node)	return; // already in!
		}
		if (MAX_REPLICA_COUNT == p->n_read) { // full, replace 0 for fun
#ifdef DEBUG
			fprintf(stderr, "read replica set full\n");
#endif
			p->read[0] = node;
		}
		else {
			p->read[p->n_read] = node;
			p->n_read++;
		}
	}
		
	return;
}

static uint32_t round_robin_counter = 0;

cl_cluster_node *
cl_partition_table_get( cl_cluster *asc, char *ns, cl_partition_id pid, bool write)
{
	cl_partition_table *pt = cl_partition_table_get_byns(asc,ns);
	if (!pt)	{
#ifdef DEBUG        
        fprintf(stderr, "partition table: no partition table\n");
#endif        
        return(0);
    }
	
	cl_cluster_node *node;
	if (write) {

		node = pt->partitions[pid].write;

	}
	else {
		cl_partition *p = &pt->partitions[pid];
		if (p->n_read) {
			round_robin_counter++;
			uint32_t my_rr = round_robin_counter;
			node = p->read[ my_rr % p->n_read ];
		} else {
			node = 0;
		}
	}

#ifdef DEBUG_VERBOSE
		fprintf(stderr, "partition-table-get: ns %s pid %d write %d: node %s \n",ns,pid,write,
			node ? node->name : "nope" );
#endif		
	
	return(node);
}


