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
#pragma once

#include <citrusleaf/citrusleaf.h>

 
// do this both the new skool and old skool way which gives the highest correctness,
// speed, and compatibility
#pragma once
#ifndef __CL_C_H__
#define __CL_C_H__

#define MAX_INTERVALS_ABSENT 1

typedef struct cl_cluster_node_s {
	
	char		name[NODE_NAME_SIZE];
	
	// How many tend periods this node has been out of partitions map.
	uint32_t	intervals_absent;

	// How many tend periods this node has been unreachable (for XDR only).
	cf_atomic32	intervals_unreachable;
	
	// A vector of sockaddr_in which the host is currently known by
	cf_vector		sockaddr_in_v;
	
	// the server's generation count for all its partition management
	uint32_t	partition_generation;
	
	// pool of current, cached FDs
	cf_queue	*conn_q;
	cf_queue	*conn_q_asyncfd;	// FDs for async command execution

	int		asyncfd;
	cf_queue	*asyncwork_q;
	
} cl_cluster_node;

typedef struct cl_partition_s {
	// Mutex to cover master/prole transitions for this partition.
	pthread_mutex_t			lock;

	// Which node, if any, is the master.
	cl_cluster_node*		master;

	// Which node, if any, is the prole.
	// TODO - not ideal for replication factor > 2.
	cl_cluster_node*		prole;
} cl_partition;

typedef struct cl_partition_table_s {
	
	struct cl_partition_table_s *next;
	
	char ns[33];  // the namespace name
	
	cl_partition partitions[];
	
} cl_partition_table;

//Structure to hold information about compression.
struct cl_cluster_compression_stat_s {
    int compression_threshold; // Minimum size of packet, to be compressed. 0 = No cpmpression.
    uint64_t actual_sz;        // Accumulative count. Actual size of data, compressed till now.
    uint64_t compressed_sz;    // Accumulative count. Size of data after compression.
};

struct cl_cluster_s {
	// Linked list element should be first element in the structure
	cf_ll_element		ll_e;

	uint32_t		state;		//bitmap representing state information
	
	bool		follow;   // possible to create a no-follow cluster
	bool		nbconnect;
						  // mostly for testing
						  // that only targets specific nodes
						  
	volatile bool		found_all; // have, at some time, found all cluster memebers
	
	// List of host-strings added by the user.
	cf_vector		host_str_v;	// vector is pointer-type
	cf_vector		host_port_v;  // vector is integer-type
	
	cf_vector		host_addr_map_v; //Mapping from host string to its alternate
	
	// list actual node objects that represent the cluster
	uint32_t		last_node;
	cf_vector		node_v;      // vector is pointer-type, host objects are ref-counted
	
	// information about where all the partitions are
	cl_partition_id		n_partitions;
	cl_partition_table *partition_table_head;

    struct cl_cluster_compression_stat_s compression_stat;
	
	uint32_t		ref_count;
	uint32_t		tend_speed;
    int             info_timeout;   // timeout in ms for info requests
	// Need a lock
	pthread_mutex_t	LOCK;
	
};

#define CLS_TENDER_RUNNING	0x00000001
#define CLS_FREED		0x00000002
#define CLS_UNUSED1		0x00000004
#define CLS_UNUSED2		0x00000008
#define CLS_UNUSED3		0x00000010

extern cf_ll cluster_ll;
// Cluster calls
extern cl_cluster_node *cl_cluster_node_get_random(cl_cluster *asc);  // get node from cluster
extern cl_cluster_node *cl_cluster_node_get(cl_cluster *asc, const char *ns, const cf_digest *d, bool write);  // get node from cluster
extern void cl_cluster_node_release(cl_cluster_node *cn, const char *tag);
extern void cl_cluster_node_reserve(cl_cluster_node *cn, const char *tag);
extern void cl_cluster_node_put(cl_cluster_node *cn);          // put node back
extern int cl_cluster_node_fd_get(cl_cluster_node *cn, bool asyncfd, bool nbconnect);	// get an FD to the node
extern void cl_cluster_node_fd_put(cl_cluster_node *cn, int fd, bool asyncfd); 		// put the FD back
extern int citrusleaf_cluster_init();
extern cl_cluster_node *cl_cluster_node_get_byname(cl_cluster *asc, char *name);

// must free node_names when done
extern int cl_cluster_get_node_names_byhostportlist(cl_cluster *asc, char *list_nodes, int *n_nodes, char **node_names);

//
extern int citrusleaf_info_parse_single(char *values, char **value);

// Partition table calls
// --- all these assume the partition lock is held
extern void cl_partition_table_destroy_all(cl_cluster *asc);
extern bool cl_partition_table_is_node_present(cl_cluster* asc, cl_cluster_node* node);
extern void cl_partition_table_update(cl_cluster* asc, cl_cluster_node* node, const char* ns, bool* masters, bool* proles);
extern cl_cluster_node* cl_partition_table_get(cl_cluster* asc, const char* ns, cl_partition_id pid, bool write);

#endif

#ifdef __cplusplus
} // end extern "C"
#endif


