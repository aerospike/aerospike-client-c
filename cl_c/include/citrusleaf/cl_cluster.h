/*
 * The Aerospike C interface. A good, basic library that many clients can be based on.
 *
 * This is the internal, non-public header file.
 *
 * this code currently assumes that the server is running in an ASCII-7 based
 * (ie, utf8 or ISO-LATIN-1)
 * character set, as values coming back from the server are UTF-8. We currently
 * don't bother to convert to the character set of the machine we're running on
 * but we advertise these values as 'strings'
 *
 * All rights reserved
 * Brian Bulkowski, 2009
 * CitrusLeaf
 */

#include "citrusleaf.h"

 
// do this both the new skool and old skool way which gives the highest correctness,
// speed, and compatibility
#pragma once
#ifndef __CL_C_H__
#define __CL_C_H__

#define NODE_DUN_THRESHOLD	800
#define NODE_DUN_INFO_ERR	300
#define NODE_DUN_NAME_CHG	801
#define NODE_DUN_NET_ERR	50
#define NODE_DUN_TIMEOUT	1

typedef struct cl_cluster_node_s {
	
	char		name[NODE_NAME_SIZE];
	
	cf_atomic32	dun_score;	// keep track of how "unhealthy" a node is
	bool 		dunned;		// had a problem. Will get deleted next pass through.
	
	// A vector of sockaddr_in which the host is currently known by
	cf_vector		sockaddr_in_v;
	
	// the server's generation count for all its partition management
	uint32_t	partition_generation;
	
	// pool of current, cached FDs
	cf_queue	*conn_q;
	cf_queue	*conn_q_asyncfd;	// FDs for async command execution

	int		asyncfd;
	cf_queue	*asyncwork_q;
	
	pthread_mutex_t LOCK;
	
} cl_cluster_node;

#define MAX_REPLICA_COUNT 5

typedef struct cl_partition_s {
	cl_cluster_node	*write;
	int n_read;
	cl_cluster_node *read[MAX_REPLICA_COUNT];
} cl_partition;


typedef struct cl_partition_table_s {
	
	struct cl_partition_table_s *next;
	
	char ns[33];  // the namespace name
	
	cl_partition partitions[];
	
} cl_partition_table;


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
	uint			last_node;
	cf_vector		node_v;      // vector is pointer-type, host objects are ref-counted
	
	// information about where all the partitions are
	cl_partition_id		n_partitions;
	cl_partition_table *partition_table_head;
	
	uint32_t		ref_count;
	uint32_t		tend_speed;
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
extern void cl_cluster_node_release(cl_cluster_node *cn);
extern void cl_cluster_node_put(cl_cluster_node *cn);          // put node back
extern void cl_cluster_node_dun(cl_cluster_node *cn, int32_t score);			// node is less healthy!
extern void cl_cluster_node_ok(cl_cluster_node *cn);
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
extern void cl_partition_table_remove_node( cl_cluster *asc, cl_cluster_node *node );
extern void cl_partition_table_destroy_all(cl_cluster *asc);
extern void cl_partition_table_set( cl_cluster *asc, cl_cluster_node *node, char *ns, cl_partition_id pid, bool write);
extern cl_cluster_node *cl_partition_table_get( cl_cluster *asc, char *ns, cl_partition_id pid, bool write);

#endif

#ifdef __cplusplus
} // end extern "C"
#endif


