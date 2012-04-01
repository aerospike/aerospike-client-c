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


// do this both the new skool and old skool way which gives the highest correctness,
// speed, and compatibility
#pragma once

#include "citrusleaf/cf_atomic.h"
#include "ev2citrusleaf-internal.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CLUSTER_NODE_MAGIC 0x9B00134C

typedef struct cl_cluster_node_s {
	
	uint32_t    MAGIC;
	
	char		name[20];
	
	cf_atomic_int	dunned;		// had a problem. Will get deleted next pass through. 0 - false, 1 - true
	cf_atomic_int	dun_count;  // number of sequential dunns
	
	
	// A vector of sockaddr_in which the host is currently known by
	cf_vector		sockaddr_in_v;
	
	// pool of current, cached FDs
	cf_queue	*conn_q;

	// the cluster we belong to
	ev2citrusleaf_cluster	*asc;
	
	// 
	cf_atomic_int	partition_generation;
	// flooding the server with partition requests is bad.
	cf_atomic_int	partition_last_req_ms;
	
	
	// timer for node maintaince - PERPETUAL - but the timer keeps a refcount,
	// so have to be careful
	// AKG - not currently used
	bool timer_event_registered;
//	struct event *timer_event;
	
	uint8_t	event_space[];
	
} cl_cluster_node;


#define MAX_REPLICA_COUNT 5

typedef struct cl_partition_s {
	void *lock;
	cl_cluster_node	*write;
	int n_read;
	cl_cluster_node *read[MAX_REPLICA_COUNT];
} cl_partition;


typedef struct cl_partition_table_s {
	
	struct cl_partition_table_s *next;
	
	char ns[33];  // the namespace name
	
	cl_partition partitions[];
	
} cl_partition_table;


#define CLUSTER_MAGIC 0x91916666

struct ev2citrusleaf_cluster_s {
	
	cf_ll_element		ll_e; // global list of all clusters - good for debugging
	
	uint32_t    MAGIC;
	
	bool		follow;   // possible to create a no-follow cluster
						  // mostly for testing
						  // that only targets specific nodes
						  
	// AKG - multi-thread access, shutdown usage
	bool		shutdown; // we might be in shutdown phase, don't start more info
							// requests or similar

	struct event_base *base; // base that this cluster serves		
	struct evdns_base *dns_base;
							
	// List of host-strings added by the user.
	cf_vector		host_str_v;	// vector is pointer-type
	cf_vector		host_port_v;  // vector is integer-type
	
	// actual **node objects** that represent the cluster
	void 			*node_v_lock;
	cf_atomic_int	last_node;
	cf_vector		node_v;      // vector is pointer-type, host objects are ref-counted
	
	// this timeout does maintance on the cluster
	// AKG - not currently used
	bool         timer_set;
	
	// There are occasions where we want to stash pending transactions in a queue
	// for when nodes come available (like, embarrasingly, the first request)
	cf_queue	*request_q;
	
	// in progress requests pointing to this asc.
	// necessary so we can drain out on shutdown.
	// *includes* requests in the request queue above (everything needing a callback)
	cf_atomic_int	requests_in_progress; 
	
	cf_atomic_int	infos_in_progress;
	
	// information about where all the partitions are
	// AKG - multi-thread access, but never changes on server
	cl_partition_id		n_partitions;
	// AKG - pointer set/read only in cluster thread
	cl_partition_table *partition_table_head;
	
	// statistics?
	
	uint8_t 	event_space[];
	
};


enum cl_cluster_dun_type {
	DUN_USER_TIMEOUT=0, DUN_INFO_FAIL=1, DUN_REPLICAS_FETCH=2, DUN_NETWORK_ERROR=3, DUN_RESTART_FD=4, DUN_BAD_NAME=5, DUN_NO_SOCKADDR=6
};
extern char *cl_cluster_dun_human[];

//
// a global list of all clusters is interesting sometimes
//
// AKG - only changed in create/destroy, read in print_stats
extern cf_ll		cluster_ll;


// Do a lookup with this name and port, and add the sockaddr to the
// vector using the unique lookup
extern int cl_lookup_immediate(char *hostname, short port, struct sockaddr_in *sin);
typedef void (*cl_lookup_async_fn) (int result, cf_vector *sockaddr_in_v, void *udata);
extern int cl_lookup(struct evdns_base *base, char *hostname, short port, cl_lookup_async_fn cb, void *udata);

// Cluster calls
extern cl_cluster_node *cl_cluster_node_get(ev2citrusleaf_cluster *asc, char *ns, cf_digest *d, bool write);  // get node from cluster
extern void cl_cluster_node_release(cl_cluster_node *cn, char *msg);
extern void cl_cluster_node_reserve(cl_cluster_node *cn, char *msg);
extern void cl_cluster_node_put(cl_cluster_node *cn);          // put node back
extern void cl_cluster_node_dun(cl_cluster_node *cn, enum cl_cluster_dun_type dun);			// node is bad!
extern void cl_cluster_node_ok(cl_cluster_node *cn);			// node is good!
extern int cl_cluster_node_fd_get(cl_cluster_node *cn);			// get an FD to the node
extern void cl_cluster_node_fd_put(cl_cluster_node *cn, int fd); // put the FD back

//
extern int citrusleaf_info_host(struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms);
extern int citrusleaf_info_parse_single(char *values, char **value);

extern int citrusleaf_cluster_init();
extern int citrusleaf_cluster_shutdown();

// Partition table calls
// --- all these assume the partition lock is held
extern void cl_partition_table_remove_node( ev2citrusleaf_cluster *asc, cl_cluster_node *node );
extern void cl_partition_table_destroy_all(ev2citrusleaf_cluster *asc);
extern void cl_partition_table_set( ev2citrusleaf_cluster *asc, cl_cluster_node *node, char *ns, cl_partition_id pid, bool write);
extern cl_cluster_node *cl_partition_table_get( ev2citrusleaf_cluster *asc, char *ns, cl_partition_id pid, bool write);

#ifdef __cplusplus
} // end extern "C"
#endif


