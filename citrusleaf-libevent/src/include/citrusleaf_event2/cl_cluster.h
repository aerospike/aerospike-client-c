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

#include <pthread.h>
#include <stdint.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_ll.h>
#include <citrusleaf/cf_proto.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_vector.h>
#include <citrusleaf/cf_types.h>

#include "ev2citrusleaf.h"


#ifdef __cplusplus
extern "C" {
#endif

struct sockaddr_in;

#define CLUSTER_NODE_MAGIC 0x9B00134C
#define MAX_INTERVALS_ABSENT 1
#define MAX_HISTORY_INTERVALS 64 // power of 2 makes mod operation fast
#define MAX_THROTTLE_WINDOW (MAX_HISTORY_INTERVALS + 1)

typedef enum {
	INFO_REQ_NONE			= 0,
	INFO_REQ_CHECK			= 1,
	INFO_REQ_GET_REPLICAS	= 2
} node_info_req_type;

#define NODE_INFO_REQ_MAX_INTERVALS 5

// Must be >= longest "names" string sent in a node info request.
#define INFO_STR_MAX_LEN 64

typedef struct node_info_req_s {
	// What type of info request is in progress, if any.
	node_info_req_type		type;

	// How many node timer periods this request has lasted.
	uint32_t				intervals;

	// Buffer for writing to socket.
	uint8_t					wbuf[sizeof(cl_proto) + INFO_STR_MAX_LEN];
	size_t					wbuf_size;
	size_t					wbuf_pos;

	// Buffer for reading proto header from socket.
	uint8_t					hbuf[sizeof(cl_proto)];
	size_t					hbuf_pos;

	// Buffer for reading proto body from socket.
	uint8_t*				rbuf;
	size_t					rbuf_size;
	size_t					rbuf_pos;
} node_info_req;

typedef struct cl_cluster_node_s {
	// Sanity-checking field.
	uint32_t				MAGIC;

	// This node's name, a null-terminated hex string.
	char					name[20];

	// A vector of sockaddr_in which the host (node) is currently known by.
	cf_vector				sockaddr_in_v;

	// The cluster we belong to.
	ev2citrusleaf_cluster*	asc;

	// How many node timer periods this node has been out of partitions map.
	uint32_t				intervals_absent;

	// Transaction successes & failures since this node's last timer event.
	cf_atomic32				n_successes;
	cf_atomic32				n_failures;

	// This node's recent transaction successes & failures.
	uint32_t				successes[MAX_HISTORY_INTERVALS];
	uint32_t				failures[MAX_HISTORY_INTERVALS];
	uint32_t				current_interval;

	// Rate at which transactions to this node are being throttled.
	cf_atomic32				throttle_pct;

	// Socket pool for (non-info) transactions on this node.
	cf_queue*				conn_q;

	// Number of sockets open on this node - for now just for stats.
	cf_atomic32				n_fds_open;

	// What version of partition information we have for this node.
	cf_atomic_int			partition_generation;

	// Socket for info transactions on this node.
	int						info_fd;

	// The info transaction in progress, if any.
	node_info_req			info_req;

	// Space for two events: periodic node timer, and info request.
	uint8_t					event_space[];
} cl_cluster_node;


#define CLUSTER_MAGIC 0x91916666

// Must be in-sync with ev2citrusleaf_cluster_runtime_options.
typedef struct threadsafe_runtime_options_s {
	cf_atomic32				socket_pool_max;

	cf_atomic32				read_master_only;

	cf_atomic32				throttle_reads;
	cf_atomic32				throttle_writes;

	// These change together under the lock.
	uint32_t				throttle_threshold_failure_pct;
	uint32_t				throttle_window_seconds;
	uint32_t				throttle_factor;

	// For groups of options that need to change together:
	void*					lock;
} threadsafe_runtime_options;

typedef struct cl_partition_s {
	// Mutex to cover master/prole transitions for this partition.
	void*					lock;

	// Which node, if any, is the master.
	cl_cluster_node*		master;

	// Which node, if any, is the prole.
	// TODO - not ideal for replication factor > 2.
	cl_cluster_node*		prole;
} cl_partition;

typedef struct cl_partition_table_s {
	// Pointer to next element in this linked list.
	struct cl_partition_table_s* next;

	// The namespace name.
	char					ns[33];

	// For logging - only dump table to log if it changed since last time.
	bool					was_dumped;

	// Space for array of cl_partition objects.
	cl_partition			partitions[];
} cl_partition_table;

struct ev2citrusleaf_cluster_s {
	// Global linked list of all clusters.
	cf_ll_element			ll_e;

	// Sanity-checking field.
	uint32_t				MAGIC;

	// Seems this flag isn't used, but is set from public API. TODO - deprecate?
	bool					follow;

	// Used only with internal cluster management option.
	pthread_t				mgr_thread;
	bool					internal_mgr;

	// Cluster management event base, specified by app or internally created.
	struct event_base*		base;

	// Associated cluster management DNS event base.
	struct evdns_base*		dns_base;

	// Cluster-specific functionality options.
	ev2citrusleaf_cluster_static_options	static_options;
	threadsafe_runtime_options				runtime_options;

	// List of host-strings and ports added by the user.
	cf_vector				host_str_v;		// vector is pointer-type
	cf_vector				host_port_v;	// vector is integer-type

	// List of node objects in this cluster.
	cf_vector				node_v;			// vector is pointer-type
	void* 					node_v_lock;
	cf_atomic_int			last_node;

	// If we can't get a node for transactions we internally queue the
	// transactions until nodes become available.
	cf_queue*				request_q;
	void*					request_q_lock;

	// Transactions in progress. Includes transactions in the request queue
	// above (everything needing a callback). No longer used for clean shutdown
	// other than to issue a warning if there are incomplete transactions.
	cf_atomic_int			requests_in_progress;

	// Internal non-node info requests in progress, used for clean shutdown.
	cf_atomic_int			pings_in_progress;

	// Number of partitions. Not atomic since it never changes on the server.
	cl_partition_id			n_partitions;

	// Head of linked list of partition tables (one table per namespace).
	cl_partition_table*		partition_table_head;

	// How many tender timer periods this cluster has lasted.
	uint32_t				tender_intervals;

	// Statistics for this cluster. (Some are atomic only because the public API
	// can dump the statistics in any thread.)

		// History of nodes in the cluster.
	cf_atomic_int			n_nodes_created;
	cf_atomic_int			n_nodes_destroyed;

		// Totals for tender transactions.
	cf_atomic_int			n_ping_successes;
	cf_atomic_int			n_ping_failures;

		// Totals for node info transactions.
	cf_atomic_int			n_node_info_successes;
	cf_atomic_int			n_node_info_failures;
	cf_atomic_int			n_node_info_timeouts;

		// Totals for "ordinary" transactions.
	cf_atomic_int			n_req_successes;
	cf_atomic_int			n_req_failures;
	cf_atomic_int			n_req_timeouts;
	cf_atomic_int			n_req_throttles;
	cf_atomic_int			n_internal_retries;
	cf_atomic_int			n_internal_retries_off_q;

		// Totals for batch transactions.
	cf_atomic_int			n_batch_node_successes;
	cf_atomic_int			n_batch_node_failures;
	cf_atomic_int			n_batch_node_timeouts;

	// Space for cluster tender periodic timer event.
	uint8_t					event_space[];
};


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
extern cl_cluster_node *cl_cluster_node_get(ev2citrusleaf_cluster *asc, const char *ns, const cf_digest *d, bool write);  // get node from cluster
extern void cl_cluster_node_release(cl_cluster_node *cn, char *msg);
extern void cl_cluster_node_reserve(cl_cluster_node *cn, char *msg);
extern void cl_cluster_node_put(cl_cluster_node *cn);          // put node back
extern int cl_cluster_node_fd_get(cl_cluster_node *cn);			// get an FD to the node
extern void cl_cluster_node_fd_put(cl_cluster_node *cn, int fd); // put the FD back
extern bool cl_cluster_node_throttle_drop(cl_cluster_node* cn);

// Count a transaction as a success or failure.
// TODO - add a tag parameter for debugging or detailed stats?

static inline void
cl_cluster_node_had_success(cl_cluster_node* cn)
{
	cf_atomic32_incr(&cn->n_successes);
}

static inline void
cl_cluster_node_had_failure(cl_cluster_node* cn)
{
	cf_atomic32_incr(&cn->n_failures);
}

//
extern int citrusleaf_info_host(struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms);
extern int citrusleaf_info_parse_single(char *values, char **value);

extern int citrusleaf_cluster_init();
extern int citrusleaf_cluster_shutdown();

// Partition table calls
// --- all these assume the partition lock is held
extern void cl_partition_table_destroy_all(ev2citrusleaf_cluster *asc);
extern bool cl_partition_table_is_node_present(cl_cluster_node* node);
extern void cl_partition_table_update(cl_cluster_node* node, const char* ns, bool* masters, bool* proles);
extern cl_cluster_node *cl_partition_table_get( ev2citrusleaf_cluster *asc, const char *ns, cl_partition_id pid, bool write);
extern void cl_partition_table_dump(ev2citrusleaf_cluster* asc);

#ifdef __cplusplus
} // end extern "C"
#endif


