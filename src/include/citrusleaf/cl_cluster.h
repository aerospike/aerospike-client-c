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
 
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_vector.h>

#include <citrusleaf/cl_types.h>

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define MAX_INTERVALS_ABSENT 1

#define NUM_BATCH_THREADS	6
#define NUM_SCAN_THREADS	5
#define NUM_QUERY_THREADS	5


/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct cl_cluster_s cl_cluster;
typedef struct cl_cluster_node_s cl_cluster_node;
typedef struct cl_partition_s cl_partition;
typedef struct cl_partition_table_s cl_partition_table;


struct cl_cluster_node_s {
	char			name[NODE_NAME_SIZE];
	uint32_t		intervals_absent;		// how many tend periods this node has been out of partitions map
	cf_vector		sockaddr_in_v;			// a vector of sockaddr_in which the host is currently known by
	uint32_t		partition_generation;	// the server's generation count for all its partition management
	cf_queue*		conn_q;					// pool of current, cached FDs
	cf_queue*		conn_q_asyncfd;			// FDs for async command execution
	cf_queue*		asyncwork_q;
	int				info_fd;				// socket for internal info transactions on this node
};

//Structure to hold information about compression.
struct cl_cluster_compression_stat_s {
	int compression_threshold;	// Minimum size of packet, to be compressed. 0 = no compression.
	uint64_t actual_sz;			// Accumulative count. Actual size of data, compressed till now.
	uint64_t compressed_sz;		// Accumulative count. Size of data after compression.
};

struct cl_cluster_s {
	// Cluster management.
	pthread_t			tender_thread;
	cf_atomic32			tender_running;
	uint32_t			tend_speed;

	bool				follow;				// possible to create a no-follow cluster

	volatile bool		found_all;			// have, at some time, found all cluster members

	// List of host-strings added by the user.
	cf_vector			host_str_v;			// vector is pointer-type
	cf_vector			host_port_v;		// vector is integer-type

	cf_vector			host_addr_map_v;	// mapping from host string to its alternate

	// list actual node objects that represent the cluster.
	uint32_t			last_node;
	cf_vector			node_v;				// vector is pointer-type, host objects are ref-counted

	// Information about where all the partitions are.
	cl_partition_id		n_partitions;
	cl_partition_table* partition_table_head;

	struct cl_cluster_compression_stat_s compression_stat;

	int					info_timeout;		// timeout in ms for info requests

	pthread_mutex_t		LOCK;

	// Batch transaction management.
	cf_atomic32			batch_initialized;
	cf_queue*			batch_q;
	pthread_t			batch_threads[NUM_BATCH_THREADS];

	// Scan transaction management.
	cf_atomic32			scan_initialized;
	cf_queue*			scan_q;
	pthread_t			scan_threads[NUM_SCAN_THREADS];

	// Query transaction management.
	cf_atomic32			query_initialized;
	cf_queue*			query_q;
	pthread_t			query_threads[NUM_QUERY_THREADS];
};

struct cl_partition_s {
	// Mutex to cover master/prole transitions for this partition.
	pthread_mutex_t		lock;

	// Which node, if any, is the master.
	cl_cluster_node*	master;

	// Which node, if any, is the prole.
	// TODO - not ideal for replication factor > 2.
	cl_cluster_node*	prole;
};

struct cl_partition_table_s {
	cl_partition_table*	next;
	char				ns[33];
	cl_partition		partitions[];
};


/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

// Cluster calls
extern cl_cluster_node * cl_cluster_node_get_random(cl_cluster *asc);
extern cl_cluster_node * cl_cluster_node_get(cl_cluster *asc, const char *ns, const cf_digest *d, bool write);
extern void cl_cluster_node_release(cl_cluster_node *cn, const char *tag);
extern void cl_cluster_node_reserve(cl_cluster_node *cn, const char *tag);
extern void cl_cluster_node_put(cl_cluster_node *cn);
extern int cl_cluster_node_fd_get(cl_cluster_node *cn, bool asyncfd);
extern void cl_cluster_node_fd_put(cl_cluster_node *cn, int fd, bool asyncfd);
extern cl_cluster_node *cl_cluster_node_get_byname(cl_cluster *asc, const char *name);

extern int citrusleaf_info_parse_single(char *values, char **value);

extern cl_cluster * citrusleaf_cluster_create(void);
extern void citrusleaf_cluster_destroy(cl_cluster *asc);

extern cl_cluster * citrusleaf_cluster_get_or_create(char *host, short port, int timeout_ms);
extern void citrusleaf_cluster_release_or_destroy(cl_cluster **asc);
extern void citrusleaf_cluster_change_info_timeout(struct cl_cluster_s *asc, int msecs);
extern void citrusleaf_cluster_change_tend_speed(struct cl_cluster_s *asc, int secs);

// the timeout is how long to wait before the cluster is "settled"
// 0 - a sensible default
// N - some number of MS
// -1 - don't wait this time

extern cl_rv citrusleaf_cluster_add_host(cl_cluster *asc, char const *host, short port, int timeout_ms);

extern void	citrusleaf_cluster_add_addr_map(cl_cluster *asc, char *orig, char *alt);

extern bool citrusleaf_cluster_settled(cl_cluster *asc);

extern int citrusleaf_cluster_get_nodecount(cl_cluster *asc);

// must free node_names when done
extern void cl_cluster_get_node_names(cl_cluster *asc, int *n_nodes, char **node_names);


// in the PHP system, URLs are lingua franca. We expect that
// a single cluster will be created with one name - the URL - and 
// will be used over and over.
//
// URLs are of the form;
// citrusleaf://host:port (or similar)
extern cl_cluster * citrusleaf_cluster_get(char const *url);


// By default, the C client will "follow" the cluster, that is,
// track all the nodes in the cluster and continually update the cluster
// members. If, for testing, you wish to disable this feature, set this
// flag to false. This must be done before any 'addhost' calls, because
// even at the first one, the following of the cluster might start.
//
// Currently, setting this flags after hosts has an undefined effect.

extern void citrusleaf_cluster_follow(cl_cluster *asc, bool flag);

