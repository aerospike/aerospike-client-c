/*
 * The Aerospike libevent C interface. 
 *
 * A porting of the standard C interface into libevent land.
 *
 * This is the external, public header file
 *
 * All rights reserved
 * Brian Bulkowski, 2009
 * CitrusLeaf
 */

// do this both the new skool and old skool way which gives the highest correctness,
// speed, and compatibility
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <event2/dns.h>
#include <event2/event.h>

#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_hooks.h"
#include "citrusleaf/proto.h"

#include "ev2citrusleaf.h"


#ifdef __cplusplus
extern "C" {
#endif

//
// Some log-oriented primitives.
//

// how much of a delay in any processing loop is considered 'info' material?
#define CL_LOG_DELAY_INFO 10

#define CL_LOG_STATS_INTERVAL 10 // how often (in seconds) to dump 5 lines of stats

#define CL_LOG_TRANSACTION 1   // turn this on if you want verbose per-transaction logging

#define CL_LOG_RESTARTLOOP_WARN 5


//
//
//

extern bool g_ev2citrusleaf_initialized;

extern ev2citrusleaf_lock_callbacks *g_lock_cb;

#define MUTEX_ALLOC(__l)	{ __l = g_lock_cb ? g_lock_cb->alloc() : 0; }
#define MUTEX_FREE(__l)		if (__l) { g_lock_cb->free(__l); }
#define MUTEX_LOCK(__l)		if (__l) { g_lock_cb->lock(__l); }
#define MUTEX_UNLOCK(__l)	if (__l) { g_lock_cb->unlock(__l); }

struct cl_cluster_node_s;
struct sockaddr_in;


#define CL_REQUEST_MAGIC 0xBEEF1070

typedef struct cl_request_s {
	
	uint32_t MAGIC;
	
	int fd;
	struct event_base *base;
	ev2citrusleaf_cluster	*asc;
	struct cl_cluster_node_s  *node;
	int						timeout_ms;
	ev2citrusleaf_write_policy			wpol;
	
	ev2citrusleaf_callback user_cb;
	void 					*user_data;

	char 			ns[33];
	cf_digest 		d;
	bool 			write;
	
	uint8_t			*wr_buf;     // citrusleaf request packet
	size_t		    wr_buf_pos;  // current write location
	size_t		 	wr_buf_size;   // total inuse size of buffer
	
	uint8_t			rd_header_buf[sizeof(cl_proto)]; // is: a cl_proto
	size_t		    rd_header_pos;
	
	uint8_t			*rd_buf; // cl_msg[data] starts here
	size_t		    rd_buf_pos;
	size_t		    rd_buf_size;
	
	uint32_t		network_set;
	uint32_t		timeout_set;
	uint32_t		base_hop_set;
	
	uint8_t	wr_tmp[1024];
	uint8_t rd_tmp[1024];

    uint64_t start_time;
    
    uint8_t   event_space[]; // this will be preallocated
    						 // based on the size of struct event
	
} cl_request;

typedef struct cl_info_request_s {
	
	struct event_base *base;
	
	ev2citrusleaf_info_callback user_cb;
	void			*user_data;
	
	uint8_t			*wr_buf;     // citrusleaf request packet
	size_t		    wr_buf_pos;  // current write location
	size_t		 	wr_buf_size;   // total inuse size of buffer

	uint8_t			rd_header_buf[sizeof(cl_proto)]; // is: a cl_proto
	size_t		    rd_header_pos;

	uint8_t			*rd_buf; // cl_msg[data] starts here
	size_t		    rd_buf_pos;
	size_t		    rd_buf_size;

	// todo: make info requests properly timeout?
	
	uint8_t wr_tmp[1024];
	
	uint8_t event_space[];
	
} cl_info_request;

//
// having good statistics is crucial to being managable - and they exist outside 
// cluster contexts
//
typedef struct cl_statistics_s {
	// info stats
	cf_atomic_int	info_requests; // AKG - not used
	cf_atomic_int	info_host_requests;
	cf_atomic_int	info_complete;
	cf_atomic_int	info_events;
	
	// partition table stats
	cf_atomic_int	partition_process;
	cf_atomic_int	partition_create;
	cf_atomic_int	partition_destroy;
	
	// connection stats
	cf_atomic_int	conns_created;
	cf_atomic_int	conns_connected;
	cf_atomic_int	conns_destroyed;
	cf_atomic_int	conns_destroyed_timeout;
	cf_atomic_int	conns_destroyed_queue;
	
	// node stats
	cf_atomic_int	nodes_created;
	cf_atomic_int	nodes_destroyed;
	
	// request stats
	cf_atomic_int	req_start;
	cf_atomic_int	req_restart;
	cf_atomic_int	req_success;
	cf_atomic_int	req_timedout;
	cf_atomic_int	event_counter; // number of times through the main event loop

	
} cl_statistics;

extern cl_statistics g_cl_stats;

extern int ev2citrusleaf_info_host(struct event_base *base, struct sockaddr_in *sa_in, char *names, int timeout_ms,
	ev2citrusleaf_info_callback cb, void *udata); 
extern void ev2citrusleaf_info_shutdown();

extern void ev2citrusleaf_request_complete(cl_request *req, bool timedout);


// a very useful function to see if connections are still connected

#define CONNECTED 0
#define CONNECTED_NOT 1
#define CONNECTED_ERROR 2
#define CONNECTED_BADFD 3 // bad FD

extern int ev2citrusleaf_is_connected(int fd);

// Used in ev2citrusleaf.c and cl_batch.c:
void cl_set_value_particular(cl_msg_op* op, ev2citrusleaf_bin* value);
uint8_t* cl_write_header(uint8_t* buf, size_t msg_size, int info1, int info2,
		uint32_t generation, uint32_t expiration, uint32_t timeout,
		uint32_t n_fields, uint32_t n_ops);


#ifdef __cplusplus
} // end extern "C"
#endif

