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

#include <inttypes.h>
#include <stdbool.h>
#include <netinet/in.h>
#include <event.h>
#include <stdarg.h>

#include "evcitrusleaf.h"

#include "cf_vector.h"
#include "cf_queue.h"
#include "cf_alloc.h"
#include "cf_ll.h"
#include "cf_digest.h"

#include "proto.h"

#ifdef __cplusplus
extern "C" {
#endif

//
// Some log-oriented primitives.
//

// how much of a delay in any processing loop is considered 'warning' material?
#define CL_LOG_DELAY_WARN 10

#define CL_LOG_STATS_INTERVAL 10 // how often (in seconds) to dump 5 lines of stats

#define CL_LOG_TRANSACTION 1   // turn this on if you want verbose per-transaction logging

#define CL_LOG_RESTARTLOOP_WARN 5


//
//
//

struct cl_cluster_node_s;


#define CL_REQUEST_MAGIC 0xBEEF1070

typedef struct cl_request_s {
	
	uint32_t MAGIC;
	
	int fd;
	evcitrusleaf_cluster	*asc;
	struct cl_cluster_node_s  *node;
	int						timeout_ms;
	evcitrusleaf_write_policy			wpol;
	
	evcitrusleaf_callback user_cb;
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
	
	bool		 network_set;
	struct event network_event;
	
	bool         timeout_set;
	struct event timeout_event;
	
	uint8_t	wr_tmp[1024];
	uint8_t rd_tmp[1024];

    uint64_t start_time;
	
} cl_request;

typedef struct cl_info_request_s {
	
	evcitrusleaf_info_callback user_cb;
	void			*user_data;
	
	uint8_t			*wr_buf;     // citrusleaf request packet
	size_t		    wr_buf_pos;  // current write location
	size_t		 	wr_buf_size;   // total inuse size of buffer

	uint8_t			rd_header_buf[sizeof(cl_proto)]; // is: a cl_proto
	size_t		    rd_header_pos;

	uint8_t			*rd_buf; // cl_msg[data] starts here
	size_t		    rd_buf_pos;
	size_t		    rd_buf_size;

	struct event network_event;

	// todo: make info requests properly timeout?
	
	uint8_t wr_tmp[1024];
	
} cl_info_request;

//
// having good statistics is crucial to being managable - and they exist outside 
// cluster contexts
//
typedef struct cl_statistics_s {
	// info stats
	uint64_t		info_requests;
	uint64_t    	info_host_requests;
	uint64_t		info_complete;
	uint64_t    	info_events;
	
	// partition table stats
	uint64_t		partition_process;
	uint64_t		partition_create;
	uint64_t		partition_destroy;
	
	// connection stats
	uint64_t		conns_created;
	uint64_t		conns_connected;
	uint64_t        conns_destroyed;
	uint64_t		conns_destroyed_timeout;
	uint64_t		conns_destroyed_queue;
	
	// node stats
	uint64_t        nodes_created;
	uint64_t		nodes_destroyed;
	
	// request stats
	uint64_t		req_start;
	uint64_t		req_restart;
	uint64_t        req_success;
	uint64_t        req_timedout;
	uint64_t		event_counter; // number of times through the main event loop

	
} cl_statistics;

extern cl_statistics g_cl_stats;

extern int evcitrusleaf_info_host(struct sockaddr_in *sa_in, char *names, int timeout_ms,
	evcitrusleaf_info_callback cb, void *udata); 
extern void evcitrusleaf_info_shutdown();

extern void evcitrusleaf_request_complete(cl_request *req, bool timedout);


// a very useful function to see if connections are still connected

#define CONNECTED 0
#define CONNECTED_NOT 1
#define CONNECTED_ERROR 2
#define CONNECTED_BADFD 3 // bad FD

extern int evcitrusleaf_is_connected(int fd);


//
//

extern evcitrusleaf_log_callback cl_log_fn;
extern int cl_log_level;

#define CL_NOLOG      EVCITRUSLEAF_NOLOGGING // not allowed to use this in the code
#define CL_WARNING    EVCITRUSLEAF_ERR
#define CL_INFO       EVCITRUSLEAF_NOTICE   
#define CL_DEBUG      EVCITRUSLEAF_INFO
#define CL_VERBOSE    EVCITRUSLEAF_DEBUG


#define CL_LOG( __level,  ... )  if ((cl_log_fn) && (__level <= cl_log_level)) { (*cl_log_fn) (__level,  __VA_ARGS__ ); }  

static inline bool
CL_LOG_CHK(int __level) {
    if (cl_log_fn == 0) return(false);
    if (__level > cl_log_level) return(false);
    return(true);
}



#ifdef __cplusplus
} // end extern "C"
#endif

