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
#ifndef __EVCITRUSLEAF_H__
#define __EVCITRUSLEAF_H__

#include <inttypes.h>
#include <stdbool.h>
#include <event.h>

#include "citrusleaf/cf_digest.h"

#ifdef __cplusplus
extern "C" {
#endif


#define EVCITRUSLEAF_OK	0
#define EVCITRUSLEAF_FAIL_CLIENT_ERROR -1
#define EVCITRUSLEAF_FAIL_TIMEOUT -2
#define EVCITRUSLEAF_FAIL_UNKNOWN 1
#define EVCITRUSLEAF_FAIL_NOTFOUND 2
#define EVCITRUSLEAF_FAIL_GENERATION 3
#define EVCITRUSLEAF_FAIL_PARAMETER 4
#define EVCITRUSLEAF_FAIL_KEY_EXISTS 5 // if 'WRITE_ADD', could fail because already exists
#define EVCITRUSLEAF_FAIL_BIN_EXISTS 6
#define EVCITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH 7
#define EVCITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE 8
#define EVCITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT 9 // internal - this is mapped to EV2CITRUSLEAF_FAIL_TIMEOUT
#define EVCITRUSLEAF_FAIL_NOXDS 10
#define EVCITRUSLEAF_FAIL_UNAVAILABLE 11
#define EVCITRUSLEAF_FAIL_INCOMPATIBLE_TYPE 12 // specified operation cannot be performed on that data type
#define EVCITRUSLEAF_FAIL_RECORD_TOO_BIG 13
#define EVCITRUSLEAF_FAIL_KEY_BUSY 14



#define EVCITRUSLEAF_NO_GENERATION 0xFFFFFFFF

enum evcitrusleaf_type { CL_NULL = 0x00, CL_INT = 0x01, CL_FLOAT = 2, CL_STR = 0x03, CL_BLOB = 0x04,
	CL_TIMESTAMP = 5, CL_DIGEST = 6, CL_JAVA_BLOB = 7, CL_CSHARP_BLOB = 8, CL_PYTHON_BLOB = 9, 
	CL_RUBY_BLOB = 10, CL_UNKNOWN = 666666};
typedef enum evcitrusleaf_type evcitrusleaf_type;

enum evcitrusleaf_write_policy { CL_WRITE_ASYNC, CL_WRITE_ONESHOT, CL_WRITE_RETRY, CL_WRITE_ASSURED };

typedef enum evcitrusleaf_write_policy evcitrusleaf_write_policy;

typedef char evcitrusleaf_bin_name[32];

//
// An object is the value in a bin, or it is used as a key
// The object is typed according to the citrusleaf typing system
// These are often stack allocated, and are assigned using the 'wrap' calls
//

typedef struct evcitrusleaf_object_s {
	enum evcitrusleaf_type    type;
	size_t			size;
	union {
		char 		*str; // note for str: size is strlen (not strlen+1 
		void 		*blob;
		int64_t		i64;   // easiest to have one large int type
	} u;

	void *free; // if this is set, this must be freed on destructuion	

} evcitrusleaf_object;

// A bin is a name and an object

typedef struct evcitrusleaf_bin_s {
	evcitrusleaf_bin_name		bin_name;
	evcitrusleaf_object			object;
} evcitrusleaf_bin;

enum evcitrusleaf_operation_type { CL_OP_WRITE, CL_OP_READ, CL_OP_ADD };

typedef struct evcitrusleaf_operation_s {
	evcitrusleaf_bin_name		bin_name;
	enum evcitrusleaf_operation_type op;
	evcitrusleaf_object		object;
} evcitrusleaf_operation;

//
// All citrusleaf functions return an integer. This integer is 0 if the
// call has succeeded, and a negative number if it has failed.
// All returns of pointers and objects are done through the parameters.
// (When in C++, use & parameters for return, but we're not there yet)
//
// 'void' return functions are only used for functions that are syntactically
// unable to fail.
//

//
// evcitrusleaf_object calls
// 

// fill out the object structure with the string in question - no allocs
void evcitrusleaf_object_init(evcitrusleaf_object *o);
void evcitrusleaf_object_set_null(evcitrusleaf_object *o);
void evcitrusleaf_object_init_str(evcitrusleaf_object *o, char *str);
void evcitrusleaf_object_init_str2(evcitrusleaf_object *o, char *str, size_t buf_len);
void evcitrusleaf_object_dup_str(evcitrusleaf_object *o, char *str);
void evcitrusleaf_object_init_blob(evcitrusleaf_object *o, void *buf, size_t buf_len);
void evcitrusleaf_object_init_blob2(enum evcitrusleaf_type btype,evcitrusleaf_object *o, void *buf, size_t buf_len);
void evcitrusleaf_object_dup_blob(evcitrusleaf_object *o, void *buf, size_t buf_len);
void evcitrusleaf_object_dup_blob2(enum evcitrusleaf_type btype, evcitrusleaf_object *o, void *buf, size_t buf_len);
void evcitrusleaf_object_init_int(evcitrusleaf_object *o, int64_t i);
void evcitrusleaf_object_free(evcitrusleaf_object *o); 
void evcitrusleaf_bins_free(evcitrusleaf_bin *bins, int n_bins);



// use:
// Calling this will return an cl_conn, which is a structure internal to the library.
// You can use this to do multiple calls to Aerospike instead of returning
// and re-getting each time
// although if you have an error, it might be because that particular cluster
// went down and you might need to get a new connection to the cluster.
// (perhaps we'll hide that detail in future APIs)
//
// You will be required to return your connections! That would be called a leak.
//
// the host-port combo should be some way to get to the cluster (often a DNS name)
// However, it might be done. Using the cluster name allows the clustering
// internals to keep track of other host-ports which are the same cluster,
// and match your request to other hosts

/*
** when your result comes back,
** the 'bins' field will be allocated for you as a single block.
** you'll have to free it. Because, let's face it, we need to allocate out some
** space like this anyway, and we'll try to do that with only one alloc, and you
** might as well be able to enqueue the answer and send it somewhere else.
*/

typedef void (*evcitrusleaf_callback) (int return_value,  evcitrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata );
/**
  Initialize the asynchronous Citrusleaf library (necessary?)
*/
int evcitrusleaf_init(void);

void evcitrusleaf_shutdown(bool fail_requests);

//
// This call will print stats to stderr
//
void evcitrusleaf_print_stats(void);


/**
 * Create a cluster object - all requests are made on a cluster
 */

struct evcitrusleaf_cluster_s;
typedef struct evcitrusleaf_cluster_s evcitrusleaf_cluster;

evcitrusleaf_cluster *evcitrusleaf_cluster_create(void);
void evcitrusleaf_cluster_destroy(evcitrusleaf_cluster *asc);

// Adding a host to the cluster list which will always be checked for membership
// As this entire interface is async, the number of hosts in the cluster must be
// checked with a different, non-blocking, call
int evcitrusleaf_cluster_add_host(evcitrusleaf_cluster *cl, char *host, short port);

// Following is the act of tracking the cluster members as there are changes in
// ownership of the cluster, and load balancing. Following is enabled by default,
// turn it off only for debugging purposes
void evcitrusleaf_cluster_follow(evcitrusleaf_cluster *cl, bool flag);

// Gets the number of active nodes
// -1 means the call failed - the cluster object is invalid
// 0 means no nodes - won't get fast response
// more is good!
//
// Warning!  A typical code pattern would be to create the cluster, add a host,
// and loop on this call. That will never succeed, because libevent doesn't
// have an active thread. You will need to give libevent a thread, which is shown
// in the example distributed with this client. Or don't use threads and just
// dispatch.
int evcitrusleaf_cluster_get_active_node_count(evcitrusleaf_cluster *cl);


//
// An extended information structure
// when you want to control every little bit of write information you can
//
// Expiration is in *seconds from now*.
//
typedef struct {
	bool	use_generation;
	uint32_t generation;
	uint32_t expiration;
	evcitrusleaf_write_policy wpol;
} evcitrusleaf_write_parameters;

// If you'd like to start out with default parameters, call this function
static inline void evcitrusleaf_write_parameters_init( evcitrusleaf_write_parameters *wparam)
{
	wparam->use_generation = false; // ignore the following generation count
//	wparam->generation = 0;
	wparam->expiration = 0; // no per-item expiration
	wparam->wpol = CL_WRITE_RETRY;
}

//
// Get and put calls
//

int 
evcitrusleaf_get_all(evcitrusleaf_cluster *cl, char *ns, char *set, evcitrusleaf_object *key, 
	int timeout_ms, evcitrusleaf_callback cb, void *udata);

int
evcitrusleaf_get_all_digest(evcitrusleaf_cluster *cl, char *ns, cf_digest *d, int timeout_ms, 
	evcitrusleaf_callback cb, void *udata);

int 
evcitrusleaf_put(evcitrusleaf_cluster *cl, char *ns, char *set, evcitrusleaf_object *key,
	evcitrusleaf_bin *bins, int n_bins, evcitrusleaf_write_parameters *wparam, 
	int timeout_ms, evcitrusleaf_callback cb, void *udata);

int 
evcitrusleaf_put_digest(evcitrusleaf_cluster *cl, char *ns, cf_digest *d,
	evcitrusleaf_bin *bins, int n_bins, evcitrusleaf_write_parameters *wparam, 
	int timeout_ms, evcitrusleaf_callback cb, void *udata);

int 
evcitrusleaf_get(evcitrusleaf_cluster *cl, char *ns, char *set, evcitrusleaf_object *key,
	const char **bins, int n_bins, int timeout_ms, evcitrusleaf_callback cb, void *udata);

int 
evcitrusleaf_get_digest(evcitrusleaf_cluster *cl, char *ns, cf_digest *d,
	const char **bins, int n_bins, int timeout_ms, evcitrusleaf_callback cb, void *udata);


int 
evcitrusleaf_delete(evcitrusleaf_cluster *cl, char *ns, char *set, evcitrusleaf_object *key,
	evcitrusleaf_write_parameters *wparam, int timeout_ms, evcitrusleaf_callback cb, void *udata);

int 
evcitrusleaf_delete_digest(evcitrusleaf_cluster *cl, char *ns, cf_digest *d,
	evcitrusleaf_write_parameters *wparam, int timeout_ms, evcitrusleaf_callback cb, void *udata);


int
evcitrusleaf_operate(evcitrusleaf_cluster *cl, char *ns, char *set, evcitrusleaf_object *key,
	evcitrusleaf_operation *ops, int n_ops, evcitrusleaf_write_parameters *wparam, 
	int timeout_ms, evcitrusleaf_callback cb, void *udata);

int
evcitrusleaf_operate_digest(evcitrusleaf_cluster *cl, char *ns, cf_digest *d,
	evcitrusleaf_operation *ops, int n_ops, evcitrusleaf_write_parameters *wparam, 
	int timeout_ms, evcitrusleaf_callback cb, void *udata);


//
// the info interface allows
// information about specific cluster features to be retrieved on a host by host basis
// size_t is in number of bytes. String is null terminated as well
// API CONTRACT: *callee* frees the 'response' buffer
typedef void (*evcitrusleaf_info_callback) (int return_value, char *response, size_t response_len, void *udata);

int
evcitrusleaf_info(char *host, short port, char *names, int timeout_ms,
	evcitrusleaf_info_callback cb, void *udata);

//
// This debugging call can be useful for tracking down errors and coordinating with server failures
//
int
evcitrusleaf_calculate_digest(const char *set, const evcitrusleaf_object *key, cf_digest *digest);

//
// Logging. Register to be called back on every callback. For higher performance
// and filtering, you may set the severity mask and have the library not call you
//

typedef void (*evcitrusleaf_log_callback) (int level, const char *fmt, ... );


void evcitrusleaf_log_register( evcitrusleaf_log_callback cb );

#define EVCITRUSLEAF_NOLOGGING  -1
#define EVCITRUSLEAF_EMERG    0  // system is unusable                 
#define EVCITRUSLEAF_ALERT    1  /* action must be taken immediately */
#define EVCITRUSLEAF_CRIT     2  /* critical conditions              */
#define EVCITRUSLEAF_ERR      3  /* error conditions                 */
#define EVCITRUSLEAF_WARNING  4  /* warning conditions               */
#define EVCITRUSLEAF_NOTICE   5  /* normal but significant condition */
#define EVCITRUSLEAF_INFO     6  /* informational                    */
#define EVCITRUSLEAF_DEBUG    7  /* debug-level messages      */



void evcitrusleaf_log_level_set( int level );

// An example logging function that simply prints to standard out
//
// void sample_logging_function( int level, const char *format, ...) {
//			va_list ap;
//        	va_start(ap, format);
//          (void) vfprintf(stderr, format, ap);
//        	va_end(ap);
//  }
//

#ifdef __cplusplus
} // end extern "C"
#endif


#endif

