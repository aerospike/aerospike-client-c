/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include <inttypes.h>
#include <stdbool.h>
#include <netinet/in.h>
 
/**
 * Hack for the sake of XDS. XDS includes the main CF libs. 
 * We do not want to include them again from client API
 */
#ifndef XDS
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_log.h>
#include <citrusleaf/cf_ll.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_vector.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_alloc.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_shash.h>
#include <citrusleaf/cf_rchash.h>
#endif

#define STACK_BUF_SZ (1024 * 16) // provide a safe number for your system - linux tends to have 8M stacks these days
#define DEFAULT_PROGRESS_TIMEOUT 50
#define NODE_NAME_SIZE 20
#define CL_BINNAME_SIZE 16

#define CL_MAX_NUM_FUNC_ARGC    10 


typedef struct cl_conn_s cl_conn;

typedef enum cl_rv_e cl_rv;
typedef enum cl_rvclient_e cl_rvclient;
typedef enum cl_type_e cl_type;
typedef enum cl_write_policy_e cl_write_policy;
typedef enum cl_operator_type_e cl_operator_type;

typedef struct cl_object_s cl_object;
typedef struct cl_bin_s cl_bin;
typedef struct cl_rec_s cl_rec;
typedef struct cl_batchresult_s cl_batchresult;
typedef struct cl_operation_s cl_operation;
typedef struct cl_addrmap_s cl_addrmap;

typedef struct cl_node_response_s cl_node_response;
typedef struct cl_write_parameters_s cl_write_parameters;



enum cl_rv_e {
    CITRUSLEAF_FAIL_ASYNCQ_FULL = -3,
    CITRUSLEAF_FAIL_TIMEOUT = -2,
    CITRUSLEAF_FAIL_CLIENT = -1,                // an out of memory or similar locally
    CITRUSLEAF_OK = 0,
    CITRUSLEAF_FAIL_UNKNOWN = 1,                // unknown failure on the server side
    CITRUSLEAF_FAIL_NOTFOUND = 2,
    CITRUSLEAF_FAIL_GENERATION = 3,             // likely a CAS write, and the write failed
    CITRUSLEAF_FAIL_PARAMETER = 4,              // you passed in bad parameters
    CITRUSLEAF_FAIL_KEY_EXISTS = 5,
    CITRUSLEAF_FAIL_BIN_EXISTS = 6,
    CITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH = 7,
    CITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE = 8,
    CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT = 9,
    CITRUSLEAF_FAIL_NOXDS = 10,
    CITRUSLEAF_FAIL_UNAVAILABLE = 11,
    CITRUSLEAF_FAIL_INCOMPATIBLE_TYPE = 12,     // specified operation cannot be performed on that data type
    CITRUSLEAF_FAIL_RECORD_TOO_BIG = 13,
    CITRUSLEAF_FAIL_KEY_BUSY = 14,
    CITRUSLEAF_FAIL_SPROC_EXECUTION = 100,

    // Secondary Index Query Codes 200 - 230
    CITRUSLEAF_FAIL_INDEX_KEY_NOTFOUND = 200,
    CITRUSLEAF_FAIL_INDEX_TYPE_MISMATCH = 201,
    CITRUSLEAF_FAIL_INDEX_NOTFOUND = 202,
    CITRUSLEAF_FAIL_INDEX_OOM = 203,
    CITRUSLEAF_FAIL_INDEX_GENERIC = 204,
    CITRUSLEAF_FAIL_INDEX_EXISTS = 205,
    CITRUSLEAF_FAIL_INDEX_SINGLEBIN_NS = 206,
    CITRUSLEAF_FAIL_INDEX_UNKNOWN_TYPE = 207,
    CITRUSLEAF_FAIL_INDEX_FOUND = 208
};

enum cl_rvclient_e {
    CITRUSLEAF_FAIL_DC_DOWN = 1,
    CITRUSLEAF_FAIL_DC_UP = 2
};


enum cl_type_e { 
    CL_NULL        = 0,     CL_INT         = 1,     CL_FLOAT     = 2,   CL_STR       = 3,
    CL_BLOB        = 4,     CL_TIMESTAMP   = 5,     CL_DIGEST    = 6,   CL_JAVA_BLOB = 7,
    CL_CSHARP_BLOB = 8,     CL_PYTHON_BLOB = 9,     CL_RUBY_BLOB = 10,  CL_PHP_BLOB = 11,
    CL_ERLANG_BLOB = 12,    CL_APPEND      = 13,    CL_LUA_BLOB  = 14,  CL_MAP      = 15,
    CL_UNKNOWN     = 666666
};


enum cl_write_policy_e { 
    CL_WRITE_ASYNC, 
    CL_WRITE_ONESHOT, 
    CL_WRITE_RETRY, 
    CL_WRITE_ASSURED
};

enum cl_operator_type_e { 
    CL_OP_WRITE, 
    CL_OP_READ, 
    CL_OP_INCR, 
    CL_OP_MC_INCR, 
    CL_OP_PREPEND, 
    CL_OP_APPEND, 
    CL_OP_MC_PREPEND, 
    CL_OP_MC_APPEND, 
    CL_OP_TOUCH, 
    CL_OP_MC_TOUCH
};



// typedef struct cl_map_object_s cl_map_object;
// typedef struct cl_map_entry_s cl_map_entry;


// // MAP OBJECT (of MapEntry's)
// typedef struct cl_map_object_s {
//     joke_hash *hash;
// } cl_map_object;

// // MAP ENTRY
// struct cl_map_entry_s {
//     cl_type  type;
//     void    *s;
// };

// // THE JOKE HASH (throw away immediately)
// typedef struct joke_hash_s { void *k[100]; void *v[100]; int nels; } joke_hash;
// typedef int (*joke_hash_reduce_fn)(void *key, void *object, void *udata);
// void joke_hash_reduce(joke_hash *jhash, joke_hash_reduce_fn reduce_fn,
//                       void *udata);


// UDF's need to take a CL_LUA_BLOB -> cmsgpack.unpack() -> cl_map_object
// cl_object *unpack_to_map(char *luat, int luatlen);

/**
 * An object is the value in a bin, or it is used as a key
 * The object is typed according to the citrusleaf typing system
 * These are often stack allocated, and are assigned using the 'wrap' calls
 */
struct cl_object_s {
    cl_type         type;
    size_t          sz; 
    union {
        char *      str;    // note for str: sz is strlen (not strlen+1 
        void *      blob;
        int64_t     i64;    // easiest to have one large int type
    } u;
    void *          free;   // if this is set, this must be freed on destructuion   
};

/**
 * A bin is the bin name, and the value set or gotten
 */
struct cl_bin_s {
    char        bin_name[CL_BINNAME_SIZE];
    cl_object   object;
};


/**
 * A record structure containing the most common fileds of a record
 */
struct cl_rec_s {
    cf_digest   digest;
    uint32_t    generation;
    uint32_t    record_voidtime;
    cl_bin *    bins;
    int         n_bins;
};

/**
 * Structure used by functions which want to return a bunch of records
 */
struct cl_batchresult_s {
    pthread_mutex_t     lock;
    int                 numrecs;
    cl_rec *            records;
};

/**
 * An operation is the bin, plus the operator (write, read, add, etc)
 * This structure is used for the more complex 'operate' call,
 * which can specify simultaneous operations on multiple bins
 */
struct cl_operation_s {
    cl_bin              bin;
    cl_operator_type    op;
};
    
/**
 * Structure to map the internal address to the external address
 */
struct cl_addrmap_s {
    char *  orig;
    char *  alt;
};

struct cl_node_response_s {
    char    node_name[NODE_NAME_SIZE];
    cl_rv   node_response;
};

//
// write info structure
// There's a lot of info that can go into a write ---
struct cl_write_parameters_s {
    bool            unique;  // write unique - means success if didn't exist before
    bool            unique_bin;  // write unique bin - means success if the bin didn't exist before
    bool            use_generation;     // generation must be exact for write to succeed
    bool            use_generation_gt;  // generation must be less - good for backup & restore
    bool            use_generation_dup;    // on generation collision, create a duplicat
    uint32_t        generation;
    int             timeout_ms;
    uint32_t        record_ttl;    // seconds, from now, when the record would be auto-removed from the DBcd 
    cl_write_policy w_pol;
};

static inline void cl_write_parameters_set_default(cl_write_parameters *cl_w_p) {
    cl_w_p->unique = false;
    cl_w_p->unique_bin = false;
    cl_w_p->use_generation = false;
    cl_w_p->use_generation_gt = false;
    cl_w_p->use_generation_dup = false;
    cl_w_p->timeout_ms = 0;
    cl_w_p->record_ttl = 0;
    cl_w_p->w_pol = CL_WRITE_RETRY;
}

static inline void cl_write_parameters_set_generation( cl_write_parameters *cl_w_p, uint32_t generation) {
    cl_w_p->generation = generation;
    cl_w_p->use_generation = true;
}

static inline void cl_write_parameters_set_generation_gt( cl_write_parameters *cl_w_p, uint32_t generation) {
    cl_w_p->generation = generation;
    cl_w_p->use_generation_gt = true;
}

static inline void cl_write_parameters_set_generation_dup( cl_write_parameters *cl_w_p, uint32_t generation) {
    cl_w_p->generation = generation;
    cl_w_p->use_generation_dup = true;
}
