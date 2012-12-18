/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "object.h"

#include <inttypes.h>
#include <stdbool.h>
#include <netinet/in.h>
 
/**
 * Hack for the sake of XDS. XDS includes the main CF libs. 
 * We do not want to include them again from client API
 */
#ifndef XDS
#include <citrusleaf/cf_atomic.h>
// #include <citrusleaf/cf_log.h>
#include <citrusleaf/cf_ll.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_vector.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_alloc.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_shash.h>
#include <citrusleaf/cf_rchash.h>
#endif

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define STACK_BUF_SZ (1024 * 16) // provide a safe number for your system - linux tends to have 8M stacks these days
#define DEFAULT_PROGRESS_TIMEOUT 50
#define NODE_NAME_SIZE 20
#define CL_BINNAME_SIZE 16
#define CL_MAX_NUM_FUNC_ARGC    10 

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct cl_conn_s cl_conn;

typedef enum cl_rv_e cl_rv;
typedef enum cl_rvclient_e cl_rvclient;
typedef enum cl_operator_type_e cl_operator;

typedef struct cl_bin_s cl_bin;
typedef struct cl_rec_s cl_rec;
typedef struct cl_batchresult_s cl_batchresult;
typedef struct cl_operation_s cl_operation;
typedef struct cl_addrmap cl_addrmap;


enum cl_rv_e {
    CITRUSLEAF_FAIL_ASYNCQ_FULL             = -3,
    CITRUSLEAF_FAIL_TIMEOUT                 = -2,
    CITRUSLEAF_FAIL_CLIENT                  = -1,   // an out of memory or similar locally
    CITRUSLEAF_OK                           = 0,
    CITRUSLEAF_FAIL_UNKNOWN                 = 1,    // unknown failure on the server side
    CITRUSLEAF_FAIL_NOTFOUND                = 2,
    CITRUSLEAF_FAIL_GENERATION              = 3,    // likely a CAS write, and the write failed
    CITRUSLEAF_FAIL_PARAMETER               = 4,    // you passed in bad parameters
    CITRUSLEAF_FAIL_KEY_EXISTS              = 5,
    CITRUSLEAF_FAIL_BIN_EXISTS              = 6,
    CITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH    = 7,
    CITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE  = 8,
    CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT      = 9,
    CITRUSLEAF_FAIL_NOXDS                   = 10,
    CITRUSLEAF_FAIL_UNAVAILABLE             = 11,
    CITRUSLEAF_FAIL_INCOMPATIBLE_TYPE       = 12,   // specified operation cannot be performed on that data type
    CITRUSLEAF_FAIL_RECORD_TOO_BIG          = 13,
    CITRUSLEAF_FAIL_KEY_BUSY                = 14,

    // UDF RANGE 100-110
    CITRUSLEAF_FAIL_UDF_BAD_RESPONSE        = 100,

    // Secondary Index Query Codes 200 - 230
    CITRUSLEAF_FAIL_INDEX_KEY_NOTFOUND      = 200,
    CITRUSLEAF_FAIL_INDEX_TYPE_MISMATCH     = 201,
    CITRUSLEAF_FAIL_INDEX_NOTFOUND          = 202,
    CITRUSLEAF_FAIL_INDEX_OOM               = 203,
    CITRUSLEAF_FAIL_INDEX_GENERIC           = 204,
    CITRUSLEAF_FAIL_INDEX_EXISTS            = 205,
    CITRUSLEAF_FAIL_INDEX_SINGLEBIN_NS      = 206,
    CITRUSLEAF_FAIL_INDEX_UNKNOWN_TYPE      = 207,
    CITRUSLEAF_FAIL_INDEX_FOUND             = 208,
    CITRUSLEAF_FAIL_INDEX_NOTREADABLE       = 209,
    CITRUSLEAF_FAIL_QUERY_ABORTED           = 210,
    CITRUSLEAF_FAIL_QUERY_QUEUEFULL         = 211
};

enum cl_rvclient_e {
    CITRUSLEAF_FAIL_DC_DOWN     = 1,
    CITRUSLEAF_FAIL_DC_UP       = 2
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
    cl_operator         op;
};
    
/**
 * Structure to map the internal address to the external address
 */
struct cl_addrmap {
    char *  orig;
    char *  alt;
};

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

void citrusleaf_bins_free(cl_bin * bins, int n_bins);
int citrusleaf_copy_bins(cl_bin ** destbins, cl_bin * srcbins, int n_bins);

