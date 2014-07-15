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

#include <citrusleaf/cl_object.h>

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
#define CL_BINNAME_SIZE 15
#define CL_MAX_NUM_FUNC_ARGC    10 

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct cl_conn_s cl_conn;

// These numbers match with proto.h on the server (AS_PROTO_RESULT_FAIL....)

typedef enum cl_rv_e {

	// negative = client
	// positive = server


    CITRUSLEAF_FAIL_ASYNCQ_FULL             = -3,
    CITRUSLEAF_FAIL_TIMEOUT                 = -2,
    CITRUSLEAF_FAIL_CLIENT                  = -1,   // an out of memory or similar locally

    CITRUSLEAF_OK                           = 0,
    CITRUSLEAF_FAIL_UNKNOWN                 = 1,    // unknown failure on the server side

    // record not found
    // currently only used for reads, but with REPLACE ONLY op will be pertinent.
    CITRUSLEAF_FAIL_NOTFOUND                = 2,

    // can be a read or write error
    CITRUSLEAF_FAIL_GENERATION              = 3,    // likely a CAS write, and the write failed

    // bad parameter response from server
    CITRUSLEAF_FAIL_PARAMETER               = 4,    // you passed in bad parameters

    // digest/record exists when attempting to CREATE ONLY
    // SCOPE: WRITE ONLY
    CITRUSLEAF_FAIL_RECORD_EXISTS           = 5,

    // @todo ??
    CITRUSLEAF_FAIL_BIN_EXISTS              = 6,

    // cluster errors
    CITRUSLEAF_FAIL_CLUSTER_KEY_MISMATCH    = 7,
    CITRUSLEAF_FAIL_PARTITION_OUT_OF_SPACE  = 8,

    // collapsible timeout, server timeout is based on client-sent value
    // for the most part
    CITRUSLEAF_FAIL_SERVERSIDE_TIMEOUT      = 9,

    // xdr errors
    CITRUSLEAF_FAIL_NOXDS                   = 10,

    // server (node) not avaialble (??)
    CITRUSLEAF_FAIL_UNAVAILABLE             = 11,

    // bin operation cannot be performed on bin b/c of its type
    // SCOPE: WRITE ONLY
    CITRUSLEAF_FAIL_INCOMPATIBLE_TYPE       = 12,   // specified operation cannot be performed on that data type

    // record is larger than the write block (1MB)
    // SCOPE: WRITE ONLY
    CITRUSLEAF_FAIL_RECORD_TOO_BIG          = 13,

    // hot key - essentially the record's transaction proc queue is full
    CITRUSLEAF_FAIL_KEY_BUSY                = 14,

    // scan was aborted ... but why?
    CITRUSLEAF_FAIL_SCAN_ABORT				= 15,

    // Server does not (yet) support this function
    CITRUSLEAF_FAIL_UNSUPPORTED_FEATURE		= 16,

    // Bin-level replace-only supported on server but not on client.
    CITRUSLEAF_FAIL_BIN_NOT_FOUND			= 17,

    // Storage device(s) can't keep up with the current write load.
    CITRUSLEAF_FAIL_DEVICE_OVERLOAD			= 18,

    // Record key sent with transaction did not match key stored on server.
    CITRUSLEAF_FAIL_KEY_MISMATCH			= 19,

	CITRUSLEAF_QUERY_END					= 50,
	CITRUSLEAF_SECURITY_NOT_SUPPORTED		= 51,
	CITRUSLEAF_SECURITY_NOT_ENABLED			= 52,
	CITRUSLEAF_SECURITY_SCHEME_NOT_SUPPORTED = 53,
	CITRUSLEAF_INVALID_COMMAND				= 54,
	CITRUSLEAF_INVALID_FIELD				= 55,
	CITRUSLEAF_ILLEGAL_STATE				= 56,
	CITRUSLEAF_INVALID_USER					= 60,
	CITRUSLEAF_USER_ALREADY_EXISTS			= 61,
	CITRUSLEAF_INVALID_PASSWORD				= 62,
	CITRUSLEAF_EXPIRED_PASSWORD				= 63,
	CITRUSLEAF_FORBIDDEN_PASSWORD			= 64,
	CITRUSLEAF_INVALID_CREDENTIAL			= 65,
	CITRUSLEAF_INVALID_ROLE					= 70,
	CITRUSLEAF_INVALID_PRIVILEGE			= 71,
	CITRUSLEAF_NOT_AUTHENTICATED			= 80,
	CITRUSLEAF_ROLE_VIOLATION				= 81,
	
    // ???
    CITRUSLEAF_FAIL_INVALID_DATA              = 99,

    // UDF RANGE 100-110
    CITRUSLEAF_FAIL_UDF_BAD_RESPONSE        = 100,
    CITRUSLEAF_FAIL_UDF_LUA_EXECUTION       = 101,
    CITRUSLEAF_FAIL_LUA_FILE_NOTFOUND       = 102,

    // Secondary Index Query Codes 200 - 230
    CITRUSLEAF_FAIL_INDEX_FOUND             = 200,
    CITRUSLEAF_FAIL_INDEX_NOTFOUND          = 201,
    CITRUSLEAF_FAIL_INDEX_OOM               = 202,
    CITRUSLEAF_FAIL_INDEX_NOTREADABLE       = 203,
    CITRUSLEAF_FAIL_INDEX_GENERIC           = 204,
    CITRUSLEAF_FAIL_INDEX_NAME_MAXLEN       = 205,
    CITRUSLEAF_FAIL_INDEX_MAXCOUNT          = 206,
    
    CITRUSLEAF_FAIL_QUERY_ABORTED           = 210,
    CITRUSLEAF_FAIL_QUERY_QUEUEFULL         = 211,
    CITRUSLEAF_FAIL_QUERY_TIMEOUT           = 212,
    CITRUSLEAF_FAIL_QUERY_GENERIC           = 213
} cl_rv;

typedef enum cl_rvclient_e {
    CITRUSLEAF_FAIL_DC_DOWN     = 1,
    CITRUSLEAF_FAIL_DC_UP       = 2
} cl_rvclient;


typedef enum cl_operator_type_e { 
    CL_OP_WRITE, 		// 0
    CL_OP_READ, 		// 1
    CL_OP_INCR, 		// 2
    CL_OP_MC_INCR, 		// 3
    CL_OP_PREPEND, 		// 4
    CL_OP_APPEND, 		// 5
    CL_OP_MC_PREPEND, 	// 6
    CL_OP_MC_APPEND, 	// 7
    CL_OP_TOUCH, 		// 8
    CL_OP_MC_TOUCH		// 9
} cl_operator;

/**
 * A bin is the bin name, and the value set or gotten
 */
typedef struct cl_bin_s {
    char        bin_name[CL_BINNAME_SIZE];
    cl_object   object;
} cl_bin;

/**
 * A record structure containing the most common fileds of a record
 */
typedef struct cl_rec_s {
    cf_digest   digest;
    uint32_t    generation;
    uint32_t    record_voidtime;
    cl_bin *    bins;
    int         n_bins;
} cl_rec;

/**
 * Structure used by functions which want to return a bunch of records
 */
typedef struct cl_batchresult_s {
    pthread_mutex_t     lock;
    int                 numrecs;
    cl_rec *            records;
} cl_batchresult;

/**
 * An operation is the bin, plus the operator (write, read, add, etc)
 * This structure is used for the more complex 'operate' call,
 * which can specify simultaneous operations on multiple bins
 */
typedef struct cl_operation_s {
    cl_bin              bin;
    cl_operator         op;
} cl_operation;
    
/**
 * Structure to map the internal address to the external address
 */
typedef struct cl_addrmap {
    char *  orig;
    char *  alt;
} cl_addrmap;

/**
 * Callback function type used by batch and scan
 */
typedef int (*citrusleaf_get_many_cb) (char *ns, cf_digest *keyd, char *set,
		cl_object *key, int result, uint32_t generation, uint32_t ttl,
		cl_bin *bins, uint16_t n_bins, void *udata);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

void citrusleaf_bins_free(cl_bin * bins, int n_bins);
int citrusleaf_copy_bins(cl_bin ** destbins, cl_bin * srcbins, int n_bins);

