/*
 *     cl_scan.h
 *
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

#pragma once

#include "cl_types.h"
#include "cluster.h"
/******************************************************************************
 * TYPES
 ******************************************************************************/
typedef uint8_t as_scan_pct;

typedef enum as_scan_priority { 
    AS_SCAN_PRIORITY_AUTO, 
    AS_SCAN_PRIORITY_LOW, 
    AS_SCAN_PRIORITY_MEDIUM, 
    AS_SCAN_PRIORITY_HIGH
} as_scan_priority;

typedef enum as_scan_udf_type_s { 
    AS_SCAN_UDF_NONE,             // Background job no response
    AS_SCAN_UDF_BACKGROUND,             // Background job no response
    AS_SCAN_UDF_CLIENT_RECORD,          // Job per record
} as_scan_udf_type;

typedef struct as_scan_udf_s {
    as_scan_udf_type    type;
    char *              filename;
    char *              function;
    as_list *           arglist;
} as_scan_udf;

typedef struct as_scan_parameters {
    bool                fail_on_cluster_change; // honored by server: terminate scan if cluster in fluctuating state
    as_scan_priority    priority;               // honored by server: priority of scan
    bool                concurrent_nodes;       // honored on client: work on nodes in parallel or serially
    uint8_t             threads_per_node;       // honored on client: have multiple threads per node. @TODO
    bool                nobindata;              // honored by server: No bin data is returned
    						// only for non UDF scans
    as_scan_pct         pct;
    bool                get_key;
} as_scan_params;

typedef struct as_scan_s {
    char            * ns;
    char            * setname;
    as_scan_params    params;
    as_scan_udf       udf;
    void            * res_streamq;
    uint64_t          job_id;
} as_scan;

typedef struct as_scan_response_record_t {
    char        * ns;
    cf_digest     keyd;
    char        * set;
    uint32_t      generation;
    uint32_t      record_ttl;
    cl_bin      * bins;
    int           n_bins;
    bool          ismalloc;
} as_scan_response_rec;

struct as_node_response_s {
    char    node_name[NODE_NAME_SIZE];
    cl_rv   node_response;
};

typedef bool (* as_scan_cb) (const as_val * val, void * udata);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Allocates and initializes a new as_scan
 */
as_scan * as_scan_new(const char *ns, const char *setname);

/**
 * Initializes an as_scan
 */
as_scan * as_scan_init(as_scan *scan , const char *ns, const char *setname);
cl_rv as_scan_udf_init(as_scan_udf * udf, as_scan_udf_type type, const char * filename, const char * function, as_list * arglist);

/**
 * Destroy and free an as_scan
 */
void      as_scan_destroy(as_scan * scan);

/**
 * Query Builders
 */
int       as_scan_foreach       (as_scan *scan, const char *filename, const char *function, as_list *arglist);

/**
 * Return vector of cl_rv for each node
 */
cl_rv       as_scan_node        (cl_cluster *asc, const as_scan *scan, char *node_name, void *udata, bool( *foreach)(const as_val *, void *));
cf_vector * as_scan_all_node    (cl_cluster *asc, const as_scan *scan, void *udata, bool( *foreach)(const as_val *, void *));
cl_rv citrusleaf_scan_background  (cl_cluster *asc, const as_scan *scan, void *udata);

/*
 * Init and destroy for client scan environment. Should be called for once per client
 * instance before performing citrusleaf query
 */
int    citrusleaf_scan_init();
void   citrusleaf_scan_shutdown();
