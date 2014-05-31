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

#include <citrusleaf/cl_types.h>
#include <aerospike/as_cluster.h>
#include <citrusleaf/cl_scan.h>

#include <aerospike/as_list.h>
#include <stdbool.h>

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef uint8_t cl_scan_pct;

typedef enum cl_scan_udf_execution_type_s { 
    CL_SCAN_UDF_NONE,             // Scan w/o udfs -- noop
    CL_SCAN_UDF_CLIENT_RECORD,          // Job per record
    CL_SCAN_UDF_BACKGROUND,             // Background job no response
} udf_execution_type;

typedef struct cl_scan_udf_s {
    udf_execution_type	 	   	type;
    char *              		filename;
    char *              		function;
    as_list *           		arglist;
} cl_scan_udf;

typedef struct cl_scan_parameters {
    bool                fail_on_cluster_change; // honored by server: terminate scan if cluster in fluctuating state
    cl_scan_priority    priority;               // honored by server: priority of scan
    cl_scan_pct         pct;                    // honored by server: % of data to be scanned
    bool                concurrent;				// honored by client: if all the nodes should be scanned in parallel or not
} cl_scan_params;

typedef struct cl_scan_s {
    char            * ns;
    char            * setname;
    cl_scan_params    params;
    cl_scan_udf       udf;
    void            * res_streamq;
    uint64_t          job_id;
} cl_scan;

typedef struct cl_scan_response_record_t {
    char        * ns;
    cf_digest     keyd;
    char        * set;
    uint32_t      generation;
    uint32_t      record_ttl;
    cl_bin      * bins;
    int           n_bins;
    bool          ismalloc;
} cl_scan_response_rec;


typedef bool (* cl_scan_cb) (const as_val * val, void * udata);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Allocates and initializes a new cl_scan
 */
cl_scan * cl_scan_new(const char *ns, const char *setname, uint64_t *job_id);

/**
 * Initializes an cl_scan
 */
cl_scan * cl_scan_init(cl_scan *scan , const char *ns, const char *setname, uint64_t *job_id);
cl_rv cl_scan_udf_init(cl_scan_udf * udf, udf_execution_type type, const char * filename, const char * function, as_list * arglist);
cl_rv cl_scan_params_init(cl_scan_params * oparams, cl_scan_params *iparams);

/**
 * Destroy and free an cl_scan
 */
void      cl_scan_destroy(cl_scan * scan);

/**
 * Initializing udfs
 */
int       cl_scan_foreach       (cl_scan *scan, const char *filename, const char *function, as_list *arglist);

/**
 * Return vector of cl_rv for each node
 */
cl_rv citrusleaf_udf_scan_node        (as_cluster *asc, cl_scan *scan, char *node_name, int( *callback)(as_val *, void *), void * udata);
cf_vector * citrusleaf_udf_scan_all_nodes   (as_cluster *asc, cl_scan *scan, int( *callback)(as_val *, void *), void * udata);
cf_vector * citrusleaf_udf_scan_background  (as_cluster *asc, cl_scan *scan);
cl_rv citrusleaf_udf_scan_node_background  (as_cluster *asc, cl_scan *scan, char *node_name);

/*
 * Init and destroy for client scan environment. Should be called for once per cluster
 * instance before performing citrusleaf scan
 */
int    cl_cluster_scan_init(as_cluster* asc);
void   cl_cluster_scan_shutdown(as_cluster* asc);
