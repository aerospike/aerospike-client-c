/*
 *      cl_scan.h
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

typedef struct cl_scan_param_field_s cl_scan_param_field;
typedef struct cl_scan_parameters_s cl_scan_parameters;
typedef struct cl_node_response_s cl_node_response;

/**
 * get many call has the following properties:
 * you can pass null either with the namespace or the set, and a large iteration will occur
 * Memory available vanishes after 'return', if you want a copy, make a copy
 * Non-zero return in the callback aborts the call
 */

typedef enum cl_scan_priority { 
    CL_SCAN_PRIORITY_AUTO, 
    CL_SCAN_PRIORITY_LOW, 
    CL_SCAN_PRIORITY_MEDIUM, 
    CL_SCAN_PRIORITY_HIGH
} cl_scan_priority;

/**
 * scan fields
 * left-to-right bits
 * 0-3 priority hint = ClScanningPriority
 * 4 = failOnClusterChange
 * 5-7 = unused 
 * 8-15 = scan_pct
 */
 struct cl_scan_param_field_s {
    uint8_t byte1;
    uint8_t scan_pct;
};

/**
 * scan_option info
 */
struct cl_scan_parameters_s {
    bool fail_on_cluster_change;    // honored by server: terminate scan if cluster in fluctuating state
    cl_scan_priority    priority;   // honored by server: priority of scan
    bool concurrent_nodes;          // honored on client: work on nodes in parallel or serially
    uint8_t threads_per_node;       // honored on client: have multiple threads per node. @TODO
};

struct cl_node_response_s {
    char    node_name[NODE_NAME_SIZE];
    cl_rv   node_response;
};

/******************************************************************************
 * INLINE FUNCTIONS
 ******************************************************************************/

static inline void cl_scan_parameters_set_default(cl_scan_parameters *cl_scan_p) {
    cl_scan_p->fail_on_cluster_change = false;
    cl_scan_p->concurrent_nodes = false;
    cl_scan_p->threads_per_node = 1;    // not honored currently
    cl_scan_p->priority = CL_SCAN_PRIORITY_AUTO;
}


/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/


cl_rv citrusleaf_scan(cl_cluster *asc, char *ns, char *set, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, bool nobindata);

/**
 * response is a vector of cl_node_response
 */
cf_vector * citrusleaf_scan_all_nodes (
    cl_cluster *asc, char *ns, char *set, cl_bin *bins, int n_bins, bool nobindata, uint8_t scan_pct,
    citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_p
    );

cl_rv citrusleaf_scan_node (
    cl_cluster *asc, char *node_name, char *ns, char *set, cl_bin *bins, int n_bins, bool nobindata, uint8_t scan_pct,
    citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_p);

//
// Asynchronous calls to perform operations on many records.
//

// TODO
cf_vector * citrusleaf_terminate_job_all_nodes(cl_cluster *asc, uint64_t job_uid);

// TODO
cf_vector * citrusleaf_get_job_status_all_nodes(cl_cluster *asc, uint64_t job_uid);


