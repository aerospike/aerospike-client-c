/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"


typedef enum cl_scan_priority cl_scan_priority;

enum cl_scan_priority { 
    CL_SCAN_PRIORITY_AUTO, 
    CL_SCAN_PRIORITY_LOW, 
    CL_SCAN_PRIORITY_MEDIUM, 
    CL_SCAN_PRIORITY_HIGH
};

// scan_option info
typedef struct cl_scan_parameters_s {
    bool fail_on_cluster_change;    // honored by server: terminate scan if cluster in fluctuating state
    cl_scan_priority    priority;   // honored by server: priority of scan
    bool concurrent_nodes;          // honored on client: work on nodes in parallel or serially
    uint8_t threads_per_node;       // honored on client: have multiple threads per node. @TODO
} cl_scan_parameters;

static inline void cl_scan_parameters_set_default(cl_scan_parameters *cl_scan_p)
{
    cl_scan_p->fail_on_cluster_change = false;
    cl_scan_p->concurrent_nodes = false;
    cl_scan_p->threads_per_node = 1;    // not honored currently
    cl_scan_p->priority = CL_SCAN_PRIORITY_AUTO;
}



//
// get many call has the following properties:
// you can pass null either with the namespace or the set, and a large iteration will occur
// Memory available vanishes after 'return', if you want a copy, make a copy
// Non-zero return in the callback aborts the call
typedef int (*citrusleaf_get_many_cb) (char *ns, cf_digest *keyd, char *set, uint32_t generation, uint32_t record_ttl, cl_bin *bins, int n_bins, bool is_last, void *udata);


cl_rv
citrusleaf_scan(cl_cluster *asc, char *ns, char *set, cl_bin *bins, int n_bins, bool get_key, citrusleaf_get_many_cb cb, void *udata, bool nobindata);

// response is a vector of cl_node_response
cf_vector *
citrusleaf_scan_all_nodes (cl_cluster *asc, char *ns, char *set, cl_bin *bins, int n_bins, bool nobindata, uint8_t scan_pct,
    citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_p);

cl_rv
citrusleaf_scan_node (cl_cluster *asc, char *node_name, char *ns, char *set, cl_bin *bins, int n_bins, bool nobindata, uint8_t scan_pct,
    citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_p);

//
// Asynchronous calls to perform operations on many records.
//

// TODO
cf_vector *
citrusleaf_terminate_job_all_nodes(cl_cluster *asc, uint64_t job_uid);

// TODO
cf_vector *
citrusleaf_get_job_status_all_nodes(cl_cluster *asc, uint64_t job_uid);

//
// Get many digest
// This version of the call acts exactly as the get digest call, but takes an array
// of digests. Those digests will be retrieved with the same API conventions as the
// previous ones.
// To come: an array of keys, but it'll just be a wrapper on this.
//

cl_rv
citrusleaf_get_many_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key /*if true, retrieve key instead of simply digest*/, 
  citrusleaf_get_many_cb cb, void *udata);

//
// Get many digest without a callback
// This version of the batch-get call does not need the callback function. It will return an array of records. 
// The results are returned in an array. No ordering is guaranteed between the input digest array and 
// the returned rows. If the corresponding records for the digests are not found in the cluster, there wont 
// be any corresponding entry in the result array indicating that the records are missing. The caller must 
// call the citrusleaf_free_batchresult() to free the memory allocated during this operation.
cl_rv
citrusleaf_get_many_digest_direct(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_batchresult **br);

// Utility function to free the memory allocated by the citrusleaf_get_many_digest_direct() function
void
citrusleaf_free_batchresult(cl_batchresult *br);

//
// Key exists many digest
// This version of the call acts exactly as the exists digest call, but takes an array
// of digests. Those digests will be retrieved with the same API conventions as the
// previous ones.
// To come: an array of keys, but it'll just be a wrapper on this.
//

cl_rv
citrusleaf_exists_many_digest(cl_cluster *asc, char *ns, const cf_digest *digests, int n_digests, cl_bin *bins, int n_bins, bool get_key /*if true, retrieve key instead of simply digest*/, 
  citrusleaf_get_many_cb cb, void *udata);
