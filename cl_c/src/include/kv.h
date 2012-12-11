/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"

//
// get-all will malloc an array of values and return all current values for a row.
// thus, it is SELECT * in SQL. So pass in a pointer to cl_value to be filled, and a
// pointer-to-int to know how many.
// 
// The memory contract with get_all is that the bins pointer (*bins) must be freed by the caller.
// The data you've actually retrieved (cl_object->u.str ; cl->object->u.blob) has been allocated using malloc.
// You may use it for your own purposes, transfer it to another program, or you must free it.
// citrusleaf_object_free() will free the internal memory in these cases,
// or you can call citrusleaf_bins_free and it will take care of all memory.
//
// Note this is different from getting a specific set of bins, where the bin array was passed in.
// (the simple 'get') See that call for information there.
 
cl_rv citrusleaf_get_all(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_get_all_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_get_all_digest_getsetname(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen, char **setname);

//
// Put is like insert. Create a list of bins, and call this function to set them.
//
    
cl_rv citrusleaf_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_put_digest(cl_cluster *asc, const char *ns, const cf_digest *d, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_put_replace(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_restore(cl_cluster *asc, const char *ns, const cf_digest *digest, const char *set, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p);

//Async versions of the put calls
cl_rv citrusleaf_async_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata);
cl_rv citrusleaf_async_put_digest(cl_cluster *asc, const char *ns, const cf_digest *d, char *setname, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata);
cl_rvclient citrusleaf_check_cluster_health(cl_cluster *asc);
void citrusleaf_sleep_for_tender(cl_cluster *asc);

//
// Get is like select in SQL. Create a list of bins to get, and call this function to retrieve
// the values.

cl_rv citrusleaf_get(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_get_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);

//
// Delete simply wipes this single key off the face of the earth.
//

cl_rv citrusleaf_delete(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_delete_digest(cl_cluster *asc, const char *ns,  const cf_digest *d, const cl_write_parameters *cl_w_p);

//
// Efficiently determine if the key exists.
//  (Note:  The bins are currently ignored but may be testable in the future.)
//

cl_rv citrusleaf_exists_key(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_exists_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);
