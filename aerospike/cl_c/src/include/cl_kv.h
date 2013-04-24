/*
 *      cl_kv.h
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
#include "cl_cluster.h"
#include "cl_write.h"

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * get-all will malloc an array of values and return all current values for a row.
 * thus, it is SELECT * in SQL. So pass in a pointer to cl_value to be filled, and a
 * pointer-to-int to know how many.
 * 
 * The memory contract with get_all is that the bins pointer (*bins) must be freed by the caller.
 * The data you've actually retrieved (cl_object->u.str ; cl->object->u.blob) has been allocated using malloc.
 * You may use it for your own purposes, transfer it to another program, or you must free it.
 * citrusleaf_object_free() will free the internal memory in these cases,
 * or you can call citrusleaf_bins_free and it will take care of all memory.
 *
 * Note this is different from getting a specific set of bins, where the bin array was passed in.
 * (the simple 'get') See that call for information there.
 */
 
cl_rv citrusleaf_get_all(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_get_all_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_get_all_digest_getsetname(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen, char **setname);

/**
 * Put is like insert. Create a list of bins, and call this function to set them.
 */
cl_rv citrusleaf_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_put_digest(cl_cluster *asc, const char *ns, const cf_digest *d, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_put_replace(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_restore(cl_cluster *asc, const char *ns, const cf_digest *digest, const char *set, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p);

/**
 * Async versions of the put calls
 */
cl_rv citrusleaf_async_put(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata);
cl_rv citrusleaf_async_put_digest(cl_cluster *asc, const char *ns, const cf_digest *d, char *setname, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, uint64_t trid, void *udata);
cl_rvclient citrusleaf_check_cluster_health(cl_cluster *asc);
cl_rv citrusleaf_async_delete_digest(cl_cluster *asc, const char *ns, const cf_digest *digest, const cl_write_parameters *cl_w_p, void *udata);
void citrusleaf_sleep_for_tender(cl_cluster *asc);

/**
 * Get is like select in SQL. Create a list of bins to get, and call this function to retrieve
 * the values.
 */
cl_rv citrusleaf_get(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_get_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);

/**
 * Delete simply wipes this single key off the face of the earth.
 */
cl_rv citrusleaf_delete(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const cl_write_parameters *cl_w_p);
cl_rv citrusleaf_delete_digest(cl_cluster *asc, const char *ns,  const cf_digest *d, const cl_write_parameters *cl_w_p);

/**
 * Efficiently determine if the key exists.
 *  (Note:  The bins are currently ignored but may be testable in the future.)
 */
cl_rv citrusleaf_exists_key(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_exists_digest(cl_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen);
