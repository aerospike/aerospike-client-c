/*
 * Copyright 2008-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

#include <citrusleaf/cl_types.h>
#include <aerospike/as_cluster.h>
#include <citrusleaf/cl_write.h>

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
 
cl_rv citrusleaf_get_all(as_cluster *asc, const char *ns, const char *set, const cl_object *key, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen, uint32_t* cl_ttl, int consistency_level, as_policy_replica replica);
cl_rv citrusleaf_get_all_digest(as_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen, uint32_t* cl_ttl, int consistency_level, as_policy_replica replica);
cl_rv citrusleaf_get_all_digest_getsetname(as_cluster *asc, const char *ns, const cf_digest *d, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen, char **setname, uint32_t* cl_ttl, int consistency_level, as_policy_replica replica);

/**
 * Put is like insert. Create a list of bins, and call this function to set them.
 */
cl_rv citrusleaf_put(as_cluster *asc, const char *ns, const char *set, const cl_object *key, const cf_digest *d, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, int commit_level);
cl_rv citrusleaf_put_digest(as_cluster *asc, const char *ns, const cf_digest *d, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, int commit_level);
cl_rv citrusleaf_put_digest_with_setname(as_cluster *asc, const char *ns, const char *set, const cf_digest *d, const cl_bin *bins, int n_bins, const cl_write_parameters *cl_w_p, int commit_level);
cl_rv citrusleaf_restore(as_cluster *asc, const char *ns, const char *set, const cl_object *key, const cf_digest *digest, const cl_bin *values, int n_values, const cl_write_parameters *cl_w_p, int commit_level);

/**
 * Get is like select in SQL. Create a list of bins to get, and call this function to retrieve
 * the values.
 */
cl_rv citrusleaf_get(as_cluster *asc, const char *ns, const char *set, const cl_object *key, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen, uint32_t* cl_ttl, int consistency_level, as_policy_replica replica);
cl_rv citrusleaf_get_digest(as_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen, uint32_t* cl_ttl, int consistency_level, as_policy_replica replica);

/**
 * Delete simply wipes this single key off the face of the earth.
 */
cl_rv citrusleaf_delete(as_cluster *asc, const char *ns, const char *set, const cl_object *key, const cf_digest *d, const cl_write_parameters *cl_w_p, int commit_level);
cl_rv citrusleaf_delete_digest(as_cluster *asc, const char *ns,  const cf_digest *d, const cl_write_parameters *cl_w_p, int commit_level);

/**
 * Efficiently determine if the key exists.
 *  (Note:  The bins are currently ignored but may be testable in the future.)
 */
cl_rv citrusleaf_exists_key(as_cluster *asc, const char *ns, const char *set, const cl_object *key, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen, uint32_t* cl_ttl, int consistency_level, as_policy_replica replica);
cl_rv citrusleaf_exists_digest(as_cluster *asc, const char *ns, const cf_digest *d, cl_bin *bins, int n_bins, int timeout_ms, uint32_t *cl_gen, uint32_t* cl_ttl, int consistency_level, as_policy_replica replica);
