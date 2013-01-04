/*
 *      sproc.h
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

#include "types.h"
#include "cluster.h"
#include "scan.h"

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef enum cl_script_func_t cl_script_func_t;
typedef enum cl_script_lang_t cl_script_lang_t;
typedef struct cl_sproc_params cl_sproc_params;

enum cl_script_func_t {
    CL_SCRIPT_FUNC_TYPE_MAP,
    CL_SCRIPT_FUNC_TYPE_REDUCE,
    CL_SCRIPT_FUNC_TYPE_FINALIZE,
    CL_SCRIPT_FUNC_TYPE_RECORD,
};

enum cl_script_lang_t {
    CL_SCRIPT_LANG_LUA
};

struct cl_sproc_params {
    int         num_param;
    char        *param_key[CL_MAX_NUM_FUNC_ARGC];
    cl_object   *param_val[CL_MAX_NUM_FUNC_ARGC];
};

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

cl_rv citrusleaf_sproc_package_set(cl_cluster *asc, const char *package, const char *content, char **err_str, cl_script_lang_t lang);
cl_rv citrusleaf_sproc_package_get_content(cl_cluster *asc, const char *package_name, char **content, int *content_len, cl_script_lang_t lang);
cl_rv citrusleaf_sproc_package_delete(cl_cluster *asc, const char *package, cl_script_lang_t lang);
cl_rv citrusleaf_sproc_package_list(cl_cluster *asc, char ***package_names, int *n_packages, cl_script_lang_t lang);

cf_vector * citrusleaf_sproc_execute_all_nodes(cl_cluster *asc, char *ns, char *set, const char *package_name, const char *sproc_name, const cl_sproc_params *sproc_params, citrusleaf_get_many_cb cb, void *udata, cl_scan_parameters *scan_p, uint64_t *job_uid_p);

// Record-level Stored Procedure
cl_sproc_params *citrusleaf_sproc_params_create();
void citrusleaf_sproc_params_destroy(cl_sproc_params *sproc_params);
cl_rv citrusleaf_sproc_params_add_string(cl_sproc_params *sproc_params, const char *param_key, const char *param_value);
cl_rv citrusleaf_sproc_execute(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const char *package_name, const char *sproc_name, const cl_sproc_params *sproc_params, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen);
cl_rv citrusleaf_sproc_exec_cb(cl_cluster *asc, const char *ns, const char *set, const cl_object *key, const char *package_name, const char *fname, const cl_sproc_params *sproc_params, cl_bin **bins, int *n_bins, int timeout_ms, uint32_t *cl_gen, citrusleaf_get_many_cb cb, void *udata);
