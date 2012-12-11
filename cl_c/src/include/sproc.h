/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"


typedef enum cl_script_func_t {
    CL_SCRIPT_FUNC_TYPE_MAP,
    CL_SCRIPT_FUNC_TYPE_REDUCE,
    CL_SCRIPT_FUNC_TYPE_FINALIZE,
    CL_SCRIPT_FUNC_TYPE_RECORD,
} cl_script_func_t;


typedef enum cl_script_lang_t {
    CL_SCRIPT_LANG_LUA
} cl_script_lang_t;

typedef struct cl_sproc_params {
    int         num_param;
    char        *param_key[CL_MAX_NUM_FUNC_ARGC];
    cl_object   *param_val[CL_MAX_NUM_FUNC_ARGC];
} cl_sproc_params;




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
