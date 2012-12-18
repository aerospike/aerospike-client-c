/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"
#include "sproc.h"

typedef struct cl_mr_job {
    char        *package;
    char        *map_fname;
    char        *rdc_fname;
    char        *fnz_fname;
    int         map_argc;
    char        *map_argk[CL_MAX_NUM_FUNC_ARGC];
    cl_object   *map_argv[CL_MAX_NUM_FUNC_ARGC];
    int         rdc_argc;
    char        *rdc_argk[CL_MAX_NUM_FUNC_ARGC];
    cl_object   *rdc_argv[CL_MAX_NUM_FUNC_ARGC];
    int         fnz_argc;
    char        *fnz_argk[CL_MAX_NUM_FUNC_ARGC];
    cl_object   *fnz_argv[CL_MAX_NUM_FUNC_ARGC];
} cl_mr_job; 


cl_mr_job *citrusleaf_mr_job_create(const char *package, const char *map_fname, const char *rdc_fname, const char *fnz_fname );
cl_rv citrusleaf_mr_job_add_parameter_string(cl_mr_job *mr_job, cl_script_func_t ftype, const char *key, const char *value);
cl_rv citrusleaf_mr_job_add_parameter_numeric(cl_mr_job *mr_job, cl_script_func_t ftype, const char *key, uint64_t value);
cl_rv citrusleaf_mr_job_add_parameter_blob(cl_mr_job *mr_job, cl_script_func_t ftype, cl_type blobtype, const char *key, const uint8_t *value, int val_len);
void citrusleaf_mr_job_destroy(cl_mr_job *mr_job);
