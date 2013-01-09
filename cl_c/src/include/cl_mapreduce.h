/*
 *      cl_mapreduce.h
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
#include "cl_sproc.h"

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
