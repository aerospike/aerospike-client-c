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
 
#include <inttypes.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_udf.h>

extern cl_rv aerospike_lset_create(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object *o_keyp,
        const char * bin_name,
        as_map * create_spec,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_create_using_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * bin_name,
        as_map * creation_args,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_create_and_insert(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        as_val * valuep,
        as_map * creation_spec,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_insert(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        as_val * valuep,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_search(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        as_val * search_valuep,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_search_then_filter(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        as_val * search_valuep,
        const char * filter,
        as_list * function_args,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_delete(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        as_val * delete_valuep,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_size(
        uint32_t   * size,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        uint32_t timeout_ms );

extern cl_rv aerospike_lset_config(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * bin_name,
        uint32_t timeout_ms );
