/*
 *  Citrusleaf Tools: Aerospike Large Set (LSET)
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once
 
#include <inttypes.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>

#include "citrusleaf.h"
#include "cl_udf.h"

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
