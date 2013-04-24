/*
 *  Citrusleaf Large Object Stack C API and Validation Program
 *  aerospike_lstack.c - Validates Large Stack  procedure functionality
 *
 *  Citrusleaf Tools: Large Stack Object (LSTACK)
 *
 *  Copyright 2012 by Citrusleaf, Aerospike Inc.  All rights reserved.
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

// NOTE: These lines should be deleted -- check on this:  TODO: 
// From here:::::
#include <openssl/rand.h>

#define VALUE_UNINIT    0xFFFFFFFFFFFFFFFF
#define VALUE_DELETED   0xFFFFFFFFFFFFFFFE

extern uint64_t rand_64();
// TO HERE ::::

// =====================================================================
// NOTE: New API change as of March 27, 2013 (tjl)
// =====================================================================
extern cl_rv aerospike_lstack_create(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        as_map     * creation_spec,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_create_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        as_map     * creation_spec,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_push(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        as_val * lso_valuep,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_push_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        as_val * lso_valuep,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_create_and_push(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        as_val * lso_valuep,
        as_map     * creation_spec,
        uint32_t timeout_ms );


extern cl_rv aerospike_lstack_create_and_push_with_keystring(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        as_val * lso_valuep,
        as_map     * creation_spec,
        uint32_t timeout_ms );

extern cl_rv  aerospike_lstack_peek(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        int peek_count,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_peek_with_keystring(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        int peek_count,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_peek_then_filter(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        int peek_count,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_peek_then_filter_with_keystring(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const char * keystr,
        const char * lso_bin_name,
        int peek_count,
        const char * udf_name,
        as_list * function_args,
        uint32_t timeout_ms );

extern cl_rv aerospike_lstack_trim(
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        int remainder_count,
        uint32_t timeout_ms );

// note: currently no helper function for trim()

extern int aerospike_lstack_size(
        uint32_t   * size,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        uint32_t timeout_ms );

// note: currently no helper function for size()

extern int aerospike_lstack_config(
        as_result ** resultpp,
        cl_cluster * asc,
        const char * namespace,
        const char * set,
        const cl_object * o_keyp,
        const char * lso_bin_name,
        uint32_t timeout_ms );

// note: currently no helper function for size()
