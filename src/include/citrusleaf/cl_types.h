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

#include <citrusleaf/cl_object.h>

#include <inttypes.h>
#include <stdbool.h>
#include <netinet/in.h>
 
/**
 * Hack for the sake of XDR. XDR includes the main CF libs.
 * We do not want to include them again from client API
 */
#ifndef XDR
#include <citrusleaf/cf_atomic.h>
// #include <citrusleaf/cf_log.h>
#include <citrusleaf/cf_ll.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_vector.h>
#include <citrusleaf/cf_queue.h>
#include <citrusleaf/cf_digest.h>
#include <citrusleaf/cf_shash.h>
#include <citrusleaf/cf_rchash.h>
#endif

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define STACK_BUF_SZ (1024 * 16) // provide a safe number for your system - linux tends to have 8M stacks these days
#define DEFAULT_PROGRESS_TIMEOUT 50
#define NODE_NAME_SIZE 20
#define CL_BINNAME_SIZE 15
#define CL_MAX_NUM_FUNC_ARGC    10 

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct cl_conn_s cl_conn;

typedef int cl_rv;

typedef enum cl_operator_type_e { 
    CL_OP_WRITE, 		// 0
    CL_OP_READ, 		// 1
    CL_OP_INCR, 		// 2
    CL_OP_MC_INCR, 		// 3
    CL_OP_PREPEND, 		// 4
    CL_OP_APPEND, 		// 5
    CL_OP_MC_PREPEND, 	// 6
    CL_OP_MC_APPEND, 	// 7
    CL_OP_TOUCH, 		// 8
    CL_OP_MC_TOUCH		// 9
} cl_operator;

/**
 * A bin is the bin name, and the value set or gotten
 */
typedef struct cl_bin_s {
    char        bin_name[CL_BINNAME_SIZE];
    cl_object   object;
} cl_bin;

/**
 * A record structure containing the most common fileds of a record
 */
typedef struct cl_rec_s {
    cf_digest   digest;
    uint32_t    generation;
    uint32_t    record_voidtime;
    cl_bin *    bins;
    int         n_bins;
} cl_rec;

/**
 * Structure used by functions which want to return a bunch of records
 */
typedef struct cl_batchresult_s {
    pthread_mutex_t     lock;
    int                 numrecs;
    cl_rec *            records;
} cl_batchresult;

/**
 * An operation is the bin, plus the operator (write, read, add, etc)
 * This structure is used for the more complex 'operate' call,
 * which can specify simultaneous operations on multiple bins
 */
typedef struct cl_operation_s {
    cl_bin              bin;
    cl_operator         op;
} cl_operation;
    
/**
 * Structure to map the internal address to the external address
 */
typedef struct cl_addrmap {
    char *  orig;
    char *  alt;
} cl_addrmap;

/**
 * Callback function type used by batch and scan
 */
typedef int (*citrusleaf_get_many_cb) (char *ns, cf_digest *keyd, char *set,
		cl_object *key, int result, uint32_t generation, uint32_t ttl,
		cl_bin *bins, uint16_t n_bins, void *udata);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

void citrusleaf_bins_free(cl_bin * bins, int n_bins);
int citrusleaf_copy_bins(cl_bin ** destbins, cl_bin * srcbins, int n_bins);

