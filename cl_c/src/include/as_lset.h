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

#ifndef ATOMIC_INT 
typedef struct atomic_int_s {
        uint64_t        val;
            pthread_mutex_t lock;
} atomic_int;
#define ATOMIC_INT
#endif

extern void *start_counter_thread(atomic_int *records, atomic_int *bytes);
extern void stop_counter_thread(void *id);

/**
 * Hold the basic (default) information needed to configure and run the tests
 */
#ifndef LS_STRUCT_CONFIG 
typedef struct config_s {
        char  *host;
        int    port;
        char  *ns;      // Namespace
        char  *set;
        uint32_t timeout_ms;
        uint32_t record_ttl;
        char *package_name;
        char *filter_name;
        cl_cluster      *asc;
        bool    verbose;
        cf_atomic_int success;
        cf_atomic_int fail;
} config;
#define LS_STRUCT_CONFIG
#endif

extern int as_lset_create(cl_cluster * asc, char * namespace, char * set,
                          char * keystr, char * lset_bin_name, int distribution,
                          char * lso_package, uint32_t timeout_ms);

extern int as_lset_insert(cl_cluster * asc, char * ns, char * set,
                          char * keystr, char * lset_bin_name,
                          as_val * lset_valuep, char * lso_package,
                          uint32_t timeout_ms );

extern as_result * as_lset_search(cl_cluster * asc, char * ns, char * set,
                           char * keystr, char * lset_bin_name,
                           as_val * search_valuep, bool exists,
                           char * lso_package, uint32_t timeout_ms);

extern int as_lset_delete(cl_cluster * asc, char * ns, char * set,
                          char * keystr, char * lset_bin_name,
                          as_val * delete_valuep, char * lso_package,
                          uint32_t timeout_ms);

#if 0
//NOTE: (REMOVED_FROM: BETA_3.0) QA pass (ADDTO: BETA_3.1)
extern int as_lset_insert_with_transform(cl_cluster * asc, char * ns,
                                  char * set,
                                  char * keystr, char * lset_bin_name,
                                  as_val * lset_valuep, char * lso_package,
                                  char * udf_file, char * udf_name,
                                  as_list * function_args, uint32_t timeout_ms);
extern as_result * as_lset_search_with_transform(cl_cluster * asc, char * ns,
                           char * set, char * keystr, char * lset_bin_name,
                           as_val * search_valuep, char * lso_package,
                           bool exists, char * udf_file, char * udf_name,
                           as_list * function_args, uint32_t timeout_ms );
#endif

#define DEBUG

#ifdef DEBUG

#define INFO(fmt, args...) \
    __log_append(stderr,"", fmt, ## args);

#define ERROR(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

#define LOG(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

#else

#define INFO(fmt, args...)
#define ERROR(fmt, args...)
#define LOG(fmt, args...) 

#endif
