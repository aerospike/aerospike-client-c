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

#include "utils.h"
#include "citrusleaf/citrusleaf.h"


extern void *start_counter_thread(atomic_int *records, atomic_int *bytes);
extern void stop_counter_thread(void *id);

/**
 * Hold the basic (default) information needed to configure and run the tests
 */
typedef struct config_s {

        char  *host;
        int    port;
        char  *ns;      // Namespace
        char  *set;
        uint32_t timeout_ms;
        uint32_t record_ttl;
        char *package_file;
        char *package_name;
        char *filter_name;
        cl_cluster      *asc;
        bool    verbose;
        cf_atomic_int success;
        cf_atomic_int fail;
} config;


#define INFO(fmt, args...) \
    __log_append(stderr,"", fmt, ## args);

#define ERROR(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

#define LOG(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

