/*
 *  Citrusleaf Tools: Large Stack Object (LSO)
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

#include <openssl/rand.h>

extern uint64_t rand_64();

typedef struct atomic_int_s {
	uint64_t		val;
	pthread_mutex_t	lock;
} atomic_int;

extern void *start_counter_thread(atomic_int *records, atomic_int *bytes);
extern void stop_counter_thread(void *id);

extern atomic_int	*atomic_int_create(uint64_t val);
extern void			atomic_int_destroy(atomic_int *ai);
extern uint64_t		atomic_int_add(atomic_int *ai, int val);
extern uint64_t		atomic_int_get(atomic_int *ai);

typedef struct config_s {
        char  *host;
        int    port;
        char  *ns;
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

extern int as_lso_create( cl_cluster * asc, char * namespace, char * set,
                  		  char * keystr, char * lso_bin_name,
						  as_map * creation_args, char * lso_package,
						  uint32_t timeout_ms );
extern int as_lso_push(cl_cluster * asc, char * ns, char * set, char * keystr,
					   char * lso_bin_name, as_val * lso_valuep,
					   char * lso_package, uint32_t timeout_ms );
extern int as_lso_push_with_transform(cl_cluster * asc, char * ns, char * set,
									  char * keystr, char * lso_bin_name,
									  as_val * lso_valuep, char * lso_package,
									  char * udf_name, as_list * function_args,
									  uint32_t timeout_ms );

extern as_result *as_lso_peek(cl_cluster * asc, char * ns, char * set,
                              char * keystr, char * lso_bin_name,
                              int peek_count, char * lso_package,
                              uint32_t timeout_ms );

#define INFO(fmt, args...) \
    __log_append(stderr,"", fmt, ## args);

#define ERROR(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

#define LOG(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

