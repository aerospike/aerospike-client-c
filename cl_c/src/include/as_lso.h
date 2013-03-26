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

#define VALUE_UNINIT    0xFFFFFFFFFFFFFFFF
#define VALUE_DELETED   0xFFFFFFFFFFFFFFFE

extern uint64_t rand_64();

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
extern as_result * as_lso_peek_with_transform(cl_cluster * asc, char * ns,
							  char * set, char * keystr, char * lso_bin_name,
							  int peek_count, char * lso_package,
							  char * udf_name, as_list * function_args,
							  uint32_t timeout_ms );
