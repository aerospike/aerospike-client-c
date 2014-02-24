/*
 *  Aerospike LSET Test Suite
 *  This instance is specific to the Large Set Test
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 */
#pragma once

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>
#include <ctype.h>
#include <getopt.h>
#include <fcntl.h>  // open the random file
#include <sys/stat.h>
#include <openssl/rand.h>   // get the nice juicy SSL random bytes
// #include <pthread.h>

#include "../test.h"
#include "../util/udf.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/aerospike_lset.h>
#include <aerospike/as_types.h>

// #include "citrusleaf/as_map.h"
// #include "citrusleaf/as_list.h"
// #include "citrusleaf/as_val.h"
// #include "citrusleaf/cl_udf.h"
// #include "citrusleaf/cf_hist.h"

#include "test_log.h"         // Error/Tracing
#include "test_config.h"  // Our general Test config file
//
// Define the functions in the lset_operations.c module

extern int lset_create_test(char * keystr, char * lso_bin);

extern int lset_size_test(char * keystr, char * ldt_bin, uint32_t   * size);

extern int lset_config_test(char * keystr, char * ldt_bin);

extern int lset_create_test (char * keystr, char * ldt_bin );

extern int lset_insert_test(char * keystr, char * lso_bin, int iterations,
        int seed, int format );

extern int lset_search_test(char * keystr, char * lso_bin, int iterations,
        int seed, int format );

extern int lset_insert_with_transform_test(char * keystr, char * lso_bin,
             int iterations);
    
extern int lset_search_with_transform_test(char * keystr, char * lso_bin,
             char * filter_function, as_list * fargs, int iterations );
