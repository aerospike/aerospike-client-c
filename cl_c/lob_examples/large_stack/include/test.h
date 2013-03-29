/*
 *  Aerospike general Performance Test
 *  This instance is specific to the Large Stack Test
 *  An example program of LSO Performance using the C interface
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
#include <assert.h>
#include <ctype.h>
#include <getopt.h>
#include <fcntl.h>  // open the rnadom file
#include <sys/stat.h>
#include <openssl/rand.h>   // get the nice juicy SSL random bytes
#include <pthread.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/as_map.h"
#include "citrusleaf/as_list.h"
#include "citrusleaf/as_val.h"
#include "citrusleaf/cl_udf.h"
#include "citrusleaf/cf_hist.h"

#include "log.h"         // Error/Tracing
#include "test_config.h"  // Our general Test config file
#include "counter.h" // include the atomic structure and methods

//
// Define the functions in the lstack.c module
extern int setup_test( int argc, char **argv );
extern int shutdown_test();
extern int lso_push_test(char * keystr, char * lso_bin, int iterations, int seed);
extern int lso_peek_test(char * keystr, char * lso_bin, int iterations );

extern int lso_push_with_transform_test(char * keystr, char * lso_bin,
             char * compress_func, as_list * compress_args, int iterations);
    
extern int lso_peek_with_transform_test(char * keystr, char * lso_bin,
             char * uncompress_func, as_list * uncompress_args, int iterations );
    

// Functions in the run_tests.c module
extern int run_test1(char *user_key);
extern int run_test2(char *user_key);
