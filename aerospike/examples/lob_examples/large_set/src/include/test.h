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

#include "test_log.h"         // Error/Tracing
#include "test_config.h"  // Our general Test config file
#include "test_counter.h" // include the atomic structure and methods

//
// Define the functions in the lstack.c module
extern int
setup_test( int argc, char **argv );

extern int
shutdown_test();

extern cl_rv
ldt_write_test( char * keystr, char * ldt_bin, int iterations,
        int seed, int format );

extern cl_rv
ldt_read_test( char * keystr, char * ldt_bin, int iterations,
        int seed, int format);

extern cl_rv
ldt_write_number_with_transform_test(char * keystr, char * ldt_bin,
         char * create_package, int iterations );

extern cl_rv
ldt_write_list_with_transform_test(char * keystr, char * ldt_bin,
         char * create_package, int iterations );
    
extern cl_rv
ldt_read_number_with_filter_test(char * keystr, char * ldt_bin,
        char * filter, as_list * fargs, int iterations );

extern cl_rv
ldt_read_list_with_filter_test(char * keystr, char * ldt_bin,
        char * filter, as_list * fargs, int iterations );
    
extern cl_rv
ldt_simple_insert_test(char * keystr, char * lset_bin, int iterations);

extern cl_rv
ldt_simple_search_test(char * keystr, char * lset_bin, int iterations );

// Functions in the run_tests.c module
extern int run_test0(char *user_key);
extern int run_test1(char *user_key);
extern int run_test2(char *user_key);
extern int run_test3(int seed );
