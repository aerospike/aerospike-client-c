/*  Citrusleaf/Aerospike General Performance Test
 *
 *  Tailored to the Large Object Stack Test Program
 *  RUN The Large Stack Tests (1 thru N)
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include "test.h"

static char * MOD = "run_tests.c::04_18_C";

// Define the tests that run in "run_test()", which is the main test
// that is launched in each of the threads fired off by main.
//
// ++==================++
// || HELPER FUNCTIONS ||
// ++==================++
// Create a Quintuplet value (to mimic StumbleUpon)
int generate_quintuplet( int buffer[], int seed ){

}


int run_test0( char * user_key ) {

	static char * meth         = "run_test()";
	int           rc           = 0;
	char        * ldt_bin_name = "urlid_stack";
    int         iterations = g_config->n_iterations;

	// (1) Insert Test
	INFO("[DEBUG]:<%s:%s>: calling ldt_simple_insert_test()\n", MOD, meth);
	rc = ldt_simple_insert_test(user_key, ldt_bin_name, iterations );
	if (rc) {
		INFO("[ERROR]<%s:%s>ldt_simple_insert_test() RC(%d)\n", MOD, meth, rc);
		return( rc );
	}

	// (2) Search Test
	INFO("[DEBUG]:<%s:%s>: calling ldt_simple_search_test()\n", MOD, meth );
	rc = ldt_simple_search_test( user_key, ldt_bin_name, iterations );
    if (rc) {
		INFO("[ERROR]<%s:%s>ldt_simple_search_test() RC(%d)\n",MOD, meth, rc);
		return( rc );
	}
    return 0;
} // end run_test0()

/**
 * Test ONE is relatively simple -- just some basic writes and reads
 */
int run_test1(char *user_key) {
    static char * meth         = "run_test1()";
    int           rc           = 0;
    char        * ldt_bin_name = "LSET_TEST1_BIN";

    int iterations = g_config->n_iterations;
    int format = NUMBER_FORMAT;
    int seed = 1;
    INFO("[DEBUG]:<%s:%s>: calling ldt_write_test(): It(%d)\n",
            MOD, meth, iterations );
    rc = ldt_write_test( user_key, ldt_bin_name, iterations, seed, format);
    if (rc) {
        INFO("[ERROR]:<%s:%s>: ldt_write_test() RC(%d)\n", MOD, meth, rc );
        return( rc );
    }

    INFO("[DEBUG]:<%s:%s>: calling ldt_read_test(): It\n",
            MOD, meth, iterations );
    rc = ldt_read_test( user_key, ldt_bin_name, iterations, seed, format );
    if (rc) {
        INFO("[ERROR]:<%s:%s>: ldt_read_test() RC(%d)\n", MOD, meth, rc );
        return( rc );
    }
    return ( rc );
} // end run_test1()


/**
 * Test TWO is one more step in complexity -- it does the simple
 * push and peek with the Stumble data -- and so does a compress
 * and uncompress of the data using the inner UDF.  Notice that
 * the CREATE of this LSTACK BIN must have the correct parameters
 * set for doing the inner UDF call (e.g. BINARY type, and entry
 * size).
 */
int run_test2(char *user_key) {
    static char * meth         = "run_test2()";
    int           rc           = 0;
    char        * ldt_bin_name = "LSET_TEST2_BIN";
    char * compress_func       = "stumbleCompress5";
    as_list *compress_args     = as_arraylist_new( 1, 1 );
    as_list *uncompress_args   = as_arraylist_new( 1, 1 );
    as_list_add_integer( compress_args, 1 ); // dummy argument
    int iterations = g_config->n_iterations;

    INFO("[DEBUG]<%s:%s>Call ldt_write_with_transform_test()\n", MOD, meth );
    rc = ldt_write_list_with_transform_test( user_key, ldt_bin_name,
                                       NULL, iterations );
    if (rc) {
        INFO("[ERROR]<%s:%s>write result: RC(%d)\n", MOD, meth, rc);
		goto Cleanup;
    }

    char * uncompress_func = "stumbleUnCompress5";
    as_list_add_integer( uncompress_args, 1 ); // dummy argument

    INFO("[DEBUG]:<%s:%s>: calling ldt_read_with_filter_test()\n", MOD, meth );
    rc = ldt_read_list_with_filter_test( user_key, ldt_bin_name,
                                   uncompress_func, uncompress_args,
                                       g_config->n_iterations );
    if (rc) {
        INFO("[ERROR]:<%s:%s>: ldt_read_with_filter_test() RC(%d)\n", MOD, meth, rc);
		goto Cleanup;
    }
Cleanup:
	as_list_destroy( uncompress_args );
	as_list_destroy( compress_args );
    return ( rc );
} // end run_test2


/**
 * Test THREE expands on test ONE and adds more variety of users/keys
 * and then multiple operations per key.
 */
int run_test3( int seed ) {
    static char * meth         = "run_test3()";
    int           rc           = 0;
    char        * ldt_bin_name = "LSET_TEST3_BIN";
    char user_key[32];
    int iterations = g_config->n_iterations;
    int format = NUMBER_FORMAT;

    INFO("[DEBUG]:<%s:%s>: Running Test3:: NumKeys(%d) NumIterations(%d)\n",
            MOD, meth, g_config->n_keys, g_config->n_iterations );

    for( int k = 0; k < g_config->n_keys; k++ ){
        sprintf( user_key, "User_%d", rand() % 100 );
        rc = ldt_write_test(user_key,ldt_bin_name,iterations,seed,format);
        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: ldt_write_test() RC(%d)\n", MOD, meth, rc );
            return( rc );
        }

        rc = ldt_read_test( user_key, ldt_bin_name, iterations, seed, format );
        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: ldt_read_test() RC(%d)\n", MOD, meth, rc );
            return( rc );
        }
    } // end for each key
    return ( rc );

} // end run_test3
