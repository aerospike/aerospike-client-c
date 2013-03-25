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

// Define the tests that run in "run_test()", which is the main test
// that is launched in each of the threads fired off by main.
//
// ++==================++
// || HELPER FUNCTIONS ||
// ++==================++
// Create a Quintuplet value (to mimic StumbleUpon)
int generate_quintuplet( int buffer[], int seed ){
}


/**
 * Test ONE is relatively simple -- just some basic pushes and peeks.
 */
int run_test1(char *user_key) {
    static char * meth         = "run_test1()";
    int           rc           = 0;
    char        * lso_bin_name = "LSO_TEST1_BIN";

    INFO("[DEBUG]:[%s]: calling lso_push_test(): It(%d)\n",
            meth, g_config->n_iterations );
    rc = lso_push_test( user_key, lso_bin_name, g_config->n_iterations, 1);
    if (rc) {
        INFO("[ERROR]:[%s]: lso_push_test() RC(%d)\n", meth, rc );
        return( rc );
    }

    INFO("[DEBUG]:[%s]: calling lso_peek_test(): It\n",
            meth, g_config->n_iterations );
    rc = lso_peek_test( user_key, lso_bin_name, g_config->n_iterations );
    if (rc) {
        INFO("[ERROR]:[%s]: lso_peek_test() RC(%d)\n", meth, rc );
        return( rc );
    }
    return ( rc );
}


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
    char        * lso_bin_name = "LSO_TEST2_BIN";
    char * compress_func       = "stumbleCompress5";
    as_list *compress_args     = as_arraylist_new( 1, 1 );
    as_list *uncompress_args   = as_arraylist_new( 1, 1 );
    as_list_add_integer( compress_args, 1 ); // dummy argument

    INFO("[DEBUG]:[%s]: calling lso_push_with_transform_test()\n", meth );
    rc = lso_push_with_transform_test( user_key, lso_bin_name,
                                       compress_func, compress_args,
                                       g_config->n_iterations );
    if (rc) {
        INFO("[ERROR]:[%s]: lso_push_with_transform_test() RC(%d)\n", meth, rc);
		goto Cleanup;
    }

    char * uncompress_func = "stumbleUnCompress5";
    as_list_add_integer( uncompress_args, 1 ); // dummy argument

    INFO("[DEBUG]:[%s]: calling lso_peek_with_transform_test()\n", meth );
    rc = lso_peek_with_transform_test( user_key, lso_bin_name,
                                       uncompress_func, uncompress_args,
                                       g_config->n_iterations );
    if (rc) {
        INFO("[ERROR]:[%s]: lso_peek_with_transform_test() RC(%d)\n", meth, rc);
		goto Cleanup;
    }
Cleanup:
	as_list_destroy( uncompress_args );
	as_list_destroy( compress_args );
    return ( rc );
}


/**
 * Test THREE expands on test ONE and adds more variety of users/keys
 * and then multiple operations per key.
 */
int run_test3( int seed ) {
    static char * meth         = "run_test3()";
    int           rc           = 0;
    char        * lso_bin_name = "LSO_TEST3_BIN";
    char user_key[32];

    INFO("[DEBUG]:[%s]: Running Test3:: NumKeys(%d) NumIterations(%d)\n",
            meth, g_config->n_keys, g_config->n_iterations );

    for( int k = 0; k < g_config->n_keys; k++ ){
        sprintf( user_key, "User_%d", rand() % 100 );
        rc = lso_push_test(user_key,lso_bin_name,g_config->n_iterations,seed);
        if (rc) {
            INFO("[ERROR]:[%s]: lso_push_test() RC(%d)\n", meth, rc );
            return( rc );
        }

        rc = lso_peek_test( user_key, lso_bin_name, g_config->n_iterations );
        if (rc) {
            INFO("[ERROR]:[%s]: lso_peek_test() RC(%d)\n", meth, rc );
            return( rc );
        }
    } // end for each key
    return ( rc );

} // end test3
