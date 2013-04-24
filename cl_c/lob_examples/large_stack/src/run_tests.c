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

// Module name to help tracing/debugging.
static char * MOD = "run_tests.c::0422";

// Define the tests that run in "run_test()", which is the main test
// that is launched in each of the threads fired off by main.
//
// ++==================++
// || HELPER FUNCTIONS ||
// ++==================++


/**
 * Test ONE is relatively simple -- just some basic pushes and peeks using
 * non-transformed values.
 * Do a push/peek cycle with each of the three types:
 * (*) INTEGERS
 * (*) STRINGS
 * (*) LISTS (in this case, a list of 5 ints)
 */
int run_test1(char *user_key, int seed ) {
    static char * meth         = "run_test1()";
    int           rc           = 0;
    char        * lso_bin_name1 = "LSO_TEST1_NUM";
    char        * lso_bin_name2 = "LSO_TEST1_STR";
    char        * lso_bin_name3 = "LSO_TEST1_LST";
    int         it = g_config->n_iterations;

    INFO("[DEBUG]:<%s:%s>: calling lso_push_test(): It(%d)",
            MOD, meth, g_config->n_iterations );
    // ------------------------------------------------------------
    // Do a series of PUSHES, for a single KEY, for each of the types
    // ------------------------------------------------------------
    rc = lso_push_test( user_key, lso_bin_name1, it, seed, NUMBER_FORMAT);
    if( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_push_test() RC(%d)", MOD, meth, rc );
        return( rc );
    }

    rc = lso_push_test( user_key, lso_bin_name2, it, seed, STRING_FORMAT);
    if( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_push_test() RC(%d)", MOD, meth, rc );
        return( rc );
    }

    rc = lso_push_test( user_key, lso_bin_name3, it, seed, LIST_FORMAT);
    if( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_push_test() RC(%d)", MOD, meth, rc );
        return( rc );
    }

    // ------------------------------------------------------------
    // Now do a series of Peeks, for a single KEY, for each type.
    // ------------------------------------------------------------
    INFO("[DEBUG]:<%s:%s>: calling lso_peek_test(): It(%d)",
            MOD, meth, g_config->n_iterations );
    rc = lso_peek_test( user_key, lso_bin_name1, it, seed, NUMBER_FORMAT );
    if( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_peek_test() RC(%d)", MOD, meth, rc );
        return( rc );
    }

    rc = lso_peek_test( user_key, lso_bin_name2, it, seed, STRING_FORMAT );
    if( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_peek_test() RC(%d)", MOD, meth, rc );
        return( rc );
    }

    rc = lso_peek_test( user_key, lso_bin_name3, it, seed, LIST_FORMAT );
    if( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_peek_test() RC(%d)", MOD, meth, rc );
        return( rc );
    }

    return ( rc );
} // end run_test1()


/**
 * Test TWO is one more step in complexity -- it does the simple
 * push and peek with LIST data -- and so does a compress
 * and uncompress of the data using the transformation UDF.  Notice that
 * the CREATE of this LSTACK BIN must have the correct parameters
 * set for doing the inner UDF call (e.g. BINARY type, and entry size).
 * NOTE: The Transformation is now implicit -- defined in the create
 * phase (usually using a pre-defined package).  So, we no longer pass
 * in compress functions or arguments here.
 */
int run_test2(char *user_key, int seed ) {
    static char * meth         = "run_test2()";
    int           rc           = 0;
    char        * lso_bin_name = "LSO_TEST2_BIN";

    INFO("[DEBUG]:<%s:%s>: calling lso_push_with_transform_test()", MOD, meth );
    // NOTE: The Compression/Transformation function is now implicit in
    // the create package -- which is done inside the
    // lso_push_with_transform_test() call -- so we do NOTHING special here.
    rc = lso_push_with_transform_test( user_key, lso_bin_name,
                                       g_config->n_iterations );
    if( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_push_with_transform_test() RC(%d)",
                MOD, meth, rc );
		goto Cleanup;
    }

    INFO("[DEBUG]:<%s:%s>: calling lso_peek_with_transform_test()", MOD, meth );
    rc = lso_peek_with_transform_test( user_key, lso_bin_name, NULL, NULL,
                                       g_config->n_iterations );
    if ( rc != CITRUSLEAF_OK ) {
        INFO("[ERROR]:<%s:%s>: lso_peek_with_transform_test() RC(%d)",
                MOD, meth, rc );
		goto Cleanup;
    }
Cleanup:
	// as_list_destroy( uncompress_args );
	// as_list_destroy( compress_args );
    return ( rc );
}


/**
 * Test THREE expands on test ONE and adds more variety of users/keys
 * and then multiple operations per key.
 */
int run_test3( int seed ) {
    static char * meth         = "run_test3()";
    int           rc           = 0;
    char user_key[32];
    char        * lso_bin_name1 = "LSO_TEST1_NUM";
    char        * lso_bin_name2 = "LSO_TEST1_STR";
    char        * lso_bin_name3 = "LSO_TEST1_LST";
    int         it = g_config->n_iterations;

    INFO("[DEBUG]:<%s:%s>: Running Test3:: NumKeys(%d) Iterations(%d) Seed(%d)",
            MOD, meth, g_config->n_keys, g_config->n_iterations, seed );

    for( int k = 0; k < g_config->n_keys; k++ ){
        // ------------------------------------------------------------
        // Do a series of PUSHES, for this KEY, for each of the types
        // ------------------------------------------------------------
        sprintf( user_key, "User_%d", rand() % 100 );
        rc = lso_push_test( user_key, lso_bin_name1, it, seed, NUMBER_FORMAT);
        if( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: lso_push_test() RC(%d)", MOD, meth, rc );
            return( rc );
        }

        rc = lso_push_test( user_key, lso_bin_name2, it, seed, STRING_FORMAT);
        if( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: lso_push_test() RC(%d)", MOD, meth, rc );
            return( rc );
        }

        rc = lso_push_test( user_key, lso_bin_name3, it, seed, LIST_FORMAT);
        if( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: lso_push_test() RC(%d)", MOD, meth, rc );
            return( rc );
        }

        // ------------------------------------------------------------
        // Now do a series of Peeks, for a single KEY, for each type.
        // ------------------------------------------------------------
        INFO("[DEBUG]:<%s:%s>: calling lso_peek_test(): It(%d)",
                MOD, meth, g_config->n_iterations );
        rc = lso_peek_test( user_key, lso_bin_name1, it, seed, NUMBER_FORMAT );
        if( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: lso_peek_test() RC(%d)", MOD, meth, rc );
            return( rc );
        }

        rc = lso_peek_test( user_key, lso_bin_name2, it, seed, STRING_FORMAT );
        if( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: lso_peek_test() RC(%d)", MOD, meth, rc );
            return( rc );
        }

        rc = lso_peek_test( user_key, lso_bin_name3, it, seed, LIST_FORMAT );
        if( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>: lso_peek_test() RC(%d)", MOD, meth, rc );
            return( rc );
        }

    } // end for each key
    INFO("[DEBUG]:<%s:%s>: calling lso_push_test(): It(%d)",
            MOD, meth, g_config->n_iterations );

    return ( rc );

} // end test3
