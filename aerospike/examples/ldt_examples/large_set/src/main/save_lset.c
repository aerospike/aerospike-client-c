/*  Citrusleaf General Performance Test Program Template
 *  Tailored for the Large Stack Object Test
 *  lset.c - Simple LSET example
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <citrusleaf/aerospike_lset.h>
#include "test.h"

// ==========================================================================
// This module contains the main test code for the Large Stack Feature
// in Aespike 3.0.  It does the Setup (sets parameters, attaches to the
// cluster) and it does the basic Large Stack operations:
// (*) Create()
// (*) Push()
// (*) PushWithTransform()
// (*) Peek()
// (*) PeekWithTransform()
// ==========================================================================
/** 
 *  Initialize Test: Do the set up for a test so that the regular
 *  Aerospike functions can run.
 */
int setup_test( int argc, char **argv ) {
    static char * meth = "setup_test()";
    int rc = 0;
    char * host; // for each iteration of "add host"
    int port; // for each iteration of "add host"
    uint32_t timeout_ms;

    // show cluster setup
    INFO("[DEBUG]:[%s]Startup: host %s port %d ns %s set %s",
            meth, g_config->host, g_config->port, g_config->ns,
            g_config->set == NULL ? "" : g_config->set);

    citrusleaf_init();
    citrusleaf_set_debug(true);

    // create the cluster object
    cl_cluster *asc = citrusleaf_cluster_create();
    if (!asc) { 
        INFO("[ERROR]:[%s]: Fail on citrusleaf_cluster_create()");
        return(-1); 
    }

    // If we have "cluster" defined, then we'll go with that (manually
    // set up in main.c: setup_cluster().  Otherwise, we will default
    // to local host (also defined in g_config).
    if( g_config->cluster_count <= 0 ) {
        g_config->cluster_count = 1;
        g_config->cluster_name[0] = g_config->host; 
        g_config->cluster_port[0] = g_config->port; 
    }
    timeout_ms = g_config->timeout_ms;
    for( int i = 0; i < g_config->cluster_count; i++ ){
        host = g_config->cluster_name[i];
        port = g_config->cluster_port[i];
        INFO("[DEBUG]:[%s]:Adding host(%s) port(%d)", meth, host, port);
        rc = citrusleaf_cluster_add_host(asc, host, port, timeout_ms);
        if (rc) {
            INFO("[ERROR]:[%s]:could not connect to host(%s) port(%d)",
                    meth, host, port);

            INFO("[ERROR]:[%s]:Trying more nodes", meth );
            // return(-1);
        }
    } // end for each cluster server

    g_config->asc  = asc;

    return 0;
} // end setup_test()

// ======================================================================
/**
 * Close up the shop.
 */
int shutdown_test() {
    if (g_config->asc) citrusleaf_cluster_destroy(g_config->asc);
    citrusleaf_shutdown();
    return 0;
} // end shutdown_test()


/**
 * Generate Insert Value. Use the Stumble format.
 * Use the seed to generate random numbers.
 * User has already passed in the appropriate array list (size 5),
 * so we just fill it in using random numbers (in the appropriate ranges)
 */
int gen_stumble_insert_value( as_list * listp, int seed ){

    srand( seed );
    int64_t urlid   = seed; // Generate URL_ID
    as_list_add_integer( listp, urlid );
    int64_t created =  rand() % 500; // Generate CREATED
    as_list_add_integer( listp, created );
    int64_t meth_a  = rand() % 50000; // Generate first half of method
    as_list_add_integer( listp, meth_a );
    int64_t meth_b  = rand() % 50000; // Generate 2nd half of method
    as_list_add_integer( listp, meth_b );
    int64_t status  = rand() % 8000; // Generate status
    as_list_add_integer( listp, status );

}


// ======================================================================
/**
 *  LSET PUSH TEST
 *  For a single record, perform a series of STACK PUSHES.
 *  Create a new record, then repeatedly call stack push.
 */
int ldt_write_test(char * keystr, char * ldt_bin, int iterations, int seed) {
    static char * meth = "ldt_write_test()";
    int rc = 0;

    INFO("[ENTER]:[%s]: It(%d) Key(%s) ldt_bin(%s)\n",
            meth, iterations, keystr, ldt_bin );

    if( rc < 0 ){
        INFO("[ERROR]:[%s]: LSET Create Error: rc(%d)\n", meth, rc );
        return rc;
    }

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * key   = keystr;
    char       * bname = ldt_bin;

    INFO("[DEBUG]:[%s]: Run push() iterations(%d)\n", meth, iterations );
    for ( int i = 0; i < iterations; i++ ) {
        int val = i * 10;
        as_list * listp = as_arraylist_new( 5, 5 );
        gen_stumble_insert_value( listp, val );

        rc = aerospike_lset_insert_with_keystring(
                c, ns, set, key, bname, (as_val *)listp,
                g_config->timeout_ms);
        if (rc) {
            INFO("[ERROR]:[%s]: LSET PUSH Error: i(%d) rc(%d)\n", meth, i, rc );
            as_val_destroy ( listp );
            return -1;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
        as_val_destroy( listp ); // must destroy every iteration.
        listp = NULL;
    } // end for

    return rc;
} // end ldt_write_test()


// ======================================================================
/**
 *  LSET READ TEST
 *  For a single record, perform a series of SET READS.
 *  Using the previously created record, repeatedly call set read with
 *  varying keys.
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 */
int ldt_read_test(char * keystr, char * ldt_bin, int iterations ) {
    static char * meth = "ldt_read_test()";
    int rc = 0;
    as_result * resultp;

    INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) ldt_bin(%s)\n",
            meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * key   = keystr;
    char       * bname = ldt_bin;

    INFO("[DEBUG]:[%s]: Run peek() iterations(%d)\n", meth, iterations );

    int    peek_count = 1;
    char * valstr     = NULL; // Hold Temp results from as_val_tostring()
    as_val * result_valp;
    // NOTE: Must FREE the result for EACH ITERATION.
    for ( int i = 0; i < iterations ; i ++ ){
        peek_count++;
        resultp = aerospike_lset_read_with_keystring(
                c, ns, set, key, bname, peek_count, g_config->timeout_ms);
        if ( resultp && resultp->is_success ) {
//            valstr = as_val_tostring( resultp->value );
//            printf("LSET PEEK SUCCESS: peek_count(%d) Val(%s)\n",
//                   peek_count, valstr);
//            free( valstr );
            // Check that the result is a LIST and has "peek_count" elements
            result_valp = resultp->value;
            if( as_val_type( result_valp ) == AS_LIST &&
                    result_valp->count == peek_count)
            {
            	atomic_int_add( g_config->success_counter, 1);
            } else {
            	valstr = as_val_tostring( resultp->value );
            	ERROR("[PEEK ERROR]:<%s>: Peek results BAD :[[%s]]\n",
                        meth, valstr );
            	free( valstr );
            }

        } else {
            ERROR("[PEEK ERROR]:[%s]: LSET PEEK Error: i(%d) \n", meth, i );
            // Don't break (for now) just keep going.
        }
        // Clean up -- release the result object
        if( resultp != NULL ) as_result_destroy( resultp );

        // Count up the reads (total)
        atomic_int_add( g_config->read_ops_counter, 1 );
        atomic_int_add( g_config->read_vals_counter, peek_count );
    } // end for each peek iteration

    INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    return rc;
} // end ldt_read_test()


// ======================================================================
/**
 *  LSET Write a NUMBER WITH_TRANSFORM TEST
 *  For a single record, perform a series of SET WRITES of BYTE-PACKED data.
 *  Create a new record, then repeatedly call set Insert.
 */
int ldt_write_number_with_transform_test(
        char * keystr, char * ldt_bin, char * create_package, int iterations)
{
    static char * meth = "ldt_write_number_with_transform_test()";
    int rc = 0;

    INFO("[ENTER]:[%s]: It(%d) Key(%s) ldt_bin(%s) Package(%s)\n",
            meth, iterations, keystr, ldt_bin, create_package );

    // Abbreviate for simplicity.
    cl_cluster * c  = g_config->asc;
    char       * ns = g_config->ns;
    char       * set  = g_config->set;
    char       * key  = keystr;
    char       * bname  = ldt_bin;

    // Build our as_map ONCE, and reuse it. In this type of usage, we won't
    // know in advance whether or not our SET is created, so we want to pass
    // in the "Creation Spec" to create the LSET if it is not already there.
    // Package="StandardList" -> Overriding Default PageMode(Bytes)
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
                            (as_val *) as_string_new( create_package, false));

    // Iterate N times, each time creating a new (ish) value to insert.
    // Pick a different seed and range based on the number of iterations
    // we're doing.
    //
    int range = iterations * 4;
    as_integer as_int_val;
    // Start off the pseudo-random number generator based on the iterations.
    srand( iterations );

    INFO("[DEBUG]:[%s]: Run write_with_transform() iterations(%d)\n",
          meth, iterations );
    for ( int i = 0; i < iterations; i++ ) {
        int64_t base_value = rand() % range;
        as_integer_init( as_int_val, base_value );

        rc = aerospike_lset_create_and_insert(
                c, ns, set, key, bname, (as_val *) &as_int_val,
                create_spec, g_config->timeout_ms);
        if (rc) {
            INFO("[ERROR]:[%s]: LSET WRITE WITH TRANSFROM Err: i(%d) rc(%d)\n",
                  meth, i, rc );
            return rc;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
    } // end for each iteration.

    return rc;
} // end ldt_write_number_with_transform_test()


// ======================================================================
/**
 *  LSET Write a LIST  WITH_TRANSFORM TEST
 *  For a single record, perform a series of SET WRITES of BYTE-PACKED data.
 *  Create a new record, then repeatedly call set Insert.
 */
int ldt_write_list_with_transform_test(
        char * keystr, char * ldt_bin, char * create_package, int iterations)
{
    static char * meth = "ldt_write_list_with_transform_test()";
    int rc = 0;

    INFO("[ENTER]:[%s]: It(%d) Key(%s) ldt_bin(%s)\n",
            meth, iterations, keystr, ldt_bin );

    // Abbreviate for simplicity.
    cl_cluster * c  = g_config->asc;
    char       * ns = g_config->ns;
    char       * set  = g_config->set;
    char       * key  = keystr;
    char       * bname  = ldt_bin;

    // Build our as_map ONCE, and reuse it. In this type of usage, we won't
    // know in advance whether or not our SET is created, so we want to pass
    // in the "Creation Spec" to create the LSET if it is not already there.
    // Package="StandardList" -> Overriding Default PageMode(Bytes)
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
                            (as_val *) as_string_new("StandardList", false));

    INFO("[DEBUG]:[%s]: Run write_with_transform() iterations(%d)\n",
          meth, iterations );
    for ( int i = 0; i < iterations; i++ ) {
        int val         = i * 10;
        as_list * listp = as_arraylist_new( 5, 5 );
        int64_t urlid   = val + 1;
        as_list_add_integer( listp, urlid );
        int64_t created = val + 2;
        as_list_add_integer( listp, created );
        int64_t meth_a  = val + 3;
        as_list_add_integer( listp, meth_a );
        int64_t meth_b  = val + 4;
        as_list_add_integer( listp, meth_b );
        int64_t status  = val + 5;
        as_list_add_integer( listp, status );

        rc = aerospike_lset_create_and_insert(
                c, ns, set, key, bname, (as_val *)listp,
                create_spec, g_config->timeout_ms);
        if (rc) {
            INFO("[ERROR]:[%s]: LSET PUSH WITH TRANSFROM Error: i(%d) rc(%d)\n",
                  meth, i, rc );
            as_val_destroy ( listp );
            return -1;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
        as_val_destroy( listp ); // must destroy every iteration.
        listp = NULL;
    } // end for

    return rc;
} // end ldt_write_list_with_transform_test()

// ======================================================================
/** 
 *  LSET READ NUMBER WITH FILTER TEST
 *  For a single record, perform a series of SET READS,
 *  and do a server side transform of the byte-packed data.
 *  Using the previously created record, repeatedly call set read with
 *  varying keys.
 */
int ldt_read_number_with_filter_test(char * keystr, char * ldt_bin,
                                 char * filter,
                                 as_list * fargs,
                                 int iterations ) {
    static char * meth = "ldt_read_with_transform_test()";
    int rc = 0;

    INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) ldt_bin(%s)\n",
            meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * key   = keystr;
    char       * bname = ldt_bin;
    as_result * resultp;

    INFO("[DEBUG]:[%s]: Run read() iterations(%d)\n", meth, iterations );

    // NOTE: Must FREE the result for EACH ITERATION.
    char * valstr = NULL; // Hold Temp results from as_val_tostring()
    cl_object o_key;
    as_integer search_value;
    int range = iterations * 4;
    // Start off the pseudo-random number generator based on the iterations.
    srand( iterations );
    int64_t base_value;

    for ( int i = 0; i < iterations ; i ++ ){
        base_value = rand() % range;
        as_integer_init( search_value, base_value );

        // Load up the key that we'll feed into the call (and we'll remember
        // to free it after we're done).
        citrusleaf_object_init_str( &o_key, keystr );
        resultp = aerospike_lset_read_with_filter(
                c, ns, set, &o_key, bname, (as_val *) search_value,
                filter, fargs, g_config->timeout_ms);
        citrusleaf_object_free( &o_key );

        if ( resultp && resultp->is_success ) {
            valstr = as_val_tostring( resultp->value );
            printf("LSET READ WITH TRANSFORM SUCCESS: Val(%s)\n", valstr);
            free( valstr );
            // Clean up -- release the result object
            if( resultp != NULL ) as_result_destroy( resultp );
        } else {
            INFO("[ERROR]:[%s]: LSET READ WITH TRANSFORM Error: i(%d) \n",
                 meth, i );
            // Don't break (for now) just keep going.
        }
        // Count up the reads (total)
        atomic_int_add( g_config->read_vals_counter, 1 );
        atomic_int_add( g_config->read_ops_counter, 1 );
    } // end for each read iteration

    INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    return rc;
} // end ldt_read_number_with_filter_test()

/** 
 *  LSET READ LIST WITH FILTER TEST
 *  For a single record, perform a series of SET READS,
 *  and do a server side transform of the byte-packed data.
 *  Using the previously created record, repeatedly call set read with
 *  varying keys.
 */
int ldt_read_list_with_filter_test(
        char * keystr, char * ldt_bin, char * filter,
        as_list * fargs, int iterations )
{
    static char * meth = "ldt_read_list_with_transform_test()";
    int rc = 0;

    INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) ldt_bin(%s)\n",
            meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * key   = keystr;
    char       * bname = ldt_bin;
    as_result * resultp;

    INFO("[DEBUG]:[%s]: Run read() iterations(%d)\n", meth, iterations );

    // NOTE: Must FREE the result for EACH ITERATION.
    char * valstr = NULL; // Hold Temp results from as_val_tostring()
    cl_object o_key;
    as_list * search_value;
    range = iterations * 4;
    srand( iterations );

    for ( int i = 0; i < iterations ; i ++ ){
        search_value = generate_search_list_value( i, rand(), range );
        citrusleaf_object_init_str( &o_key, keystr );
        resultp = aerospike_lset_read_with_transform_with_keystring(
                c, ns, set, &o_key, bname, search_value,
                filter, fargs, g_config->timeout_ms);
        citrusleaf_object_free( &o_key );
        if ( resultp && resultp->is_success ) {
            valstr = as_val_tostring( resultp->value );
            printf("LSET READ WITH TRANSFORM SUCCESS: Val(%s)\n", valstr);
            free( valstr );
            // Clean up -- release the result object
            if( resultp != NULL ) as_result_destroy( resultp );
        } else {
            INFO("[ERROR]:[%s]: LSET Read WITH TRANSFORM Error: i(%d) \n",
                 meth, i );
            // Don't break (for now) just keep going.
        }
        // Count up the reads (total)
        atomic_int_add( g_config->read_vals_counter, 1 );
        atomic_int_add( g_config->read_ops_counter, 1 );
    } // end for each read iteration

    INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
    return rc;
} // end ldt_read_list_with_filter_test()
