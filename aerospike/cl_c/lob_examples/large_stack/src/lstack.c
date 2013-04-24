/*  Citrusleaf General Performance Test Program Template
 *  Tailored for the Large Stack Object Test
 *  lstack_test.c - Simple LSO example
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <citrusleaf/aerospike_lstack.h>
#include "test.h"

static char * MOD = "lstack.c::0418.A";
static char * LDT = "LSTACK";

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
    INFO("[DEBUG]:<%s:%s>Startup: host %s port %d ns %s set %s",
            MOD, meth, g_config->host, g_config->port, g_config->ns,
            g_config->set == NULL ? "" : g_config->set);

    citrusleaf_init();
    citrusleaf_set_debug(true);

    // create the cluster object
    cl_cluster *asc = citrusleaf_cluster_create();
    if (!asc) { 
        INFO("[ERROR]:<%s:%s>: Fail on citrusleaf_cluster_create()",MOD,meth);
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
        INFO("[DEBUG]:<%s:%s>:Adding host(%s) port(%d)", MOD, meth, host, port);
        rc = citrusleaf_cluster_add_host(asc, host, port, timeout_ms);
        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>:could not connect to host(%s) port(%d)",
                    MOD, meth, host, port);

            INFO("[ERROR]:<%s:%s>:Trying more nodes", MOD, meth );
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
 * Process all read results the same way.
 * TODO: Check the SIZE of the returned list against the asked for
 * "peek count".  Notice that when filters are applied, then we may get LESS
 * than what we asked for.
 */
void process_read_results( char * meth, cl_rv rc, as_result * resultp,
        int i, int * valsp, int * missesp, int * errsp, int count )
{
    static char * tm = "process_read_results()";
    INFO("[ENTER]:<%s:%s>: From(%s) i(%d) Count(%d)", MOD, tm, meth, i, count );

    char * valstr;
    int success_count = 0;
    int fail_count= 0;
    as_val *result_valp;

    if( rc == CITRUSLEAF_OK ){
        if ( resultp && resultp->is_success ) {
            if( TRA_DEBUG ){
                valstr = as_val_tostring( resultp->value );
                printf("[DEBUG]<%s:%s>(%s) READ SUCCESS: Val(%s)\n",
                        MOD, meth, LDT, valstr);
                free( valstr );
                (*valsp)++;
            }
            result_valp = resultp->value;
            // Check result type.  Notice that we can NOT check the count
            // because we will often get back a different amount than what
            // we asked for (e.g. peek_count == 0, peek_count > stack size).
            if( as_val_type( result_valp ) == AS_LIST ) {
            	valstr = as_val_tostring( resultp->value );
            	INFO("[SUCCESS]:<%s:%s>:Peek results:PK(%d) Count(%d) LIST[%s]",
                        MOD, meth, count, result_valp->count, valstr );
            	free( valstr );
            	success_count = 1;
            } else {
            	INFO("[UNSURE]:<%s:%s>:Peek results: Wanted List: TYPE[%s]",
                        MOD, meth, as_val_type(resultp->value) );
                fail_count = 1;
            }
        } else {
            (*missesp)++;
            INFO("[ERROR]<%s:%s>(%s) Read OK: Result Error: i(%d) rc(%d)",
                 MOD, meth, LDT, i, rc);
            // Don't break (for now) just keep going.
            fail_count = 1;
        }
    } else if( rc == CITRUSLEAF_FAIL_NOTFOUND ){
        (*errsp)++;
        INFO("[ERROR]<%s:%s>(%s) Read Record NOT FOUND: i(%d) rc(%d)",
             MOD, meth, LDT, i, rc);
        fail_count = 1;
    } else {
        (*errsp)++;
        fail_count = 1;
        INFO("[ERROR]<%s:%s>(%s) OTHER ERROR: i(%d) rc(%d)",
             MOD, meth, LDT, i, rc);
    }

    // Update success/fail stats
    atomic_int_add( g_config->success_counter, success_count);
    atomic_int_add( g_config->fail_counter, fail_count);

} // end process_read_results()


/**
 * Create a list tuple for inserting/reading LIST values.
 */
as_val * gen_list_val(int seed ) {

    as_list * listp = as_arraylist_new( 5, 0 ); // must destroy later.
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

    return( as_val * ) listp;
} // end gen_list_val()


/**
* Generate Insert Value.  Pick the format based on the setting in the
 * config structure:
 * 0: List value (of numbers)
 * 1: Simple Number
 * 2: Simple String (with a length)
 * 3: Complex  Object (type 1)
 * 4: Complex  Object (type 2)
 * 5: Complex  Object (type 3)
 * Use the seed to generate random numbers.
 * User has already passed in the appropriate array list (size 5),
 * so we just fill it in using random numbers (in the appropriate ranges)
 */
int generate_value( as_val ** return_valpp, int seed, int val_type ){
    static char * meth = "generate_value()";
    int rc = 0;
    *return_valpp = NULL;  // Start with nothing.
    char * mallocd_buf = NULL;

    switch( val_type ){
        case LIST_FORMAT:
            *return_valpp = gen_list_val( seed );
            break;
        case NUMBER_FORMAT:
            // We have to malloc an int here because someone else will have
            // to reclaim (destroy) it.
            srand( seed );
            as_integer * intp = as_integer_new( rand() % g_config->key_max);
            *return_valpp = (as_val *) intp;
            break;
        case STRING_FORMAT:
            // Malloc a string buffer, write in it, and then create a 
            // as_string object for it.
            // NOTE: RIght now, this is just a simple, fixed size string.
            // We should add in the ability to create a variable size string
            // based on the KEY_LENGTH parameter in the config structure.
            // TODO: Make general for key length;
            mallocd_buf = (char *) malloc( 32 );
            srand( seed );
            int new_val = rand() % g_config->key_max;
            sprintf( mallocd_buf, "%10d", new_val );
            as_string * str_val = as_string_new( mallocd_buf, true );
            *return_valpp = (as_val *) str_val;
            break;
//        case COMPLEX_1_FORMAT:
//        case COMPLEX_2_FORMAT:
//        case COMPLEX_3_FORMAT:
//            printf("[ERROR]<%s:%s>WE ARE NOT YET HANDLING COMPLEX FORMATS\n",
//                    MOD, meth );
//            break;
        case NO_FORMAT:
        default:
            printf("[ERROR]<%s:%s>UNKNOWN FORMAT: %d \n",
                    MOD, meth, val_type );
    } // end switch object type

    return rc;
} // end generate_value()



// ======================================================================
/**
 *  LSO PUSH TEST
 *  For a single record, perform a series of STACK PUSHES.
 *  Create a new record, then repeatedly call stack push.
 *  This should work for data that is a NUMBER, a STRING or a LIST.
 *  Parms:
 *  + keystr: String Key to find the record
 *  + lso_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 */
int lso_push_test(char * keystr, char * lso_bin, int iterations, int seed,
        int data_format ) {
    static char * meth = "lso_push_test()";
    int rc = 0;

    INFO("[ENTER]:<%s:%s>: It(%d) Key(%s) LSOBin(%s) Seed(%d)",
            MOD, meth, iterations, keystr, lso_bin, seed);

    // We have two choices:  We can create the LSO bin here, and then
    // do a bunch of inserts into it -- or we can just do the combined
    // "create_and_push" insert, which upon reflection, is really the
    // most likely mode we'll be in. We'll choose the later.

    // Set up the Creation Spec parameter -- mostly setting the Package
    // (which is the name for a canned set of settings).
    char * create_package = "StandardList";
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
    (as_val *) as_string_new( create_package, false));

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    as_val     * valp;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = lso_bin;
    int          iseed;

    INFO("[DEBUG]:<%s:%s>: Run push() iterations(%d)", MOD, meth, iterations );
    citrusleaf_object_init_str( &o_key, keystr );
    for ( int i = 0; i < iterations; i++ ) {
        iseed = i * 10;
        generate_value( &valp, iseed, data_format );

        rc = aerospike_lstack_create_and_push(
                c, ns, set, &o_key, bname, valp, create_spec,
                g_config->timeout_ms);

        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>:PUSH Error: i(%d) rc(%d)", MOD, meth,i,rc );
            as_val_destroy ( valp );
            goto cleanup;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
        as_val_destroy( valp ); // must destroy every iteration.
        valp = NULL; // unnecessary insurance
    } // end for

cleanup:
    citrusleaf_object_free( &o_key );
    as_val_destroy( create_spec );

    return rc;
} // end lso_push_test()


// ======================================================================
/**
 *  LSO PEEK TEST
 *  For a single record, perform a series of STACK PEEKS.
 *  Using the previously created record, repeatedly call stack peek with
 *  varying numbers of peek counts.
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 *  + keystr: String Key to find the record
 *  + lso_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 */
int lso_peek_test(char * keystr, char * lso_bin, int iterations,
        int seed, int data_format ) {
    static char * meth = "lso_peek_test()";
    cl_rv rc = 0;
    as_result * resultp;

    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s) Sd(%d) DF(%d)",
            MOD, meth, iterations, keystr, lso_bin, seed, data_format);

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = lso_bin;
    int        vals_read;
    int        misses;
    int        errs;

    INFO("[DEBUG]:<%s:%s>: Run peek() iterations(%d)", MOD, meth, iterations );

    int    peek_count;
    srand( seed );
    // NOTE: Must FREE the result for EACH ITERATION.
    citrusleaf_object_init_str( &o_key, keystr );
    for ( int i = 0; i < iterations ; i ++ ){
        peek_count = rand() % g_config->peek_max;
        INFO("[DEBUG]:<%s:%s>: Peek(%d)", MOD, meth, iterations );
        rc = aerospike_lstack_peek( &resultp,
                c, ns, set, &o_key, bname, peek_count, g_config->timeout_ms);

        process_read_results( meth, rc, resultp, i, &vals_read, &misses,
                &errs, peek_count );

        // Clean up -- release the result object
        if( resultp != NULL ) as_result_destroy( resultp );

        // Count up the reads (total)
        atomic_int_add( g_config->read_ops_counter, 1 );
        atomic_int_add( g_config->read_vals_counter, peek_count );
    } // end for each peek iteration
    citrusleaf_object_free( &o_key );

    INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end lso_peek_test()


// ======================================================================
/**
 *  LSO PUSH WITH_TRANSFORM TEST
 *  For a single record, perform a series of STACK PUSHES of BYTE-PACKED data.
 *  Create a new record, then repeatedly call stack push.
 *  We are going to use a five piece list as the new stack value, so we'll 
 *  use the "StumbleUpon" creation package (which just happens to have
 *  in it the things we need.
 */
int lso_push_with_transform_test(char * keystr, char * lso_bin, int iterations) {
    static char * meth = "lso_push_with_transform_test()";
    int rc = 0;

    INFO("[ENTER]:<%s:%s>: It(%d) Key(%s) LSOBin(%s)",
            MOD, meth, iterations, keystr, lso_bin );

    // Abbreviate for simplicity.
    cl_cluster * c  = g_config->asc;
    char       * ns = g_config->ns;
    char       * set  = g_config->set;
    char       * bname  = lso_bin;
    cl_object o_key;

    // Set up the Creation Spec parameter -- mostly setting the Package
    // (which is the name for a canned set of settings).
    char * create_package = "ProdListValBinStore";
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec,
            (as_val *) as_string_new("Package", false),
            (as_val *) as_string_new( create_package, false));

    INFO("[DEBUG]:<%s:%s>: Run push_with_transform() iterations(%d)",
          MOD, meth, iterations );
    citrusleaf_object_init_str( &o_key, keystr );
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

        rc = aerospike_lstack_create_and_push( c, ns, set, &o_key, bname,
                (as_val *)listp, create_spec, g_config->timeout_ms);
        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>:LSO PUSH WITH TRANSFROM Error: i(%d) rc(%d)",
                  MOD, meth, i, rc );
            as_val_destroy ( listp );
            goto cleanup;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
        as_val_destroy( listp ); // must destroy every iteration.
        listp = NULL;
    } // end for

cleanup:
    citrusleaf_object_free( &o_key );
    as_val_destroy( create_spec );

    return rc;
} // end lso_push_with_transform_test()

// ======================================================================
/** 
 *  LSO PEEK WITH TRANSFORM TEST
 *  For a single record, perform a series of STACK PEEKS.
 *  and do a server side transform of the byte-packed data
 *  Using the previously created record, repeatedly call stack peek with
 *  varying numbers of peek counts.
 */
int lso_peek_with_transform_test(char * keystr, char * lso_bin,
                                 char * filter_function,
                                 as_list * fargs,
                                 int iterations ) {
    static char * meth = "lso_peek_with_transform_test()";
    cl_rv rc = 0;

    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s)",
            MOD, meth, iterations, keystr, lso_bin );

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = lso_bin;
    int        vals_read;
    int        misses;
    int        errs;
    as_result * resultp;

    INFO("[DEBUG]:<%s:%s>: Run peek() iterations(%d)", MOD, meth, iterations );

    // NOTE: Must FREE the result (resultp) for EACH ITERATION.
    int peek_count = 2; // Soon -- set by Random Number
    citrusleaf_object_init_str( &o_key, keystr );
    for ( int i = 0; i < iterations ; i ++ ){
        peek_count++;
        rc = aerospike_lstack_peek_then_filter(
                &resultp, c, ns, set, &o_key, bname, peek_count,
                filter_function, fargs, g_config->timeout_ms);

        process_read_results( meth, rc, resultp, i, &vals_read, &misses,
                &errs, peek_count );

        // Count up the reads (total)
        atomic_int_add( g_config->read_vals_counter, peek_count );
        atomic_int_add( g_config->read_ops_counter, 1 );
    } // end for each peek iteration
    citrusleaf_object_free( &o_key );

    INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end lso_peek_with_transform_test()
