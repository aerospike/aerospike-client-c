/*  Citrusleaf General Performance Test Program Template
 *  Tailored for the Large Stack Object Test
 *  lset.c - Simple LSET example
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <citrusleaf/aerospike_lset.h>
#include <stdlib.h>
#include "test.h"

// This module, with version info (for tracing)
#define MOD "LDTex:lset.c_04_18"

// ==========================================================================
// This module contains the main test code for the Large Set Feature
// in Aespike 3.0.  It does the Setup (sets parameters, attaches to the
// cluster) and it does the basic Large Stack operations:
// (*) aerospike_lset_create()
// (*) aerospike_lset_create_and_insert()
// (*) aerospike_lset_insert()
// (*) aerospike_lset_search()
// (*) aerospike_lset_search_then_filter()
// (*) aerospike_lset_exists()
// (*) aerospike_lset_config()
// (*) aerospike_lset_size()
// ==========================================================================
/** 
 *  Initialize Test: Do the set up for a test so that the regular
 *  Aerospike functions can run.
 */
int setup_test( int argc, char **argv ) {
    static char * meth = "setup_test()";
    int rc = CITRUSLEAF_OK;
    char * host; // for each iteration of "add host"
    int port; // for each iteration of "add host"
    uint32_t timeout_ms;

    // show cluster setup
    INFO("[ENTER]<%s:%s>Startup: host %s port %d ns %s set %s",
            MOD, meth, g_config->host, g_config->port, g_config->ns,
            g_config->set == NULL ? "" : g_config->set);

    citrusleaf_init();
    citrusleaf_set_debug(true);

    // create the cluster object
    cl_cluster *asc = citrusleaf_cluster_create();
    if (!asc) { 
        INFO("[ERROR]<%s:%s>: Fail on citrusleaf_cluster_create()", MOD, meth);
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
        INFO("[DEBUG]<%s:%s>:Adding host(%s) port(%d)", MOD, meth, host, port);
        rc = citrusleaf_cluster_add_host(asc, host, port, timeout_ms);
        if (rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]<%s:%s>:could not connect to host(%s) port(%d)",
                    MOD, meth, host, port);

            INFO("[ERROR]<%s:%s>:Trying more nodes", MOD, meth );
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


// Object Formats: for both Key/Object generation and search values.
// defined in test_config.h

/**
 * Generate Insert Value.  Pick the format based on the setting in the
 * config structure:
 * 0: List Value
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

void process_read_results( char * meth, cl_rv rc, as_result * resultp,
        int i, int * hitsp, int * missesp, int * errsp, as_val * valp)
{
    char * valstr;

    if( TRA_DEBUG ){
        char * valstr = as_val_tostring( valp );
        printf("<%s:%s> Result(%d) Search(%s)\n", MOD, meth, rc, valstr);
        free( valstr );
    }

    if( rc == CITRUSLEAF_OK ){
        if ( resultp && resultp->is_success ) {
            if( TRA_DEBUG ){
                valstr = as_val_tostring( resultp->value );
                printf("[DEBUG]<%s:%s>LSET READ SUCCESS: Val(%s)\n",
                        MOD, meth, valstr);
                free( valstr );
                (*hitsp)++;
            }
        } else {
            (*missesp)++;
            INFO("[ERROR]<%s:%s>LSET Read OK: Result Error: i(%d) rc(%d)",
                 MOD, meth, i, rc);
            // Don't break (for now) just keep going.
        }
    } else if( rc == CITRUSLEAF_FAIL_NOTFOUND ){
        (*errsp)++;
        INFO("[ERROR]<%s:%s>LSET Read Record NOT FOUND: i(%d) rc(%d)",
             MOD, meth, i, rc);
    } else {
        (*errsp)++;
        INFO("[ERROR]<%s:%s>OTHER ERROR: i(%d) rc(%d)",
             MOD, meth, i, rc);
    }

} // end process_read_results()

// ======================================================================
/**
 *  LSET WRITE TEST
 *  For a single record, perform a series of Set Writes.
 *  Create a new record, then repeatedly call Set_insert().
 *  Create values of the specified format
 */
int ldt_write_test(char * keystr, char * ldt_bin, int iterations,
        int seed, int format )
{
    static char * meth = "ldt_write_test()";
    cl_rv rc = CITRUSLEAF_OK;

    INFO("[ENTER]<%s:%s>: It(%d) Key(%s) ldt_bin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    //    Leave create as IMPLICIT for now.
    //    Later we can add an explicit create for example purposes.

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;
    cl_object o_key;
    as_val * valp;
    int val;

    citrusleaf_object_init_str( &o_key, keystr ); // One Key -- set once
    INFO("[DEBUG]<%s:%s>: Run INSERT() iterations(%d)", MOD, meth, iterations );
    for ( int i = 0; i < iterations; i++ ) {
        val = i * 10;
        as_list * listp = as_arraylist_new( 5, 0 );
        generate_value( &valp, seed + val, format );

        // Use the correct interface, with key type cl_object.
        rc = aerospike_lset_insert(
                c, ns, set, &o_key, bname, valp,
                g_config->timeout_ms);
        if (rc) {
            INFO("[ERROR]<%s:%s>LSET Write Error: i(%d) rc(%d)",MOD, meth,i,rc);
            if( valp != NULL )
                as_val_destroy ( valp );
            return -1;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
        as_val_destroy( valp ); // must destroy every iteration.
        listp = NULL;
    } // end for
    citrusleaf_object_free( &o_key ); // release key object

    return rc;
} // end ldt_write_test()


// ======================================================================
/**
 *  LSET READ TEST
 *  For a single record, perform a series of SET READS.
 *  Using the previously created record, repeatedly call set read with
 *  varying keys (value type is passed in via "format").
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 */
cl_rv ldt_read_test(char * keystr, char * ldt_bin, int iterations,
        int seed, int format )
{
    static char * meth = "ldt_read_test()";
    cl_rv rc = CITRUSLEAF_OK;

    INFO("[ENTER]<%s:%s>: Iterations(%d) Key(%s) ldt_bin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;

    INFO("[DEBUG]<%s:%s>: Run read() iterations(%d)", MOD, meth, iterations );

    int hits = 0;
    int misses = 0;
    int errs = 0;

    char * valstr     = NULL; // Hold Temp results from as_val_tostring()
    as_val * valp;
    as_val * result_valp;
    cl_object o_key;
    int val;
    // NOTE: Must FREE the result for EACH ITERATION.
    as_result * resultp;

    citrusleaf_object_init_str( &o_key, keystr );
    for ( int i = 0; i < iterations ; i ++ ){
        val = i * 10;
        generate_value( &valp, seed + val, format );

        // Use the current interface for passing in a key (cl_object).
        rc  = aerospike_lset_search( &resultp, 
            c, ns, set, &o_key, bname, valp, g_config->timeout_ms);

        process_read_results( meth, rc, resultp,i,&hits, &misses, &errs, valp);

        // Clean up: release the result object
        if( resultp != NULL ) as_result_destroy( resultp );

        // Count up the reads (total)
        atomic_int_add( g_config->read_ops_counter, 1 );
        atomic_int_add( g_config->read_vals_counter, 1 );
    } // end for each read iteration
    citrusleaf_object_free( &o_key );

	printf("[RESULTS]:<%s:%s>:HIT_TEST: It(%d) Hits(%d) Misses(%d) Errs(%d)\n", 
					MOD, meth, iterations, hits, misses, errs );

    INFO("[EXIT]<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end ldt_read_test()

// ======================================================================
/**
 *  LSET Write a NUMBER WITH_TRANSFORM TEST
 *  For a single record, perform a series of SET WRITES of BYTE-PACKED data.
 *  Create a new record, then repeatedly call set Insert.
 */
cl_rv ldt_write_number_with_transform_test(
        char * keystr, char * ldt_bin, char * create_package, int iterations)
{
    static char * meth = "ldt_write_number_with_transform_test()";
    cl_rv rc = CITRUSLEAF_OK;

    INFO("[ENTER]<%s:%s>: It(%d) Key(%s) ldt_bin(%s) Package(%s)",
            MOD, meth, iterations, keystr, ldt_bin, create_package );

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
    int range = iterations * 4;
    as_integer as_int_val;
    // Start off the pseudo-random number generator based on the iterations.
    srand( iterations );

    INFO("[DEBUG]<%s:%s>: Run write_with_transform() iterations(%d)",
          MOD, meth, iterations );
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );
    for ( int i = 0; i < iterations; i++ ) {
        int64_t base_value = rand() % range;
        as_integer_init( &as_int_val, base_value );

        rc = aerospike_lset_create_and_insert(
                c, ns, set, &o_key, bname, (as_val *) &as_int_val,
                create_spec, g_config->timeout_ms);
        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]<%s:%s>: LSET WRITE WITH TRANSFROM Err: i(%d) rc(%d)",
                  MOD, meth, i, rc );
            return rc;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
    } // end for each iteration.
    citrusleaf_object_free( &o_key );

    return rc;
} // end ldt_write_number_with_transform_test()


// ======================================================================
/**
 *  LSET Write a LIST  WITH_TRANSFORM TEST
 *  For a single record, perform a series of SET WRITES of BYTE-PACKED data.
 *  Create a new record, then repeatedly call set Insert.
 */
cl_rv ldt_write_list_with_transform_test(
        char * keystr, char * ldt_bin, char * create_package, int iterations)
{
    static char * meth = "ldt_write_list_with_transform_test()";
    cl_rv rc = CITRUSLEAF_OK;

    INFO("[ENTER]<%s:%s>: It(%d) Key(%s) ldt_bin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    // Abbreviate for simplicity.
    cl_cluster * c  = g_config->asc;
    char       * ns = g_config->ns;
    char       * set  = g_config->set;
    char       * bname  = ldt_bin;
    cl_object o_key;

    // Build our as_map ONCE, and reuse it. In this type of usage, we won't
    // know in advance whether or not our SET is created, so we want to pass
    // in the "Creation Spec" to create the LSET if it is not already there.
    // Package="StandardList" -> Overriding Default PageMode(Bytes)
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
                            (as_val *) as_string_new("StandardList", false));

    INFO("[DEBUG]<%s:%s>: Run write_with_transform() iterations(%d)",
          MOD, meth, iterations );

    citrusleaf_object_init_str( &o_key, keystr );
    for ( int i = 0; i < iterations; i++ ) {
        int val         = i * 10;
        as_list * listp = as_arraylist_new( 5, 0 );
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
                c, ns, set, &o_key, bname, (as_val *)listp,
                create_spec, g_config->timeout_ms);
        if (rc) {
            INFO("[ERROR]<%s:%s>WRITE WITH TRANSFROM Error: i(%d) rc(%d)",
                  MOD, meth, i, rc );
            as_val_destroy ( listp );
            return -1;
        }
        // Count the write operation for stats gathering
        atomic_int_add( g_config->write_ops_counter, 1 );
        atomic_int_add( g_config->write_vals_counter, 1 );
        as_val_destroy( listp ); // must destroy every iteration.
        listp = NULL;
    } // end for
    citrusleaf_object_free( &o_key );

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
cl_rv ldt_read_number_with_filter_test(char * keystr, char * ldt_bin,
        char * filter, as_list * fargs, int iterations )
{
    static char * meth = "ldt_read_with_transform_test()";

    if( TRA_ENTER )
        INFO("[ENTER]<%s:%s>: Iterations(%d) Key(%s) ldt_bin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * key   = keystr;
    char       * bname = ldt_bin;
    as_result * resultp;
    cl_rv rc = CITRUSLEAF_OK; // zero
    as_val * valp;

    int hits = 0;
    int misses = 0;
    int errs = 0;

    INFO("[DEBUG]<%s:%s>: Run read() iterations(%d)", MOD, meth, iterations );

    // NOTE: Must FREE the result for EACH ITERATION.
    char * valstr = NULL; // Hold Temp results from as_val_tostring()
    as_integer search_value;
    int range = iterations * 4;
    // Start off the pseudo-random number generator based on the iterations.
    srand( iterations );
    int64_t base_value;

    // Set up the key once and reuse it.
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );
    for ( int i = 0; i < iterations ; i ++ ){
        generate_value( &valp, i, NUMBER_FORMAT );

        // Load up the key that we'll feed into the call (and we'll remember
        // to free it after we're done).
        rc = aerospike_lset_search_then_filter( &resultp,
                c, ns, set, &o_key, bname, valp,
                filter, fargs, g_config->timeout_ms);

        process_read_results( meth, rc, resultp, i, &hits, &misses, &errs,
                (as_val *) &search_value);

        // Count up the reads (total)
        atomic_int_add( g_config->read_vals_counter, 1 );
        atomic_int_add( g_config->read_ops_counter, 1 );
    } // end for each read iteration
    citrusleaf_object_free( &o_key ); // done with the key object.

	printf("[RESULTS]:<%s:%s>:HIT_TEST: It(%d) Hits(%d) Misses(%d) Errs(%d)\n", 
					MOD, meth, iterations, hits, misses, errs );

    INFO("[EXIT]<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end ldt_read_number_with_filter_test()

/** 
 *  LSET READ LIST WITH FILTER TEST
 *  For a single record, perform a series of SET READS,
 *  and do a server side transform of the byte-packed data.
 *  Using the previously created record, repeatedly call set read with
 *  varying keys.
 */
cl_rv ldt_read_list_with_filter_test(
        char * keystr, char * ldt_bin, char * filter,
        as_list * fargs, int iterations )
{
    static char * meth = "ldt_read_list_with_transform_test()";
    cl_rv rc = CITRUSLEAF_OK;

    INFO("[ENTER]<%s:%s>: Iterations(%d) Key(%s) ldt_bin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = g_config->asc;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * key   = keystr;
    char       * bname = ldt_bin;
    as_result * resultp;

    INFO("[DEBUG]<%s:%s>: Run read() iterations(%d)", MOD, meth, iterations );

    // NOTE: Must FREE the result for EACH ITERATION.
    char * valstr = NULL; // Hold Temp results from as_val_tostring()
    int range = iterations * 4;
    as_val * valp;
    srand( iterations );

    // Set up the key once and reuse it (same key for all test iterations)
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );
    int hits = 0;
    int misses = 0;
    int errs = 0;
    for ( int i = 0; i < iterations ; i ++ ){
        generate_value( &valp, rand(), NUMBER_FORMAT);
        resultp = NULL; // start clean
        rc = aerospike_lset_search_then_filter( &resultp,
                c, ns, set, &o_key, bname, valp,
                filter, fargs, g_config->timeout_ms);
        process_read_results( meth, rc, resultp, i, &hits, &misses, &errs,
                valp);

        // Clean up: release the result object
        // We hope that if the resultp is NON-NULL, then there's a valid
        // object to release.
        if( resultp != NULL ) as_result_destroy( resultp );

        // Count up the reads (total)
        atomic_int_add( g_config->read_vals_counter, 1 );
        atomic_int_add( g_config->read_ops_counter, 1 );
    } // end for each read iteration
    citrusleaf_object_free( &o_key ); // done with key:  Release.

    printf("[RESULTS]:: Iterations(%d) Hits(%d) Misses(%d) Errs(%d)\n",
            iterations, hits, misses, errs );

    INFO("[EXIT]<%s:%s>: RC(%d)", MOD, MOD, meth, rc );
    return rc;
} // end ldt_read_list_with_filter_test()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  AS Large Set INSERT TEST
 *  For a single record, perform a series of Set Inserts
 *  Create a new record, then repeatedly call Set Insert
 */
cl_rv ldt_simple_insert_test(char * keystr, char * lset_bin, int iterations) {
	static char * meth = "ldt_simple_insert_test()";
	cl_rv rc = CITRUSLEAF_OK;

	INFO("[ENTER]:[%s]: It(%d) Key(%s) LSETBin(%s)\n",
			meth, iterations, keystr, lset_bin );

    char * create_package = "StandardList";
	cl_cluster * c     = g_config->asc;
	char       * ns    = g_config->ns;
	char       * set   = g_config->set;
	char       * bname = lset_bin;

	INFO("[DEBUG]:[%s]: lset_insert() iterations(%d)\n", meth, iterations );
	srand( 200 ); // Init our random number generator with a fixed seed.

    // Set up the Creation Spec parameter -- mostly setting the Package
    // (which is the name for a canned set of settings).
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
                            (as_val *) as_string_new( create_package, false));

	int num_ok   = 0;
	int num_errs = 0;
    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr );
	for ( int i = 0; i < iterations; i++ ) {
		unsigned int   base       = rand() % 500;
		as_val       * newSetItem = (as_val *) as_integer_new( base );

		rc = aerospike_lset_create_and_insert(
                c, ns, set, &o_key, bname, newSetItem,
                create_spec, g_config->timeout_ms );
		if (rc) {
			INFO("[ERROR]:[%s]: LSET INSERT Error: i(%d) rc(%d)\n",
				 meth, i, rc );
			num_errs++;
		} else {
			num_ok++;
		}
		as_val_destroy( newSetItem ); // must destroy every iteration.
	} // end for
    citrusleaf_object_free( &o_key );

	fprintf(stderr, "[RESULTS]:<%s>Test Results: Success(%d) Errors(%d)\n", 
			meth, num_ok, num_errs );

	return rc;
} // end ldt_simple_insert_test()


/** ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 *  AS Large Set SEARCH TEST
 *  For a single record, perform a series of Set Searches
 *  Using the previously created record, repeatedly call Set Search with
 *  various values (some valid, some invalid)
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 */
cl_rv ldt_simple_search_test(char * keystr, char * lset_bin, int iterations ) {
	static char * meth = "lset_search()";
    cl_rv rc = CITRUSLEAF_OK;

	INFO("[ENTER]:[%s]: Iterations(%d) Key(%s) LSETBin(%s)\n",
			meth, iterations, keystr, lset_bin );

	cl_cluster * c     = g_config->asc;
	char       * ns    = g_config->ns;
	char       * set   = g_config->set;
	char       * bname = lset_bin;

	INFO("[DEBUG]:[%s]: lset_search() iterations(%d)\n", meth, iterations );
	srand( 200 ); // Init our random number generator with a fixed seed.

	int hits   = 0; // Got what we were looking for
	int misses = 0; // Query ok: but did not find object
	int errs = 0; // Errors
    as_result * resultp;

    cl_object o_key;
    citrusleaf_object_init_str( &o_key, keystr ); // same key for whole loop
    for ( int i = 0; i < iterations; i++ ) {
        unsigned int base = rand() % 500;
		as_val    * newSetItem = (as_val *) as_integer_new( base ); 

        rc  = aerospike_lset_search( &resultp, c, ns, set, &o_key, bname,
                newSetItem, g_config->timeout_ms );

        process_read_results( meth, rc, resultp, i, &hits, &misses, &errs,
                newSetItem );

        if( resultp != NULL )
            as_result_destroy( resultp ); // Release the result object
        as_val_destroy( newSetItem ); // must destroy every iteration.
	} // end for each iteration

	printf("[RESULTS]:<%s:%s>:HIT_TEST: It(%d) Hits(%d) Misses(%d) Errs(%d)\n", 
					MOD, meth, iterations, hits, misses, errs );

    int miss_count = 5; // NEXT TEST: let's miss 5
	hits   = 0;
	errs = 0;
	misses = 0;
	for ( int i = 0; i < miss_count; i++ ) {
        resultp = NULL;
		unsigned int base = rand() % 500;
		as_val    * newSetItem = (as_val *) as_integer_new( base ); 
		rc = aerospike_lset_search( &resultp, c, ns, set, &o_key, bname,
                newSetItem, g_config->timeout_ms );
        process_read_results( meth, rc, resultp, i, &hits, &misses, &errs,
                newSetItem );

		as_result_destroy( resultp ); // Clean up: release the result object
		as_val_destroy( newSetItem ); // must destroy every iteration.
	} // end for each iteration

	printf("[RESULTS]:<%s:%s>:HIT_TEST: It(%d) Hits(%d) Misses(%d) Errs(%d)\n", 
					MOD, meth, miss_count, hits, misses, errs );

    citrusleaf_object_free( &o_key ); // release key

	INFO("[EXIT]:[%s]: RC(%d)\n", meth, rc );
	return rc;
} // end ldt_simple_search_test()
