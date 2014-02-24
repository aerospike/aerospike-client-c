
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lstack.h>

#include "lstack_test.h"
#include "stdlib.h"

// Foward delare our config structure for all test files.

// LSTACK OPERATIONS
// Hold the main OPERATION functions for LSTACK.  This function is what
// all of the OTHER files will call when they want something done.
// (*) push()
// (*) peek()
// (*) size()
// (*) config()
//
// ALSO -- this module handles the TESTS for the basic operations:
// (*) small push()
// (*) small peek()
//
// (*) medium push()
// (*) medium peek()
//
// (*) large push()
// (*) large peek()
//


static char * MOD = "lstack_operations.c::13_04_26";
static char * LDT = "LSTACK";

#define LSTACK_DEBUG false
/******************************************************************************
 * OPERATION FUNCTIONS
 *****************************************************************************/

/**
 * Process all read results the same way.
 * TODO: Check the SIZE of the returned list against the asked for
 * "peek count".  Notice that when filters are applied, then we may get LESS
 * than what we asked for.
 */
int process_read_results( char * meth, cl_rv rc, as_result * resultp,
        int i, int * valsp, int * missesp, int * errsp, int count )
{
    static char * tm = "process_read_results()";
    if( LSTACK_DEBUG ) {
        INFO("	[ENTER]:<%s:%s>:%s From (%s) i(%d) Count(%d)", MOD, LDT, meth, tm, i, count );
    }
    char * valstr;
    int success_count = 0;
    int fail_count= 0;
    as_val *result_valp;

    if( rc == CITRUSLEAF_OK ){
        if ( resultp && resultp->is_success ) {
            if( LSTACK_DEBUG ){
                valstr = as_val_tostring( resultp->value );
                    printf("	[DEBUG]<%s:%s>(%s) READ SUCCESS: Val(%s)\n",
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
                if( LSTACK_DEBUG ) {
                	INFO("[SUCCESS]:<%s:%s>:Peek results:PK(%d) Count(%d) LIST[%s]",
                        MOD, meth, count, result_valp->count, valstr );
                }
            	free( valstr );
            	
            } else {
            	INFO("[UNSURE]:<%s:%s>:Peek results: Wanted List: TYPE[%s]",
                        MOD, meth, as_val_type(resultp->value) );
                fail_count = 1;
                return CITRUSLEAF_FAIL_INVALID_DATA;
            }
        } else {
            (*missesp)++;
            INFO("[ERROR]<%s:%s>(%s) Read OK: Result Error: i(%d) rc(%d)",
                 MOD, meth, LDT, i, rc);
            // Don't break (for now) just keep going.
            fail_count = 1;
            return CITRUSLEAF_FAIL_INVALID_DATA;
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
    g_config->success_counter +=  success_count;
    g_config->fail_counter += fail_count;
    return rc;
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
    char sourceChars[26] = "abcdefghijklmnopqrstuvwxyz";
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
            // NOTE: RIght now, this is just a simple, variable size string
            // based on the value_len parameter in the config structure.
            mallocd_buf = (char *) malloc( g_config->value_len+1);
            srand( seed );

            //generate random string length from the given value_len
            int new_val = rand() % g_config->value_len;
            int i;
            for (i=0; i<new_val; ++i) {
                mallocd_buf[i] = sourceChars[rand()%26];
            }
            mallocd_buf[new_val] = '\0';

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
 *  LSO SIZE TEST
 *  For the given record (associated with 'keystr'), return the size in terms
 *  of number of elements in the stack.
 *
 *  Parms:
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + size: Size of lstack
 */

int lstack_size_test(char * keystr, char * ldt_bin, uint32_t   * size) {
    static char * meth = "lstack_size_test()";
    if( LSTACK_DEBUG ) {
        INFO("      [ENTER]:<%s:%s>:From %s", MOD, LDT, meth );
    }
    int rc = 1;

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;

    citrusleaf_object_init_str( &o_key, keystr );

    //check size of lstack
    rc = aerospike_lstack_size( size,
                c, ns, set, &o_key, bname, g_config->timeout_ms);
    citrusleaf_object_free( &o_key );

    return rc;
} 

// ======================================================================
/**
 *  LSO Config TEST
 *  For the given record (associated with 'keystr'), return the Config information.
 *
 *  Parms:
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 */

int lstack_config_test(char * keystr, char * ldt_bin) {
    static char * meth = "lstack_config_test()";
    if( LSTACK_DEBUG ) {
        INFO("      [ENTER]:<%s:%s>:From %s", MOD, LDT, meth );
    }
    int rc = 0;

    char * valstr;
    as_result * resultp;
    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;

    citrusleaf_object_init_str( &o_key, keystr );

    //print config of lstack
    rc = aerospike_lstack_config( &resultp,
                c, ns, set, &o_key, bname, g_config->timeout_ms);

    if ( rc == CITRUSLEAF_OK ) {
        valstr = as_val_tostring( resultp->value );
        printf("	Config parameters:\n	%s \n", valstr);
    } else {
        // print error code
    }
    citrusleaf_object_free( &o_key );

    return rc;
}

// ======================================================================
/**
 *  LSO ALL DATA VALIDATION TEST
 *  For a single record, perform a series of STACK VALIDATION.
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 *  + peek_count: Number of peek from stack
 */

int lstack_alldata_validation(char * user_key, char * ldt_bin, int iterations, 
        int seed, int format, int peek_count){
    static char * meth = "lstack_random_validation()";

    char * valstr;
    int iseed;
    as_val * valp ;

    as_result * resultp;
    cl_object  o_key;

    int rc = lstack_push_test( user_key, ldt_bin, iterations, seed, format );

    if( rc == 0) {
        citrusleaf_object_init_str( &o_key, user_key );
        rc = aerospike_lstack_peek( &resultp,
                g_config->asc, g_config->ns, g_config->set, &o_key, ldt_bin, peek_count, g_config->timeout_ms);
    }

    srand(seed);
    //skip random numbers upto (iterations-peek_count)
    for(int j = 0; j<(iterations-peek_count);j++){
         iseed = j * 10;
         generate_value( &valp, iseed, format );
    }
    //validate the stack elements
    for ( int i = 0; i < peek_count; i++ ) {
        iseed = (iterations-peek_count + i) * 10;

        generate_value( &valp, iseed, format );

        as_list * l = (as_list *) resultp->value;
        valstr = as_val_tostring(as_list_get(l,peek_count-i-1));

        if(strcmp(as_val_tostring(valp), valstr))
             return CITRUSLEAF_FAIL_INVALID_DATA;
        //printf("push value=%s..... peek_value=%s\n",as_val_tostring(valp), valstr);

    }
    return rc;
}

// ======================================================================
/**
 *  LSO DATA VALIDATION TEST
 *  For a single record, perform a series of push and peek to validate top of stack.
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 */

int lstack_data_validation(char * keystr, char * ldt_bin, int iterations,
        int seed, int data_format ) {
    static char * meth = "lstack_data_validation()";
    if( LSTACK_DEBUG ) {
        INFO("      [ENTER]:<%s:%s>:From %s", MOD, LDT, meth );
    }
    cl_rv rc = 0;
    int i;
    as_result * resultp;

    as_val * valp ;
    //    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s) Sd(%d) DF(%d)",
    //            MOD, meth, iterations, keystr, ldt_bin, seed, data_format);

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;
    int        vals_read;
    int        misses;
    int        errs;
    int        read_success = 0;
    char * create_package = "StandardList";
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
    (as_val *) as_string_new( create_package, false));



    //    INFO("[DEBUG]:<%s:%s>: Run peek() iterations(%d)", MOD, meth, iterations );
    int    peek_count = 1;
    srand( seed );
    int iseed;
    // NOTE: Must FREE the result for EACH ITERATION.
    citrusleaf_object_init_str( &o_key, keystr );

    for ( i = 0; i < iterations ; i ++ ){
        //     INFO("[DEBUG]:<%s:%s>: Peek(%d)", MOD, meth, iterations );

        iseed = i * 10 ;
        generate_value( &valp, iseed, data_format );

        rc = aerospike_lstack_create_and_push(
                c, ns, set, &o_key, bname, valp, create_spec,
                g_config->timeout_ms);
        //after each push validate top element
        if(rc == CITRUSLEAF_OK) {
            rc = aerospike_lstack_peek( &resultp,
                    c, ns, set, &o_key, bname, peek_count, g_config->timeout_ms);

            if(rc == CITRUSLEAF_OK) {
                char comp[6+strlen(as_val_tostring(valp))];
                sprintf(comp,"List(%s)",as_val_tostring(valp));
                if(strcmp(comp, as_val_tostring(resultp->value)) == 0) {
                    //printf("result: act-%s exp-%s\n ",comp, as_val_tostring(resultp->value));
                    rc = process_read_results( meth, rc, resultp, i, &vals_read, &misses,
                        &errs, peek_count );
                    read_success++;
                }
            } 
        }
        // Count up the reads (total)
        g_config->write_ops_counter += 1;
        g_config->write_vals_counter += 1;
        // Count up the write (total)
        g_config->read_ops_counter += 1;
        g_config->read_vals_counter += peek_count;
    } // end for each push iteration

    if( resultp != NULL ) {
        as_result_destroy( resultp );
    }

    if(iterations != read_success)
            rc =  CITRUSLEAF_FAIL_INVALID_DATA; 
    citrusleaf_object_free( &o_key );

    //INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end lstack_peek_test()

// ======================================================================
/**
 *  LSO PUSH TEST
 *  For a single record, perform a series of STACK PUSHES.
 *  Create a new record, then repeatedly call stack push.
 *  This should work for data that is a NUMBER, a STRING or a LIST.
 *  Parms:
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 */
int lstack_push_test(char * keystr, char * ldt_bin, int iterations, int seed,
        int data_format ) {
    static char * meth = "lstack_push_test()";
    int rc = 0;
    int i;
    if( LSTACK_DEBUG ) {
    INFO("[ENTER]:<%s:%s>: It(%d) Key(%s) LSOBin(%s) Seed(%d)",
            MOD, meth, iterations, keystr, ldt_bin, seed);
    }
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
    char       * bname = ldt_bin;
    int          iseed;

//    INFO("[DEBUG]:<%s:%s>: Run push() iterations(%d)", MOD, meth, iterations );
    citrusleaf_object_init_str( &o_key, keystr );
    for ( i = 0; i < iterations; i++ ) {
        iseed = i * 10;
        generate_value( &valp, iseed, data_format );

        rc = aerospike_lstack_create_and_push(
                c, ns, set, &o_key, bname, valp, create_spec,
                g_config->timeout_ms);

        if ( rc != CITRUSLEAF_OK ) {
//            INFO("[ERROR]:<%s:%s>:PUSH Error: i(%d) rc(%d)", MOD, meth,i,rc );
            as_val_destroy ( valp );
            goto cleanup;
        }
        // Count the write operation for stats gathering
        g_config->write_ops_counter += 1;
        g_config->write_vals_counter += 1;
        as_val_destroy( valp ); // must destroy every iteration.
        valp = NULL; // unnecessary insurance
    } // end for

cleanup:
    citrusleaf_object_free( &o_key );
    as_val_destroy( create_spec );

    return rc;
} // end lstack_push_test()


// ======================================================================
/**
 *  LSO PEEK TEST
 *  For a single record, perform a series of STACK PEEKS.
 *  Using the previously created record, repeatedly call stack peek with
 *  varying numbers of peek counts.
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 */
int lstack_peek_test(char * keystr, char * ldt_bin, int iterations,
        int seed, int data_format ) {
    static char * meth = "lstack_peek_test()";
    cl_rv rc = 0;
    int i;
    as_result * resultp;

//    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s) Sd(%d) DF(%d)",
//            MOD, meth, iterations, keystr, ldt_bin, seed, data_format);

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;
    int        vals_read;
    int        misses;
    int        errs;

//    INFO("[DEBUG]:<%s:%s>: Run peek() iterations(%d)", MOD, meth, iterations );

    int    peek_count;
    srand( seed );
    // NOTE: Must FREE the result for EACH ITERATION.
    citrusleaf_object_init_str( &o_key, keystr );
    for ( i = 0; i < iterations ; i ++ ){
        peek_count = rand() % g_config->peek_max;
        //     INFO("[DEBUG]:<%s:%s>: Peek(%d)", MOD, meth, iterations );
        rc = aerospike_lstack_peek( &resultp,
                c, ns, set, &o_key, bname, peek_count, g_config->timeout_ms);
        if(rc == CITRUSLEAF_OK) {

            rc = process_read_results( meth, rc, resultp, i, &vals_read, &misses,
                    &errs, peek_count );
        }
        // Clean up -- release the result object
        if( resultp != NULL ) as_result_destroy( resultp );

        // Count up the reads (total)
        g_config->read_ops_counter += 1;
        g_config->read_vals_counter += peek_count;
    } // end for each peek iteration
    citrusleaf_object_free( &o_key );

    //INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end lstack_peek_test()


// ======================================================================
/**
 *  LSO PUSH WITH_TRANSFORM TEST
 *  For a single record, perform a series of STACK PUSHES of BYTE-PACKED data.
 *  Create a new record, then repeatedly call stack push.
 *  We are going to use a five piece list as the new stack value, so we'll 
 *  use the "StumbleUpon" creation package (which just happens to have
 *  in it the things we need.
 */
int lstack_push_with_transform_test(char * keystr, char * ldt_bin, int iterations) {
    static char * meth = "lstack_push_with_transform_test()";
    int rc = 0;
    int i;

    INFO("[ENTER]:<%s:%s>: It(%d) Key(%s) LSOBin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    // Abbreviate for simplicity.
    cl_cluster * c  = g_config->asc;
    char       * ns = g_config->ns;
    char       * set  = g_config->set;
    char       * bname  = ldt_bin;
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
    for ( i = 0; i < iterations; i++ ) {
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
        g_config->write_ops_counter += 1;
        g_config->write_vals_counter += 1;
        as_val_destroy( listp ); // must destroy every iteration.
        listp = NULL;
    } // end for

cleanup:
    citrusleaf_object_free( &o_key );
    as_val_destroy( create_spec );

    return rc;
} // end lstack_push_with_transform_test()

// ======================================================================
/** 
 *  LSO PEEK WITH TRANSFORM TEST
 *  For a single record, perform a series of STACK PEEKS.
 *  and do a server side transform of the byte-packed data
 *  Using the previously created record, repeatedly call stack peek with
 *  varying numbers of peek counts.
 */
int lstack_peek_with_transform_test(char * keystr, char * ldt_bin,
                                 char * filter_function,
                                 as_list * fargs,
                                 int iterations ) {
    static char * meth = "lstack_peek_with_transform_test()";
    cl_rv rc = 0;

    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;
    int        vals_read;
    int        misses;
    int        errs;
    int        i;
    as_result * resultp;

    INFO("[DEBUG]:<%s:%s>: Run peek() iterations(%d)", MOD, meth, iterations );

    // NOTE: Must FREE the result (resultp) for EACH ITERATION.
    int peek_count = 2; // Soon -- set by Random Number
    citrusleaf_object_init_str( &o_key, keystr );
    for ( i = 0; i < iterations ; i ++ ){
        peek_count++;
        rc = aerospike_lstack_peek_then_filter(
                &resultp, c, ns, set, &o_key, bname, peek_count,
                filter_function, fargs, g_config->timeout_ms);

        process_read_results( meth, rc, resultp, i, &vals_read, &misses,
                &errs, peek_count );

        // Count up the reads (total)
        g_config->read_vals_counter += peek_count;
        g_config->read_ops_counter += 1;
    } // end for each peek iteration
    citrusleaf_object_free( &o_key );

    INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end lstack_peek_with_transform_test()


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

// ++====================++
// || REGULAR OPERATIONS ||
// ++====================++

TEST( lstack_operations_small_push, "lstack push small" ) {
    static char * meth = "lstack_operations_small_push()";
    int rc = 0;

    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_small";
    char * ldt_bin_str    = "str_small";
    char * ldt_bin_list   = "list_small";

    int    iterations = 100 ;
    int    seed       = 111;

    printf("\tTest(%s) called\n", meth );

    
    rc = lstack_push_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_num );

    g_config->value_len = 10;
    rc = lstack_push_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_str );


    rc = lstack_push_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_list );
} // end lstack_operations_small_push()

TEST( lstack_operations_medium_push, "lstack push medium" ) {
    static char * meth = "lstack_operations_medium_push()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_medium";
    char * ldt_bin_str    = "str_medium";
    char * ldt_bin_list   = "list_medium";

    int    iterations = 1000 ;
    int    seed       = 111;

    rc = lstack_push_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_num );

    g_config->value_len = 100;
    rc = lstack_push_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_str );

    rc = lstack_push_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_list );
} // end   lstack_operations_medium_push()

TEST( lstack_operations_large_push, "lstack push large" ) {
    static char * meth = "lstack_operations_large_push()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_large";
    char * ldt_bin_str    = "str_large";
    char * ldt_bin_list   = "list_large";

    int    iterations = 10000 ;
    int    seed       = 111;

    rc = lstack_push_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );    
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_num );

    g_config->value_len = 1000;
    rc = lstack_push_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_str );

    rc = lstack_push_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_list );
} // end   lstack_operations_large_push()


TEST( lstack_operations_small_peek, "lstack peek small" ) {
    static char * meth = "lstack_operations_small_peek()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_small";
    char * ldt_bin_str    = "str_small";
    char * ldt_bin_list   = "list_small";

    int    iterations = 1;
    int    seed       = 111;
    g_config->peek_max = 50;

    rc = lstack_peek_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_num );

    rc = lstack_peek_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_str );

    rc = lstack_peek_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_list );
} // end   lstack_operations_small_peek()

TEST( lstack_operations_medium_peek, "lstack peek medium" ) {
    static char * meth = "lstack_operations_medium_peek()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_medium";
    char * ldt_bin_str    = "str_medium";
    char * ldt_bin_list   = "list_medium";

    int    iterations = 1;
    int    seed       = 111;
    g_config->peek_max = 500;
    
    rc = lstack_peek_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_num );

    rc = lstack_peek_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_str );

    rc = lstack_peek_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_list );
} // end   lstack_operations_medium_peek()

TEST( lstack_operations_large_peek, "lstack peek large" ) {
    static char * meth = "lstack_operations_large_peek()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_large";
    char * ldt_bin_str    = "str_large";
    char * ldt_bin_list   = "list_large";

    int    iterations = 1;
    int    seed       = 111;
    g_config->peek_max = 5000;

    rc = lstack_peek_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_num );

    rc = lstack_peek_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_str );
        
    rc = lstack_peek_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
    assert_int_eq( rc, 0 );
    printf("\tTest(%s) Passed for %s\n", meth, ldt_bin_list );
 } // end   lstack_operations_large_peek()
