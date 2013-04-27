
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lstack.h>
#include "lstack_test.h"

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
/******************************************************************************
 * OPERATION FUNCTIONS
 *****************************************************************************/

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
    g_config->success_counter +=  success_count;
    g_config->fail_counter += fail_count;

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

    INFO("[ENTER]:<%s:%s>: It(%d) Key(%s) LSOBin(%s) Seed(%d)",
            MOD, meth, iterations, keystr, ldt_bin, seed);

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

    INFO("[DEBUG]:<%s:%s>: Run push() iterations(%d)", MOD, meth, iterations );
    citrusleaf_object_init_str( &o_key, keystr );
    for ( i = 0; i < iterations; i++ ) {
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

    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s) Sd(%d) DF(%d)",
            MOD, meth, iterations, keystr, ldt_bin, seed, data_format);

    cl_cluster * c     = g_config->asc;
    cl_object  o_key;
    char       * ns    = g_config->ns;
    char       * set   = g_config->set;
    char       * bname = ldt_bin;
    int        vals_read;
    int        misses;
    int        errs;

    INFO("[DEBUG]:<%s:%s>: Run peek() iterations(%d)", MOD, meth, iterations );

    int    peek_count;
    srand( seed );
    // NOTE: Must FREE the result for EACH ITERATION.
    citrusleaf_object_init_str( &o_key, keystr );
    for ( i = 0; i < iterations ; i ++ ){
        peek_count = rand() % g_config->peek_max;
        INFO("[DEBUG]:<%s:%s>: Peek(%d)", MOD, meth, iterations );
        rc = aerospike_lstack_peek( &resultp,
                c, ns, set, &o_key, bname, peek_count, g_config->timeout_ms);

        process_read_results( meth, rc, resultp, i, &vals_read, &misses,
                &errs, peek_count );

        // Clean up -- release the result object
        if( resultp != NULL ) as_result_destroy( resultp );

        // Count up the reads (total)
        g_config->read_ops_counter += 1;
        g_config->read_vals_counter += peek_count;
    } // end for each peek iteration
    citrusleaf_object_free( &o_key );

    INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
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

TEST( lstack_operations_small_push, "lstack push small" ) {
    static char * meth = "lstack_operations_small_push()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end lstack_operations_small_push()

TEST( lstack_operations_medium_push, "lstack push medium" ) {
    static char * meth = "lstack_operations_medium_push()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_operations_medium_push()

TEST( lstack_operations_large_push, "lstack push large" ) {
    static char * meth = "lstack_operations_large_push()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_operations_large_push()


TEST( lstack_operations_small_peek, "lstack peek small" ) {
    static char * meth = "lstack_operations_small_peek()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_operations_small_peek()

TEST( lstack_operations_medium_peek, "lstack peek medium" ) {
    static char * meth = "lstack_operations_medium_peek()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_operations_medium_peek()

TEST( lstack_operations_large_peek, "lstack peek large" ) {
    static char * meth = "lstack_operations_large_peek()";
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    return true;
} // end   lstack_operations_large_peek()

