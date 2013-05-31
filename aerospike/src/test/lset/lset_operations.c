
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lset.h>

#include "lset_test.h"
#include "stdlib.h"

// Foward delare our config structure for all test files.

// LSET OPERATIONS
// Hold the main OPERATION functions for LSET.  This function is what
// all of the OTHER files will call when they want something done.
// (*) insert()
// (*) search()
// (*) size()
// (*) config()
//
// ALSO -- this module handles the TESTS for the basic operations:
// (*) small insert()
// (*) small search()
//
// (*) medium insert()
// (*) medium search()
//
// (*) large insert()
// (*) large search()
//


static char * MOD = "lset_operations.c::13_04_26";
static char * LDT = "LSET";
/******************************************************************************
 * OPERATION FUNCTIONS
 *****************************************************************************/

/**
 * Process all read results the same way.
 * TODO: Check the SIZE of the returned list against the asked for
 * "search count".  Notice that when filters are applied, then we may get LESS
 * than what we asked for.
 */

void lset_process_read_results( char * meth, cl_rv rc, as_result * resultp,
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



/**
 * Create a list tuple for inserting/reading LIST values.
 */

as_val * lset_gen_list_val(int seed ) {

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


int lset_generate_value( as_val ** return_valpp, int seed, int val_type ){
    static char * meth = "generate_value()";
    int rc = 0;
    *return_valpp = NULL;  // Start with nothing.
    char * mallocd_buf = NULL;
    char sourceChars[26] = "abcdefghijklmnopqrstuvwxyz";
    switch( val_type ){
        case LIST_FORMAT:
            *return_valpp = lset_gen_list_val( seed );
            break;
        case NUMBER_FORMAT:
            // We have to malloc an int here because someone else will have
            // to reclaim (destroy) it.
            srand( seed );
            as_integer * intp = as_integer_new( rand() % lset_g_config->key_max);
            *return_valpp = (as_val *) intp;
            break;
        case STRING_FORMAT:
            // Malloc a string buffer, write in it, and then create a 
            // as_string object for it.
            // NOTE: RIght now, this is just a simple, variable size string
            // based on the value_len parameter in the config structure.
            mallocd_buf = (char *) malloc( lset_g_config->value_len+1);
            srand( seed );

            //generate random string length from the given value_len
            int new_val = rand() % lset_g_config->value_len;
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

int lset_create_test (char * keystr, char * ldt_bin ){

    cl_cluster * c     = lset_g_config->asc;
    cl_object  o_key;
    char       * ns    = lset_g_config->ns;
    char       * set   = lset_g_config->set;
    char       * bname = ldt_bin;


    char * create_package = "StandardList";
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
    (as_val *) as_string_new( create_package, false));

    citrusleaf_object_init_str( &o_key, keystr );

    cl_rv rv = 1;
    rv = aerospike_lset_create( c, ns, set, &o_key, bname, create_spec, lset_g_config->timeout_ms);

    citrusleaf_object_free( &o_key );
    as_val_destroy( create_spec );
    return rv;

}

// ======================================================================
/**
 *  LSET SIZE TEST
 *  For the given record (associated with 'keystr'), return the size in terms
 *  of number of elements in the set.
 *
 *  Parms:
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + size: Return size of lset
 */

int lset_size_test(char * keystr, char * ldt_bin, uint32_t   * size) {
    static char * meth = "lset_size_test()";
    int rc = 1;

    cl_cluster * c     = lset_g_config->asc;
    cl_object  o_key;
    char       * ns    = lset_g_config->ns;
    char       * set   = lset_g_config->set;
    char       * bname = ldt_bin;

    citrusleaf_object_init_str( &o_key, keystr );

    //check size of lset
    rc = aerospike_lset_size( size,
                c, ns, set, &o_key, bname, lset_g_config->timeout_ms);

    citrusleaf_object_free( &o_key );
    return rc;
} 

int lset_config_test(char * keystr, char * ldt_bin) {
    static char * meth = "lset_config_test()";
    int rc = 1;

    char * valstr;
    as_result * resultp;

    time_t cur_t;
    cur_t = time(NULL);

    cl_cluster * c     = lset_g_config->asc;
    cl_object  o_key;
    char       * ns    = lset_g_config->ns;
    char       * set   = lset_g_config->set;
    char       * bname = ldt_bin;

    citrusleaf_object_init_str( &o_key, keystr );

    //print config of lset
    rc = aerospike_lset_config( &resultp,
                c, ns, set, &o_key, bname, lset_g_config->timeout_ms);
    if ( rc == CITRUSLEAF_OK ) {
        valstr = as_val_tostring( resultp->value );
        printf("	Config parameters:\n	%s \n", valstr);
    }

    citrusleaf_object_free( &o_key );
    return rc;
}
// ======================================================================
/**
 *  LSET INSERT TEST
 *  For a single record, perform a series of SET insert.
 *  Create a new record, then repeatedly call lset insert.
 *  This should work for data that is a NUMBER, a STRING or a LIST.
 *  Parms:
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 */
int lset_insert_test(char * keystr, char * ldt_bin, int iterations, int seed,
        int data_format ) {
    static char * meth = "lset_insert_test()";
    int rc = CITRUSLEAF_OK;
    int i;
    as_val *valp;

    time_t cur_t;
    cur_t = time(NULL);

    //    INFO("[ENTER]:<%s:%s>: It(%d) Key(%s) LSOBin(%s) Seed(%d)",
    //            MOD, meth, iterations, keystr, ldt_bin, seed);

    // We have two choices:  We can create the LSO bin here, and then
    // do a bunch of inserts into it -- or we can just do the combined
    // "create_and_insert" insert, which upon reflection, is really the
    // most likely mode we'll be in. We'll choose the later.

    // Set up the Creation Spec parameter -- mostly setting the Package
    // (which is the name for a canned set of settings).
    char * create_package = "StandardList";
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec, (as_val *) as_string_new("Package", false),
    (as_val *) as_string_new( create_package, false));

    cl_cluster * c     = lset_g_config->asc;
    cl_object  o_key;
    char       * ns    = lset_g_config->ns;
    char       * set   = lset_g_config->set;
    char       * bname = ldt_bin;
    int          iseed;

    //INFO("[DEBUG]:<%s:%s>: Run insert() iterations(%d)", MOD, meth, iterations );
    citrusleaf_object_init_str( &o_key, keystr );
    for ( i = 0; i < iterations; i++ ) {
        iseed = i * 10;
        lset_generate_value( &valp, iseed, data_format );

        rc = aerospike_lset_create_and_insert(
                c, ns, set, &o_key, bname, valp, create_spec,
                lset_g_config->timeout_ms);

        if ( rc != CITRUSLEAF_OK ) {
        //INFO("[ERROR]:<%s:%s>:H Error: i(%d) rc(%d)", MOD, meth,i,rc );
            as_val_destroy ( valp );
            goto cleanup;
        } 
        // Count the write operation for stats gathering
        lset_g_config->write_ops_counter += 1;
        lset_g_config->write_vals_counter += 1;
        as_val_destroy( valp ); // must destroy every iteration.
        valp = NULL; // unnecessary insurance
    } // end for
cleanup:
    citrusleaf_object_free( &o_key );
    as_val_destroy( create_spec );
    return rc;
} // end lset_insert_test()

// ======================================================================
/**
 *  LSET READ TEST
 *  For a single record, perform a series of SET READS.
 *  Using the previously created record, repeatedly call set read with
 *  varying keys (value type is passed in via "format").
 *  NOTE: We must EXPLICITLY FREE the result, as it is a malloc'd
 *  object that is handed to us.
 *  + keystr: String Key to find the record
 *  + ldt_bin: Bin Name of the LDT
 *  + iterations: Number of iterations to run this test
 *  + seed:  Seed value for the random number pattern
 *  + data_format: Type of value (number, string, list)
 */
int lset_search_test(char * keystr, char * ldt_bin, int iterations,
        int seed, int data_format ) {
    static char * meth = "lset_search_test()";
    cl_rv rc = CITRUSLEAF_OK;
    as_result * resultp;

    char * valstr;

//    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s) Sd(%d) DF(%d)",
//            MOD, meth, iterations, keystr, ldt_bin, seed, data_format);

    cl_cluster * c     = lset_g_config->asc;
    cl_object  o_key;
    char       * ns    = lset_g_config->ns;
    char       * set   = lset_g_config->set;
    char       * bname = ldt_bin;
    int        vals_read = 0;
    int        misses = 0;
    int        errs = 0;

//    INFO("[DEBUG]:<%s:%s>: Run search() iterations(%d)", MOD, meth, iterations );

    as_val * valp ;
    srand( seed );
    // NOTE: Must FREE the result for EACH ITERATION.
    citrusleaf_object_init_str( &o_key, keystr );

    int          iseed;
    for ( int i = 0; i < iterations ; i ++ ){
        iseed = i * 10;
        lset_generate_value( &valp, iseed, data_format );

        //     INFO("[DEBUG]:<%s:%s>: Peek(%d)", MOD, meth, iterations );
        rc = aerospike_lset_search( &resultp,
                c, ns, set, &o_key, bname, valp, lset_g_config->timeout_ms);

        //        lset_process_read_results( meth, rc, resultp, i, &vals_read, &misses,
          //          &errs, valp);

        // Clean up -- release the result object
        if( resultp != NULL ) as_result_destroy( resultp );

        // Count up the reads (total)
         lset_g_config->read_ops_counter += 1;
         lset_g_config->read_vals_counter += 1;
    } // end for each search iteration
    citrusleaf_object_free( &o_key );

//    INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end lset_search_test()

// ======================================================================
/**
 *  LSET Insert WITH_TRANSFORM TEST
 *  For a single record, perform a series of SET Insert of BYTE-PACKED data.
 *  Create a new record, then repeatedly call stack insert.
 */
int lset_insert_with_transform_test(char * keystr, char * ldt_bin, int iterations) {
    static char * meth = "lset_insert_with_transform_test()";
    int rc = 0;
    int i;

    INFO("[ENTER]:<%s:%s>: It(%d) Key(%s) LSOBin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    // Abbreviate for simplicity.
    cl_cluster * c  = lset_g_config->asc;
    char       * ns = lset_g_config->ns;
    char       * set  = lset_g_config->set;
    char       * bname  = ldt_bin;
    cl_object o_key;

    // Set up the Creation Spec parameter -- mostly setting the Package
    // (which is the name for a canned set of settings).
    char * create_package = "ProdListValBinStore";
    as_map *create_spec = as_hashmap_new(2);
    as_map_set(create_spec,
            (as_val *) as_string_new("Package", false),
            (as_val *) as_string_new( create_package, false));

    INFO("[DEBUG]:<%s:%s>: Run insert_with_transform() iterations(%d)",
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

        rc = aerospike_lset_create_and_insert( c, ns, set, &o_key, bname,
                (as_val *)listp, create_spec, lset_g_config->timeout_ms);
        if ( rc != CITRUSLEAF_OK ) {
            INFO("[ERROR]:<%s:%s>:LSO PUSH WITH TRANSFROM Error: i(%d) rc(%d)",
                  MOD, meth, i, rc );
            as_val_destroy ( listp );
            goto cleanup;
        }
        // Count the write operation for stats gathering
        lset_g_config->write_ops_counter += 1;
        lset_g_config->write_vals_counter += 1;
        as_val_destroy( listp ); // must destroy every iteration.
        listp = NULL;
    } // end for

cleanup:
    citrusleaf_object_free( &o_key );
    as_val_destroy( create_spec );

    return rc;
} // end lset_insert_with_transform_test()

// ======================================================================
/** 
 *  LSET Search WITH TRANSFORM TEST
 *  For a single record, perform a series of SET Search.
 *  and do a server side transform of the byte-packed data
 *  Using the previously created record, repeatedly call stack search with
 *  varying numbers of search counts.
 */
int lset_search_with_transform_test(char * keystr, char * ldt_bin,
                                 char * filter_function,
                                 as_list * fargs,
                                 int iterations ) {
    static char * meth = "lset_search_with_transform_test()";
    cl_rv rc = 0;

    INFO("[ENTER]:<%s:%s>: Iterations(%d) Key(%s) LSOBin(%s)",
            MOD, meth, iterations, keystr, ldt_bin );

    cl_cluster * c     = lset_g_config->asc;
    cl_object  o_key;
    char       * ns    = lset_g_config->ns;
    char       * set   = lset_g_config->set;
    char       * bname = ldt_bin;
    int        vals_read;
    int        misses;
    int        errs;
    int        i;
    as_result * resultp;

    INFO("[DEBUG]:<%s:%s>: Run search() iterations(%d)", MOD, meth, iterations );

    // NOTE: Must FREE the result (resultp) for EACH ITERATION.
    int search_count = 2; // Soon -- set by Random Number
    citrusleaf_object_init_str( &o_key, keystr );
    for ( i = 0; i < iterations ; i ++ ){
        search_count++;
        rc = aerospike_lset_search_then_filter(
                &resultp, c, ns, set, &o_key, bname, search_count,
                filter_function, fargs, lset_g_config->timeout_ms);

        //lset_process_read_results( meth, rc, resultp, i, &vals_read, &misses,
          //      &errs, search_count );

        // Count up the reads (total)
        lset_g_config->read_vals_counter += search_count;
        lset_g_config->read_ops_counter += 1;
    } // end for each search iteration
    citrusleaf_object_free( &o_key );

    INFO("[EXIT]:<%s:%s>: RC(%d)", MOD, meth, rc );
    return rc;
} // end lset_search_with_transform_test()


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

// ++====================++
// || REGULAR OPERATIONS ||
// ++====================++

TEST( lset_operations_small_insert, "lset insert small" ) {
    static char * meth = "lset_operations_small_insert()";
    int rc = 0;

	char * user_key   = "User_111";
    char * ldt_bin_num    = "lset_num_s";
    char * ldt_bin_str    = "lset_str_s";
    char * ldt_bin_list   = "lset_list_s";

	int    iterations = 100 ;
	int    seed       = 111;
	int    format     = NUMBER_FORMAT;

    printf("\tTest(%s) called\n", meth );

    
    rc = lset_insert_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );

    lset_g_config->value_len = 10;
    rc = lset_insert_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );

//    rc = lset_insert_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
//    assert_int_eq( rc, 0 );
} // end lset_operations_small_insert()

TEST( lset_operations_medium_insert, "lset insert medium" ) {
    static char * meth = "lset_operations_medium_insert()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

	char * user_key   = "User_111";
    char * ldt_bin_num    = "lset_num_m";
    char * ldt_bin_str    = "lset_str_m";
    char * ldt_bin_list   = "lset_list_m";

	int    iterations = 1000 ;
	int    seed       = 111;
	int    format     = NUMBER_FORMAT;

    
    rc = lset_insert_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );

    lset_g_config->value_len = 100;
    rc = lset_insert_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );

//    rc = lset_insert_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
//    assert_int_eq( rc, 0 );
} // end   lset_operations_medium_insert()

TEST( lset_operations_large_insert, "lset insert large" ) {
    static char * meth = "lset_operations_large_insert()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

	char * user_key   = "User_111";
    char * ldt_bin_num    = "lset_num_l";
    char * ldt_bin_str    = "lset_str_l";
    char * ldt_bin_list   = "lset_list_l";
   	int    iterations = 10000 ;
	int    seed       = 111;
	int    format     = NUMBER_FORMAT;

    rc = lset_insert_test( user_key, ldt_bin_num, iterations, seed, format );    
    assert_int_eq( rc, 0 );

    lset_g_config->value_len = 1000;
    rc = lset_insert_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );

//    rc = lset_insert_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
//    assert_int_eq( rc, 0 );
} // end   lset_operations_large_insert()


TEST( lset_operations_small_search, "lset search small" ) {
    static char * meth = "lset_operations_small_search()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

	char * user_key   = "User_111";
    char * ldt_bin_num    = "lset_num_s";
    char * ldt_bin_str    = "lset_str_s";
    char * ldt_bin_list   = "lset_list_s";
	int    iterations = 10;
	int    seed       = 111;
	int    format     = NUMBER_FORMAT;
	lset_g_config->peek_max = 50;

    rc = lset_search_test( user_key, ldt_bin_num, iterations, seed, NUMBER_FORMAT );
    assert_int_eq( rc, 0 );

    rc = lset_search_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );

//    rc = lset_search_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
//    assert_int_eq( rc, 0 );
} // end   lset_operations_small_search()

TEST( lset_operations_medium_search, "lset search medium" ) {
    static char * meth = "lset_operations_medium_search()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

	char * user_key   = "User_111";
    char * ldt_bin_num    = "lset_num_m";
    char * ldt_bin_str    = "lset_str_m";
    char * ldt_bin_list   = "lset_list_m";

	int    iterations = 10;
	int    seed       = 111;
	int    format     = NUMBER_FORMAT;
	lset_g_config->peek_max = 500;
    
    rc = lset_search_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0 );

    rc = lset_search_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );

//    rc = lset_search_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
//    assert_int_eq( rc, 0 );
} // end   lset_operations_medium_search()

TEST( lset_operations_large_search, "lset search large" ) {
    static char * meth = "lset_operations_large_search()";
    int rc = 0;
    printf("\tTest(%s) called\n", meth );

	char * user_key   = "User_111";
	char * ldt_bin_num    = "lset_num_l";
    char * ldt_bin_str    = "lset_str_l";
    char * ldt_bin_list   = "lset_list_l";

	int    iterations = 10;
	int    seed       = 111;
	int    format     = NUMBER_FORMAT;
	lset_g_config->peek_max = 5000;

    rc = lset_search_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0 );

    rc = lset_search_test( user_key, ldt_bin_str, iterations, seed, STRING_FORMAT );
    assert_int_eq( rc, 0 );
        
//    rc = lset_search_test( user_key, ldt_bin_list, iterations, seed, LIST_FORMAT );
//    assert_int_eq( rc, 0 );
 } // end   lset_operations_large_search()
