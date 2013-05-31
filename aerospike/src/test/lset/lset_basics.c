
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lset.h>

#include "lset_test.h"

// LSET BASICS: 
// In the LSET ATF, define the tests for the very basic operations:
// (*) basic insert()
// (*) basic search()
// (*) basic size()
// (*) basic config()

static char * MOD = "lset_basics.c::13_04_26";
/******************************************************************************
 * TEST CASES
 *****************************************************************************/
// int lset_insert_test(char * keystr, char * ldt_bin, int iterations,
//                       int seed, int data_format )
// int lset_search_test(char * keystr, char * ldt_bin, int iterations,
//                       int seed, int data_format )

// ==================================================================
TEST( lset_basics_insert, "Insert N items on the set" ) {
    char * user_key   = "User_111";
    char * ldt_bin_num    = "lset_basics";
    int    iterations = 10;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    printf("\tlset_basics_insert:: Calling insert test\n");

    int rc = lset_insert_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0 );
} // end TEST

// ==================================================================
TEST( lset_basics_search, "Search N items on the set" ) {
    char * user_key   = "User_111";
    char * ldt_bin_num    = "lset_basics";
    int    iterations = 5;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    printf("\tlset_basics_search:: Calling search test\n");

    int rc = lset_search_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0 );
} // end TEST

// ==================================================================
TEST( lset_basics_create, "Validate creating new bin" ) {
    char * user_key     = "User_1111";
    char * ldt_bin_num = "lset_new";
    int rc = 1;

    rc = lset_create_test( user_key, ldt_bin_num);
    assert_int_eq( rc, 0 );
}

// ==================================================================
TEST( lset_basics_size, "Validate the set size" ) {
    char * user_key     = "User_111";
    char * ldt_bin_num = "lset_basics";
    int    iterations = 10;
    int rc = 1;
    int    seed       = 111;
    int    format     = STRING_FORMAT;
    uint32_t   size_before,size_after;
    lset_g_config->value_len = 20;
    rc = lset_size_test( user_key, ldt_bin_num, &size_before);
    rc = lset_insert_test( user_key, ldt_bin_num, iterations, seed, format );
    rc = lset_size_test( user_key, ldt_bin_num, &size_after);

    assert_int_eq( rc, 0 );

    printf("\tlset_basics_size:: Size of Lset before is %d, after %d insert is %d\n", size_before, iterations, size_after);
}

// ==================================================================
TEST( lset_basics_config, "Validate Config Settings" ) {
    char * user_key     = "User_111";
    char * ldt_bin_num = "lset_basics";
    int rc = 1;

    rc = lset_config_test( user_key, ldt_bin_num);
    assert_int_eq( rc, 0 );
}
