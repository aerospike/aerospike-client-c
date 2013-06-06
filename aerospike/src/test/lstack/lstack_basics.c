
// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lstack.h>

#include "lstack_test.h"

// LSTACK BASICS: 
// In the LSTACK ATF, define the tests for the very basic operations:
// (*) basic push()
// (*) basic peek()
// (*) basic size()
// (*) basic config()

static char * MOD = "lstack_basics.c::13_04_26";
/******************************************************************************
 * TEST CASES
 *****************************************************************************/
// int lstack_push_test(char * keystr, char * ldt_bin, int iterations,
//                       int seed, int data_format )
// int lstack_peek_test(char * keystr, char * ldt_bin, int iterations,
//                       int seed, int data_format )
// ==================================================================
TEST( lstack_basics_push, "Push N items on the stack" ) {
    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_basics";
    int    iterations = 100;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    g_config->value_len = 10;
    printf("\tlstack_basics_push:: Calling push test\n");

    int rc = lstack_push_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0 );
    printf("\tTest(lstack_basics_push) Passed.\n");
} // end TEST

// ==================================================================
TEST( lstack_basics_peek, "Read N elements from the stack" ) {
    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_basics";
    int    iterations = 100;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    g_config->peek_max = 5;
    printf("\tlstack_basics_peek:: Calling peek test\n");

    int rc = lstack_peek_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0);
    printf("\tTest(lstack_basics_peek) Passed.\n");
}

// ==================================================================
TEST( lstack_basics_size, "Validate the stack size" ) {
    char * user_key     = "User_111";
    char * ldt_bin = "num_basics";
    int    iterations = 100;
    int rc = 1;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    uint32_t   size_before, size_after;

    g_config->value_len = 5;

    rc = lstack_size_test( user_key, ldt_bin, &size_before);
    assert_int_eq( rc, 0 );
    rc = lstack_push_test( user_key, ldt_bin, iterations, seed, format );
    assert_int_eq( rc, 0 );
    rc = lstack_size_test( user_key, ldt_bin, &size_after);
    assert_int_eq( rc, 0 );

    printf("\tlstack_basics_size:: Size of Lstack before push is %d, after %d push is %d\n", size_before, iterations, size_after);
}

// ==================================================================
TEST( lstack_basics_config, "Validate Config Settings" ) {
    char * user_key     = "User_111";
    char * ldt_bin = "num_basics";
    int rc = 1;

    rc = lstack_config_test( user_key, ldt_bin);
    assert_int_eq( rc, 0 );
}

TEST( lstack_basics_validation, "Data Validation" ) {
    char * user_key     = "User_111";
    char * ldt_bin = "num_basics";
    int    iterations = 50;

    int    seed       = 111;
    int    format     = NUMBER_FORMAT;

    printf("\tlstack_data_validation: Calling Validation test\n");

    int rc = lstack_data_validation( user_key, ldt_bin, iterations, seed, format );
    assert_int_eq( rc, 0);
    printf("\tTest(lstack_basics_validation) Passed.\n");
}

TEST( lstack_all_validation, "Random Data Validation" ) {
    char * user_key     = "User_111";
    char * ldt_bin = "random";
    int    iterations = 1000;
    int    peek_count = 1000;

    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    printf("\tlstack_alldata_validation: Calling Validation test\n");

    int rc = lstack_alldata_validation(user_key, ldt_bin, iterations, seed, format, peek_count);
    assert_int_eq( rc, 0);
    printf("\tTest(lstack_alldata_validation) Passed for NUMBER\n");

    rc = lstack_alldata_validation(user_key, ldt_bin, iterations, seed, STRING_FORMAT, peek_count);
    assert_int_eq( rc, 0);
    printf("\tTest(lstack_alldata_validation) Passed for STRING\n");

    rc = lstack_alldata_validation(user_key, ldt_bin, iterations, seed, LIST_FORMAT, peek_count);
    assert_int_eq( rc, 0);
    printf("\tTest(lstack_alldata_validation) Passed for LIST\n");


}
// ==================================================================
