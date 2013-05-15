
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
    int    iterations = 10;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    g_config->value_len = 10;
    printf("\tlstack_basics_push:: Calling push test\n");

    int rc = lstack_push_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0 );
} // end TEST

// ==================================================================
TEST( lstack_basics_peek, "Read N elements from the stack" ) {
    char * user_key   = "User_111";
    char * ldt_bin_num    = "num_basics";
    int    iterations = 10;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;
    g_config->peek_max = 5;
    printf("\tlstack_basics_peek:: Calling peek test\n");

    int rc = lstack_peek_test( user_key, ldt_bin_num, iterations, seed, format );
    assert_int_eq( rc, 0);
}

// ==================================================================
TEST( lstack_basics_size, "validate the stack size" ) {
    char * user_key     = "User_111";
    char * ldt_bin_name = "number_stack";
    int rc = 1;

    assert_int_eq( rc, 1 );
}

// ==================================================================
TEST( lstack_basics_config, "Validate Config Settings" ) {
    char * user_key     = "User_111";
    char * ldt_bin_name = "number_stack";
    int rc = 1;

    assert_int_eq( rc, 1 );
}

// ==================================================================
