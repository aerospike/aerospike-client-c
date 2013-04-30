// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lstack.h>

#include "lstack_test.h"  // Everything a growing test needs

// LSTACK TESTS: ATF Overview
// In the global Aerospike Test Framework (ATF), there is a mechanism to
// automate tests for each Aerospike feature.  At the high level, there is
// one (or potentially multiple) Test Plans.
// A Test Plan comprises multiple Test Suite.  A Suite will usually correspond
// to a feature (like LSTACK).  In a Test Suite will be multiple TESTS.
//
// The General structure is this:
// A Suite defines the "before test" function, the individual test functions
// and the "after test" function.
// Each Test is expected to exercise some specific function of the feature
// (i.e. the Suite).  It will end with an assertion that the output of the
// system matches the expected output.
// Version 1: We down our own tests on output (awkward and ugly).
// Version 2: We capture the output into a file -- and then invoke the output
// compare function to tell us if the files match (easier).

static char * MOD = "lstack_test.c::13_04_26";

/******************************************************************************
 * TESTS
 *****************************************************************************/

// ++====================++
// || BASIC   OPERATIONS ||
// ++====================++

TEST( lstack_basics_push, "Push N items on the stack" ) {
    char * user_key   = "User_111";
    char * ldt_bin    = "number_stack";
    int    iterations = 10;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;

    int rc = lstack_push_test( user_key, ldt_bin, iterations, seed, format );
    assert_int_eq( rc, 0 );
} // end TEST

TEST( lstack_basics_peek, "Read N elements from the stack" ) {
    char * user_key   = "User_111";
    char * ldt_bin    = "number_stack";
    int    iterations = 10;
    int    seed       = 111;
    int    format     = NUMBER_FORMAT;

    int rc = lstack_peek_test( user_key, ldt_bin, iterations, seed, format );
    assert_int_eq( rc, 1 );
}

TEST( lstack_basics_size, "validate the stack size" ) {
    char * user_key     = "User_111";
    char * ldt_bin_name = "number_stack";
    int rc = 1;

    assert_int_eq( rc, 1 );
}

TEST( lstack_basics_config, "Validate Config Settings" ) {
    char * user_key     = "User_111";
    char * ldt_bin_name = "number_stack";
    int rc = 1;

    assert_int_eq( rc, 1 );
}


// ++====================++
// || REGULAR OPERATIONS ||
// ++====================++

TEST( lstack_operations_small_push, "lstack push small" ) {
    static char * meth = "lstack_operations_small_push()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end lstack_operations_small_push()

TEST( lstack_operations_medium_push, "lstack push medium" ) {
    static char * meth = "lstack_operations_medium_push()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_operations_medium_push()

TEST( lstack_operations_large_push, "lstack push large" ) {
    static char * meth = "lstack_operations_large_push()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_operations_large_push()


TEST( lstack_operations_small_peek, "lstack peek small" ) {
    static char * meth = "lstack_operations_small_peek()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_operations_small_peek()

TEST( lstack_operations_medium_peek, "lstack peek medium" ) {
    static char * meth = "lstack_operations_medium_peek()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_operations_medium_peek()

TEST( lstack_operations_large_peek, "lstack peek large" ) {
    static char * meth = "lstack_operations_large_peek()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_operations_large_peek()

// ++=====================++
// || ADVANCED OPERATIONS ||
// ++=====================++

TEST( lstack_advanced_push_compact, "lstack Adv push compact" ) {
    static char * meth = "lstack_advanced_push_compact";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end lstack_advanced_push_compact

TEST( lstack_advanced_push_objects, "lstack Adv push objects" ) {
    static char * meth = "lstack_advanced_push_objects()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_advanced_push_objects()

TEST( lstack_advanced_push_objects_compact, "lstack adv push Obj Compact" ) {
    static char * meth = "lstack_advanced_push_objects_compact()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_advanced_push_objects_compact()


TEST( lstack_advanced_peek_compact, "lstack Adv peek compact" ) {
    static char * meth = "lstack_advanced_peek_compact()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_advanced_peek_compact()

TEST( lstack_advanced_peek_objects, "lstack Adv peek obj" ) {
    static char * meth = "lstack_advanced_peek_objects()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_advanced_peek_objects()

TEST( lstack_advanced_peek_objects_and_filter, "lstack Adv peek Obj & Filter" ) {
    static char * meth = "lstack_advanced_peek_objects_and_filter()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_advanced_peek_objects_and_filter()

TEST( lstack_advanced_peek_objects_compact_and_filter,
        "lstack Adv peek Obj Compact & Filter" )
{
    static char * meth = "lstack_advanced_peek_objects_compact_and_filter()";
    int rc = 0;
    printf("Test(%s) called\n", meth );

    // TODO: Fill in Function
    
    assert_int_eq( rc, 0 );
} // end   lstack_advanced_peek_objects_compact_and_filter()


/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool lstack_before(atf_suite * suite) {
    extern int setup_test();
    lstack_setup();
    return true;
}

static bool lstack_after(atf_suite * suite) {
    extern int shutdown_test();
    shutdown_test();
    return true;
}

SUITE( lstack_basics, "LSTACK.lua" ) {
    suite_before( lstack_before );
    suite_after( lstack_after );

    suite_add( lstack_basics_push );
    suite_add( lstack_basics_peek );
    suite_add( lstack_basics_size );
    suite_add( lstack_basics_config );

    suite_add( lstack_operations_small_push );
    suite_add( lstack_operations_small_peek );
    suite_add( lstack_operations_medium_push );
    suite_add( lstack_operations_medium_peek );
    suite_add( lstack_operations_large_peek );
    suite_add( lstack_operations_large_push );

    suite_add( lstack_advanced_push_compact );
    suite_add( lstack_advanced_peek_compact );
    suite_add( lstack_advanced_push_objects );
    suite_add( lstack_advanced_peek_objects );
    suite_add( lstack_advanced_peek_objects_and_filter );
}
