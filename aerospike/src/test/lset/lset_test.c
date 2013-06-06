// #include "../test.h"
// #include "../util/udf.h"
// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/as_types.h>
// #include <citrusleaf/aerospike_lset.h>

#include "lset_test.h"  // Everything a growing test needs

// LSET TESTS: ATF Overview
// In the global Aerospike Test Framework (ATF), there is a mechanism to
// automate tests for each Aerospike feature.  At the high level, there is
// one (or potentially multiple) Test Plans.
// A Test Plan comprises multiple Test Suite.  A Suite will usually correspond
// to a feature (like LSET).  In a Test Suite will be multiple TESTS.
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

static char * MOD = "lset_test.c::13_04_26";

// delare our config structure for all test files.
test_config   lset_global_config;
test_config * lset_g_config = &lset_global_config;

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool lset_before(atf_suite * suite) {
    extern int lset_setup_test();
    lset_setup_test();
    return true;
}

static bool lset_after(atf_suite * suite) {
    extern int lset_shutdown_test();
    lset_shutdown_test();
    return true;
}

SUITE( lset_basics, "LSET.lua" ) {
    suite_before( lset_before );
    suite_after( lset_after );

    suite_add( lset_basics_create );
    suite_add( lset_basics_insert );
    suite_add( lset_basics_search);
    suite_add( lset_all_search);
    suite_add( lset_basics_size );
    suite_add( lset_basics_config );
    suite_add( lset_basics_delete);

    suite_add( lset_operations_small_insert );
    suite_add( lset_operations_small_search );
    suite_add( lset_operations_medium_insert );
    suite_add( lset_operations_medium_search );
//    suite_add( lset_operations_large_insert );
//    suite_add( lset_operations_large_search );

//    suite_add( lset_advanced_push_compact );
//    suite_add( lset_advanced_peek_compact );
//    suite_add( lset_advanced_push_objects );
//    suite_add( lset_advanced_peek_objects );
//    suite_add( lset_advanced_peek_objects_and_filter );
//    suite_add( lset_small_test);
}

