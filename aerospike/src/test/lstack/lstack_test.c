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

// delare our config structure for all test files.
test_config * g_config;

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool lstack_before(atf_suite * suite) {
    extern int setup_test();
    setup_test();
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

