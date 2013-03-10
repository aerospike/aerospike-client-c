
#include "../test.h"
#include "../util/udf.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/as_types.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( lstack_basics_1, "stub" ) {
    char * user_key     = "User_111";
    char * lso_bin_name = "urlid_stack";

    assert( true );
    assert_not_null( "abc" );
    assert_int_eq( rc, 0 );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    return true;
}

static bool after(atf_suite * suite) {
    return true;
}

SUITE( lstack_basics, "test basics.lua" ) {
    suite_before( before );
    suite_after( after );

    suite_add( lstack_basics_1 );
}
