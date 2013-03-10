
#include "../test.h"
#include "../util/udf.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/as_types.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/client_record_basics.lua"
#define UDF_FILE "client_record_basics"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( lstack_basics_1, "stub" ) {
    assert( true );
    assert_not_null( "abc" );
    assert_int_eq( rc, 0 );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    int rc = 0;

    rc = udf_put(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while uploading: %s (%d)", LUA_FILE, rc);
        return false;
    }

    rc = udf_exists(LUA_FILE);
    if ( rc != 0 ) {
        error("lua file does not exist: %s (%d)", LUA_FILE, rc);
        return false;
    }

    return true;
}

static bool after(atf_suite * suite) {
    int rc = udf_remove(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while removing: %s (%d)", LUA_FILE, rc);
        return false;
    }

    return true;
}

SUITE( lstack_basics, "test basics.lua" ) {

    suite_before( before );
    suite_after( after );

    suite_add( lstack_basics_1 );
}