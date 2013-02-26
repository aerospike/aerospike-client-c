
#include "../test.h"
#include "../util/udf.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/as_types.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/client_record_lists.lua"
#define UDF_FILE "client_record_lists"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( record_lists_exists, UDF_FILE" exists" ) {
    int rc = udf_exists(LUA_FILE);
    assert_int_eq( rc, 0 );
}

TEST( record_lists_getlist, "getlist() - get a list" ) {

    int rc = 0;

    as_list * arglist = NULL;

    as_result r;
    as_success_init(&r,NULL);

    // rc = udf_apply_record("test", "test", "test", UDF_FILE, "getlist", arglist, &r);

    // assert_int_eq( rc, 0 );
    // assert_true( r.is_success );
    // assert_not_null( r.value );
    // assert( as_val_type(r.value) == AS_LIST );
    // assert_int_eq( as_list_size((as_list *) r.value), 3);

    // as_result_destroy(&r);
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

SUITE( record_lists, "test lists.lua" ) {

    suite_before( before );
    suite_after( after );

    suite_add( record_lists_exists );
    suite_add( record_lists_getlist );
}