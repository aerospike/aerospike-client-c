
#include "../test.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/as_types.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/lists.lua"

/******************************************************************************
 * EXTERNS
 *****************************************************************************/

extern int udf_put(const char * filename);
extern int udf_remove(const char * filename);
extern int udf_exists(const char * filename);
extern int udf_call(const char * ns, const char * set, const char * key, const char * file, const char * func, as_list * args, as_result * result);

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( udf_lists_exists, "lists.lua exists" ) {
    int rc = udf_exists(LUA_FILE);
    assert_int_eq( rc, 0 );
}

TEST( udf_lists_getlist, "getlist() - get a list" ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "lists", "getlist", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_LIST );
    assert_int_eq( as_list_size((as_list *) r.value), 3);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {
    int rc = 0;

    rc = udf_put(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while uploading %s: %d", LUA_FILE, rc);
        return false;
    }

    rc = udf_exists(LUA_FILE);
    if ( rc != 0 ) {
        error("basics.lua does not exist.");
        return false;
    }

    return true;
}

static bool after(atf_suite * suite) {
    int rc = udf_remove(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while removing %s: %d", LUA_FILE, rc);
        return false;
    }

    return true;
}

SUITE( udf_lists, "test lists.lua" ) {

    suite_before( before );
    suite_after( after );

    suite_add( udf_lists_exists );
    suite_add( udf_lists_getlist );
}