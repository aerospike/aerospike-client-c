
#include "../test.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_arglist.h>
#include <citrusleaf/as_types.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/basics.lua"

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

TEST( udf_basics_exists, "basics.lua exists" ) {
    int rc = udf_exists(LUA_FILE);
    assert_int_eq( rc, 0 );
}

/**
 *
 * @TODO FIX Server and/or Client handling of AS_BOOLEAN.
 *
 * This test fails because the function returns a true (Boolean),
 * yet, the server and/or client says it is a string.
 *
 * The database does not actually support Boolean type, so the call should
 * have resulted in a failire (!r.is_success), with a type error.
 *
 */
TEST( udf_basics_getboolean, "getboolean() - get a boolean" ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "getboolean", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_BOOLEAN );
    assert_true( as_boolean_tobool((as_boolean *) r.value) );
}

TEST( udf_basics_getfloat, "getfloat() - get a float. Will be converted to an integer." ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "getfloat", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 123 );
}

TEST( udf_basics_getinteger, "getinteger() - get an integer" ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "getinteger", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 123 );
}

TEST( udf_basics_getstring, "getstring() - get a string" ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "getstring", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "abc" );
}

TEST( udf_basics_gettable, "gettable() - get a table" ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "gettable", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "Invalid response. (2)" );
}

TEST( udf_basics_getlist, "getlist() - get a list" ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "getlist", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_LIST );
    assert_int_eq( as_list_size((as_list *) r.value), 3 );
}

TEST( udf_basics_getmap, "getmap() - get a map" ) {

    int rc = 0;

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "getmap", NULL, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_MAP );
    assert_int_eq( as_map_size((as_map *) r.value), 3 );
}



TEST( udf_basics_concat, "concat() - concatenate two strings, return the result" ) {

    int rc = 0;

    as_list * arglist = as_arglist_new(2);
    as_list_add_string(arglist, "abc");
    as_list_add_string(arglist, "def");

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "concat", arglist, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "abcdef" );
}

TEST( udf_basics_add, "add() - add two integer, return the result" ) {

    int rc = 0;

    as_list * arglist = as_arglist_new(2);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "add", arglist, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 1001 );
}

TEST( udf_basics_sum, "sum() - UDF calling another UDF should fail" ) {

    int rc = 0;

    as_list * arglist = as_arglist_new(2);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "sum", arglist, &r);
    assert_int_eq( rc, 100 );
}

TEST( udf_basics_diff, "diff() - UDF calling a local function" ) {

    int rc = 0;

    as_list * arglist = as_arglist_new(2);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "diff", arglist, &r);
    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 999 );
}

TEST( udf_basics_difference, "difference() - calling a local function should fail" ) {

    int rc = 0;

    as_list * arglist = as_arglist_new(2);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "difference", arglist, &r);
    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );
}

TEST( udf_basics_func_does_not_exist, "calling a non-existent function should fail" ) {

    int rc = 0;

    as_list * arglist = as_arglist_new(2);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;

    rc = udf_call("test", "test", "test", "basics", "does_not_exist", arglist, &r);
    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );
}


TEST( udf_basics_file_does_not_exist, "non-existent UDF file should fail" ) {

    int rc = 0;

    as_list * arglist = as_arglist_new(2);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;

    rc = udf_call("test", "test", "test", "does_not_exist", "does_not_exist", arglist, &r);
    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );
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

SUITE( udf_basics, "test basics.lua" ) {

    suite_before( before );
    suite_after( after );

    suite_add( udf_basics_exists );

    suite_add( udf_basics_getboolean );
    suite_add( udf_basics_getfloat );
    suite_add( udf_basics_getinteger );
    suite_add( udf_basics_getstring );
    suite_add( udf_basics_gettable );
    suite_add( udf_basics_getlist );
    suite_add( udf_basics_getmap );
    

    suite_add( udf_basics_concat );
    suite_add( udf_basics_add );
    suite_add( udf_basics_sum );
    suite_add( udf_basics_diff );
    suite_add( udf_basics_difference );

    suite_add( udf_basics_func_does_not_exist );
    suite_add( udf_basics_file_does_not_exist );
}