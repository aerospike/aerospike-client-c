
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
 * STATIC FUNCTIONS
 *****************************************************************************/

static void print_result(uint32_t rc, as_result * r) {
    if ( !r->is_success ) {
        char * s = as_val_tostring(r->value);
        info("failure: %s (%d)", s, rc);
        free(s);
    }
    else {
        char * s = as_val_tostring(r->value);
        info("success: %s", s);
        free(s);
    }
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( record_basics_exists, LUA_FILE" exists" ) {
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
TEST( record_basics_getboolean, "getboolean() - get a boolean" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getboolean", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_BOOLEAN );
    assert_true( as_boolean_tobool((as_boolean *) r.value) );

    as_result_destroy(&r);
}

TEST( record_basics_getfloat, "getfloat() - get a float. Will be converted to an integer." ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getfloat", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 123 );

    as_result_destroy(&r);
}

TEST( record_basics_getinteger, "getinteger() - get an integer" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getinteger", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 123 );

    as_result_destroy(&r);
}

TEST( record_basics_getstring, "getstring() - get a string" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getstring", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "abc" );

    as_result_destroy(&r);
}

TEST( record_basics_gettable, "gettable() - get a table" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "gettable", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "Invalid response. (2)" );

    as_result_destroy(&r);
}

TEST( record_basics_getlist, "getlist() - get a list" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getlist", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_LIST );
    assert_int_eq( as_list_size((as_list *) r.value), 3 );

    as_result_destroy(&r);
}

TEST( record_basics_getmap, "getmap() - get a map" ) {

    int rc = 0;

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "getmap", NULL, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_MAP );
    assert_int_eq( as_map_size((as_map *) r.value), 3 );

    as_result_destroy(&r);
}



TEST( record_basics_concat, "concat() - concatenate two strings, return the result" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_string(arglist, "abc");
    as_list_add_string(arglist, "def");

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "concat", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "abcdef" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_add, "add() - add two integer, return the result" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "add", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 1001 );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_sum, "sum() - UDF calling another UDF should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "sum", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_diff, "diff() - UDF calling a local function" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "diff", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 0 );
    assert_true( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_INTEGER );
    assert_int_eq( as_integer_toint((as_integer *) r.value), 999 );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_difference, "difference() - calling a local function should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "difference", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}

TEST( record_basics_func_does_not_exist, "calling a non-existent function should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", UDF_FILE, "does_not_exist", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
}


TEST( record_basics_file_does_not_exist, "non-existent UDF file should fail" ) {

    int rc = 0;

    as_list * arglist = as_arraylist_new(2, 8);
    as_list_add_integer(arglist, 1000);
    as_list_add_integer(arglist, 1);

    as_result r;
    as_success_init(&r,NULL);

    rc = udf_apply_record("test", "test", "test", "does_not_exist", "does_not_exist", arglist, &r);

    print_result(rc, &r);

    assert_int_eq( rc, 100 );
    assert_false( r.is_success );
    assert_not_null( r.value );
    assert( as_val_type(r.value) == AS_STRING );
    assert_string_eq( as_string_tostring((as_string *) r.value), "function not found" );

    as_list_destroy(arglist);
    as_result_destroy(&r);
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

SUITE( record_basics, "test basics.lua" ) {

    suite_before( before );
    suite_after( after );

    suite_add( record_basics_exists );

    suite_add( record_basics_getboolean );
    suite_add( record_basics_getfloat );
    suite_add( record_basics_getinteger );
    suite_add( record_basics_getstring );
    suite_add( record_basics_gettable );
    suite_add( record_basics_getlist );
    suite_add( record_basics_getmap );
    

    suite_add( record_basics_concat );
    suite_add( record_basics_add );
    suite_add( record_basics_sum );
    suite_add( record_basics_diff );
    suite_add( record_basics_difference );

    suite_add( record_basics_func_does_not_exist );
    suite_add( record_basics_file_does_not_exist );
}