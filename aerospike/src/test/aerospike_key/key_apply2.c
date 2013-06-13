
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/udf.h"
#include "../util/info_util.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/key_apply2.lua"
#define UDF_FILE "key_apply2"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool before(atf_suite * suite) {

    if ( ! udf_put(LUA_FILE) ) {
        error("failure while uploading: %s", LUA_FILE);
        return false;
    }

    if ( ! udf_exists(LUA_FILE) ) {
        error("lua file does not exist: %s", LUA_FILE);
        return false;
    }

    return true;
}

static bool after(atf_suite * suite) {

    if ( ! udf_remove(LUA_FILE) ) {
        error("failure while removing: %s", LUA_FILE);
        return false;
    }

    return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/
TEST( key_apply2_file_exists , "apply2: (test,test,foo) <!> key_apply2.record_exists() => 1" ) {

	as_error err;
	as_error_reset(&err);

	const char *filename = LUA_FILE;

    as_udf_file file;
    as_udf_file_init(&file);

    char * base = basename(filename);

    if ( aerospike_udf_get(as, &err, NULL, base, AS_UDF_TYPE_LUA, &file) != AEROSPIKE_OK ) {
        error("error caused by aerospike_udf_get(%s): (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

    as_udf_file_destroy(&file);

    return err.code == AEROSPIKE_OK;

#if 0
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;
	as_list arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_list_append_str(&arglist, "src/test/lua/client_record_basics.lua");

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "file_exists", &arglist, &res);

    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq(  as_integer_toint(i), 1 );
#endif
}

TEST( key_apply2_getboolean, "getboolean() - get a boolean" ) {

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

/* TODO : This must be preceded by a put_boolean of a, un-supported API as of now
 * along with float api's.
 */
TEST( key_apply2_getboolean , "apply2: (test,test,foo) <!> key_apply2.getboolean() => 1" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_list arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_list_append_str(&arglist, "a");

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "getboolean", &arglist, &res);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_BOOLEAN );

}

TEST( key_apply2_getinteger , "apply2: (test,test,foo) <!> key_apply2.getinteger() => 123" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_list arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_list_append_str(&arglist, "a");

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "getinteger", &arglist, &res);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_INTEGER );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( key_apply2, "aerospike_key_apply2 tests" ) {

    suite_before( before );
    suite_after( after );
    suite_add( key_apply2_file_exists );
    suite_add ( key_apply2_getinteger );

}
