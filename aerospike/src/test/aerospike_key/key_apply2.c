
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>

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
#include <aerospike/as_udf.h>

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

	WAIT_MS(100);

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

TEST( key_apply2_file_exists , "apply2: key_apply2 exists" ) {

	as_error err;
	as_error_reset(&err);

	const char * filename = LUA_FILE;

    as_udf_file file;
    as_udf_file_init(&file);

    char * base = basename(filename);

    if ( aerospike_udf_get(as, &err, NULL, base, AS_UDF_TYPE_LUA, &file) != AEROSPIKE_OK ) {
        error("error caused by aerospike_udf_get(%s): (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

    as_udf_file_destroy(&file);

}

// TEST( key_apply2_getboolean , "apply2: (test,test,foo) <!> key_apply2.getboolean() => 1" ) {

// 	as_error err;
// 	as_error_reset(&err);

// 	as_val * res = NULL;

// 	as_list arglist;
// 	as_arraylist_init(&arglist, 1, 0);
// 	as_list_append_str(&arglist, "a");

// 	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "getboolean", &arglist, &res);

// 	assert_int_eq( rc, AEROSPIKE_OK );
// 	assert_not_null( res );
// 	assert( res->type == AS_BOOLEAN );

// }

TEST( key_apply2_getinteger , "apply2: (test,test,foo) <!> key_apply2.getinteger() => 123" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_list arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_list_append_str(&arglist, "a");

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "getinteger", &arglist, &res);

	if ( rc != AEROSPIKE_OK ) {
		error("[%s:%d][%s][%d] %s", err.file, err.line, err.func, err.code, err.message);
	}

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_INTEGER );
    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq(  as_integer_toint(i), 123 );

}

TEST( key_apply2_getstring , "apply2: (test,test,foo) <!> key_apply2.getstring() => abc" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_list arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_list_append_str(&arglist, "b");

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "getstring", &arglist, &res);

	if ( rc != AEROSPIKE_OK ) {
		error("[%s:%d][%s][%d] %s", err.file, err.line, err.func, err.code, err.message);
	}

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_STRING );
	as_string *str = as_string_fromval(res);
	assert_not_null( str );
	assert_int_eq( as_string_tostring(str), "abc" );
}

// Table is the same as list, so no test for gettable()

TEST( key_apply2_getlist , "apply2: (test,test,foo) <!> key_apply2.getlist() => [1,2,3]" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_list arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_list_append_str(&arglist, "e");

	as_list compare_list;
	as_arraylist_init(&compare_list, 3, 0);
	as_list_append_int64(&compare_list, 1);
	as_list_append_int64(&compare_list, 2);
	as_list_append_int64(&compare_list, 3);

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "getlist", &arglist, &res);

	if ( rc != AEROSPIKE_OK ) {
		error("[%s:%d][%s][%d] %s", err.file, err.line, err.func, err.code, err.message);
	}

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_LIST );
	as_list *list =  as_list_fromval(res);
	assert_not_null( list );
	// Not sure if this comparison is valid : needs testing
	// assert_int_eq( list,'[1,2,3]' );
	assert_int_eq( list, compare_list )
}

TEST( key_apply2_getmap , "apply2: (test,test,foo) <!> key_apply2.getmap() => {x: 7, y: 8, z: 9}" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_list arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_list_append_str(&arglist, "f");

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "getmap", &arglist, &res);

	if ( rc != AEROSPIKE_OK ) {
		error("[%s:%d][%s][%d] %s", err.file, err.line, err.func, err.code, err.message);
	}
	as_map map;
	as_hashmap_init(&map, 32);
	as_stringmap_set_int64(&map, "x", 7);
	as_stringmap_set_int64(&map, "y", 8);
	as_stringmap_set_int64(&map, "z", 9);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_MAP );
	as_map *res_map =  as_map_fromval(res);
	assert_not_null( res_map );
	//assert_int_eq( map, '{x: 7, y: 8, z: 9}' );
	assert_int_eq( res_map, map );
}

TEST( key_apply2_add_strings , "apply: (test,test,foo) <!> key_apply.add_strings('abc','def') => 'abcdef'" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_list arglist;
	as_arraylist_init(&arglist, 2, 0);
	as_list_append_str(&arglist, "abc");
	as_list_append_str(&arglist, "def");

	as_status rc = aerospike_key_apply(as, &err, NULL, "test", "test", "foo", UDF_FILE, "add_strings", &arglist, &res);

    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	as_string *str = as_string_fromval(res);
	assert_not_null( str );
	assert_int_eq( as_string_tostring(str), "abc" );
}

// skipping record_basics_add, already present in key_apply.c

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( key_apply2, "aerospike_key_apply2 tests" ) {

    suite_before( before );
    suite_after( after );
    suite_add( key_apply2_file_exists );
    suite_add( key_apply2_getinteger );
    suite_add( key_apply2_getstring );
    suite_add( key_apply2_getlist );
    suite_add( key_apply2_getmap );
    suite_add( key_apply2_add_strings );

}
