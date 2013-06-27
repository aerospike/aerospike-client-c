
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_digest.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_stringmap.h>
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

#define LUA_FILE "src/test/lua/key_apply.lua"
#define UDF_FILE "key_apply"

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

TEST( digest_apply_put , "put: (test,test,foo) = {a: 123, b: 'abc', c: 456, d: 'def', e: [1,2,3], f: {x: 7, y: 8, z: 9}}" ) {

	as_error err;
	as_error_reset(&err);
	
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	
	as_hashmap map;
	as_hashmap_init(&map, 32);
	as_stringmap_set_int64(&map, "x", 7);
	as_stringmap_set_int64(&map, "y", 8);
	as_stringmap_set_int64(&map, "z", 9);

	as_record r;
	as_record_init(&r, 10);
	as_record_set_int64(&r, "a", 123);
	as_record_set_str(&r, "b", "abc");
	as_record_set_integer(&r, "c", as_integer_new(456));
	as_record_set_string(&r, "d", as_string_new("def",true));
	as_record_set_list(&r, "e", &list);
	as_record_set_map(&r, "f", &map);

	as_digest digest;
	as_digest_init(&digest, "test", (as_key *) as_string_new("foo",false));

	as_status rc = aerospike_digest_put(as, &err, NULL, "test", &digest, &r);

	assert_int_eq( rc, AEROSPIKE_OK );
}

TEST( digest_apply_one , "apply: (test,test,foo) <!> digest_apply.one() => 1" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", (as_key *) as_string_new("foo",false));

	as_val * res = NULL;

	as_status rc = aerospike_digest_apply(as, &err, NULL, "test", &digest, UDF_FILE, "one", NULL, &res);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq(  as_integer_toint(i), 1 );

}

TEST( digest_apply_ten , "apply: (test,test,foo) <!> digest_apply.one() => 10" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", (as_key *) as_string_new("foo",false));

	as_val * res = NULL;

	as_status rc = aerospike_digest_apply(as, &err, NULL, "test", &digest, UDF_FILE, "ten", NULL, &res);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq(  as_integer_toint(i), 10 );
}

TEST( digest_apply_add_1_2 , "apply: (test,test,foo) <!> digest_apply.add(1,2) => 3" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", (as_key *) as_string_new("foo",false));

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 3, 0);
	as_arraylist_append_int64(&arglist, 1);
	as_arraylist_append_int64(&arglist, 2);

	as_status rc = aerospike_digest_apply(as, &err, NULL, "test", &digest, UDF_FILE, "add", &arglist, &res);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq(  as_integer_toint(i), 3 );
}

TEST( digest_apply_record_exists , "apply: (test,test,foo) <!> digest_apply.record_exists() => 1" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", (as_key *) as_string_new("foo",false));

	as_val * res = NULL;

	as_status rc = aerospike_digest_apply(as, &err, NULL, "test", &digest, UDF_FILE, "record_exists", NULL, &res);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq(  as_integer_toint(i), 1 );
}


TEST( digest_apply_get_bin_a , "apply: (test,test,foo) <!> digest_apply.get_bin_a() => 123" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", (as_key *) as_string_new("foo",false));

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_arraylist_append_str(&arglist, "a");

	as_status rc = aerospike_digest_apply(as, &err, NULL, "test", &digest, UDF_FILE, "get_bin_a", &arglist, &res);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq(  as_integer_toint(i), 123 );
}


/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( digest_apply, "aerospike_digest_apply tests" ) {

    suite_before( before );
    suite_after( after );
    
	suite_add( digest_apply_put );
	suite_add( digest_apply_one );
	suite_add( digest_apply_ten );
	suite_add( digest_apply_add_1_2 );
	suite_add( digest_apply_record_exists );
	// suite_add( digest_apply_get_bin_a );
}
