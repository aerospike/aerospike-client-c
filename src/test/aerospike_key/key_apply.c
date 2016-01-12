/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_apply"

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

 TEST( key_apply_map , "apply: (test,test,foo) = map: {x: 7, y: 8, z: 9}" ) {

	as_error err;
	as_error_reset(&err);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);

	as_hashmap map;
	as_hashmap_init(&map, 1);

	as_stringmap_set_int64((as_map *) &map, "x", (int64_t)7);
	as_stringmap_set_int64((as_map *) &map, "y", (int64_t)8);
	as_stringmap_set_int64((as_map *) &map, "z", (int64_t)9);

	as_val_reserve((as_map *) &map);
	as_arraylist_append(&arglist, (as_val *)((as_map *) &map));

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "map_arg", (as_list*) &arglist, &res);

	as_key_destroy(&key);
	as_hashmap_destroy(&map);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_val_destroy(&arglist);
    as_val_destroy(res);
}

TEST( key_apply_put , "put: (test,test,foo) = {a: 123, b: 'abc', c: 456, d: 'def', e: [1,2,3], f: {x: 7, y: 8, z: 9}}" ) {

	as_error err;
	as_error_reset(&err);
	
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	
	as_hashmap map;
	as_hashmap_init(&map, 32);
	as_stringmap_set_int64((as_map *) &map, "x", 7);
	as_stringmap_set_int64((as_map *) &map, "y", 8);
	as_stringmap_set_int64((as_map *) &map, "z", 9);

	as_record r;
	as_record_inita(&r, 6);
	as_record_set_int64(&r, "a", 123);
	as_record_set_str(&r, "b", "abc");
	as_record_set_integer(&r, "c", as_integer_new(456));
	as_record_set_string(&r, "d", as_string_new("def",false));
	as_record_set_list(&r, "e", (as_list *) &list);
	as_record_set_map(&r, "f", (as_map *) &map);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_put(as, &err, NULL, &key, &r);

	as_key_destroy(&key);

	as_record_destroy(&r);
	assert_int_eq( rc, AEROSPIKE_OK );
}

TEST( key_apply_one , "apply: (test,test,foo) <!> key_apply.one() => 1" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "one", NULL, &res);

	as_key_destroy(&key);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq( as_integer_toint(i), 1 );

    as_val_destroy(res);
}

TEST( key_apply_ten , "apply: (test,test,foo) <!> key_apply.one() => 10" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "ten", NULL, &res);

	as_key_destroy(&key);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq( as_integer_toint(i), 10 );

    as_val_destroy(res);
}

TEST( key_apply_add_1_2 , "apply: (test,test,foo) <!> key_apply.add(1,2) => 3" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_arraylist arglist;
	as_arraylist_init(&arglist, 3, 0);
	as_arraylist_append_int64(&arglist, 1);
	as_arraylist_append_int64(&arglist, 2);

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "add", (as_list *) &arglist, &res);

	as_key_destroy(&key);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq( as_integer_toint(i), 3 );

    as_val_destroy(&arglist);
    as_val_destroy(res);
}

TEST( key_apply_record_exists , "apply: (test,test,foo) <!> key_apply.record_exists() => 1" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "record_exists", NULL, &res);

	as_key_destroy(&key);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq( as_integer_toint(i), 1 );

    as_val_destroy(res);
}


TEST( key_apply_get_bin_a , "apply: (test,test,foo1) <!> key_apply.get_bin_a() => 123" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_arraylist arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_arraylist_append_str(&arglist, "a");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "get_bin_a", (as_list *) &arglist, &res);

	as_key_destroy(&key);
	
    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

    as_integer * i = as_integer_fromval(res);
    assert_not_null( i );
    assert_int_eq( as_integer_toint(i), 123 );

    as_val_destroy(&arglist);
    as_val_destroy(res);
}

TEST( key_apply_null_rc , "apply: with null result value" ) {
	
	as_error err;
	as_error_reset(&err);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");
	
	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "one", NULL, NULL);
	
	as_key_destroy(&key);
	
    assert_int_eq( rc, AEROSPIKE_OK );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( key_apply, "aerospike_key_apply tests" ) {

    suite_before( before );
    suite_after( after );
    
    suite_add( key_apply_map );
	suite_add( key_apply_put );
    suite_add( key_apply_one );
	suite_add( key_apply_ten );
	suite_add( key_apply_add_1_2 );
	suite_add( key_apply_record_exists );
	suite_add( key_apply_get_bin_a );
	suite_add( key_apply_null_rc );
}
