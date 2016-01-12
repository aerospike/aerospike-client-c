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
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_udf.h>
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
#define SET "test_apply2"

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

	as_string filename_string;
	const char * base = as_basename(&filename_string, filename);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	if ( aerospike_udf_get(as, &err, NULL, base, AS_UDF_TYPE_LUA, &file) != AEROSPIKE_OK ) {
		error("error caused by aerospike_udf_get(%s): (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
	}

	as_string_destroy(&filename_string);
	as_udf_file_destroy(&file);
	
}

// TEST( key_apply2_getboolean , "apply2: (test,test,foo) <!> key_apply2.getboolean() => 1" ) {

// 	as_error err;
// 	as_error_reset(&err);

// 	as_val * res = NULL;

// 	as_arraylist arglist;
// 	as_arraylist_init(&arglist, 1, 0);
// 	as_arraylist_append_str(&arglist, "a");

// 	as_status rc = aerospike_key_apply(as, &err, NULL, NAMESPACE, SET, "foo", UDF_FILE, "getboolean", &arglist, &res);

// 	assert_int_eq( rc, AEROSPIKE_OK );
// 	assert_not_null( res );
// 	assert( res->type == AS_BOOLEAN );

// }

TEST( key_apply2_getinteger , "apply2: (test,test,foo) <!> key_apply2.getinteger() => 123" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_arraylist_append_str(&arglist, "a");

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "getinteger", (as_list *) &arglist, &res);

	as_key_destroy(&key);

	if ( rc != AEROSPIKE_OK ) {
		error("[%s:%d][%s][%d] %s", err.file, err.line, err.func, err.code, err.message);
	}

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_INTEGER );
	as_integer * i = as_integer_fromval(res);
	assert_not_null( i );
	assert_int_eq(  as_integer_toint(i), 123 );

	as_val_destroy(&arglist);
	as_val_destroy(res);

}

TEST( key_apply2_getstring , "apply2: (test,test,foo) <!> key_apply2.getstring() => abc" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_arraylist_append_str(&arglist, "b");

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "getstring", (as_list *) &arglist, &res);

	as_key_destroy(&key);

	if ( rc != AEROSPIKE_OK ) {
		error("[%s:%d][%s][%d] %s", err.file, err.line, err.func, err.code, err.message);
	}

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_STRING );
	as_string * str = as_string_fromval(res);
	assert_not_null( str );
	assert_string_eq( as_string_tostring(str), "abc" );
	as_val_destroy(&arglist);
	as_val_destroy(res);
}

// Table is the same as list, so no test for gettable()

TEST( key_apply2_getlist , "apply2: (test,test,foo) <!> key_apply2.getlist() => [1,2,3]" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_arraylist_append_str(&arglist, "e");

	as_arraylist compare_list;
	as_arraylist_init(&compare_list, 3, 0);
	as_arraylist_append_int64(&compare_list, 1);
	as_arraylist_append_int64(&compare_list, 2);
	as_arraylist_append_int64(&compare_list, 3);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "getlist", (as_list *) &arglist, &res);

	as_key_destroy(&key);

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
	// assert_int_eq( list, compare_list )

	as_val_destroy(res);
	as_val_destroy(&arglist);
	as_val_destroy(&compare_list);
}

TEST( key_apply2_getmap , "apply2: (test,test,foo) <!> key_apply2.getmap() => {x: 7, y: 8, z: 9}" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 1, 0);
	as_arraylist_append_str(&arglist, "f");

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "getmap", (as_list *) &arglist, &res);

	as_key_destroy(&key);
	as_val_destroy(&arglist);

	if ( rc != AEROSPIKE_OK ) {
		error("[%s:%d][%s][%d] %s", err.file, err.line, err.func, err.code, err.message);
	}
	as_hashmap map;
	as_hashmap_init(&map, 32);
	as_stringmap_set_int64((as_map *) &map, "x", 7);
	as_stringmap_set_int64((as_map *) &map, "y", 8);
	as_stringmap_set_int64((as_map *) &map, "z", 9);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	assert( res->type == AS_MAP );
	as_map *res_map =  as_map_fromval(res);
	assert_not_null( res_map );
	
	as_val_destroy(res);
	as_hashmap_destroy(&map);
	// assert_int_eq( map, '{x: 7, y: 8, z: 9}' );
	// assert_int_eq( res_map, map );
}

TEST( key_apply2_add_strings , "apply: (test,test,foo) <!> key_apply2.add_strings('abc','def') => 'abcdef'" ) {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 2, 0);
	as_arraylist_append_str(&arglist, "abc");
	as_arraylist_append_str(&arglist, "def");

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "add_strings", (as_list *) &arglist, &res);

	as_key_destroy(&key);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );
	as_string * str = as_string_fromval(res);
	assert_not_null( str );
	assert_string_eq( as_string_tostring(str), "abcdef" );

	as_val_destroy(&arglist);
	as_val_destroy(res);
}

// skipping record_basics_add, already present in key_apply.c

TEST( key_apply2_call_nonlocal_sum, "apply: (test,test,foo) <!> key_apply2.call_nonlocal_sum(1,2) => 'FAIL'") {
	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 3, 0);
	as_arraylist_append_int64(&arglist, 1);
	as_arraylist_append_int64(&arglist, 2);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "sum", (as_list *) &arglist, &res);

	// rc is OK, but result is NULL : verify !!

	assert_int_ne( rc, AEROSPIKE_OK );
	as_val_destroy(&arglist);
	as_val_destroy(res);
	// assert_not_null( res );

   // as_integer * i = as_integer_fromval(res);
	// assert_not_null( i );
	// assert_int_ne(  as_integer_toint(i), 3 );
}

TEST( key_apply2_call_local_sum, "apply: (test,test,foo) <!> key_apply2.call_local_sum(1,2) => 3") {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_arraylist arglist;
	as_arraylist_init(&arglist, 3, 0);
	as_arraylist_append_int64(&arglist, 1);
	as_arraylist_append_int64(&arglist, 2);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "sum_local", (as_list *) &arglist, &res);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( res );

	as_integer * i = as_integer_fromval(res);
	assert_not_null( i );
	assert_int_eq(  as_integer_toint(i), 3 );

	as_val_destroy(&arglist);
	as_val_destroy(res);
}

TEST( key_apply2_udf_func_does_not_exist, "apply: (test,test,foo) <!> key_apply2.udf_func_does_not_exist() => 1" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "udf_does_not_exist", NULL, &res);

	assert_int_ne( rc, AEROSPIKE_OK );

}

TEST( key_apply2_udf_file_does_not_exist, "apply: (test,test,foo) <!> key_apply2.udf_file_does_not_exist() => 1" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_apply(as, &err, NULL, &key, "udf_does_not_exist", "udf_does_not_exist", NULL, &res);

	assert_int_ne( rc, AEROSPIKE_OK );

}

#define KVPAIR_KEY_MAX 64
#define KVPAIR_KEY_LEN 64 - 1

#define KVPAIR_VAL_MAX 64
#define KVPAIR_VAL_LEN 64 - 1

#define KVPAIR_VALS_MAX 256

typedef char kvpair_key[KVPAIR_KEY_MAX];
typedef char kvpair_val[KVPAIR_VAL_MAX];

typedef struct {
	kvpair_key	key;
	kvpair_val	vals[KVPAIR_VALS_MAX];
	uint32_t 	vals_size;
} kvpair;

#define kvpair_init(__name) { \
		.key = __name, \
		.vals = {{0}}, \
		.vals_size = 0 \
	}

#define each_stats(__query, __name, __key, __val, __block) \
	if ( strlen(__query) > 0 && strlen(__name) > 0 ) { \
		kvpair kvp = kvpair_init(__name); \
		aerospike_info_foreach(as, &err, NULL, __query, kvpair_search, &kvp);\
		if ( err.code != AEROSPIKE_OK ) {\
			error("(%d) %s [%s:%d]", err.code, err.message, err.file, err.line);\
			assert(err.code != AEROSPIKE_OK);\
		}\
		for( int i = 0; i < kvp.vals_size && kvp.vals[i] != '\0'; i++ ) {\
			char * __val = kvp.vals[i]; \
			__block; \
			__val = NULL; \
		}\
	}

bool kvpair_search(const as_error * err, const as_node * node, const char * req, char * res, void * udata)
{
	kvpair * kvp = (kvpair *) udata;

	if ( kvp->vals_size >= KVPAIR_VALS_MAX ) {
		return false;
	}

	// Skip the query and only take the response
	char * response = strchr(res, '\t') + 1;

	char * pair_ = NULL;
	for ( char * pair = strtok_r(response, ";", &pair_); pair; pair = strtok_r(NULL, ";", &pair_) ) {

		char * del = strchr(pair, '=');

		if ( del == NULL ) {
			continue;
		}

		del[0] = '\0';
		char * key = pair;

		if ( strcmp(key, kvp->key) != 0 ) {
			continue;
		}

		char * val = del + 1;

		strcpy(kvp->vals[kvp->vals_size], val);
		kvp->vals_size++;

		return true;
	}

	return true;
}

// Check to see if the record is getting replicated on a delete from UDF
TEST( key_apply2_delete_record_test_replication, "apply: (test,test,foo) <!> key_apply2.delete_record_test_replication() => 1" )
{
	int64_t mem_pre = 0;
	int64_t mem_post = 0;
	
	as_error err;
	as_error_reset(&err);
	as_status rc;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	rc = aerospike_key_remove(as, &err, NULL, &key);

	// Insert 3 bins
	as_record r;
	as_record_init(&r, 3);
	as_record_set_string(&r, "a", as_string_new("String 1",false));
	as_record_set_string(&r, "b", as_string_new("String 2",false));
	as_record_set_string(&r, "c", as_string_new("String 3",false));
	as_error_reset(&err);
	as_key_init(&key, NAMESPACE, SET, "foo");
	rc = aerospike_key_put(as, &err, NULL, &key, &r);

	assert_int_eq( rc, AEROSPIKE_OK );

	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_pre += mem;
		debug("used-bytes-memory(pre): %ld", mem);
	});


	// Apply udf to delete bins
	as_error_reset(&err);
	as_val * res = NULL;
	as_key_init(&key, NAMESPACE, SET, "foo");
	rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "delete", NULL, &res);

	assert_int_eq( rc, AEROSPIKE_OK );
	as_val_destroy(res);
	as_record_destroy(&r);

	// Get bins
	as_error_reset(&err);
	as_record_init(&r, 0);
	as_record *rec = &r;
	as_key_init(&key, NAMESPACE, SET, "foo");
	rc = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq( rc, AEROSPIKE_OK );

	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_post += mem;
		debug("used-bytes-memory(post): %ld", mem);
	});

	debug("memory: pre=%ld post=%ld diff=%ld", mem_pre, mem_post, mem_pre - mem_post);
	as_record_destroy(&r);
}

TEST( key_apply2_update_record_test_memory, "apply: (test,test,foo) <!> key_apply2.update_record_test_memory() => 1" )
{
	int64_t mem_pre = 0;
	int64_t mem_post = 0;

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_remove(as, &err, NULL, &key);

	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_pre += mem;
		debug("used-bytes-memory(pre): %ld", mem);
	});

	// Create & Update record
	as_error_reset(&err);
	as_val * res = NULL;
	as_key_init(&key, NAMESPACE, SET, "foo");
	rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "update_record", NULL, &res);

	assert_int_eq( rc, AEROSPIKE_OK );

	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_post += mem;
		debug("used-bytes-memory(post): %ld", mem);
	});

	as_val_destroy(res);
	debug("memory: pre=%ld post=%ld diff=%ld", mem_pre, mem_post, mem_pre - mem_post);
}

TEST( key_apply2_bad_update_test_memory, "apply: (test,test,foo) <!> key_apply2.bad_update_test_memory() => 1" )
{
	int64_t mem_pre = 0;
	int64_t mem_post = 0;

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_remove(as, &err, NULL, &key);

	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_pre += mem;
		debug("used-bytes-memory(pre): %ld", mem);
	});

	// Create & Update record
	as_error_reset(&err);
	as_val * res = NULL;
	as_key_init(&key, NAMESPACE, SET, "foo");
	rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "bad_update", NULL, &res);

	assert_int_eq( rc, AEROSPIKE_OK );
	as_val_destroy(res);

	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_post += mem;
		debug("used-bytes-memory(post): %ld", mem);
	});

	debug("memory: pre=%ld post=%ld diff=%ld", mem_pre, mem_post, mem_pre - mem_post);
}

TEST( key_apply2_bad_create_test_memory, "apply: (test,test,foo) <!> key_apply2.bad_create_test_memory() => 1" )
{
	int64_t mem_pre = 0;
	int64_t mem_post = 0;

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_remove(as, &err, NULL, &key);

	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_pre += mem;
		debug("used-bytes-memory(pre): %ld", mem);
	});

	// Create & Update record
	as_error_reset(&err);
	as_val * res = NULL;
	as_key_init(&key, NAMESPACE, SET, "foo");

	rc = aerospike_key_apply(as, &err, NULL, &key, UDF_FILE, "bad_create", NULL, &res);

	assert_int_eq( rc, AEROSPIKE_OK );
	as_val_destroy(res);
	
	each_stats("namespace/test", "used-bytes-memory", key, val, {
		uint64_t mem = atol(val);
		mem_post += mem;
		debug("used-bytes-memory(post): %ld", mem);
	});

	debug("memory: pre=%ld post=%ld diff=%ld", mem_pre, mem_post, mem_pre - mem_post);
}

TEST( key_apply2_ttl, "apply: (test,test,foo_ttl_check)" ) {

	as_error err;
	as_error_reset(&err);

	as_val * res = NULL;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo_ttl_check");

	as_policy_apply policy;
	as_policy_apply_init(&policy);
	policy.ttl = 5;

	as_status rc = aerospike_key_apply(as, &err, &policy, &key, UDF_FILE, "update_record", NULL, &res);
    assert_int_eq( rc, AEROSPIKE_OK );

    as_val_destroy(res);

	as_record r, *rec = &r;
	as_record_init(&r, 0);

	rc = aerospike_key_get(as, &err, NULL, &key, &rec);
	info ("ttl = %d",rec->ttl);
	assert_true (rec->ttl<=5);

	as_record_destroy(rec);

}

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
	suite_add( key_apply2_call_nonlocal_sum );
	suite_add( key_apply2_call_local_sum );
	suite_add( key_apply2_udf_func_does_not_exist );
	suite_add( key_apply2_udf_file_does_not_exist );
	suite_add( key_apply2_delete_record_test_replication );
	suite_add( key_apply2_update_record_test_memory );
	suite_add( key_apply2_bad_update_test_memory );
	suite_add( key_apply2_bad_create_test_memory );
	suite_add( key_apply2_ttl );

}
