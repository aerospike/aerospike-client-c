/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_index.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>

#include <aerospike/mod_lua.h>

#include "../test.h"
#include "../util/udf.h"
#include "../util/consumer_stream.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/client_stream_simple.lua"
#define UDF_FILE "client_stream_simple"

#define NAMESPACE "test"
#define SET "test"

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

TEST( query_foreach_exists, UDF_FILE" exists" ) {
	assert_true( udf_exists(LUA_FILE) );
}

/**
 * Creates 100 records and 4 indices.
 *
 * Records are structured as:
 *      {a: String, b: Integer, c: Integer, d: Integer, e: Integer}
 *
 * The key is "a-b-c-d-e"
 *
 * The values are:
 *      a = "abc"
 *      b = 100
 *      c = <current index>
 *      d = c % 10
 *      e = b + (c + 1) * (d + 1) / 2
 */
TEST( query_foreach_create, "create 100 records and 4 indices" ) {

	as_error err;
	as_error_reset(&err);

	int n_recs = 100;
	
	as_status status;

	// create index on "a"
	
	status = aerospike_index_create(as, &err, 0, NULL, NAMESPACE, SET, "a", "idx_test_a", AS_INDEX_STRING);
	if ( status != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}

	// create index on "b"

	status = aerospike_index_create(as, &err, 0, NULL, NAMESPACE, SET, "b", "idx_test_b", AS_INDEX_NUMERIC);
	if ( status != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}

	// create index on "c"

	status = aerospike_index_create(as, &err, 0, NULL, NAMESPACE, SET, "c", "idx_test_c", AS_INDEX_NUMERIC);
	if ( status != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}

	// create index on "d"

	as_index_task task;
	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "d", "idx_test_d", AS_INDEX_NUMERIC);
	if ( status == AEROSPIKE_OK ) {
		aerospike_index_create_wait(&err, &task, 0);
	}
	else {
		info("error(%d): %s", err.code, err.message);
	}

	// insert records
	for ( int i = 0; i < n_recs; i++ ) {

		char * 	a = "abc";
		int 	b = n_recs;
		int 	c = i;
		int 	d = i % 10;
		int 	e = b + (c + 1) * (d + 1) / 2;

		char keystr[64] = { '\0' };
		snprintf(keystr, 64, "%s-%d-%d-%d-%d", a, b, c, d, e);

		as_record r;
		as_record_init(&r, 5);
		as_record_set_str(&r, 	"a", a);
		as_record_set_int64(&r, "b", b);
		as_record_set_int64(&r, "c", c);
		as_record_set_int64(&r, "d", d);
		as_record_set_int64(&r, "e", e);

		as_key key;
		as_key_init(&key, NAMESPACE, SET, keystr);

		aerospike_key_put(as, &err, NULL, &key, &r);

		assert_int_eq( err.code, AEROSPIKE_OK);


		as_record_destroy(&r);

		as_record *r1 = NULL;

		aerospike_key_exists(as, &err, NULL, &key, &r1);

		as_key_destroy(&key);	
		assert_int_eq( err.code, AEROSPIKE_OK );
		assert_not_null( r1 );

		as_record_destroy(r1);
	}
}

static bool query_foreach_1_callback(const as_val * v, void * udata) {
	int * count = (int *) udata;
	if ( v == NULL ) {
		info("count: %d", (*count));
	}
	else {
		*count += 1;
	}
	return true;
}

TEST( query_foreach_1, "count(*) where a == 'abc' (non-aggregating)" ) {

	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", string_equals("abc"));
	
	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_1_callback, &count);

	assert_int_eq( err.code, 0 );
	assert_int_eq( count, 100 );

	as_query_destroy(&q);
}

static bool query_foreach_2_callback(const as_val * v, void * udata) {
	if ( v != NULL ) {
		as_integer * i = as_integer_fromval(v);
		if ( i ) {
			int64_t * count = (int64_t *) udata;
			*count = i ? as_integer_toint(i) : 0;
		}
	}
	return true;
}

TEST( query_foreach_2, "count(*) where a == 'abc' (aggregating)" ) {

	as_error err;
	as_error_reset(&err);

	int64_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", string_equals("abc"));

	as_query_apply(&q, UDF_FILE, "count", NULL);
	
	if ( aerospike_query_foreach(as, &err, NULL, &q, query_foreach_2_callback, &count) != AEROSPIKE_OK ) {
		error("%s (%s) [%s:%d]", err.message, err.code, err.file, err.line);
	}

	info("count: %d",count);
	
	assert_int_eq( err.code, 0 );
	assert_int_eq( count, 100 );

	as_query_destroy(&q);
}


static bool query_foreach_3_callback(const as_val * v, void * udata) {
	if ( v != NULL ) {
		as_integer * result = as_integer_fromval(v);
		if ( result != NULL ) {
			int64_t * value = (int64_t *) udata;
			*value = as_integer_get(result);
		}
	}
	return true;
}

TEST( query_foreach_3, "sum(e) where a == 'abc'" ) {
	
	as_error err;
	as_error_reset(&err);

	int64_t value = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", string_equals("abc"));

	as_query_apply(&q, UDF_FILE, "sum", NULL);

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_3_callback, &value);

	if ( err.code != AEROSPIKE_OK ) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	info("value: %ld", value);


	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( value, 24275 );

	as_query_destroy(&q);
}

static bool query_foreach_4_callback(const as_val * v, void * udata) {
	if ( v != NULL ) {
		as_integer * result = as_integer_fromval(v);
		if ( result != NULL ) {
			int64_t * value = (int64_t *) udata;
			*value = as_integer_get(result);
		}
	}
	return true;
}

TEST( query_foreach_4, "sum(d) where b == 100 and d == 1" ) {
	
	as_error err;
	as_error_reset(&err);

	int64_t value = 0;

	as_arraylist args;
	as_arraylist_init(&args, 2,0);
	as_arraylist_append_str(&args, "d");
	as_arraylist_append_int64(&args, 1);

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "b", integer_equals(100));

	as_query_apply(&q, UDF_FILE, "sum_on_match", (as_list *) &args);

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_4_callback, &value);

	if ( err.code != AEROSPIKE_OK ) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	info("value: %ld", value);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( value, 10 );

	as_query_destroy(&q);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( query_foreach, "aerospike_query_foreach tests" ) {

	suite_before( before );
	suite_after( after   );
	
	suite_add( query_foreach_create );
	suite_add( query_foreach_1 );
	suite_add( query_foreach_2 );
	suite_add( query_foreach_3 );
	suite_add( query_foreach_4 );
}
