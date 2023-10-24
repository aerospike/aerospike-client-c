/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#if !defined(_MSC_VER)
#include <regex.h>
#else
#define REG_ICASE 2
#endif

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_query.h>
#include <aerospike/as_map.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/index_util.h"
#include "../util/info_helper.h"
#include "../util/udf.h"
#include "../util/consumer_stream.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;
static uint64_t g_epochns;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE AS_START_DIR "src/test/lua/client_stream_simple.lua"
#define UDF_FILE "client_stream_simple"

#define NAMESPACE "test"
#define SET "query_foreach"
#define NAMESPACE_INFO "namespace/test"

bool namespace_has_persistence = false;
bool namespace_in_memory = false;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

/**
 * Creates 100 records and 9 indexes.
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
bool
query_foreach_create(void)
{
	as_error err;
	as_error_reset(&err);
	
	int n_recs = 100;
	
	as_status status;
	as_index_task task;
	
	// create index on "a"	
	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "a", "idx_test_a", AS_INDEX_STRING);
	index_process_return_code(status, &err, &task);

	// create index on "b"
	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "b", "idx_test_b", AS_INDEX_NUMERIC);
	index_process_return_code(status, &err, &task);

	// create index on "c"
	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "c", "idx_test_c", AS_INDEX_NUMERIC);
	index_process_return_code(status, &err, &task);

	// create index on "d"
	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "d", "idx_test_d", AS_INDEX_NUMERIC);
	index_process_return_code(status, &err, &task);


	// create complex index on "x"
	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, "x", "idx_test_x", AS_INDEX_TYPE_LIST, AS_INDEX_STRING);
	index_process_return_code(status, &err, &task);

	as_cdt_ctx ctx;

	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, 0);

	status = aerospike_index_create_ctx(as, &err, &task, NULL, NAMESPACE, SET, "x", "idx_ctx_test_x", AS_INDEX_TYPE_DEFAULT, AS_INDEX_STRING, &ctx);
	index_process_return_code(status, &err, &task);

	as_cdt_ctx_destroy(&ctx);

	// create complex index on "y"
	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, "y", "idx_test_y", AS_INDEX_TYPE_MAPKEYS, AS_INDEX_STRING);
	index_process_return_code(status, &err, &task);

	// create complex index on "y"	 
	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, "y", "idx_test_y1", AS_INDEX_TYPE_MAPVALUES, AS_INDEX_STRING);
	index_process_return_code(status, &err, &task);

	as_string ykey;
	as_string_init(&ykey, "ykey", false);

	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)&ykey);

	status = aerospike_index_create_ctx(as, &err, &task, NULL, NAMESPACE, SET, "y", "idx_ctx_test_y", AS_INDEX_TYPE_DEFAULT, AS_INDEX_STRING, &ctx);
	index_process_return_code(status, &err, &task);

	as_cdt_ctx_destroy(&ctx);

	// create complex index on "z"	 
	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, "z", "idx_test_z", AS_INDEX_TYPE_LIST, AS_INDEX_NUMERIC);
	index_process_return_code(status, &err, &task);

	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_rank(&ctx, -1);

	status = aerospike_index_create_ctx(as, &err, &task, NULL, NAMESPACE, SET, "z", "idx_ctx_test_z", AS_INDEX_TYPE_DEFAULT, AS_INDEX_NUMERIC, &ctx);
	index_process_return_code(status, &err, &task);

	as_cdt_ctx_destroy(&ctx);

	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, "blob", "idx_blob_test", AS_INDEX_TYPE_DEFAULT, AS_INDEX_BLOB);
	index_process_return_code(status, &err, &task);

	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, "blob_list", "idx_list_blob_test", AS_INDEX_TYPE_LIST, AS_INDEX_BLOB);
	index_process_return_code(status, &err, &task);

	char* buffer = alloca(n_recs * 1024 + 1);
	uint32_t the_ttl = AS_RECORD_NO_EXPIRE_TTL;
	
	// insert records
	for (int i = 0; i < n_recs; i++) {

		if (i == 10) {
			// We change the TTL from never to 100 days
			the_ttl = 100 * 24 * 60 * 60;
		}
		else if (i == 42) {
			// NOTE - We pause on the 42nd iteration for a few
			// milliseconds and note the time. We can then use the
			// filter_rec_last_update test below to find
			// the later records.

			as_sleep(5);
			g_epochns = cf_clock_getabsolute() * 1000 * 1000;
			as_sleep(5);

			// Also on the 42nd iteration we change the TTL to
			// 10 days for the remaining records.
			the_ttl = 10 * 24 * 60 * 60;
		}
		
		char * 	a = "abc";
		int 	b = n_recs;
		int 	c = i;
		int 	d = i % 10;
		int 	e = b + (c + 1) * (d + 1) / 2;
		int		g = i;	// Only set on odd records.
		
		char f[64];
		snprintf(f, sizeof(f), "0x%04x", i);
		
		char keystr[64] = { '\0' };
		snprintf(keystr, 64, "%s-%d-%d-%d-%d", a, b, c, d, e);
		
		// Make list		
		as_arraylist list;
		as_arraylist_init(&list, 3, 0);
		if ((i%3) == 0) {
			as_arraylist_append_str(&list, "x");
			as_arraylist_append_str(&list, "x1");
			as_arraylist_append_str(&list, "x2");
		} else {
			as_arraylist_append_str(&list, "not_x1");
			as_arraylist_append_str(&list, "not_x2");
			as_arraylist_append_str(&list, "not_x3");

		}
		
		// Make map
		as_hashmap map;
		as_hashmap_init(&map, 1);
		if ((i%7) == 0) {
			as_stringmap_set_str((as_map *) &map, "ykey", "yvalue");
		} else {
			as_stringmap_set_str((as_map *) &map, "ykey_not", "yvalue_not");

		}		
		
		// Make list of integers		
		as_arraylist list2;
		as_arraylist_init(&list2, 5, 0);
		as_arraylist_append_int64(&list2, i);
		as_arraylist_append_int64(&list2, i+1);
		as_arraylist_append_int64(&list2, i+2);
		as_arraylist_append_int64(&list2, i+3);
		as_arraylist_append_int64(&list2, i+4);

		// Make a string of variable size
		for (int jj = 0; jj < i * 1024; ++jj) {
			buffer[jj] = 'X';
		}
		buffer[i * 1024] = '\0';

		// Make blob.
		uint8_t blob[4];
		uint8_t* blob_ptr = blob;
		*(uint32_t*)blob_ptr = 50000 + i;

		// Make list of blobs
		as_arraylist list3;
		as_arraylist_init(&list3, 1, 0);
		as_bytes bytes;
		as_bytes_init_wrap(&bytes, blob_ptr, 4, false);
		as_arraylist_append_bytes(&list3, &bytes);

		// We only create the g bin for odd records.
		bool create_g_bin = i % 2 == 1;
		
		as_record r;
		as_record_init(&r, 12 + (create_g_bin ? 1 : 0));
		as_record_set_str(&r,   "a", a);
		as_record_set_int64(&r, "b", b);
		as_record_set_int64(&r, "c", c);
		as_record_set_int64(&r, "d", d);
		as_record_set_int64(&r, "e", e);
		as_record_set_str(&r,   "f", f);
		if (create_g_bin) {
			as_record_set_int64(&r, "g", g);
		}
		as_record_set_list(&r, "x", (as_list *) &list);
		as_record_set_map(&r, "y", (as_map *) &map);
		as_record_set_list(&r, "z", (as_list *) &list2);
		as_record_set_list(&r, "blob_list", (as_list *) &list3);
		as_record_set_str(&r, "bigstr", buffer);
		as_record_set_rawp(&r, "blob", blob_ptr, sizeof(uint32_t), false);

		r.ttl = the_ttl;
		
		as_key key;
		as_key_init(&key, NAMESPACE, SET, keystr);
		
		aerospike_key_put(as, &err, NULL, &key, &r);
		as_record_destroy(&r);
		
		if (err.code != AEROSPIKE_OK) {
			error("aerospike_key_put() failed %d %s", err.code, err.message);
			return false;
		}
		
		as_record *r1 = NULL;
		
		aerospike_key_exists(as, &err, NULL, &key, &r1);
		as_key_destroy(&key);
		
		if (err.code != AEROSPIKE_OK) {
			error("aerospike_key_exists() failed %d %s", err.code, err.message);
			return false;
		}
		
		if (! r1) {
			error("key not found %s", keystr);
			return false;
		}
		
		as_record_destroy(r1);
	}
	return true;
}

/**
 * Destroy 9 indexes.
 */
bool
query_foreach_destroy(void)
{
	as_error err;
	as_error_reset(&err);

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_a");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_b");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_c");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_d");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_x");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_ctx_test_x");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_y");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_y1");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_ctx_test_y");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_z");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_ctx_test_z");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_blob_test");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_list_blob_test");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	return true;
}

static bool
before(atf_suite * suite)
{
	if (! udf_put(LUA_FILE)) {
		error("failure while uploading: %s", LUA_FILE);
		return false;
	}

	if (! udf_exists(LUA_FILE)) {
		error("lua file does not exist: %s", LUA_FILE);
		return false;
	}

	return query_foreach_create();
}

static bool
after(atf_suite * suite)
{
	if (! udf_remove(LUA_FILE)) {
		error("failure while removing: %s", LUA_FILE);
		return false;
	}

	return query_foreach_destroy();
}

static bool
count_callback(const as_val* v, void* udata)
{
	uint32_t * count = (uint32_t *) udata;
	if (v == NULL) {
		info("count: %d", as_load_uint32(count));
	}
	else {
		as_incr_uint32(count);
	}
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(query_foreach_exists, UDF_FILE" exists")
{
	assert_true(udf_exists(LUA_FILE));
}

static bool
query_foreach_count_callback(const as_val* v, void* udata)
{
	uint32_t * count = (uint32_t *) udata;
	if (v == NULL) {
		info("count: %d", as_load_uint32(count));
	}
	else {
		as_incr_uint32(count);
	}
	return true;
}

TEST(query_foreach_1, "count(*) where a == 'abc' (non-aggregating)")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));
	
	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	assert_int_eq(err.code, 0);
	assert_int_eq(count, 100);

	as_query_destroy(&q);
}

static bool
query_foreach_2_callback(const as_val* v, void* udata)
{
	if (v != NULL) {
		as_integer * i = as_integer_fromval(v);
		if (i) {
			int64_t * count = (int64_t *) udata;
			*count = i ? as_integer_toint(i) : 0;
		}
	}
	return true;
}

TEST(query_foreach_2, "count(*) where a == 'abc' (aggregating)")
{
	as_error err;
	as_error_reset(&err);

	int64_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_query_apply(&q, UDF_FILE, "count", NULL);
	
	if (aerospike_query_foreach(as, &err, NULL, &q, query_foreach_2_callback, &count) != AEROSPIKE_OK) {
		error("%s (%d) [%s:%d]", err.message, err.code, err.file, err.line);
	}

	info("count: %d",count);
	
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 100);

	as_query_destroy(&q);
}

static bool
query_foreach_3_callback(const as_val* v, void* udata)
{
	if (v != NULL) {
		as_integer * result = as_integer_fromval(v);
		if (result != NULL) {
			int64_t * value = (int64_t *) udata;
			*value = as_integer_get(result);
		}
	}
	return true;
}

TEST(query_foreach_3, "sum(e) where a == 'abc'")
{
	as_error err;
	as_error_reset(&err);

	int64_t value = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_query_apply(&q, UDF_FILE, "sum", NULL);

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_3_callback, &value);

	if (err.code != AEROSPIKE_OK) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	info("value: %ld", value);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(value, 24275);

	as_query_destroy(&q);
}

static bool
query_foreach_4_callback(const as_val* v, void* udata)
{
	if (v != NULL) {
		as_integer * result = as_integer_fromval(v);
		if (result != NULL) {
			int64_t * value = (int64_t *) udata;
			*value = as_integer_get(result);
		}
	}
	return true;
}

TEST(query_foreach_4, "sum(d) where b == 100 and d == 1")
{
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
	as_query_where(&q, "b", as_integer_equals(100));

	as_query_apply(&q, UDF_FILE, "sum_on_match", (as_list *) &args);

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_4_callback, &value);

	if (err.code != AEROSPIKE_OK) {
		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	info("value: %ld", value);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(value, 10);

	as_arraylist_destroy(&args);
	as_query_destroy(&q);
}

TEST(query_foreach_5, "IN LIST count(*) where x contains 'x'")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "x", as_contains(LIST, STRING, "x"));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	if (err.code != AEROSPIKE_OK) {
		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(count, 34);

	as_query_destroy(&q);
}

TEST(query_foreach_6, "IN MAPKEYS count(*) where y contains 'ykey'")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "y", as_contains(MAPKEYS, STRING, "ykey"));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	if (err.code != AEROSPIKE_OK) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(count, 15);

	as_query_destroy(&q);
}

TEST(query_foreach_7, "IN MAPVALUES count(*) where y contains 'yvalue'")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "y", as_contains(MAPVALUES, STRING, "yvalue"));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	if (err.code != AEROSPIKE_OK) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(count, 15);

	as_query_destroy(&q);
}

TEST(query_foreach_8, "IN LIST count(*) where z between 50 and 51")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "z", as_range(LIST, NUMERIC, (int64_t)50, (int64_t)51));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	if (err.code != AEROSPIKE_OK) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	// The following records should be found:
	//
	// [46, 47, 48, 49, 50]
	// [47, 48, 49, 50, 51] *
	// [48, 49, 50, 51, 52] *
	// [49, 50, 51, 52, 53] *
	// [50, 51, 52, 53, 54] *
	// [51, 52, 53, 54, 55]
	//
	// The middle 4 are found twice in the secondary index. We might return
	// duplicates, so may see any count between 6 and 10.

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert(count >= 6 && count <= 10);

	as_query_destroy(&q);
}

TEST(query_foreach_9, "CTX on LIST count(*) where max value in list 'z' is between 51 and 54")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_rank(&ctx, -1);

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where_with_ctx(&q, "z", &ctx, as_range(DEFAULT, NUMERIC, (int64_t)51,
			(int64_t)54));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	if (err.code != AEROSPIKE_OK) {
		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	// The following records should be found:
	//
	// [46, 47, 48, 49, 50]
	// [47, 48, 49, 50, 51] *
	// [48, 49, 50, 51, 52] *
	// [49, 50, 51, 52, 53] *
	// [50, 51, 52, 53, 54] *
	// [51, 52, 53, 54, 55]

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(count, 4);

	as_query_destroy(&q);
	as_cdt_ctx_destroy(&ctx);
}

TEST(query_with_range_filter, "query_with_range_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_and(
			as_exp_and(
				as_exp_cmp_le(as_exp_bin_int("d"), as_exp_int(5)),
				as_exp_cmp_ge(as_exp_bin_int("d"), as_exp_int(3))),
			as_exp_and(
				as_exp_cmp_le(as_exp_bin_int("c"), as_exp_int(20)),
				as_exp_cmp_ge(as_exp_bin_int("c"), as_exp_int(11)))));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// The first range filter should match records 11, 12, ... 20.
	// The second range filter should match all records that mod 10
	// returns 3, 4 and 5.  The combination should match only 13, 14
	// and 15.
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 3);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_equality_filter, "query_with_equality_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_str("0x001d"), as_exp_bin_str("f")));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// We should only match one record.
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 1);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_rec_size_filter, "query_with_rec_size_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_cmp_ge(as_exp_record_size(), as_exp_int(65 * 1024)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// We should match 100 - 65 records
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 35);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_rec_device_size_filter, "query_with_rec_device_size_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_cmp_ge(as_exp_device_size(), as_exp_int(65 * 1024)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// We should match 100 - 65 records
	assert_int_eq(err.code, 0);

	if (namespace_has_persistence) {
		assert_int_eq(count, 35);
	}
	else {
		assert_int_eq(count, 0);
	}

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_rec_memory_size_filter, "query_with_rec_memory_size_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_cmp_ge(as_exp_memory_size(), as_exp_int(65 * 1024)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// We should match 100 - 65 records
	assert_int_eq(err.code, 0);

	if (namespace_in_memory) {
		assert_int_eq(count, 35);
	}
	else {
		assert_int_eq(count, 0);
	}

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_intermittent_bin_filter, "query_intermittent_bin_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "c", as_integer_range(10, 30));

	as_exp_build(filter,
		as_exp_cmp_gt(as_exp_bin_int("g"), as_exp_int(20)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// Where clause matches c between 10, 11, 12 ... 28, 29, 30.
	// The "g" bin is larger than 20 for 21, 23, 25, 27, 29
	// The "g" bin is missing for even numbers, should be false.
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 5);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(scan_with_rec_last_update_filter, "scan_with_rec_last_update_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_exp_build(filter,
		as_exp_cmp_gt(as_exp_last_update(), as_exp_int(g_epochns)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// We should match 100 - 42 records
	assert_int_eq(err.code, 0);

	// Clock skew between client and server can cause slightly different results.
	//assert_int_eq(count, 100 - 42);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(scan_with_rec_last_update_filter_less, "scan_with_rec_last_update_filter_less")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_exp_build(filter,
		as_exp_cmp_le(as_exp_last_update(), as_exp_int(g_epochns)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// We should match 42 records
	assert_int_eq(err.code, 0);

	// Clock skew between client and server can cause slightly different results.
	//assert_int_eq(count, 42);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(scan_with_rec_void_time_filter_1, "scan_with_rec_void_time_filter_1")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_void_time(), as_exp_int(-1)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// These are the 0 TTL, from [0:9]
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 10);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(scan_with_rec_void_time_filter_2, "scan_with_rec_void_time_filter_2")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	int64_t tstamp_5_days =
		((int64_t) time(NULL) + ((int64_t) 5 * 24 * 60 * 60)) * (int64_t)1e9;
	int64_t tstamp_15_days =
		((int64_t) time(NULL) + ((int64_t) 15 * 24 * 60 * 60)) * (int64_t)1e9;

	as_exp_build(filter,
		as_exp_and(
			as_exp_cmp_lt(
				as_exp_void_time(),
				as_exp_int(tstamp_15_days)),
			as_exp_cmp_gt(
				as_exp_void_time(),
				as_exp_int(tstamp_5_days))));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q,
							count_callback, &count);

	// These are the 10 day TTL, from [42:99]
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 58);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(scan_with_rec_void_time_filter_3, "scan_with_rec_void_time_filter_3")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	int64_t tstamp_15_days =
		((int64_t) time(NULL) + ((int64_t) 15 * 24 * 60 * 60)) * (int64_t)1e9;

	as_exp_build(filter,
		as_exp_cmp_gt(as_exp_void_time(), as_exp_int(tstamp_15_days)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q,
							count_callback, &count);

	// These are the 100 day TTL, from index [10:41]
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 32);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(scan_with_rec_digest_modulo_filter, "scan_with_rec_digest_modulo_filter")
{
	as_error err;
	as_error_reset(&err);

	int count[3];

	for (int ii = 0; ii < 3; ++ii) {
		count[ii] = 0;

		as_query q;
		as_query_init(&q, NAMESPACE, SET);

		as_query_select_init(&q, 1);
		as_query_select(&q, "c");

		as_exp_build(filter,
			as_exp_cmp_eq(as_exp_digest_modulo(3), as_exp_int(ii)));

		as_policy_query p;
		as_policy_query_init(&p);
		p.base.filter_exp = filter;

		aerospike_query_foreach(as, &err, &p, &q, count_callback, &count[ii]);

		as_exp_destroy(filter);
		as_query_destroy(&q);
	}

	assert_int_eq(err.code, 0);
	assert_int_eq(count[0], 31);
	assert_int_eq(count[1], 30);
	assert_int_eq(count[2], 39);
}

TEST(query_with_or_filter, "query_with_or_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_or(
			as_exp_and(
				as_exp_cmp_le(as_exp_bin_int("d"), as_exp_int(5)),
				as_exp_cmp_ge(as_exp_bin_int("d"), as_exp_int(3))),
			as_exp_and(
				as_exp_cmp_le(as_exp_bin_int("c"), as_exp_int(20)),
				as_exp_cmp_ge(as_exp_bin_int("c"), as_exp_int(11)))));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// The first range filter should match records 11, 12, ... 20 (10
	// total) The second range filter should match all records that
	// mod 10 returns 3, 4 and 5 (30 total).  The combination should
	// match 40 less the 3 in common, so 37.
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 37);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_not_filter, "query_with_not_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_not(
			as_exp_or(
				as_exp_and(
					as_exp_cmp_le(as_exp_bin_int("d"), as_exp_int(5)),
					as_exp_cmp_ge(as_exp_bin_int("d"), as_exp_int(3))),
				as_exp_and(
					as_exp_cmp_le(as_exp_bin_int("c"), as_exp_int(20)),
					as_exp_cmp_ge(as_exp_bin_int("c"), as_exp_int(11))))));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// The first range filter should match records 11, 12, ... 20 (10
	// total) The second range filter should match all records that
	// mod 10 returns 3, 4 and 5 (30 total).  The combination should
	// match 40 less the 3 in common, so 37.  The not inverts to 63.
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 63);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_regex_filter, "query_with_regex_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter, as_exp_cmp_regex(0, "0x00.[12]", as_exp_bin_str("f")));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// I think we match:
	//     0x0001, 0x0002, 0x0011, 0x0012 ... 0x0061, 0x0062
	// for a total of 14 matches
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 14);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_regex_filter_icase, "query_with_regex_filter_icase")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
			as_exp_cmp_regex(REG_ICASE, "0X00.[12]", as_exp_bin_str("f")));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// I think we match:
	//     0x0001, 0x0002, 0x0011, 0x0012 ... 0x0061, 0x0062
	// for a total of 14 matches
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 14);

	as_exp_destroy(filter);
	as_query_destroy(&q);
}

TEST(query_with_list_filter, "query_with_list_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_cmp_ne(
			as_exp_list_get_by_value(NULL, AS_LIST_RETURN_COUNT, as_exp_str("x2"),
				as_exp_bin_list("x")),
			as_exp_int(0)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// Should match on records 0, 3, 6 ... 99
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 34);

	as_query_destroy(&q);
	as_exp_destroy(filter);
}

TEST(query_with_mapkey_filter, "query_with_mapkey_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_cmp_ne(
			as_exp_map_get_by_key(NULL, AS_MAP_RETURN_COUNT, AS_EXP_TYPE_INT, as_exp_str("ykey_not"),
				as_exp_bin_map("y")),
			as_exp_int(0)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// Should skip 0, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98
	// 100 - 15 = 85
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 85);

	as_query_destroy(&q);
	as_exp_destroy(filter);
}

TEST(query_with_mapval_filter, "query_with_mapval_filter")
{
	as_error err;
	as_error_reset(&err);

	int count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_exp_build(filter,
		as_exp_and(
			as_exp_map_get_by_value(NULL, AS_MAP_RETURN_EXISTS, as_exp_str("yvalue"),
				as_exp_bin_map("y")),
			as_exp_cmp_ne(
				as_exp_map_get_by_value(NULL, AS_MAP_RETURN_COUNT, as_exp_str("yvalue"),
					as_exp_bin_map("y")),
				as_exp_int(0))));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	aerospike_query_foreach(as, &err, &p, &q, count_callback, &count);

	// Should match on  0, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98
	assert_int_eq(err.code, 0);
	assert_int_eq(count, 15);

	as_query_destroy(&q);
	as_exp_destroy(filter);
}

static bool
query_quit_early_callback(const as_val* v, void* udata)
{
	if (v) {
		uint32_t * count = (uint32_t *) udata;
		as_incr_uint32(count);
	}
	return false;
}

TEST(query_quit_early, "normal query and quit early")
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	uint32_t nodes_size = nodes->size;
	as_nodes_release(nodes);

	as_error err;
	as_error_reset(&err);
	
	uint32_t count = 0;
	
	as_query q;
	as_query_init(&q, NAMESPACE, SET);
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));
	
	if (aerospike_query_foreach(as, &err, NULL, &q, query_quit_early_callback, &count) != AEROSPIKE_OK) {
		error("%s (%d) [%s:%d]", err.message, err.code, err.file, err.line);
	}
	
	info("count: %d",count);
	
	assert_int_eq(err.code, 0);
	assert_true(count <= nodes_size);
	
	as_query_destroy(&q);
}

TEST(query_agg_quit_early, "aggregation and quit early")
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	uint32_t nodes_size = nodes->size;
	as_nodes_release(nodes);

	as_error err;
	as_error_reset(&err);
	
	uint32_t count = 0;
	
	as_query q;
	as_query_init(&q, NAMESPACE, SET);
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));
	
	as_query_apply(&q, UDF_FILE, "filter_passthrough", NULL);
	
	if (aerospike_query_foreach(as, &err, NULL, &q, query_quit_early_callback, &count) != AEROSPIKE_OK) {
		error("%s (%d) [%s:%d]", err.message, err.code, err.file, err.line);
	}
	
	info("count: %d",count);
	
	assert_int_eq(err.code, 0);
	assert_true(count <= nodes_size);
	
	as_query_destroy(&q);
}

static bool
query_quit_early_bytes_callback(const as_val* v, void* udata)
{
	if (v) {
		as_bytes * bval = as_bytes_fromval(v);

		if (bval) {
			uint32_t* byte_count = (uint32_t*)udata;
			as_add_uint32(byte_count, as_bytes_size(bval));
		}
	}
	return false;
}

TEST(query_filter_map_bytes, "return bytes from a mapper")
{
	as_error err;
	as_error_reset(&err);

	uint32_t byte_count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));

	as_query_apply(&q, UDF_FILE, "filter_passthrough_digest", NULL);

	if (aerospike_query_foreach(as, &err, NULL, &q, query_quit_early_bytes_callback, &byte_count) != AEROSPIKE_OK) {
		error("%s (%d) [%s:%d]", err.message, err.code, err.file, err.line);
	}

	info("byte count: %d",byte_count);

	assert_int_eq(err.code, 0);
	assert_int_eq(byte_count, 20); // one digest

	as_query_destroy(&q);
}

TEST(query_foreach_nullset, "test null-set behavior")
{
	as_error err;
	as_error_reset(&err);

	as_status status;

	char *setname = NULL;
	as_index_task task;

	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, setname, "NUMERIC", "idx2", AS_INDEX_NUMERIC);
	index_process_return_code(status, &err, &task);

	as_record r;
	as_record_init(&r, 2);
	as_record_set_int64(&r, "NUMERIC", 1);
	as_record_set_str(&r, 	"bn_STRING", "2");
	as_record_set_int64(&r, "bn2", 3);

	as_key key;
	as_key_init(&key, NAMESPACE, setname, "keyindex-nullset");

	aerospike_key_put(as, &err, NULL, &key, &r);
	assert_int_eq(err.code, AEROSPIKE_OK);

	as_record_destroy(&r);
	as_key_destroy(&key);

	uint32_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, setname);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "NUMERIC", as_integer_equals(1));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);
	assert_true(count == 1);

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx2");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq(err.code, AEROSPIKE_OK);
}

typedef struct foreach_double_udata_s {
	uint64_t count;
	double sum;
	pthread_mutex_t lock;
} foreach_double_udata;

static bool
query_foreach_double_callback(const as_val* v, void* udata)
{
	if (v) {
		as_record* rec = as_record_fromval(v);
		foreach_double_udata *d = (foreach_double_udata *)udata;
		pthread_mutex_lock(&d->lock);
		d->sum += as_record_get_double(rec,"double_bin", 0.0);
		d->count++;
		pthread_mutex_unlock(&d->lock);
	}
    return true;
}

TEST(query_foreach_int_with_double_bin, "test query on double behavior")
{
	as_error err;
	as_error_reset(&err);

	int n_recs = 1000;
	int n_start = 51;
	int n_end = 70;
	char *int_bin = "int_bin";
	char *double_bin = "double_bin";

	as_status status;

	as_index_task task;

	//create index on "int_bin"
	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, int_bin, "idx_test_int_bin", AS_INDEX_NUMERIC);
	index_process_return_code(status, &err, &task);

	as_record r;
	as_record_init(&r, 2);
	// insert records
	for (int i = 1; i <= n_recs; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		as_record_set_int64(&r, int_bin, i);
		as_record_set_double(&r, double_bin, i/(double)10);

		aerospike_key_put(as, &err, NULL, &key, &r);
	}
	as_record_destroy(&r);

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, double_bin);

	as_query_where_inita(&q, 1);
	as_query_where(&q, int_bin, as_integer_range(n_start, n_end));
	
	double expected_sum = 0;
	foreach_double_udata udata = {0};
	pthread_mutex_init(&udata.lock, NULL);

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_double_callback, &udata);

	for (int j = n_start; j <= n_end; j++) {
		expected_sum += j/(double)10;
	}

	assert_int_eq(err.code, 0);
	assert_double_eq(udata.sum, expected_sum);
	assert_int_eq(udata.count, 20);

	as_query_destroy(&q);

	pthread_mutex_destroy(&udata.lock);

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_int_bin");
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq(err.code, AEROSPIKE_OK);
}

TEST(query_list_ctx_is_string, "IN LIST count(*) where x[0] is 'x'")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, 0);

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where_with_ctx(&q, "x", &ctx, as_string_equals("x"));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	if (err.code != AEROSPIKE_OK) {
		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(count, 34);

	as_cdt_ctx_destroy(&ctx);
	as_query_destroy(&q);
}

TEST(query_map_ctx_is_string, "IN LIST count(*) where y['ykey'] is 'yvalue'")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	as_string ykey;
	as_string_init(&ykey, "ykey", false);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)&ykey);

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where_with_ctx(&q, "y", &ctx, as_string_equals("yvalue"));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	if (err.code != AEROSPIKE_OK) {
		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(count, 15);

	as_cdt_ctx_destroy(&ctx);
	as_query_destroy(&q);
}

TEST(query_blob_index, "query blob index")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	uint8_t blob[4];
	*((uint32_t*)blob) = 50003;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "blob");
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "blob", as_blob_equals(blob, 4, false));
	
	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	assert_int_eq(err.code, 0);
	assert_int_eq(count, 1);

	as_query_destroy(&q);
}

TEST(query_blob_list_index, "query blob list index")
{
	as_error err;
	as_error_reset(&err);

	uint32_t count = 0;

	uint8_t blob[4];
	*((uint32_t*)blob) = 50003;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_select_inita(&q, 1);
	as_query_select(&q, "blob_list");

	as_query_where_inita(&q, 1);
	as_query_where(&q, "blob_list", as_blob_contains(LIST, &blob, 4, false));
	
	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &count);

	assert_int_eq(err.code, 0);
	assert_int_eq(count, 1);

	as_query_destroy(&q);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(query_foreach, "aerospike_query_foreach tests")
{
	suite_before(before);
	suite_after(after);

	// find out storage type of namespace
	char namespace_storage[128];

	get_info_field(NAMESPACE_INFO, "storage-engine", namespace_storage, sizeof(namespace_storage));

	if (strcmp(namespace_storage, "device") == 0 || strcmp(namespace_storage, "file") == 0) {
		namespace_has_persistence = true;
	}

	if (strcmp(namespace_storage, "memory") == 0) {
		namespace_in_memory = true;

		char shadow[128];
		shadow[0] = '\0';

		get_info_field(NAMESPACE_INFO, "storage-engine.file[0]", shadow, sizeof(shadow));

		if (shadow[0] != '\0') {
			namespace_has_persistence = true;
		}
		else {
			get_info_field(NAMESPACE_INFO, "storage-engine.device[0]", shadow, sizeof(shadow));

			if (shadow[0] != '\0') {
				namespace_has_persistence = true;
			}
		}
	}

	if (! namespace_in_memory) {
		char namespace_dim[128];

		get_info_field(NAMESPACE_INFO, "storage-engine.data-in-memory",
			namespace_dim, sizeof(namespace_dim));

		if (strcmp(namespace_dim, "true") == 0) {
			namespace_in_memory = true;
		}
	}

	suite_add(query_foreach_1);
	suite_add(query_foreach_2);
	suite_add(query_foreach_3);
	suite_add(query_foreach_4);
	suite_add(query_foreach_5);
	suite_add(query_foreach_6);
	suite_add(query_foreach_7);
	suite_add(query_foreach_8);
	suite_add(query_foreach_9);
	suite_add(query_with_range_filter);
	suite_add(query_with_equality_filter);
	suite_add(query_with_rec_size_filter);
	suite_add(query_with_rec_device_size_filter);
	suite_add(query_with_rec_memory_size_filter);
	suite_add(query_intermittent_bin_filter);
	suite_add(scan_with_rec_last_update_filter);
	suite_add(scan_with_rec_last_update_filter_less);
	suite_add(scan_with_rec_void_time_filter_1);
	suite_add(scan_with_rec_void_time_filter_2);
	suite_add(scan_with_rec_void_time_filter_3);
	suite_add(scan_with_rec_digest_modulo_filter);
	suite_add(query_with_or_filter);
	suite_add(query_with_not_filter);
	suite_add(query_with_regex_filter);
	suite_add(query_with_regex_filter_icase);
	suite_add(query_with_list_filter);
	suite_add(query_with_mapkey_filter);
	suite_add(query_with_mapval_filter);
	suite_add(query_quit_early);
	suite_add(query_agg_quit_early);
	suite_add(query_filter_map_bytes);
	suite_add(query_foreach_nullset);
	suite_add(query_foreach_int_with_double_bin);
	suite_add(query_list_ctx_is_string);
	suite_add(query_map_ctx_is_string);
	suite_add(query_blob_index);
	suite_add(query_blob_list_index);
}
