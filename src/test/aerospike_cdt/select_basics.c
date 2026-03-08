/*
 * Copyright 2026 Aerospike, Inc.
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
#include <aerospike/as_arraylist.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_double.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_map.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_vector.h>

#include "../test.h"
#include "../util/log_helper.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "testselect"
#define BIN_NAME "m"

/******************************************************************************
 * HELPERS
 *****************************************************************************/

static as_status
write_int_str_map(int64_t rec_id, int64_t* keys, const char** vals,
		uint32_t count)
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, rec_id);

	as_map* m = (as_map*)as_hashmap_new(count);

	for (uint32_t i = 0; i < count; i++) {
		as_map_set(m, (as_val*)as_integer_new(keys[i]),
				(as_val*)as_string_new((char*)vals[i], false));
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN_NAME, m);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	as_record_destroy(&rec);
	return status;
}

/******************************************************************************
 * KEY_LIST TESTS
 *****************************************************************************/

TEST(select_key_list_basic, "KEY_LIST select keys [1, 3] from {1:a, 2:b, 3:c}")
{
	int64_t keys[] = {1, 2, 3};
	const char* vals[] = {"a", "b", "c"};

	assert_int_eq(write_int_str_map(100, keys, vals, 3), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 100);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);

	as_arraylist sel_keys;
	as_arraylist_init(&sel_keys, 2, 0);
	as_arraylist_append_int64(&sel_keys, 1);
	as_arraylist_append_int64(&sel_keys, 3);
	as_cdt_ctx_add_map_key_in_list(&ctx, (as_list*)&sel_keys);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_map* result = as_record_get_map(rec, BIN_NAME);
	assert_not_null(result);
	assert_int_eq(as_map_size(result), 2);

	as_integer k1;
	as_integer_init(&k1, 1);
	as_val* v1 = as_map_get(result, (as_val*)&k1);
	assert_not_null(v1);
	assert_string_eq(as_string_get((as_string*)v1), "a");

	as_integer k3;
	as_integer_init(&k3, 3);
	as_val* v3 = as_map_get(result, (as_val*)&k3);
	assert_not_null(v3);
	assert_string_eq(as_string_get((as_string*)v3), "c");

	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);
}

TEST(select_key_list_single, "KEY_LIST select single key [2]")
{
	int64_t keys[] = {10, 20, 30};
	const char* vals[] = {"hello", "world", "test"};

	assert_int_eq(write_int_str_map(101, keys, vals, 3), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 101);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);

	as_arraylist sel_keys;
	as_arraylist_init(&sel_keys, 1, 0);
	as_arraylist_append_int64(&sel_keys, 20);
	as_cdt_ctx_add_map_key_in_list(&ctx, (as_list*)&sel_keys);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_map* result = as_record_get_map(rec, BIN_NAME);
	assert_not_null(result);
	assert_int_eq(as_map_size(result), 1);

	as_integer k;
	as_integer_init(&k, 20);
	as_val* v = as_map_get(result, (as_val*)&k);
	assert_not_null(v);
	assert_string_eq(as_string_get((as_string*)v), "world");

	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);
}

TEST(select_key_list_no_match, "KEY_LIST select non-existent key [99]")
{
	int64_t keys[] = {1, 2, 3};
	const char* vals[] = {"a", "b", "c"};

	assert_int_eq(write_int_str_map(102, keys, vals, 3), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 102);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);

	as_arraylist sel_keys;
	as_arraylist_init(&sel_keys, 1, 0);
	as_arraylist_append_int64(&sel_keys, 99);
	as_cdt_ctx_add_map_key_in_list(&ctx, (as_list*)&sel_keys);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_map* result = as_record_get_map(rec, BIN_NAME);
	assert_not_null(result);
	assert_int_eq(as_map_size(result), 0);

	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);
}

/******************************************************************************
 * CTX_AND POSITIVE TESTS
 *****************************************************************************/

TEST(select_and_key_list_filter, "KEY_LIST [1,2,3] + AND filter value != b")
{
	int64_t keys[] = {1, 2, 3};
	const char* vals[] = {"a", "b", "c"};

	assert_int_eq(write_int_str_map(103, keys, vals, 3), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 103);

	// AND filter: keep entries where value != "b".
	as_exp_build(filter, as_exp_cmp_ne(
			as_exp_loopvar_str(AS_EXP_LOOPVAR_VALUE),
			as_exp_str("b")));
	assert_not_null(filter);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 2);

	as_arraylist sel_keys;
	as_arraylist_init(&sel_keys, 3, 0);
	as_arraylist_append_int64(&sel_keys, 1);
	as_arraylist_append_int64(&sel_keys, 2);
	as_arraylist_append_int64(&sel_keys, 3);
	as_cdt_ctx_add_map_key_in_list(&ctx, (as_list*)&sel_keys);
	as_cdt_ctx_add_same_level_filter(&ctx, filter);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_map* result = as_record_get_map(rec, BIN_NAME);
	assert_not_null(result);
	assert_int_eq(as_map_size(result), 2);

	as_integer k1;
	as_integer_init(&k1, 1);
	as_val* v1 = as_map_get(result, (as_val*)&k1);
	assert_not_null(v1);
	assert_string_eq(as_string_get((as_string*)v1), "a");

	as_integer k3;
	as_integer_init(&k3, 3);
	as_val* v3 = as_map_get(result, (as_val*)&k3);
	assert_not_null(v3);
	assert_string_eq(as_string_get((as_string*)v3), "c");

	// key=2 ("b") should be filtered out.
	as_integer k2;
	as_integer_init(&k2, 2);
	as_val* v2 = as_map_get(result, (as_val*)&k2);
	assert_null(v2);

	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(filter);
}

TEST(select_and_index_filter, "MAP_INDEX 0 + AND filter key >= 10")
{
	int64_t keys[] = {10, 20, 30};
	const char* vals[] = {"x", "y", "z"};

	assert_int_eq(write_int_str_map(104, keys, vals, 3), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 104);

	// AND filter: key >= 10 (passes the INDEX 0 entry which is key=10).
	as_exp_build(filter, as_exp_cmp_ge(
			as_exp_loopvar_int(AS_EXP_LOOPVAR_KEY),
			as_exp_int(10)));
	assert_not_null(filter);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_map_index(&ctx, 0);
	as_cdt_ctx_add_same_level_filter(&ctx, filter);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_map* result = as_record_get_map(rec, BIN_NAME);
	assert_not_null(result);
	assert_int_eq(as_map_size(result), 1);

	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(filter);
}

/******************************************************************************
 * CTX_AND NEGATIVE TESTS
 *****************************************************************************/

TEST(select_and_on_level0, "AND|EXP as first ctx level — expect error")
{
	int64_t keys[] = {1};
	const char* vals[] = {"a"};

	assert_int_eq(write_int_str_map(105, keys, vals, 1), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 105);

	as_exp_build(filter, as_exp_cmp_eq(
			as_exp_int(1), as_exp_int(1)));
	assert_not_null(filter);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_same_level_filter(&ctx, filter);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_ne(status, AEROSPIKE_OK);

	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(filter);
}

TEST(select_and_duplicate, "KEY_LIST + AND + AND — expect error")
{
	int64_t keys[] = {1};
	const char* vals[] = {"a"};

	assert_int_eq(write_int_str_map(106, keys, vals, 1), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 106);

	as_exp_build(filter1, as_exp_cmp_eq(
			as_exp_int(1), as_exp_int(1)));
	assert_not_null(filter1);

	as_exp_build(filter2, as_exp_cmp_eq(
			as_exp_int(2), as_exp_int(2)));
	assert_not_null(filter2);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 3);

	as_arraylist sel_keys;
	as_arraylist_init(&sel_keys, 1, 0);
	as_arraylist_append_int64(&sel_keys, 1);
	as_cdt_ctx_add_map_key_in_list(&ctx, (as_list*)&sel_keys);
	as_cdt_ctx_add_same_level_filter(&ctx, filter1);
	as_cdt_ctx_add_same_level_filter(&ctx, filter2);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_ne(status, AEROSPIKE_OK);

	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(select_and_on_exp_base, "EXP(filter) + AND|EXP — expect error")
{
	int64_t keys[] = {1};
	const char* vals[] = {"a"};

	assert_int_eq(write_int_str_map(107, keys, vals, 1), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 107);

	as_exp_build(base_filter, as_exp_cmp_eq(
			as_exp_int(1), as_exp_int(1)));
	assert_not_null(base_filter);

	as_exp_build(and_filter, as_exp_cmp_eq(
			as_exp_int(2), as_exp_int(2)));
	assert_not_null(and_filter);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_all_children_with_filter(&ctx, base_filter);
	as_cdt_ctx_add_same_level_filter(&ctx, and_filter);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_ne(status, AEROSPIKE_OK);

	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(base_filter);
	as_exp_destroy(and_filter);
}

TEST(select_and_invalid_type, "AND|KEY (not AND|EXP) — expect error")
{
	int64_t keys[] = {1};
	const char* vals[] = {"a"};

	assert_int_eq(write_int_str_map(108, keys, vals, 1), AEROSPIKE_OK);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 108);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 2);

	as_arraylist sel_keys;
	as_arraylist_init(&sel_keys, 1, 0);
	as_arraylist_append_int64(&sel_keys, 1);
	as_cdt_ctx_add_map_key_in_list(&ctx, (as_list*)&sel_keys);

	// Manually construct AND|MAP_KEY — the API only provides AND|EXP.
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_AND | AS_CDT_CTX_MAP_KEY;
	item.val.pval = (as_val*)as_integer_new(1);
	as_vector_append(&ctx.list, &item);

	as_error err;
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, &err, NULL, &rkey, &ops,
			&rec);
	assert_int_ne(status, AEROSPIKE_OK);

	as_operations_destroy(&ops);
	if (rec) {
		as_record_destroy(rec);
	}
	as_cdt_ctx_destroy(&ctx);
}

/******************************************************************************
 * REAL-WORLD AND_EXP TEST
 *****************************************************************************/

static as_map*
make_rate_map(int64_t id, int64_t a, double beta, bool c, const char* d)
{
	as_map* m = (as_map*)as_hashmap_new(5);
	as_stringmap_set_int64(m, "Id", id);
	as_stringmap_set_int64(m, "a", a);
	as_stringmap_set_double(m, "beta", beta);
	as_stringmap_set(m, "c", (as_val*)as_boolean_new(c));
	as_stringmap_set_str(m, "d", d);
	return m;
}

static as_map*
make_room_map(as_list* rates, int64_t e, bool is_deleted, int64_t time_val)
{
	as_map* m = (as_map*)as_hashmap_new(4);
	as_stringmap_set_list(m, "rates", rates);
	as_stringmap_set_int64(m, "e", e);
	as_stringmap_set(m, "isDeleted", (as_val*)as_boolean_new(is_deleted));
	as_stringmap_set_int64(m, "time", time_val);
	return m;
}

TEST(select_and_room_filter,
		"KEY_LIST + AND_EXP(room filter) + allChildren + filter(beta>0)")
{
	// Build room 10001: 2 rates, isDeleted=false, time passes threshold.
	as_arraylist* rates1 = as_arraylist_new(2, 0);
	as_arraylist_append(rates1,
			(as_val*)make_rate_map(1, 2, 3.0, true, "data"));
	as_arraylist_append(rates1,
			(as_val*)make_rate_map(2, 2, 0.0, true, "data blob"));
	as_map* room1 = make_room_map((as_list*)rates1, 4, false, 1795647328);

	// Build room 10002: isDeleted=true.
	as_arraylist* rates2 = as_arraylist_new(1, 0);
	as_arraylist_append(rates2,
			(as_val*)make_rate_map(1, 2, 3.0, true, "data blob"));
	as_map* room2 = make_room_map((as_list*)rates2, 4, true, 1795647328);

	// Build room 10003: isDeleted=false, time below threshold.
	as_arraylist* rates3 = as_arraylist_new(1, 0);
	as_arraylist_append(rates3,
			(as_val*)make_rate_map(1, 2, 3.0, true, "data blob"));
	as_map* room3 = make_room_map((as_list*)rates3, 4, false, 1764140184);

	// Outer map: "10001" -> room1, "10002" -> room2, "10003" -> room3.
	as_map* outer = (as_map*)as_hashmap_new(3);
	as_stringmap_set_map(outer, "10001", room1);
	as_stringmap_set_map(outer, "10002", room2);
	as_stringmap_set_map(outer, "10003", room3);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 200);

	as_record wrec;
	as_record_inita(&wrec, 1);
	as_record_set_map(&wrec, BIN_NAME, outer);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &rkey, &wrec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&wrec);

	// Room filter: isDeleted == false AND time > 1780000000.
	as_exp_build(room_filter,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_map_get_by_key(NULL, AS_MAP_RETURN_VALUE,
					AS_EXP_TYPE_BOOL, as_exp_str("isDeleted"),
					as_exp_loopvar_map(AS_EXP_LOOPVAR_VALUE)),
				as_exp_bool(false)),
			as_exp_cmp_gt(
				as_exp_map_get_by_key(NULL, AS_MAP_RETURN_VALUE,
					AS_EXP_TYPE_INT, as_exp_str("time"),
					as_exp_loopvar_map(AS_EXP_LOOPVAR_VALUE)),
				as_exp_int(1780000000))));
	assert_not_null(room_filter);

	// Beta filter: beta > 0.0.
	as_exp_build(beta_filter,
		as_exp_cmp_gt(
			as_exp_map_get_by_key(NULL, AS_MAP_RETURN_VALUE,
				AS_EXP_TYPE_FLOAT, as_exp_str("beta"),
				as_exp_loopvar_map(AS_EXP_LOOPVAR_VALUE)),
			as_exp_float(0.0)));
	assert_not_null(beta_filter);

	as_arraylist room_ids;
	as_arraylist_init(&room_ids, 2, 0);
	as_arraylist_append_str(&room_ids, "10001");
	as_arraylist_append_str(&room_ids, "10003");

	// CTX: KEY_LIST + AND_EXP(room) + allChildren + filter(beta).
	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 4);
	as_cdt_ctx_add_map_key_in_list(&ctx, (as_list*)&room_ids);
	as_cdt_ctx_add_same_level_filter(&ctx, room_filter);
	as_cdt_ctx_add_all_children(&ctx);
	as_cdt_ctx_add_all_children_with_filter(&ctx, beta_filter);

	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_select_by_path(&err, &ops, BIN_NAME, &ctx,
			AS_EXP_PATH_SELECT_MATCHING_TREE | AS_EXP_PATH_SELECT_NO_FAIL);

	as_record* rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_map* result = as_record_get_map(rec, BIN_NAME);
	assert_not_null(result);

	// "10001" should be present (isDeleted=false, time > threshold).
	as_string k1;
	as_string_init(&k1, "10001", false);
	as_val* v1 = as_map_get(result, (as_val*)&k1);
	assert_not_null(v1);

	// "10003" should be filtered out (time < threshold).
	as_string k3;
	as_string_init(&k3, "10003", false);
	as_val* v3 = as_map_get(result, (as_val*)&k3);
	assert_null(v3);

	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(room_filter);
	as_exp_destroy(beta_filter);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(select_basics, "CDT select basics")
{
	suite_add(select_key_list_basic);
	suite_add(select_key_list_single);
	suite_add(select_key_list_no_match);
	suite_add(select_and_key_list_filter);
	suite_add(select_and_index_filter);
	suite_add(select_and_on_level0);
	suite_add(select_and_duplicate);
	suite_add(select_and_on_exp_base);
	suite_add(select_and_invalid_type);
	suite_add(select_and_room_filter);
}
