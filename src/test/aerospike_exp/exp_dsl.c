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
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "testexpdsl"
#define BIN_DSL "dslbin"

/******************************************************************************
 * HELPERS
 *****************************************************************************/

static as_status
put_one_int(int64_t rec_id, int64_t val)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, rec_id);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN_DSL, val);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	as_record_destroy(&rec);
	return status;
}

static as_status
put_map_int_str3(int64_t rec_id)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, rec_id);

	as_hashmap map;
	as_hashmap_init(&map, 4);
	as_hashmap_set(&map, (as_val*)as_integer_new(1), (as_val*)as_string_new("a", false));
	as_hashmap_set(&map, (as_val*)as_integer_new(2), (as_val*)as_string_new("b", false));
	as_hashmap_set(&map, (as_val*)as_integer_new(3), (as_val*)as_string_new("c", false));

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN_DSL, (as_map*)&map);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	as_record_destroy(&rec);
	return status;
}

static as_status
put_map_keys_10_20_30(int64_t rec_id)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, rec_id);

	as_hashmap map;
	as_hashmap_init(&map, 4);
	as_hashmap_set(&map, (as_val*)as_integer_new(10), (as_val*)as_string_new("x", false));
	as_hashmap_set(&map, (as_val*)as_integer_new(20), (as_val*)as_string_new("y", false));
	as_hashmap_set(&map, (as_val*)as_integer_new(30), (as_val*)as_string_new("z", false));

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN_DSL, (as_map*)&map);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	as_record_destroy(&rec);
	return status;
}

static as_status
put_list_indexed_0_8(int64_t rec_id)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, rec_id);

	as_arraylist list;
	as_arraylist_init(&list, 16, 16);
	for (int64_t i = 0; i < 9; i++) {
		as_arraylist_append_int64(&list, i);
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_list(&rec, BIN_DSL, (as_list*)&list);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	as_record_destroy(&rec);
	return status;
}

static as_status
put_list_membership(int64_t rec_id)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, rec_id);

	as_arraylist list;
	as_arraylist_init(&list, 8, 8);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, 4);
	as_arraylist_append_int64(&list, 5);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_list(&rec, BIN_DSL, (as_list*)&list);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	as_record_destroy(&rec);
	return status;
}

static as_status
put_list_nested(int64_t rec_id)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, rec_id);

	as_arraylist inner0;
	as_arraylist_init(&inner0, 4, 4);
	as_arraylist_append_int64(&inner0, 9);
	as_arraylist_append_int64(&inner0, 8);

	as_arraylist inner1;
	as_arraylist_init(&inner1, 4, 4);
	as_arraylist_append_int64(&inner1, 3);
	as_arraylist_append_int64(&inner1, 4);

	as_arraylist outer;
	as_arraylist_init(&outer, 4, 4);
	as_arraylist_append_list(&outer, (as_list*)&inner0);
	as_arraylist_append_list(&outer, (as_list*)&inner1);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_list(&rec, BIN_DSL, (as_list*)&outer);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	as_record_destroy(&rec);
	return status;
}

/******************************************************************************
 * READ POLICY + as_exp_build_dsl (server parses DSL from filter field).
 * DSL shapes follow dsl.md / enterprise test_dsl.cc.
 *****************************************************************************/

TEST(exp_dsl_filter_read_literal_true, "read filter: DSL 1 == 1")
{
	assert_int_eq(put_one_int(9301, 1), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9301);

	as_exp_build_dsl(filt, "1 == 1");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	assert_int_eq(as_record_get_int64(prec, BIN_DSL, -1), 1);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_filter_read_literal_false, "read filter: DSL 1 == 2 -> FILTERED_OUT")
{
	assert_int_eq(put_one_int(9302, 1), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9302);

	as_exp_build_dsl(filt, "1 == 2");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	if (prec != NULL) {
		as_record_destroy(prec);
	}
	as_exp_destroy(filt);
}

TEST(exp_dsl_filter_read_bin_eq, "read filter: DSL $.dslbin == 42")
{
	assert_int_eq(put_one_int(9303, 42), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9303);

	as_exp_build_dsl(filt, "$.dslbin == 42");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	assert_int_eq(as_record_get_int64(prec, BIN_DSL, -1), 42);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_filter_read_bin_no_match, "read filter: DSL $.dslbin == 43 -> FILTERED_OUT")
{
	assert_int_eq(put_one_int(9304, 42), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9304);

	as_exp_build_dsl(filt, "$.dslbin == 43");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	if (prec != NULL) {
		as_record_destroy(prec);
	}
	as_exp_destroy(filt);
}

TEST(exp_dsl_filter_read_logic_and, "read filter: DSL 1 == 1 and 2 == 2")
{
	assert_int_eq(put_one_int(9305, 1), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9305);

	as_exp_build_dsl(filt, "1 == 1 and 2 == 2");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_filter_write_skipped, "write filter: DSL false skips put (record unchanged)")
{
	assert_int_eq(put_one_int(9306, 7), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9306);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN_DSL, 100);

	as_exp_build_dsl(filt, "$.dslbin == 99");
	assert_not_null(filt);

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.filter_exp = filt;

	as_error err;
	as_status rc = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	as_record_destroy(&rec);
	as_exp_destroy(filt);

	as_record* prec = NULL;
	rc = aerospike_key_get(as, &err, NULL, &key, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	assert_int_eq(as_record_get_int64(prec, BIN_DSL, -1), 7);
	as_record_destroy(prec);
}

/******************************************************************************
 * CDT path filters — DSL equivalents of richer list/map expression tests
 * (see dsl.md path examples and list_basics/map_basics suites).
 *****************************************************************************/

TEST(exp_dsl_map_top_count, "map: $.dslbin.{}.count() == 3 (cf map_keys size / map size)")
{
	assert_int_eq(put_map_int_str3(9310), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9310);

	as_exp_build_dsl(filt, "$.dslbin.{}.count() == 3");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_map_keylist_submap_count, "map: $.dslbin.{10,20}.{}.count() == 2 (getByKeyList subset)")
{
	assert_int_eq(put_map_keys_10_20_30(9311), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9311);

	as_exp_build_dsl(filt, "$.dslbin.{10,20}.{}.count() == 2");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_map_key_exists, "map: $.dslbin.20.exists() (cf in_list on map_keys for key 20)")
{
	assert_int_eq(put_map_keys_10_20_30(9312), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9312);

	as_exp_build_dsl(filt, "$.dslbin.20.exists()");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_map_key_missing, "map: $.dslbin.99.exists() -> FILTERED_OUT (cf in_list miss on keys)")
{
	assert_int_eq(put_map_keys_10_20_30(9313), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9313);

	as_exp_build_dsl(filt, "$.dslbin.99.exists()");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	if (prec != NULL) {
		as_record_destroy(prec);
	}
	as_exp_destroy(filt);
}

TEST(exp_dsl_map_value_by_key, "map: $.dslbin.2 == \"b\" (typed get by int key)")
{
	assert_int_eq(put_map_int_str3(9314), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9314);

	as_exp_build_dsl(filt, "$.dslbin.2 == \"b\"");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_list_index_range_count, "list: $.dslbin.[3:7].count() > 2 (index range COUNT)")
{
	assert_int_eq(put_list_indexed_0_8(9315), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9315);

	as_exp_build_dsl(filt, "$.dslbin.[3:7].count() > 2");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_list_membership_in, "list: 3 in $.dslbin (cf Exp IN / getByValue EXISTS)")
{
	assert_int_eq(put_list_membership(9316), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9316);

	as_exp_build_dsl(filt, "3 in $.dslbin");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

TEST(exp_dsl_list_nested_index, "list: $.dslbin.[1].[0] == 3 (nested list path)")
{
	assert_int_eq(put_list_nested(9317), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 9317);

	as_exp_build_dsl(filt, "$.dslbin.[1].[0] == 3");
	assert_not_null(filt);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filt;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &key, &prec);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(prec);
	as_record_destroy(prec);
	as_exp_destroy(filt);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(exp_dsl, "Expression DSL filters (as_exp_build_dsl)")
{
	suite_add(exp_dsl_filter_read_literal_true);
	suite_add(exp_dsl_filter_read_literal_false);
	suite_add(exp_dsl_filter_read_bin_eq);
	suite_add(exp_dsl_filter_read_bin_no_match);
	suite_add(exp_dsl_filter_read_logic_and);
	suite_add(exp_dsl_filter_write_skipped);
	suite_add(exp_dsl_map_top_count);
	suite_add(exp_dsl_map_keylist_submap_count);
	suite_add(exp_dsl_map_key_exists);
	suite_add(exp_dsl_map_key_missing);
	suite_add(exp_dsl_map_value_by_key);
	suite_add(exp_dsl_list_index_range_count);
	suite_add(exp_dsl_list_membership_in);
	suite_add(exp_dsl_list_nested_index);
}
