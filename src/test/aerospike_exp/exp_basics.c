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

/*
 * Port of Java-style exp read:
 *   CdtExp.selectByPath(LIST, SelectFlags.VALUE,
 *       mapBin(binName),
 *       CTX.mapKeysIn("room101", "room202"),
 *       CTX.allChildren(),
 *       CTX.mapKey(Value.get("guest")))
 *   client.operate(..., ExpOperation.read("guests", readExp, DEFAULT))
 */

 #include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>

#include "../test.h"
#include "../util/log_helper.h"


/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

#define NAMESPACE "test"
#define SET "testexpbasics"
/* Java: mapBin(binName) + ExpOperation.read("guests", ...) — use same names when binName is "guests". */
#define BIN_MAP "guests"
#define BIN_RESULT "guests"

static bool
list_contains_str(as_list* list, const char* want)
{
	uint32_t n = as_list_size(list);

	for (uint32_t i = 0; i < n; i++) {
		as_val* v = as_list_get(list, i);

		if (v && as_val_type(v) == AS_STRING) {
			if (strcmp(as_string_get((as_string*)v), want) == 0) {
				return true;
			}
		}
	}
	return false;
}

TEST(exp_read_select_by_path_map_keys_in, "exp read: selectByPath mapKeysIn + allChildren + mapKey")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 94001);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	// guests[room101].guest = "alice", guests[room202].guest = "bob";
	// guests[other].guest = "carl" — excluded by mapKeysIn.
	as_hashmap inner101;
	as_hashmap_init(&inner101, 2);
	as_hashmap_set(&inner101, (as_val*)as_string_new("guest", false),
			(as_val*)as_string_new("alice", false));

	as_hashmap inner202;
	as_hashmap_init(&inner202, 2);
	as_hashmap_set(&inner202, (as_val*)as_string_new("guest", false),
			(as_val*)as_string_new("bob", false));

	as_hashmap inner_other;
	as_hashmap_init(&inner_other, 2);
	as_hashmap_set(&inner_other, (as_val*)as_string_new("guest", false),
			(as_val*)as_string_new("carl", false));

	as_hashmap top;
	as_hashmap_init(&top, 4);
	as_hashmap_set(&top, (as_val*)as_string_new("room101", false),
			(as_val*)&inner101);
	as_hashmap_set(&top, (as_val*)as_string_new("room202", false),
			(as_val*)&inner202);
	as_hashmap_set(&top, (as_val*)as_string_new("other", false),
			(as_val*)&inner_other);

	as_record* rec = as_record_new(1);
	as_record_set_map(rec, BIN_MAP, (as_map*)&top);

	status = aerospike_key_put(as, &err, NULL, &key, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	test_dump_record(rec, true);
	as_record_destroy(rec);
	rec = NULL;

	as_arraylist room_keys;
	as_arraylist_init(&room_keys, 2, 0);
	as_arraylist_append_str(&room_keys, "room101");
	as_arraylist_append_str(&room_keys, "room202");

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 3);
	as_cdt_ctx_add_map_keys_in(&ctx, (as_list*)&room_keys);
//	as_cdt_ctx_add_all_children(&ctx);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)as_string_new("guest", false));

	as_exp_build(read_exp,
		as_exp_select_by_path(&ctx, AS_EXP_TYPE_LIST,
			AS_EXP_PATH_SELECT_VALUE,
			as_exp_bin_map(BIN_MAP)));
	assert_not_null(read_exp);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, BIN_RESULT, read_exp, AS_EXP_READ_DEFAULT);

	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_not_null(rec);

	as_list* guests = as_record_get_list(rec, BIN_RESULT);
	assert_not_null(guests);
	assert_int_eq(as_list_size(guests), 2);
	assert_true(list_contains_str(guests, "alice"));
	assert_true(list_contains_str(guests, "bob"));

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(read_exp);
	as_cdt_ctx_destroy(&ctx);
}

TEST(exp_bin_exists_native, "exp read: native bin_exists (op 83)")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 94010);

	as_error err;
	aerospike_key_remove(as, &err, NULL, &key);

	as_record* rec = as_record_new(1);
	as_record_set_int64(rec, "x", 42);
	as_status status = aerospike_key_put(as, &err, NULL, &key, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_exp_build(e_yes, as_exp_bin_exists("x"));
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, "r", e_yes, AS_EXP_READ_DEFAULT);
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(as_record_get_bool(rec, "r"));
	as_record_destroy(rec);
	rec = NULL;
	as_operations_destroy(&ops);
	as_exp_destroy(e_yes);

	as_exp_build(e_no, as_exp_bin_exists("nope"));
	as_operations ops2;
	as_operations_inita(&ops2, 1);
	as_operations_exp_read(&ops2, "r", e_no, AS_EXP_READ_DEFAULT);
	status = aerospike_key_operate(as, &err, NULL, &key, &ops2, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_true(as_record_get_bool(rec, "r") == false);
	as_record_destroy(rec);
	as_operations_destroy(&ops2);
	as_exp_destroy(e_no);
}

TEST(exp_size_polymorphic, "exp read: polymorphic size (op 0xfd) of a map")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 94011);

	as_error err;
	aerospike_key_remove(as, &err, NULL, &key);

	as_hashmap m;
	as_hashmap_init(&m, 3);
	as_hashmap_set(&m, (as_val*)as_string_new("a", false), (as_val*)as_integer_new(1));
	as_hashmap_set(&m, (as_val*)as_string_new("b", false), (as_val*)as_integer_new(2));
	as_hashmap_set(&m, (as_val*)as_string_new("c", false), (as_val*)as_integer_new(3));

	as_record* rec = as_record_new(1);
	as_record_set_map(rec, "m", (as_map*)&m);
	as_status status = aerospike_key_put(as, &err, NULL, &key, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_exp_build(e, as_exp_size(NULL, as_exp_bin_map("m")));
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, "r", e, AS_EXP_READ_DEFAULT);
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(as_record_get_int64(rec, "r", -1), 3);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(e);
}

TEST(exp_to_string_int, "exp read: toString(42) == \"42\"")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 94012);

	as_error err;
	aerospike_key_remove(as, &err, NULL, &key);

	as_record* rec = as_record_new(1);
	as_record_set_int64(rec, "x", 1);
	as_status status = aerospike_key_put(as, &err, NULL, &key, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_exp_build(e, as_exp_to_string(as_exp_int(42)));
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, "r", e, AS_EXP_READ_DEFAULT);
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_string_eq(as_record_get_str(rec, "r"), "42");

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(e);
}

TEST(exp_list_append_persist, "exp write: list append with ordered+persist policy")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 94013);

	as_error err;
	aerospike_key_remove(as, &err, NULL, &key);

	as_record* rec = as_record_new(1);
	as_arraylist l;
	as_arraylist_init(&l, 1, 0);
	as_arraylist_append_int64(&l, 1);
	as_record_set_list(rec, "l", (as_list*)&l);
	as_status status = aerospike_key_put(as, &err, NULL, &key, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_list_policy pol;
	as_list_policy_set_all(&pol, AS_LIST_ORDERED, AS_LIST_WRITE_DEFAULT, true);

	as_exp_build(e, as_exp_list_append(NULL, &pol, as_exp_int(5), as_exp_bin_list("l")));
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, "l", e, AS_EXP_WRITE_DEFAULT);
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(e);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(exp_basics, "Expression basics (exp read + select_by_path)")
{
	suite_add(exp_read_select_by_path_map_keys_in);
	suite_add(exp_bin_exists_native);
	suite_add(exp_size_polymorphic);
	suite_add(exp_to_string_int);
	suite_add(exp_list_append_persist);
}
