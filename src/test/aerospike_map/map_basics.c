/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_map.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_msgpack_ext.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_random.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike *as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "testmap"
#define BIN_NAME "testmap"
#define RAND_STR_LEN (26 + 26 + 1)

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static void
rand_str(as_string* s)
{
	char buf[RAND_STR_LEN + 1];
	as_random_get_str(buf, as_random_get_uint32() % RAND_STR_LEN);
	as_string_init(s, strdup(buf), true);
}

static void
example_dump_bin(const as_bin* p_bin)
{
	if (! p_bin) {
		info("  null as_bin object");
		return;
	}

	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	info("  %s : %s", as_bin_get_name(p_bin), val_as_str);

	free(val_as_str);
}

void
example_dump_record(const as_record* p_rec)
{
	if (! p_rec) {
		info("  null as_record object");
		return;
	}

	if (p_rec->key.valuep) {
		char* key_val_as_str = as_val_tostring(p_rec->key.valuep);

		info("  key: %s", key_val_as_str);

		free(key_val_as_str);
	}

	uint16_t num_bins = as_record_numbins(p_rec);

	info("  generation %u, ttl %u, %u bin%s", p_rec->gen, p_rec->ttl, num_bins,
		num_bins == 0 ? "s" : (num_bins == 1 ? ":" : "s:"));

	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

	while (as_record_iterator_has_next(&it)) {
		example_dump_bin(as_record_iterator_next(&it));
	}

	as_record_iterator_destroy(&it);
}

static bool
map_cmp(as_list* list, as_map* map)
{
	uint32_t n = as_list_size(list);

	if (as_map_size(map) != as_list_size(list) / 2) {
		info("list=%u map=%u", as_list_size(list), as_map_size(map));
		return false;
	}

	for (uint32_t i = 0; i < n;) {
		as_val* k = as_list_get(list, i++);
		as_val* v = as_list_get(list, i++);
		as_val* v1 = as_map_get(map, k);

		if (as_val_cmp(v, v1) != MSGPACK_COMPARE_EQUAL) {
			char* kk = as_val_val_tostring(k);
			char* s = as_val_val_tostring(v);
			char* s1 = as_val_val_tostring(v1);
			info("k=%s s=%s s1=%s", kk, s, s1);
			cf_free(kk);
			cf_free(s);
			cf_free(s1);
			return false;
		}
	}

	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(map_put, "Map put operations")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 1);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_map_policy put_mode;
	as_map_policy_init(&put_mode);

	as_map_policy add_mode;
	as_map_policy_set(&add_mode, AS_MAP_UNORDERED, AS_MAP_CREATE_ONLY);

	as_map_policy update_mode;
	as_map_policy_set(&update_mode, AS_MAP_UNORDERED, AS_MAP_UPDATE_ONLY);

	as_map_policy ordered_update_mode;
	as_map_policy_set(&ordered_update_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE_ONLY);

	as_operations ops;
	as_operations_init(&ops, 5);

	as_integer mkey;
	as_integer mval;
	as_integer_init(&mkey, 11);
	as_integer_init(&mval, 789);
	as_operations_add_map_put(&ops, BIN_NAME, &put_mode, (as_val*)&mkey, (as_val*)&mval);
	as_integer_init(&mkey, 10);
	as_integer_init(&mval, 999);
	as_operations_add_map_put(&ops, BIN_NAME, &put_mode, (as_val*)&mkey, (as_val*)&mval);
	as_integer_init(&mkey, 12);
	as_integer_init(&mval, 500);
	as_operations_add_map_put(&ops, BIN_NAME, &add_mode, (as_val*)&mkey, (as_val*)&mval);
	as_integer_init(&mkey, 15);
	as_integer_init(&mval, 1000);
	as_operations_add_map_put(&ops, BIN_NAME, &add_mode, (as_val*)&mkey, (as_val*)&mval);
	as_integer_init(&mkey, 10);
	as_integer_init(&mval, 1);
	// Ordered type should be ignored since map has already been created in first put().
	as_operations_add_map_put(&ops, BIN_NAME, &ordered_update_mode, (as_val*)&mkey, (as_val*)&mval);

	as_record* rec = 0;

	// Calling put() multiple times performs poorly because the server makes
	// a copy of the map for each call, but we still need to test it.
	// put_items() should be used instead for best performance.
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;
	int64_t v = results[i++].valuep->integer.value;
	assert_int_eq(v, 1);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 2);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 3);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 4);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 4);

	as_record_destroy(rec);
	rec = 0;

	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	//example_dump_record(rec);

	as_map* map = as_record_get_map(rec, BIN_NAME);
	assert_int_eq(as_map_size(map), 4);
	as_integer_init(&mkey, 10);
	as_val* val = as_map_get(map, (as_val*)&mkey);
	assert_int_eq(as_integer_fromval(val)->value, 1);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	rec = 0;

	as_operations_init(&ops, 1);
	as_integer_init(&mkey, 77);
	as_integer_init(&mval, 5);
	as_operations_add_map_put(&ops, BIN_NAME, &update_mode, (as_val*)&mkey, (as_val*)&mval);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = 0;

	as_operations_init(&ops, 1);
	as_integer_init(&mkey, 10);
	as_integer_init(&mval, 2);
	as_operations_add_map_put(&ops, BIN_NAME, &add_mode, (as_val*)&mkey, (as_val*)&mval);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = 0;
}

TEST(map_put_items, "Map put items operations")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 2);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 9);

	as_map_policy put_mode;
	as_map_policy_init(&put_mode);

	as_map_policy add_mode;
	as_map_policy_set(&add_mode, AS_MAP_KEY_ORDERED, AS_MAP_CREATE_ONLY);

	as_map_policy update_mode;
	as_map_policy_set(&update_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE_ONLY);

	as_hashmap add_map;
	as_hashmap_init(&add_map, 3);
	as_integer mkey1;
	as_string  mval1;
	as_integer_init(&mkey1, 12);
	as_string_init(&mval1, "myval", false);
	as_hashmap_set(&add_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer mkey2;
	as_string  mval2;
	as_integer_init(&mkey2, -8734);
	as_string_init(&mval2, "str2", false);
	as_hashmap_set(&add_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer mkey3;
	as_string  mval3;
	as_integer_init(&mkey3, 1);
	as_string_init(&mval3, "my default", false);
	as_hashmap_set(&add_map, (as_val*)&mkey3, (as_val*)&mval3);

	as_operations_add_map_put_items(&ops, BIN_NAME, &add_mode, (as_map*)&add_map);

	as_hashmap put_map;
	as_hashmap_init(&put_map, 3);
	as_integer_init(&mkey1, 12);
	as_string_init(&mval1, "myval12222", false);
	as_hashmap_set(&put_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer_init(&mkey2, 13);
	as_string_init(&mval2, "str13", false);
	as_hashmap_set(&put_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer_init(&mkey3, 14);
	as_string_init(&mval3, "str14", false);
	as_hashmap_set(&put_map, (as_val*)&mkey3, (as_val*)&mval3);

	as_operations_add_map_put_items(&ops, BIN_NAME, &put_mode, (as_map*)&put_map);

	as_hashmap update_map;
	as_hashmap_init(&update_map, 2);
	as_integer_init(&mkey1, 13);
	as_string_init(&mval1, "myval2", false);
	as_hashmap_set(&update_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer_init(&mkey2, 14);
	as_string_init(&mval2, "str14", false);
	as_hashmap_set(&update_map, (as_val*)&mkey2, (as_val*)&mval2);

	as_operations_add_map_put_items(&ops, BIN_NAME, &update_mode, (as_map*)&update_map);

	as_hashmap replace_map;
	as_hashmap_init(&replace_map, 2);
	as_integer_init(&mkey1, 12);
	as_string_init(&mval1, "another string", false);
	as_hashmap_set(&replace_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer_init(&mkey2, -8734);
	as_string_init(&mval2, "changed", false);
	as_hashmap_set(&replace_map, (as_val*)&mkey2, (as_val*)&mval2);

	as_operations_add_map_put_items(&ops, BIN_NAME, &update_mode, (as_map*)&replace_map);

	as_integer_init(&mkey1, 1);
	as_operations_add_map_get_by_key(&ops, BIN_NAME, (as_val*)&mkey1, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, -8734);
	as_operations_add_map_get_by_key(&ops, BIN_NAME, (as_val*)&mkey1, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, 12);
	as_integer_init(&mkey2, 15);
	as_operations_add_map_get_by_key_range(&ops, BIN_NAME, (as_val*)&mkey1, (as_val*)&mkey2, AS_MAP_RETURN_KEY_VALUE);
	as_integer_init(&mkey1, 12);
	as_integer_init(&mkey2, 15);
	as_operations_add_map_get_by_key_range(&ops, BIN_NAME, (as_val*)&mkey1, (as_val*)&mkey2, AS_MAP_RETURN_UNORDERED_MAP);
	as_integer_init(&mkey1, 12);
	as_integer_init(&mkey2, 15);
	as_operations_add_map_get_by_key_range(&ops, BIN_NAME, (as_val*)&mkey1, (as_val*)&mkey2, AS_MAP_RETURN_ORDERED_MAP);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;
	int64_t v = results[i++].valuep->integer.value;
	assert_int_eq(v, 3);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 5);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 5);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 5);

	const char* s = results[i++].valuep->string.value;
	assert_string_eq(s, "my default");

	s = results[i++].valuep->string.value;
	assert_string_eq(s, "changed");

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3 * 2);

	as_map* map0 = &results[i++].valuep->map;
	assert_int_eq(map0->flags, 0);
	assert_true(map_cmp(list, map0));

	as_map* map1 = &results[i++].valuep->map;
	assert_int_eq(map1->flags, 1);
	assert_true(map_cmp(list, map1));

	as_record_destroy(rec);
}

TEST(map_mixed, "Map mixed operations")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 3);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_map_policy mode;
	as_map_policy_set(&mode, AS_MAP_KEY_VALUE_ORDERED, AS_MAP_UPDATE);
		
	as_hashmap item_map;
	as_hashmap_init(&item_map, 3);
	as_integer mkey1;
	as_string  mval1;
	as_integer_init(&mkey1, 12);
	as_string_init(&mval1, "myval", false);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer mkey2;
	as_string  mval2;
	as_integer_init(&mkey2, -8734);
	as_string_init(&mval2, "str2", false);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer mkey3;
	as_string  mval3;
	as_integer_init(&mkey3, 1);
	as_string_init(&mval3, "my default", false);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_integer mkey4;
	as_integer mval4;
	as_integer_init(&mkey4, 7);
	as_integer_init(&mval4, 1);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_operations_add_write_strp(&ops, "otherbin", "hello", false);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;
	int64_t v = results[i++].valuep->integer.value;
	assert_int_eq(v, 4);

	as_record_destroy(rec);

	as_operations_inita(&ops, 3);

	as_integer_init(&mkey1, 12);
	as_operations_add_map_get_by_key(&ops, BIN_NAME, (as_val*)&mkey1, AS_MAP_RETURN_INDEX);

	as_operations_add_append_strp(&ops, "otherbin", "goodbye", false);
	as_operations_add_read(&ops, "otherbin");

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	results = rec->bins.entries;
	i = 0;
	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 3);

	as_val* val = (as_val*)results[i++].valuep;
	assert(val == &as_nil);

	const char* s = results[i++].valuep->string.value;
	assert_string_eq(s, "hellogoodbye");

	as_record_destroy(rec);
}

TEST(map_switch, "Switch from unordered map to a key ordered map.")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 4);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 6);

	as_map_policy mode;
	as_map_policy_init(&mode);

	as_integer mkey;
	as_integer mval;
	as_integer_init(&mkey, 4);
	as_integer_init(&mval, 4);
	as_operations_add_map_put(&ops, BIN_NAME, &mode, (as_val*)&mkey, (as_val*)&mval);
	as_integer_init(&mkey, 3);
	as_integer_init(&mval, 3);
	as_operations_add_map_put(&ops, BIN_NAME, &mode, (as_val*)&mkey, (as_val*)&mval);
	as_integer_init(&mkey, 2);
	as_integer_init(&mval, 2);
	as_operations_add_map_put(&ops, BIN_NAME, &mode, (as_val*)&mkey, (as_val*)&mval);
	as_integer_init(&mkey, 1);
	as_integer_init(&mval, 1);
	as_operations_add_map_put(&ops, BIN_NAME, &mode, (as_val*)&mkey, (as_val*)&mval);

	as_operations_add_map_get_by_index(&ops, BIN_NAME, 2, AS_MAP_RETURN_KEY_VALUE);
	as_operations_add_map_get_by_index_range(&ops, BIN_NAME, 0, 10, AS_MAP_RETURN_KEY_VALUE);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	// example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 3;
	int64_t v = results[i++].valuep->integer.value;
	assert_int_eq(v, 4);

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1 * 2);
	v = as_list_get_int64(list, 0);
	assert_int_eq(v, 3);
	v = as_list_get_int64(list, 1);
	assert_int_eq(v, 3);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4 * 2);

	as_record_destroy(rec);

	as_operations_inita(&ops, 4);

	as_map_policy_set(&mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);
	as_operations_add_map_set_policy(&ops, BIN_NAME, &mode);

	as_integer begin;
	as_integer end;
	as_integer_init(&begin, 3);
	as_integer_init(&end, 5);
	as_operations_add_map_get_by_key_range(&ops, BIN_NAME, (as_val*)&begin, (as_val*)&end, AS_MAP_RETURN_COUNT);

	as_integer_init(&begin, -5);
	as_integer_init(&end, 2);
	as_operations_add_map_get_by_key_range(&ops, BIN_NAME, (as_val*)&begin, (as_val*)&end, AS_MAP_RETURN_KEY_VALUE);

	as_operations_add_map_get_by_index_range(&ops, BIN_NAME, 0, 10, AS_MAP_RETURN_KEY_VALUE);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	results = rec->bins.entries;
	i = 0;

	as_val* val = (as_val*)results[i++].valuep;
	assert(val == &as_nil);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 2);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1 * 2);
	v = as_list_get_int64(list, 0);
	assert_int_eq(v, 1);
	v = as_list_get_int64(list, 1);
	assert_int_eq(v, 1);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4 * 2);
	v = as_list_get_int64(list, 3 * 2);
	assert_int_eq(v, 4);
	v = as_list_get_int64(list, 3 * 2 + 1);
	assert_int_eq(v, 4);

	as_record_destroy(rec);
}

TEST(map_rank, "Map rank")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 5);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Charlie", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "John", false);
	as_integer_init(&mval3, 76);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "Harry", false);
	as_integer_init(&mval4, 82);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);
		
	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_record_destroy(rec);
	as_operations_inita(&ops, 2);

	as_string_init(&mkey1, "John", false);
	as_integer_init(&mval1, 5);
	as_operations_add_map_increment(&ops, BIN_NAME, &mode, (as_val*)&mkey1, (as_val*)&mval1);

	as_string_init(&mkey1, "Jim", false);
	as_integer_init(&mval1, 4);
	as_operations_add_map_decrement(&ops, BIN_NAME, &mode, (as_val*)&mkey1, (as_val*)&mval1);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_record_destroy(rec);
	as_operations_inita(&ops, 12);

	as_operations_add_map_get_by_rank_range(&ops, BIN_NAME, -2, 2, AS_MAP_RETURN_KEY);
	as_operations_add_map_get_by_rank_range(&ops, BIN_NAME, 0, 2, AS_MAP_RETURN_KEY_VALUE);
	as_operations_add_map_get_by_rank(&ops, BIN_NAME, 0, AS_MAP_RETURN_VALUE);
	as_operations_add_map_get_by_rank(&ops, BIN_NAME, 2, AS_MAP_RETURN_KEY);

	as_integer_init(&mval1, 90);
	as_integer_init(&mval2, 95);
	as_operations_add_map_get_by_value_range(&ops, BIN_NAME, (as_val*)&mval1, (as_val*)&mval2, AS_MAP_RETURN_RANK);
	as_integer_init(&mval1, 90);
	as_integer_init(&mval2, 95);
	as_operations_add_map_get_by_value_range(&ops, BIN_NAME, (as_val*)&mval1, (as_val*)&mval2, AS_MAP_RETURN_COUNT);
	as_integer_init(&mval1, 90);
	as_integer_init(&mval2, 95);
	as_operations_add_map_get_by_value_range(&ops, BIN_NAME, (as_val*)&mval1, (as_val*)&mval2, AS_MAP_RETURN_KEY_VALUE);

	as_integer_init(&mval1, 81);
	as_integer_init(&mval2, 82);
	as_operations_add_map_get_by_value_range(&ops, BIN_NAME, (as_val*)&mval1, (as_val*)&mval2, AS_MAP_RETURN_KEY);

	as_integer_init(&mval1, 77);
	as_operations_add_map_get_by_value(&ops, BIN_NAME, (as_val*)&mval1, AS_MAP_RETURN_KEY);

	as_integer_init(&mval1, 81);
	as_operations_add_map_get_by_value(&ops, BIN_NAME, (as_val*)&mval1, AS_MAP_RETURN_RANK);

	as_string_init(&mkey1, "Charlie", false);
	as_operations_add_map_get_by_key(&ops, BIN_NAME, (as_val*)&mkey1, AS_MAP_RETURN_RANK);
	as_string_init(&mkey1, "Charlie", false);
	as_operations_add_map_get_by_key(&ops, BIN_NAME, (as_val*)&mkey1, AS_MAP_RETURN_REVERSE_RANK);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	const char* s = as_list_get_str(list, 0);
	assert_string_eq(s, "Harry");
	s = as_list_get_str(list, 1);
	assert_string_eq(s, "Jim");

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2 * 2);
	s = as_list_get_str(list, 0);
	assert_string_eq(s, "Charlie");
	int64_t v = as_list_get_int64(list, 1);
	assert_int_eq(v, 55);
	s = as_list_get_str(list, 2);
	assert_string_eq(s, "John");
	v = as_list_get_int64(list, 3);
	assert_int_eq(v, 81);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 55);

	s = results[i++].valuep->string.value;
	assert_string_eq(s, "Harry");

	list = &results[i++].valuep->list;
	v = as_list_get_int64(list, 0);
	assert_int_eq(v, 3);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 1);

	list = &results[i++].valuep->list;
	s = as_list_get_str(list, 0);
	assert_string_eq(s, "Jim");
	v = as_list_get_int64(list, 1);
	assert_int_eq(v, 94);

	list = &results[i++].valuep->list;
	s = as_list_get_str(list, 0);
	assert_string_eq(s, "John");

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 0);

	list = &results[i++].valuep->list;
	v = as_list_get_int64(list, 0);
	assert_int_eq(v, 1);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 0);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 3);

	as_record_destroy(rec);
}

TEST(map_remove, "Map remove")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 6);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 6);

	as_map_policy mode;
	as_map_policy_init(&mode);

	as_hashmap item_map;
	as_hashmap_init(&item_map, 7);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Charlie", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "John", false);
	as_integer_init(&mval3, 76);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "Harry", false);
	as_integer_init(&mval4, 82);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);
	as_string  mkey5;
	as_integer mval5;
	as_string_init(&mkey5, "Sally", false);
	as_integer_init(&mval5, 79);
	as_hashmap_set(&item_map, (as_val*)&mkey5, (as_val*)&mval5);
	as_string  mkey6;
	as_integer mval6;
	as_string_init(&mkey6, "Lenny", false);
	as_integer_init(&mval6, 84);
	as_hashmap_set(&item_map, (as_val*)&mkey6, (as_val*)&mval6);
	as_string  mkey7;
	as_integer mval7;
	as_string_init(&mkey7, "Abe", false);
	as_integer_init(&mval7, 88);
	as_hashmap_set(&item_map, (as_val*)&mkey7, (as_val*)&mval7);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_string_init(&mkey1, "NOTFOUND", false);
	as_operations_add_map_remove_by_key(&ops, BIN_NAME, (as_val*)&mkey1, AS_MAP_RETURN_VALUE);

	as_string_init(&mkey1, "Jim", false);
	as_operations_add_map_remove_by_key(&ops, BIN_NAME, (as_val*)&mkey1, AS_MAP_RETURN_VALUE);

	as_arraylist remove_list;
	as_arraylist_init(&remove_list, 3, 3);
	as_arraylist_append_str(&remove_list, "Sally");
	as_arraylist_append_str(&remove_list, "Unknown");
	as_arraylist_append_str(&remove_list, "Lenny");

	as_operations_add_map_remove_by_key_list(&ops, BIN_NAME, (as_list*)&remove_list, AS_MAP_RETURN_COUNT);
	as_arraylist_destroy(&remove_list);

	as_integer_init(&mval2, 55);
	as_operations_add_map_remove_by_value(&ops, BIN_NAME, (as_val*)&mval2, AS_MAP_RETURN_KEY);
	as_operations_add_map_size(&ops, BIN_NAME);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t v = results[i++].valuep->integer.value;
	assert_int_eq(v, 7);

	as_val* val = (as_val*)results[i++].valuep;
	assert(val == &as_nil);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 98);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 2);

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	const char* s = as_list_get_str(list, 0);
	assert_string_eq(s, "Charlie");

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 3);

	as_record_destroy(rec);
}

TEST(map_remove_range, "Map remove range")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 7);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 5);

	as_map_policy mode;
	as_map_policy_init(&mode);

	as_hashmap item_map;
	as_hashmap_init(&item_map, 7);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Charlie", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "John", false);
	as_integer_init(&mval3, 76);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "Harry", false);
	as_integer_init(&mval4, 82);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);
	as_string  mkey5;
	as_integer mval5;
	as_string_init(&mkey5, "Sally", false);
	as_integer_init(&mval5, 79);
	as_hashmap_set(&item_map, (as_val*)&mkey5, (as_val*)&mval5);
	as_string  mkey6;
	as_integer mval6;
	as_string_init(&mkey6, "Lenny", false);
	as_integer_init(&mval6, 84);
	as_hashmap_set(&item_map, (as_val*)&mkey6, (as_val*)&mval6);
	as_string  mkey7;
	as_integer mval7;
	as_string_init(&mkey7, "Abe", false);
	as_integer_init(&mval7, 88);
	as_hashmap_set(&item_map, (as_val*)&mkey7, (as_val*)&mval7);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_string_init(&mkey1, "J", false);
	as_string_init(&mkey2, "K", false);
	as_operations_add_map_remove_by_key_range(&ops, BIN_NAME, (as_val*)&mkey1, (as_val*)&mkey2, AS_MAP_RETURN_COUNT);

	as_integer_init(&mval1, 80);
	as_integer_init(&mval2, 85);
	as_operations_add_map_remove_by_value_range(&ops, BIN_NAME, (as_val*)&mval1, (as_val*)&mval2, AS_MAP_RETURN_COUNT);

	as_operations_add_map_remove_by_index_range(&ops, BIN_NAME, 0, 2, AS_MAP_RETURN_COUNT);
	as_operations_add_map_remove_by_rank_range(&ops, BIN_NAME, 0, 2, AS_MAP_RETURN_COUNT);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t v = results[i++].valuep->integer.value;
	assert_int_eq(v, 7);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 2);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 2);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 2);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 1);

	as_record_destroy(rec);
}

TEST(map_clear, "Map clear")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 8);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	as_hashmap item_map;
	as_hashmap_init(&item_map, 2);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Charlie", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t v = results[i++].valuep->integer.value;
	assert_int_eq(v, 2);

	as_record_destroy(rec);

	as_operations_inita(&ops, 2);
	as_operations_add_map_clear(&ops, BIN_NAME);
	as_operations_add_map_size(&ops, BIN_NAME);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);

	results = rec->bins.entries;
	i = 0;

	as_val* val = (as_val*)results[i++].valuep;
	assert(val == &as_nil);

	v = results[i++].valuep->integer.value;
	assert_int_eq(v, 0);

	as_record_destroy(rec);
}

TEST(map_score, "Map score")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 9);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	// Create map.
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "weiling", false);
	as_integer_init(&mval1, 0);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "briann", false);
	as_integer_init(&mval2, 0);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "brianb", false);
	as_integer_init(&mval3, 0);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "meher", false);
	as_integer_init(&mval4, 0);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// Change scores
	as_operations_inita(&ops, 4);

	as_string_init(&mkey1, "weiling", false);
	as_integer_init(&mval1, 10);
	as_operations_add_map_increment(&ops, BIN_NAME, &mode, (as_val*)&mkey1, (as_val*)&mval1);

	as_string_init(&mkey1, "briann", false);
	as_integer_init(&mval1, 20);
	as_operations_add_map_increment(&ops, BIN_NAME, &mode, (as_val*)&mkey1, (as_val*)&mval1);

	as_string_init(&mkey1, "brianb", false);
	as_integer_init(&mval1, 1);
	as_operations_add_map_increment(&ops, BIN_NAME, &mode, (as_val*)&mkey1, (as_val*)&mval1);

	as_string_init(&mkey1, "meher", false);
	as_integer_init(&mval1, 20);
	as_operations_add_map_increment(&ops, BIN_NAME, &mode, (as_val*)&mkey1, (as_val*)&mval1);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// Query top 3 scores
	as_operations_inita(&ops, 1);
	as_operations_add_map_get_by_rank_range(&ops, BIN_NAME, -3, 3, AS_MAP_RETURN_KEY);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// Remove people with score 10 and display top 3 again
	as_operations_inita(&ops, 2);
	as_integer_init(&mval1, 10);
	as_operations_add_map_remove_by_value(&ops, BIN_NAME, (as_val*)&mval1, AS_MAP_RETURN_KEY);
	as_operations_add_map_get_by_rank_range(&ops, BIN_NAME, -3, 3, AS_MAP_RETURN_KEY);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	as_list* list = &results[i++].valuep->list;
	const char* s = as_list_get_str(list, 0);
	assert_string_eq(s, "weiling");

	list = &results[i++].valuep->list;
	s = as_list_get_str(list, 0);
	assert_string_eq(s, "brianb");
	s = as_list_get_str(list, 1);
	assert_string_eq(s, "briann");
	s = as_list_get_str(list, 2);
	assert_string_eq(s, "meher");

	//example_dump_record(rec);
	as_record_destroy(rec);
}

TEST(map_remove_non_exist, "Remove non-existant keys")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 10);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap item_map;
	as_hashmap_init(&item_map, 3);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "a", false);
	as_integer_init(&mval1, 1);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "b", false);
	as_integer_init(&mval2, 2);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "c", false);
	as_integer_init(&mval3, 3);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	as_arraylist remove_list;
	as_arraylist_init(&remove_list, 8, 8);
	as_arraylist_append_str(&remove_list, "a");
	as_arraylist_append_str(&remove_list, "x");
	as_arraylist_append_str(&remove_list, "y");
	as_arraylist_append_str(&remove_list, "z");
	as_arraylist_append_str(&remove_list, "xx");
	as_arraylist_append_str(&remove_list, "yy");
	as_arraylist_append_str(&remove_list, "zz");

	for (int i = 0; i < 100; i++) {
		char buf[3] = "##";
		buf[0] = i % 10 + '0';
		buf[1] = i / 10 + '0';
		as_arraylist_append_str(&remove_list, buf);
	}

	as_arraylist_append_str(&remove_list, "c");

	for (int i = 0; i < 26; i++) {
		char buf[3] = "_x";
		buf[1] = i + 'A';
		as_arraylist_append_str(&remove_list, buf);
	}

	as_operations_inita(&ops, 1);
	as_operations_add_map_remove_by_key_list(&ops, BIN_NAME, (as_list*)&remove_list, AS_MAP_RETURN_KEY_VALUE);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_list *list = as_record_get_list(rec, BIN_NAME);
	assert_int_eq(as_list_size(list), 2 * 2);
	assert_int_eq(as_list_get_int64(list, 1), 1);
	assert_int_eq(as_list_get_int64(list, 1 * 2 + 1), 3);

	//example_dump_record(rec);

	as_record_destroy(rec);
}

TEST(map_replace_unfilled, "Map replace with unfilled index")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 11);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_map_policy put_mode;
	as_map_policy_set(&put_mode, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);

	as_operations ops;
	as_operations_init(&ops, 1);

	// Setup existing k-ordered map with un-filled index map.
	as_bytes packed_map;
	as_bytes_init(&packed_map, 100);
	uint8_t packed_buf[] = {
			0x84,
			0xC7, 0x00, 0x01, 0xC0,
			0x01, 0x01,
			0x02, 0x02,
			0x03, 0x03
	};
	as_bytes_append(&packed_map, packed_buf, sizeof(packed_buf));
	as_bytes_set_type(&packed_map, AS_BYTES_MAP);
	as_operations_add_write(&ops, BIN_NAME, (as_bin_value *)&packed_map);

	as_record* rec = NULL;

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Replace key 1.
	as_operations_init(&ops, 1);

	as_integer mkey;
	as_string s;
	as_integer_init(&mkey, 1);
	as_string_init(&s, "testing", false);
	as_operations_add_map_put(&ops, BIN_NAME, &put_mode, (as_val*)&mkey, (as_val*)&s);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	//example_dump_record(rec);
	as_record_destroy(rec);
}

TEST(map_get_by_list, "Map Get By List")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 12);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	// Create map.
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Charlie", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "John", false);
	as_integer_init(&mval3, 76);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "Harry", false);
	as_integer_init(&mval4, 82);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	as_arraylist key_list;
	as_arraylist_init(&key_list, 2, 0);
	as_arraylist_append_str(&key_list, "Harry");
	as_arraylist_append_str(&key_list, "Jim");

	as_arraylist value_list;
	as_arraylist_init(&value_list, 2, 0);
	as_arraylist_append_int64(&value_list, 76);
	as_arraylist_append_int64(&value_list, 50);

	as_operations_inita(&ops, 2);
	as_operations_add_map_get_by_key_list(&ops, BIN_NAME, (as_list*)&key_list, AS_MAP_RETURN_KEY_VALUE);
	as_operations_add_map_get_by_value_list(&ops, BIN_NAME, (as_list*)&value_list, AS_MAP_RETURN_KEY_VALUE);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2 * 2);
	assert_string_eq(as_list_get_str(list, 0 * 2), "Harry");
	assert_int_eq(as_list_get_int64(list, 0 * 2 + 1), 82);
	assert_string_eq(as_list_get_str(list, 1 * 2), "Jim");
	assert_int_eq(as_list_get_int64(list, 1 * 2 + 1), 98);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1 * 2);
	assert_string_eq(as_list_get_str(list, 0 * 2), "John");
	assert_int_eq(as_list_get_int64(list, 0 * 2 + 1), 76);

	//example_dump_record(rec);
	as_record_destroy(rec);
}

TEST(map_inverted, "Map Inverted")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 13);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	// Create map.
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Charlie", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "John", false);
	as_integer_init(&mval3, 76);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "Harry", false);
	as_integer_init(&mval4, 82);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	as_operations_inita(&ops, 7);
	as_integer_init(&mval1, 81);
	as_operations_add_map_get_by_value(&ops, BIN_NAME, (as_val*)&mval1, AS_MAP_RETURN_RANK | AS_MAP_RETURN_INVERTED);

	as_integer_init(&mval2, 82);
	as_operations_add_map_get_by_value(&ops, BIN_NAME, (as_val*)&mval2, AS_MAP_RETURN_RANK | AS_MAP_RETURN_INVERTED);

	as_integer_init(&mval3, 90);
	as_integer_init(&mval4, 95);
	as_operations_add_map_get_by_value_range(&ops, BIN_NAME, (as_val*)&mval3, (as_val*)&mval4, AS_MAP_RETURN_RANK | AS_MAP_RETURN_INVERTED);

	as_integer_init(&mval3, 90);
	as_integer_init(&mval4, 100);
	as_operations_add_map_get_by_value_range(&ops, BIN_NAME, (as_val*)&mval3, (as_val*)&mval4, AS_MAP_RETURN_RANK | AS_MAP_RETURN_INVERTED);

	as_arraylist value_list;
	as_arraylist_init(&value_list, 4, 0);
	as_arraylist_append_int64(&value_list, 76);
	as_arraylist_append_int64(&value_list, 55);
	as_arraylist_append_int64(&value_list, 98);
	as_arraylist_append_int64(&value_list, 50);
	as_operations_add_map_get_by_value_list(&ops, BIN_NAME, (as_list*)&value_list, AS_MAP_RETURN_KEY_VALUE | AS_MAP_RETURN_INVERTED);

	as_operations_add_map_get_by_rank_range(&ops, BIN_NAME, -2, 2, AS_MAP_RETURN_KEY | AS_MAP_RETURN_INVERTED);
	as_operations_add_map_get_by_rank_range(&ops, BIN_NAME, 0, 3, AS_MAP_RETURN_KEY_VALUE | AS_MAP_RETURN_INVERTED);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);
	assert_int_eq(as_list_get_int64(list, 0), 0);
	assert_int_eq(as_list_get_int64(list, 1), 1);
	assert_int_eq(as_list_get_int64(list, 2), 2);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1 * 2);
	assert_string_eq(as_list_get_str(list, 0 * 2), "Harry");
	assert_int_eq(as_list_get_int64(list, 0 * 2 + 1), 82);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_string_eq(as_list_get_str(list, 0), "Charlie");
	assert_string_eq(as_list_get_str(list, 1), "John");

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1 * 2);
	assert_string_eq(as_list_get_str(list, 0 * 2), "Jim");
	assert_int_eq(as_list_get_int64(list, 0 * 2 + 1), 98);

	as_record_destroy(rec);
}

TEST(map_get_relative, "Map Get Relative")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 14);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	// Create map.
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_integer mkey1;
	as_integer mval1;
	as_integer_init(&mkey1, 0);
	as_integer_init(&mval1, 17);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer mkey2;
	as_integer mval2;
	as_integer_init(&mkey2, 4);
	as_integer_init(&mval2, 2);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer mkey3;
	as_integer mval3;
	as_integer_init(&mkey3, 5);
	as_integer_init(&mval3, 15);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_integer mkey4;
	as_integer mval4;
	as_integer_init(&mkey4, 9);
	as_integer_init(&mval4, 10);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	as_operations_inita(&ops, 14);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_get_by_key_rel_index_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, 0, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_get_by_key_rel_index_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, 1, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_get_by_key_rel_index_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, -1, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 3);
	as_operations_add_map_get_by_key_rel_index_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, 2, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 3);
	as_operations_add_map_get_by_key_rel_index_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, -2, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_get_by_key_rel_index_range(&ops, BIN_NAME, (as_val*)&mkey1, 0, 1, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_get_by_key_rel_index_range(&ops, BIN_NAME, (as_val*)&mkey1, 1, 2, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_get_by_key_rel_index_range(&ops, BIN_NAME, (as_val*)&mkey1, -1, 1, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 3);
	as_operations_add_map_get_by_key_rel_index_range(&ops, BIN_NAME, (as_val*)&mkey1, 2, 1, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 3);
	as_operations_add_map_get_by_key_rel_index_range(&ops, BIN_NAME, (as_val*)&mkey1, -2, 2, AS_MAP_RETURN_KEY);

	as_integer_init(&mkey1, 11);
	as_operations_add_map_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, 1, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, 11);
	as_operations_add_map_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, -1, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, 11);
	as_operations_add_map_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&mkey1, 1, 1, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, 11);
	as_operations_add_map_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&mkey1, -1, 1, AS_MAP_RETURN_VALUE);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 5);
	assert_int_eq(as_list_get_int64(list, 1), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);
	assert_int_eq(as_list_get_int64(list, 0), 4);
	assert_int_eq(as_list_get_int64(list, 1), 5);
	assert_int_eq(as_list_get_int64(list, 2), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4);
	assert_int_eq(as_list_get_int64(list, 0), 0);
	assert_int_eq(as_list_get_int64(list, 1), 4);
	assert_int_eq(as_list_get_int64(list, 2), 5);
	assert_int_eq(as_list_get_int64(list, 3), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 5);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 4);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 0);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 17);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);
	assert_int_eq(as_list_get_int64(list, 0), 10);
	assert_int_eq(as_list_get_int64(list, 1), 15);
	assert_int_eq(as_list_get_int64(list, 2), 17);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 17);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 10);

	as_record_destroy(rec);
}

TEST(map_remove_relative, "Map Remove Relative")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 15);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_map_policy mode;
	as_map_policy_init(&mode);

	// Create map.
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_integer mkey1;
	as_integer mval1;
	as_integer_init(&mkey1, 0);
	as_integer_init(&mval1, 17);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer mkey2;
	as_integer mval2;
	as_integer_init(&mkey2, 4);
	as_integer_init(&mval2, 2);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer mkey3;
	as_integer mval3;
	as_integer_init(&mkey3, 5);
	as_integer_init(&mval3, 15);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_integer mkey4;
	as_integer mval4;
	as_integer_init(&mkey4, 9);
	as_integer_init(&mval4, 10);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	as_operations_inita(&ops, 3);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_remove_by_key_rel_index_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, 0, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_remove_by_key_rel_index_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, 1, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, 5);
	as_operations_add_map_remove_by_key_rel_index_range(&ops, BIN_NAME, (as_val*)&mkey1, -1, 1, AS_MAP_RETURN_VALUE);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 15);
	assert_int_eq(as_list_get_int64(list, 1), 10);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 0);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 2);

	as_record_destroy(rec);

	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap_init(&item_map, 4);
	as_integer_init(&mkey1, 0);
	as_integer_init(&mval1, 17);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer_init(&mkey2, 4);
	as_integer_init(&mval2, 2);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer_init(&mkey3, 5);
	as_integer_init(&mval3, 15);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_integer_init(&mkey4, 9);
	as_integer_init(&mval4, 10);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_inita(&ops, 1);
	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_record_destroy(rec);

	as_operations_inita(&ops, 2);

	as_integer_init(&mkey1, 11);
	as_operations_add_map_remove_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&mkey1, 1, AS_MAP_RETURN_VALUE);

	as_integer_init(&mkey1, 11);
	as_operations_add_map_remove_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&mkey1, -1, 1, AS_MAP_RETURN_VALUE);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	results = rec->bins.entries;
	i = 0;

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 17);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 10);

	as_record_destroy(rec);
}

TEST(map_partial, "Map Partial")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 16);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_map_policy mp;
	as_map_policy_init(&mp);

	// Create map.
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_integer mkey1;
	as_integer mval1;
	as_integer_init(&mkey1, 0);
	as_integer_init(&mval1, 17);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer mkey2;
	as_integer mval2;
	as_integer_init(&mkey2, 4);
	as_integer_init(&mval2, 2);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer mkey3;
	as_integer mval3;
	as_integer_init(&mkey3, 5);
	as_integer_init(&mval3, 15);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_integer mkey4;
	as_integer mval4;
	as_integer_init(&mkey4, 9);
	as_integer_init(&mval4, 10);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, BIN_NAME, &mp, (as_map*)&item_map);

	// Create map.
	as_hashmap item_map2;
	as_hashmap_init(&item_map2, 4);
	as_integer_init(&mkey1, 0);
	as_integer_init(&mval1, 17);
	as_hashmap_set(&item_map2, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer_init(&mkey2, 4);
	as_integer_init(&mval2, 2);
	as_hashmap_set(&item_map2, (as_val*)&mkey2, (as_val*)&mval2);
	as_integer_init(&mkey3, 5);
	as_integer_init(&mval3, 15);
	as_hashmap_set(&item_map2, (as_val*)&mkey3, (as_val*)&mval3);
	as_integer_init(&mkey4, 9);
	as_integer_init(&mval4, 10);
	as_hashmap_set(&item_map2, (as_val*)&mkey4, (as_val*)&mval4);

	as_operations_add_map_put_items(&ops, "bin2", &mp, (as_map*)&item_map2);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	as_operations_inita(&ops, 2);

	as_hashmap source_map;
	as_hashmap_init(&source_map, 2);
	as_integer_init(&mkey1, 3);
	as_integer_init(&mval1, 3);
	as_hashmap_set(&source_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer_init(&mkey2, 5);
	as_integer_init(&mval2, 15);
	as_hashmap_set(&source_map, (as_val*)&mkey2, (as_val*)&mval2);

	as_map_policy_set_flags(&mp, AS_MAP_UNORDERED, AS_MAP_WRITE_CREATE_ONLY | AS_MAP_WRITE_PARTIAL | AS_MAP_WRITE_NO_FAIL);
	as_operations_add_map_put_items(&ops, BIN_NAME, &mp, (as_map*)&source_map);

	as_hashmap source_map2;
	as_hashmap_init(&source_map2, 2);
	as_integer_init(&mkey1, 3);
	as_integer_init(&mval1, 3);
	as_hashmap_set(&source_map2, (as_val*)&mkey1, (as_val*)&mval1);
	as_integer_init(&mkey2, 5);
	as_integer_init(&mval2, 15);
	as_hashmap_set(&source_map2, (as_val*)&mkey2, (as_val*)&mval2);

	as_map_policy_set_flags(&mp, AS_MAP_UNORDERED, AS_MAP_WRITE_CREATE_ONLY | AS_MAP_WRITE_NO_FAIL);
	as_operations_add_map_put_items(&ops, "bin2", &mp, (as_map*)&source_map2);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 4);

	as_record_destroy(rec);
}

TEST(map_nested, "Nested Map")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 17);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap m1;
	as_hashmap_init(&m1, 2);
	as_string k11;
	as_string_init(&k11, "key11", false);
	as_integer v11;
	as_integer_init(&v11, 9);
	as_hashmap_set(&m1, (as_val*)&k11, (as_val*)&v11);
	as_string k12;
	as_string_init(&k12, "key12", false);
	as_integer v12;
	as_integer_init(&v12, 4);
	as_hashmap_set(&m1, (as_val*)&k12, (as_val*)&v12);

	as_hashmap m2;
	as_hashmap_init(&m2, 2);
	as_string k21;
	as_string_init(&k21, "key21", false);
	as_integer v21;
	as_integer_init(&v21, 3);
	as_hashmap_set(&m2, (as_val*)&k21, (as_val*)&v21);
	as_string k22;
	as_string_init(&k22, "key22", false);
	as_integer v22;
	as_integer_init(&v22, 5);
	as_hashmap_set(&m2, (as_val*)&k22, (as_val*)&v22);

	as_hashmap map;
	as_hashmap_init(&map, 2);
	as_string k1;
	as_string_init(&k1, "key1", false);
	as_hashmap_set(&map, (as_val*)&k1, (as_val*)&m1);
	as_string k2;
	as_string_init(&k2, "key2", false);
	as_hashmap_set(&map, (as_val*)&k2, (as_val*)&m2);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_cdt_ctx ctx;
	as_cdt_ctx_inita(&ctx, 1);
	as_string_init(&k2, "key2", false);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)&k2);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_string_init(&k21, "key21", false);
	as_integer_init(&v21, 11);
	as_operations_map_put(&ops, BIN_NAME, &ctx, NULL, (as_val*)&k21, (as_val*)&v21);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;
	int i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 2);

	as_map* mapr = &results[i++].valuep->map;
	assert_int_eq(as_map_size(mapr), 2);

	as_string_init(&k2, "key2", false);
	as_map* mr = (as_map*)as_map_get(mapr, (as_val*)&k2);

	as_string_init(&k2, "key21", false);
	as_integer* ir = (as_integer*)as_map_get(mr, (as_val*)&k2);
	assert_int_eq(ir->value, 11);

	as_string_init(&k2, "key22", false);
	ir = (as_integer*)as_map_get(mr, (as_val*)&k2);
	assert_int_eq(ir->value, 5);

	as_record_destroy(prec);
}

TEST(map_double_nested, "Double Nested Map")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 18);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap m11;
	as_hashmap_init(&m11, 1);
	as_string k11;
	as_string_init(&k11, "key111", false);
	as_integer v11;
	as_integer_init(&v11, 1);
	as_hashmap_set(&m11, (as_val*)&k11, (as_val*)&v11);

	as_hashmap m12;
	as_hashmap_init(&m12, 1);
	as_string k12;
	as_string_init(&k12, "key121", false);
	as_integer v12;
	as_integer_init(&v12, 5);
	as_hashmap_set(&m12, (as_val*)&k12, (as_val*)&v12);

	as_hashmap m1;
	as_hashmap_init(&m1, 2);
	as_string k1;
	as_string_init(&k1, "key11", false);
	as_hashmap_set(&m1, (as_val*)&k1, (as_val*)&m11);
	as_string k2;
	as_string_init(&k2, "key12", false);
	as_hashmap_set(&m1, (as_val*)&k2, (as_val*)&m12);

	as_hashmap m21;
	as_hashmap_init(&m21, 1);
	as_string k211;
	as_string_init(&k211, "key211", false);
	as_integer v211;
	as_integer_init(&v211, 7);
	as_hashmap_set(&m21, (as_val*)&k211, (as_val*)&v211);

	as_hashmap m2;
	as_hashmap_init(&m2, 1);
	as_string k21;
	as_string_init(&k21, "key21", false);
	as_hashmap_set(&m2, (as_val*)&k21, (as_val*)&m21);

	as_hashmap map;
	as_hashmap_init(&map, 2);
	as_string mk1;
	as_string_init(&mk1, "key1", false);
	as_hashmap_set(&map, (as_val*)&mk1, (as_val*)&m1);
	as_string mk2;
	as_string_init(&mk2, "key2", false);
	as_hashmap_set(&map, (as_val*)&mk2, (as_val*)&m2);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	// as_cdt_ctx_inita() is more efficient, but as_cdt_ctx_create() needs to be tested too.
	// as_cdt_ctx ctx;
	// as_cdt_ctx_inita(&ctx, 2);
	as_cdt_ctx* ctx = as_cdt_ctx_create(2);
	// as_string_init() is more efficient, but as_string_new() needs to be tested too.
	// as_string_init(&k1, "key1", false);
	// as_cdt_ctx_add_map_key(ctx, (as_val*)&k1);
	as_cdt_ctx_add_map_key(ctx, (as_val*)as_string_new("key1", false));
	as_cdt_ctx_add_map_rank(ctx, -1);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_string_init(&k211, "key121", false);
	as_integer_init(&v211, 11);
	as_operations_map_put(&ops, BIN_NAME, ctx, NULL, (as_val*)&k211, (as_val*)&v211);
	as_cdt_ctx_destroy(ctx);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;
	int i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 1);

	as_map* mapr = &results[i++].valuep->map;
	assert_int_eq(as_map_size(mapr), 2);

	as_string_init(&k1, "key1", false);
	as_map* mr = (as_map*)as_map_get(mapr, (as_val*)&k1);

	as_string_init(&k12, "key12", false);
	as_map* m = (as_map*)as_map_get(mr, (as_val*)&k12);
	assert_int_eq(as_map_size(m), 1);

	as_string_init(&k12, "key121", false);
	as_integer* ir = (as_integer*)as_map_get(m, (as_val*)&k12);
	assert_int_eq(ir->value, 11);

	as_record_destroy(prec);
}

TEST(map_ctx_create, "ctx create")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 19);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_cdt_ctx ctx;
	as_string k0;
	as_string k1;
	as_operations ops;
	as_string ka;
	as_integer va;
	as_record* rec = NULL;

	// Create from empty bin.
	as_cdt_ctx_inita(&ctx, 2);
	as_string_init(&k0, "aaaa5", false);
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)&k0, AS_MAP_UNORDERED);
	as_string_init(&k1, "aa3", false);
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)&k1, AS_MAP_KEY_ORDERED);

	as_operations_init(&ops, 2);
	as_string_init(&ka, "ka", false);
	as_integer_init(&va, 11);
	as_operations_map_put(&ops, BIN_NAME, &ctx, NULL, (as_val*)&ka, (as_val*)&va);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Get and print.
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BIN_NAME);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Create sub context.
	as_cdt_ctx_inita(&ctx, 2);
	as_string_init(&k0, "aaaa5", false);
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)&k0, AS_MAP_UNORDERED);
	as_string_init(&k1, "bb3", false);
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)&k1, AS_MAP_KEY_ORDERED);

	as_operations_init(&ops, 2);
	as_string_init(&ka, "la", false);
	as_integer_init(&va, -11);
	as_operations_map_put(&ops, BIN_NAME, &ctx, NULL, (as_val*)&ka, (as_val*)&va);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Get and print.
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BIN_NAME);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

}

TEST(map_simple2, "Map put simple")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 20);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_record* rec = NULL;
	as_arraylist empty_list;
	as_arraylist_init(&empty_list, 1, 1);

	as_map_policy pol;
	as_map_policy_set_flags(&pol, AS_MAP_UNORDERED, AS_MAP_WRITE_CREATE_ONLY | AS_MAP_WRITE_NO_FAIL);

	as_operations_init(&ops, 4);
	as_operations_add_map_put(&ops, BIN_NAME, &pol, (as_val*)as_string_new_strdup("sf"), (as_val*)&empty_list);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)as_string_new_strdup("sf"));

	as_arraylist data_list;
	as_arraylist_init(&data_list, 2, 2);
	as_arraylist_append_int64(&data_list, 45);
	as_arraylist_append_str(&data_list, "Jen");

	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)&data_list);
	as_operations_list_remove_by_rank_range_to_end(&ops, BIN_NAME, &ctx, 1, AS_LIST_RETURN_NONE);
	as_cdt_ctx_destroy(&ctx);
	as_operations_add_map_remove_by_rank_range_to_end(&ops, BIN_NAME, 100, AS_MAP_RETURN_NONE);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Get and print.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;
}

TEST(map_simple3, "Map put simple")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 21);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_arraylist empty_list;
	as_arraylist_init(&empty_list, 1, 1);

	as_map_policy pol;
	as_map_policy_set_flags(&pol, AS_MAP_UNORDERED, AS_MAP_WRITE_UPDATE_ONLY | AS_MAP_WRITE_NO_FAIL);

	as_operations_init(&ops, 1);
	as_operations_add_map_put(&ops, BIN_NAME, &pol, (as_val*)as_string_new_strdup("sfcd"), (as_val*)&empty_list);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;
}

TEST(map_ctx_create2, "ctx create 2")
{
	static const char *keys[] = {
		"9", "11", "11", "11", "12", "13", "14", "10", "11"
	};
	static int values[] = {
		700, 710, 705, 720, 730, 740, 750, 690, 702
	};
	int key_count = sizeof(keys) / sizeof(const char *);
	//info("key_count %d", key_count);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 22);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_cdt_ctx ctx;
	as_cdt_ctx ctx2;
	as_operations ops;
	as_record* rec = NULL;

	as_map_policy pol;
	as_map_policy_set_flags(&pol, AS_MAP_KEY_VALUE_ORDERED, AS_MAP_WRITE_CREATE_ONLY | AS_MAP_WRITE_NO_FAIL);

	for (int i = 0; i < key_count; i++) {
		char* key = (char*)keys[i];
		//info("[%d]key %s", i, key);
		as_string skey;
		as_string_init(&skey, key, false);

		as_operations_init(&ops, 3);

		as_cdt_ctx_init(&ctx, 1);
		as_cdt_ctx_add_map_key_create(&ctx, (as_val*)&skey, AS_MAP_KEY_VALUE_ORDERED);
		as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(values[i]));
		as_val_reserve((as_val*)&skey);  // reserve skey for later use.
		as_cdt_ctx_destroy(&ctx);

		as_cdt_ctx_init(&ctx2, 1);
		as_cdt_ctx_add_map_key(&ctx2, (as_val*)&skey);
		as_operations_list_remove_by_rank_range(&ops, BIN_NAME, &ctx2, 0, 1, AS_LIST_RETURN_INVERTED);
		as_cdt_ctx_destroy(&ctx2);

		status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
		assert_int_eq(status, AEROSPIKE_OK);
		as_operations_destroy(&ops);
		//example_dump_record(rec);
		as_record_destroy(rec);
		rec = NULL;

		// Get and print.
		as_operations_init(&ops, 1);
		as_operations_add_read(&ops, BIN_NAME);
		status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
		assert_int_eq(status, AEROSPIKE_OK);
		as_operations_destroy(&ops);
		//example_dump_record(rec);
		as_record_destroy(rec);
		rec = NULL;
	}
}

TEST(map_ctx_create3, "ctx create 3")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 103);

	as_error err;
	as_status status;

	const as_map_order map_types[] = {
		AS_MAP_UNORDERED,
		AS_MAP_KEY_ORDERED,
		AS_MAP_KEY_VALUE_ORDERED
	};

	as_record* rec = NULL;

	for (uint32_t i = 0; i < 3; i++) {
		status = aerospike_key_remove(as, &err, NULL, &rkey);
		assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);
		as_map_order order = map_types[i];

		for (uint32_t k = 0; k < 10; k++) {
			as_string key_str;
			rand_str(&key_str);
			as_cdt_ctx ctx;
			as_cdt_ctx_init(&ctx, 1);
			as_cdt_ctx_add_map_key_create(&ctx, (as_val*)&key_str, order);

			for (uint32_t j = 0; j < 10; j++) {
				as_map_policy pol;
				as_map_policy_set(&pol, order, AS_MAP_UPDATE);

				as_string k;
				as_string v;
				rand_str(&k);
				rand_str(&v);

				as_operations ops;
				as_operations_init(&ops, 1);
				as_operations_map_put(&ops, BIN_NAME, &ctx, &pol, (as_val*)&k, (as_val*)&v);
				status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, NULL);
				assert_int_eq(status, AEROSPIKE_OK);
				as_operations_destroy(&ops);
			}
			as_cdt_ctx_destroy(&ctx);
		}

		// Get and check.
		status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
		assert_int_eq(status, AEROSPIKE_OK);
		//example_dump_record(rec);
		as_record_destroy(rec);
		rec = NULL;
	}
	as_key_destroy(&rkey);
}

TEST(map_wild, "ctx wild")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 23);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_record* rec = NULL;
	as_map_policy mode;

	as_map_policy_init(&mode);

	// Create map.
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);

	for (int i = 0; i < 4; i++) {
		char name[2] = "a";
		name[0] += i;

		as_string *key = as_string_new_strdup(name);

		as_arraylist *v = as_arraylist_new(2, 2);
		as_arraylist_append_int64(v, i);
		as_arraylist_append_int64(v, i * 10);
		as_hashmap_set(&item_map, (as_val*)key, (as_val*)v);
	}

	as_operations_init(&ops, 1);
	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;

	as_arraylist value_list;
	as_arraylist_init(&value_list, 3, 3);
	as_arraylist *list0 = as_arraylist_new(2, 2);
	as_arraylist *list1 = as_arraylist_new(2, 2);
	as_arraylist *list2 = as_arraylist_new(2, 2);

	as_arraylist_append_int64(list0, 1);
	as_arraylist_append_int64(list0, 10);

	as_arraylist_append_int64(list1, 2);
//	as_arraylist_append_int64(list1, 20);
	as_arraylist_append(list1, (as_val*)&as_cmp_wildcard);

	as_arraylist_append_int64(list2, 3);
//	as_arraylist_append_int64(list2, 30);
	as_arraylist_append(list2, (as_val*)&as_cmp_wildcard);

	as_arraylist_append_list(&value_list, (as_list*)list0);
	as_arraylist_append_list(&value_list, (as_list*)list1);
	as_arraylist_append_list(&value_list, (as_list*)list2);

	as_operations_init(&ops, 1);
	as_operations_add_map_get_by_value_list(&ops, BIN_NAME, (as_list*)&value_list, AS_MAP_RETURN_KEY_VALUE);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
}

TEST(map_create, "Map create")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 24);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap m1;
	as_hashmap_init(&m1, 2);
	as_string k11;
	as_string_init(&k11, "key11", false);
	as_integer v11;
	as_integer_init(&v11, 9);
	as_hashmap_set(&m1, (as_val*)&k11, (as_val*)&v11);
	as_string k12;
	as_string_init(&k12, "key12", false);
	as_integer v12;
	as_integer_init(&v12, 4);
	as_hashmap_set(&m1, (as_val*)&k12, (as_val*)&v12);

	as_hashmap m2;
	as_hashmap_init(&m2, 2);
	as_string k21;
	as_string_init(&k21, "key21", false);
	as_integer v21;
	as_integer_init(&v21, 3);
	as_hashmap_set(&m2, (as_val*)&k21, (as_val*)&v21);
	as_string k22;
	as_string_init(&k22, "key22", false);
	as_integer v22;
	as_integer_init(&v22, 5);
	as_hashmap_set(&m2, (as_val*)&k22, (as_val*)&v22);

	as_hashmap map;
	as_hashmap_init(&map, 2);
	as_string k1;
	as_string_init(&k1, "key1", false);
	as_hashmap_set(&map, (as_val*)&k1, (as_val*)&m1);
	as_string k2;
	as_string_init(&k2, "key2", false);
	as_hashmap_set(&map, (as_val*)&k2, (as_val*)&m2);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_cdt_ctx ctx;
	as_cdt_ctx_inita(&ctx, 1);
	as_string k3;
	as_string_init(&k3, "key3", false);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)&k3);

	as_operations ops;
	as_operations_inita(&ops, 3);

	as_operations_map_create_all(&ops, BIN_NAME, &ctx, AS_MAP_KEY_ORDERED, true);
	as_string k31;
	as_string_init(&k31, "key31", false);
	as_integer v31;
	as_integer_init(&v31, 99);
	as_operations_map_put(&ops, BIN_NAME, &ctx, NULL, (as_val*)&k31, (as_val*)&v31);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;
	as_map* mapr = &results[2].valuep->map;
	assert_int_eq(as_map_size(mapr), 3);

	as_string_init(&k3, "key3", false);
	as_map* mr = (as_map*)as_map_get(mapr, (as_val*)&k3);

	as_string_init(&k31, "key31", false);
	as_integer* ir = (as_integer*)as_map_get(mr, (as_val*)&k31);
	assert_int_eq(ir->value, 99);

	as_record_destroy(prec);
}

TEST(map_exp_mod, "Map Modify Expression")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 25);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap map;
	as_hashmap_init(&map, 10);

	for (int i = 0; i < 10; i++) {
		as_hashmap_set(&map, (as_val*)as_integer_new(10 * i),
				(as_val*)as_integer_new(100 * i));
	}

	as_record *rec = as_record_new(1);
	as_record_set_map(rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_policy_read p;
	as_policy_read_init(&p);

	as_hashmap add;
	as_hashmap_init(&add, 2);
	as_hashmap_set(&add, (as_val*)as_integer_new(1), (as_val*)as_integer_new(2));
	as_hashmap_set(&add, (as_val*)as_integer_new(2), (as_val*)as_integer_new(4));

	as_arraylist rem;
	as_arraylist_init(&rem, 2, 2);
	as_arraylist_append_int64(&rem, 10);
	as_arraylist_append_int64(&rem, 20);

	as_exp_build(filter1,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_put(NULL, NULL, as_exp_int(5), as_exp_int(9),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(11)),
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_put_items(NULL, NULL, as_exp_val(&add),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(12)),
			as_exp_cmp_eq(
				as_exp_map_get_by_index(NULL, AS_MAP_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(5),
					as_exp_map_increment(NULL, NULL, as_exp_int(50), as_exp_int(1),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(501)),
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_clear(NULL,
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_remove_by_value(NULL, AS_MAP_RETURN_NONE, as_exp_int(700),
						as_exp_map_remove_by_key_range(NULL, AS_MAP_RETURN_NONE, as_exp_int(40), as_exp_int(51),
							as_exp_map_remove_by_key_list(NULL, AS_MAP_RETURN_NONE, as_exp_val(&rem),
								as_exp_map_remove_by_key(NULL, as_exp_int(0),
									as_exp_bin_map(BIN_NAME)))))),
				as_exp_int(4))));

	as_hashmap_destroy(&add);
	as_arraylist_destroy(&rem);
	assert_not_null(filter1);

	p.base.filter_exp = filter1;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter1);

	as_arraylist_init(&rem, 2, 2);
	as_arraylist_append_int64(&rem, 100);
	as_arraylist_append_int64(&rem, 200);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_remove_by_key_rel_index_range_to_end(NULL, AS_MAP_RETURN_NONE, as_exp_int(50), as_exp_int(1),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(6)),
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_remove_by_key_rel_index_range(NULL, AS_MAP_RETURN_NONE, as_exp_int(50), as_exp_int(1), as_exp_int(2),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(8)),
			as_exp_cmp_eq(
				as_exp_map_get_by_rank(NULL, AS_MAP_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(-1),
					as_exp_map_remove_by_value_list(NULL, AS_MAP_RETURN_NONE, as_exp_val(&rem),
						as_exp_map_remove_by_value_range(NULL, AS_MAP_RETURN_NONE, as_exp_int(400), as_exp_int(701),
							as_exp_map_remove_by_value_rel_rank_range_to_end(NULL, AS_MAP_RETURN_NONE, as_exp_int(700), as_exp_int(1),
								as_exp_bin_map(BIN_NAME))))),
				as_exp_int(300)),
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_remove_by_index_range(NULL, AS_MAP_RETURN_NONE, as_exp_int(0), as_exp_int(1),
						as_exp_map_remove_by_index(NULL, as_exp_int(0),
							as_exp_map_remove_by_value_rel_rank_range(NULL, AS_MAP_RETURN_NONE, as_exp_int(500), as_exp_int(2), as_exp_int(4),
								as_exp_bin_map(BIN_NAME))))),
				as_exp_int(5)),
			as_exp_cmp_eq(
				as_exp_map_size(NULL,
					as_exp_map_remove_by_rank_range(NULL, AS_MAP_RETURN_NONE, as_exp_int(1), as_exp_int(2),
						as_exp_map_remove_by_rank(NULL, as_exp_int(-1),
							as_exp_map_remove_by_rank_range_to_end(NULL, AS_MAP_RETURN_NONE, as_exp_int(-2),
								as_exp_map_remove_by_index_range_to_end(NULL, AS_MAP_RETURN_NONE, as_exp_int(7),
									as_exp_bin_map(BIN_NAME)))))),
				as_exp_int(2))));

	as_arraylist_destroy(&rem);
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter2);
}

TEST(map_exp_read, "Map Read Expression")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 25);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap map;
	as_hashmap_init(&map, 10);

	for (int i = 0; i < 30; i++) {
		as_hashmap_set(&map, (as_val*)as_integer_new(10 * i),
				(as_val*)as_integer_new(100 * i));
	}

	as_record *rec = as_record_new(1);
	as_record_set_map(rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_policy_read p;
	as_policy_read_init(&p);

	as_arraylist expected;
	as_arraylist_init(&expected, 5, 5);
	as_arraylist_append_int64(&expected, 40);
	as_arraylist_append_int64(&expected, 50);
	as_arraylist_append_int64(&expected, 60);
	as_arraylist_append_int64(&expected, 70);
	as_arraylist_append_int64(&expected, 80);

	as_arraylist values;
	as_arraylist_init(&values, 5, 5);
	as_arraylist_append_int64(&values, 400);
	as_arraylist_append_int64(&values, 500);
	as_arraylist_append_int64(&values, 600);
	as_arraylist_append_int64(&values, 700);
	as_arraylist_append_int64(&values, 800);

	as_exp_build(filter1,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_key_range(NULL, AS_MAP_RETURN_KEY, as_exp_int(40), as_exp_int(90),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_key_list(NULL, AS_MAP_RETURN_KEY, as_exp_val(&expected),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_get_by_rank(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
					as_exp_map_get_by_key_rel_index_range_to_end(NULL, AS_MAP_RETURN_KEY, as_exp_int(30), as_exp_int(2),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(50)),
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_key_rel_index_range(NULL, AS_MAP_RETURN_KEY, as_exp_int(30), as_exp_int(1), as_exp_int(5),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_value_range(NULL, AS_MAP_RETURN_KEY, as_exp_int(400), as_exp_int(900),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_value_list(NULL, AS_MAP_RETURN_KEY, as_exp_val(&values),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected))));

	as_arraylist_destroy(&values);
	assert_not_null(filter1);

	p.base.filter_exp = filter1;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter1);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
					as_exp_list_get_by_rank(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
					as_exp_map_get_by_value_rel_rank_range_to_end(NULL, AS_MAP_RETURN_KEY, as_exp_int(300), as_exp_int(2),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(50)),
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_value_rel_rank_range(NULL, AS_MAP_RETURN_KEY, as_exp_int(300), as_exp_int(1), as_exp_int(5),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_get_by_rank(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
					as_exp_map_get_by_index_range_to_end(NULL, AS_MAP_RETURN_VALUE, as_exp_int(4),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(400)),
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_index_range(NULL, AS_MAP_RETURN_KEY, as_exp_int(4), as_exp_int(5),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_get_by_rank(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
					as_exp_map_get_by_rank_range_to_end(NULL, AS_MAP_RETURN_KEY, as_exp_int(4),
						as_exp_bin_map(BIN_NAME))),
				as_exp_int(40)),
			as_exp_cmp_eq(
				as_exp_list_sort(NULL, AS_LIST_SORT_DEFAULT,
					as_exp_map_get_by_rank_range(NULL, AS_MAP_RETURN_KEY, as_exp_int(4), as_exp_int(5),
						as_exp_bin_map(BIN_NAME))),
				as_exp_val(&expected))));

	as_arraylist_destroy(&expected);
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter2);
}

TEST(map_exp, "Map Expression")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 26);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap map;
	as_hashmap_init(&map, 10);

	as_hashmap_set(&map, (as_val*)as_string_new("A", false), (as_val*)as_string_new("AAA", false));
	as_hashmap_set(&map, (as_val*)as_string_new("B", false), (as_val*)as_string_new("BBB", false));
	as_hashmap_set(&map, (as_val*)as_string_new("C", false), (as_val*)as_string_new("BBB", false));

	as_record *rec = as_record_new(1);
	as_record_set_map(rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_policy_read p;
	as_policy_read_init(&p);

	as_arraylist values;
	as_arraylist_init(&values, 5, 5);
	as_arraylist_append_str(&values, "A");
	as_arraylist_append_str(&values, "C");

	as_exp_build(filter1,
		as_exp_cmp_eq(
			as_exp_map_size(NULL,
				as_exp_map_get_by_key_list(NULL, AS_MAP_RETURN_KEY_VALUE, as_exp_val(&values),
					as_exp_bin_map(BIN_NAME))),
			as_exp_int(2)));

	as_arraylist_destroy(&values);
	assert_not_null(filter1);

	p.base.filter_exp = filter1;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter1);
}

TEST(map_ordered_result, "Map with ordered results")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 27);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_map_policy pol;
	as_map_policy_init(&pol);
	as_operations ops;
	as_record* rec = NULL;

	for (int i = 10; i > 0; i--) {
		as_operations_init(&ops, 1);
		as_integer k;
		as_integer v;
		as_integer_init(&k, i);
		as_integer_init(&v, i);
		as_operations_add_map_put(&ops, BIN_NAME, &pol, (as_val*)&k, (as_val*)&v);
		status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
		assert_int_eq(status, AEROSPIKE_OK);
		as_operations_destroy(&ops);
		as_record_destroy(rec);
		rec = NULL;
	}

	as_integer mkey1;
	as_integer mkey2;
	as_integer_init(&mkey1, 2);
	as_integer_init(&mkey2, 5);

	as_operations_init(&ops, 1);
	as_operations_add_map_get_by_key_range(&ops, BIN_NAME, (as_val*)&mkey1, (as_val*)&mkey2, AS_MAP_RETURN_ORDERED_MAP);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	as_bin* results = rec->bins.entries;
	as_map* map0 = &results[0].valuep->map;
	assert_int_eq(map0->flags, 1);
	assert_int_eq(as_map_size(map0), 3);

//	example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;
}

TEST(ordered_map_eq_exp, "Ordered Map Equality Expression")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 28);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_orderedmap map;
	as_orderedmap_init(&map, 10);

	as_orderedmap_set(&map, (as_val*)as_string_new("key3", false), (as_val*)as_string_new("c", false));
	as_orderedmap_set(&map, (as_val*)as_string_new("key1", false), (as_val*)as_string_new("e", false));
	as_orderedmap_set(&map, (as_val*)as_string_new("key5", false), (as_val*)as_string_new("a", false));
	as_orderedmap_set(&map, (as_val*)as_string_new("key4", false), (as_val*)as_string_new("b", false));
	as_orderedmap_set(&map, (as_val*)as_string_new("key2", false), (as_val*)as_string_new("d", false));

	// Reserve map because as_record_destroy() would otherwise delete it.
	as_val_reserve((as_val*)&map);

	as_record* rec = as_record_new(1);
	as_record_set_map(rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);

	as_policy_read p;
	as_policy_read_init(&p);

	as_exp_build(filter, as_exp_cmp_eq(as_exp_bin_map(BIN_NAME), as_exp_val(&map)));
	assert_not_null(filter);
	p.base.filter_exp = filter;

	as_orderedmap_destroy(&map);

	rec = NULL;
	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(rec->bins.size, 1);
	assert_int_eq(rec->bins.entries[0].valuep->map._.type, AS_MAP);
	assert_int_eq(rec->bins.entries[0].valuep->map.flags, 1);

	//example_dump_record(rec);
	as_record_destroy(rec);
	as_exp_destroy(filter);
}


TEST(map_inverted_exp, "Map Inverted Expression")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 29);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap map;
	as_hashmap_init(&map, 4);
	as_stringmap_set_int64((as_map*)&map, "a", 1);
	as_stringmap_set_int64((as_map*)&map, "b", 2);
	as_stringmap_set_int64((as_map*)&map, "c", 2);
	as_stringmap_set_int64((as_map*)&map, "d", 3);

	const char* bin_name = "smap";
	as_record rec;
	as_record_init(&rec, 1);
	as_record_set_map(&rec, bin_name, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	// Use INVERTED to remove all entries where value != 2.
	as_exp_build(expr, as_exp_map_remove_by_value(NULL, AS_MAP_RETURN_INVERTED, as_exp_int(2),
		as_exp_bin_map(bin_name)));

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, bin_name, expr, AS_EXP_READ_DEFAULT);

	as_record* results = NULL;

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &results);
	assert_int_eq(status, AEROSPIKE_OK);

	as_map* map_result = as_record_get_map(results, bin_name);
	assert_int_eq(as_map_size(map_result), 2);

	as_string s1;
	as_string_init(&s1, "b", false);
	as_val* v1 = as_map_get(map_result, (as_val*)&s1);
	assert_int_eq(v1->type, AS_INTEGER);
	assert_int_eq(((as_integer*)v1)->value, 2);

	as_string s2;
	as_string_init(&s2, "c", false);
	as_val* v2 = as_map_get(map_result, (as_val*)&s2);
	assert_int_eq(v2->type, AS_INTEGER);
	assert_int_eq(((as_integer*)v2)->value, 2);

	as_operations_destroy(&ops);
	as_exp_destroy(expr);
	as_record_destroy(results);
}

TEST(map_self_correct, "Test Map put with wrong order and compactness")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 30);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t buf[4096];
	as_bytes b;

	// Test out of order rejection.
	as_packer pk = {
			.buffer = buf,
			.capacity = sizeof(buf)
	};

	as_pack_map_header(&pk, 6);

	as_pack_ext_header(&pk, 0, AS_MAP_KEY_ORDERED);
	as_pack_nil(&pk);

	for (int i = 0; i < 5; i++) {
		as_pack_int64(&pk, 5 - i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	as_record* rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_ne(status, AEROSPIKE_OK); // rejected
	as_record_destroy(rec);
	rec = NULL;

	// Test compactify.
	pk.offset = 0;
	as_pack_map_header(&pk, 6);

	as_pack_ext_header(&pk, 0, AS_MAP_KEY_ORDERED);
	as_pack_nil(&pk);

	as_pack_int64(&pk, 0);
	as_pack_int64(&pk, 0x100000000);

	uint64_t* p64 = (uint64_t*)(pk.buffer + pk.offset - sizeof(uint64_t));
	*p64 = 0; // set it to 0 overpacked as a uint64_t so it cost 9 bytes instead of 1

	for (int i = 1; i < 5; i++) {
		as_pack_int64(&pk, i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	as_policy_read rp;
	as_policy_read_init(&rp);
	rp.deserialize = false;
	status = aerospike_key_get(as, &err, &rp, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_bytes *rb = as_record_get_bytes(rec, BIN_NAME);
	assert_int_eq(as_bytes_size(rb), 15);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Test compactify 2.
	pk.offset = 0;
	as_pack_map_header(&pk, 6);

	as_pack_ext_header(&pk, 0, AS_MAP_KEY_ORDERED);
	as_pack_nil(&pk);

	as_pack_int64(&pk, 0);
	as_pack_int64(&pk, -0x100000000);

	p64 = (uint64_t*)(pk.buffer + pk.offset - sizeof(uint64_t));
	*p64 = cf_swap_to_be64(4294967295);

	as_pack_int64(&pk, 1);
	as_pack_int64(&pk, -0x100000000);

	p64 = (uint64_t*)(pk.buffer + pk.offset - sizeof(uint64_t));
	*p64 = cf_swap_to_be64(4294967296);

	for (int i = 2; i < 5; i++) {
		as_pack_int64(&pk, i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);
	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, &rp, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	rb = as_record_get_bytes(rec, BIN_NAME);
	assert_int_eq(as_bytes_size(rb), 27);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Test incorrect offset index.
	pk.offset = 0;
	as_pack_map_header(&pk, 6);

	uint8_t idxbuf[10];

	for (size_t i = 0; i < sizeof(idxbuf); i++) {
		idxbuf[i] = (uint8_t)i + 55;
	}

	as_pack_ext_header(&pk, sizeof(idxbuf), AS_MAP_KEY_ORDERED);
	as_pack_append(&pk, idxbuf, sizeof(idxbuf));
	as_pack_nil(&pk);

	for (int i = 0; i < 5; i++) {
		as_pack_int64(&pk, i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, &rp, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	rb = as_record_get_bytes(rec, BIN_NAME);
	assert_int_eq(as_bytes_size(rb), 15);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Test padding rejection.
	pk.offset = 0;
	as_pack_map_header(&pk, 6);

	as_pack_ext_header(&pk, 0, AS_MAP_KEY_ORDERED);
	as_pack_nil(&pk);

	for (int i = 0; i < 5; i++) {
		as_pack_int64(&pk, i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset + 1, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_ne(status, AEROSPIKE_OK);
	as_record_destroy(rec);
}

// Add flag for the purpose of tests that bypass user functions and use low-level wire protocol.
#define AS_MAP_FLAG_PERSIST_INDEX 0x10

TEST(map_persist_index, "Test Map Persist Index")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 31);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t buf[4096];
	as_bytes b;

	// Test out of order rejection.
	as_packer pk = {
			.buffer = buf,
			.capacity = sizeof(buf)
	};

	as_pack_map_header(&pk, 6);

	as_pack_ext_header(&pk, 0, AS_MAP_KEY_ORDERED | AS_MAP_FLAG_PERSIST_INDEX);
	as_pack_nil(&pk);

	for (int i = 0; i < 5; i++) {
		as_pack_int64(&pk, 5 - i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	as_record* rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_ne(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Test compactify.
	pk.offset = 0;
	as_pack_map_header(&pk, 6);

	as_pack_ext_header(&pk, 0, AS_MAP_KEY_ORDERED | AS_MAP_FLAG_PERSIST_INDEX);
	as_pack_nil(&pk);

	as_pack_int64(&pk, 0);
	as_pack_int64(&pk, 0x100000000);

	uint64_t* p64 = (uint64_t*)(pk.buffer + pk.offset - sizeof(uint64_t));
	*p64 = 0; // set it to 0 overpacked as a uint64_t so it cost 9 bytes instead of 1

	for (int i = 1; i < 5; i++) {
		as_pack_int64(&pk, i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	as_policy_read rp;
	as_policy_read_init(&rp);
	rp.deserialize = false;
	status = aerospike_key_get(as, &err, &rp, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_bytes *rb = as_record_get_bytes(rec, BIN_NAME);
	assert_int_eq(as_bytes_size(rb), 15);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Test incorrect offset index.
	pk.offset = 0;
	as_pack_map_header(&pk, 6);

	uint8_t idxbuf[10];

	for (size_t i = 0; i < sizeof(idxbuf); i++) {
		idxbuf[i] = (uint8_t)i + 55;
	}

	as_pack_ext_header(&pk, sizeof(idxbuf), AS_MAP_KEY_ORDERED | AS_MAP_FLAG_PERSIST_INDEX);
	as_pack_append(&pk, idxbuf, sizeof(idxbuf));
	as_pack_nil(&pk);

	for (int i = 0; i < 5; i++) {
		as_pack_int64(&pk, i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, &rp, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	rb = as_record_get_bytes(rec, BIN_NAME);
	assert_int_eq(as_bytes_size(rb), 15);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Test padding rejection.
	pk.offset = 0;
	as_pack_map_header(&pk, 6);

	as_pack_ext_header(&pk, 0, AS_MAP_KEY_ORDERED | AS_MAP_FLAG_PERSIST_INDEX);
	as_pack_nil(&pk);

	for (int i = 0; i < 5; i++) {
		as_pack_int64(&pk, i); // key
		as_pack_int64(&pk, i); // value
	}

	as_bytes_init_wrap(&b, buf, pk.offset + 1, false);
	as_bytes_set_type(&b, AS_BYTES_MAP);

	rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_ne(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Test ctx create.
	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK);

	as_arraylist list;
	as_arraylist_init(&list, 1, 0);
	as_arraylist_append_int64(&list, 1);

	as_operations ops;
	as_operations_init(&ops, 2);
	as_operations_map_create_all(&ops, BIN_NAME, NULL, AS_MAP_KEY_ORDERED, true);
	as_operations_map_put(&ops, BIN_NAME, NULL, NULL, (as_val*)as_integer_new(0), (as_val*)&list);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_map* m = as_record_get_map(rec, BIN_NAME);
	example_dump_record(rec);
	assert_int_eq(m->flags, AS_MAP_KEY_ORDERED);
	as_record_destroy(rec);
	rec = NULL;

	// Test map create sub presist rejection.
	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_UNORDERED, false);
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)as_integer_new(0), AS_MAP_KEY_ORDERED);
	as_cdt_ctx_item* hack_item = as_vector_get(&ctx.list, ctx.list.size - 1);
	hack_item->type |= 0x100; // hack in a persist flag, do not do this normally

	as_operations_init(&ops, 1);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(1));

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_ne(status, AEROSPIKE_OK); // rejected
	as_record_destroy(rec);
	rec = NULL;
	as_operations_destroy(&ops);
	as_cdt_ctx_destroy(&ctx);

	// Test map create sub presist rejection 2.
	as_arraylist_init(&list, 1, 0);
	as_arraylist_append_int64(&list, 1);
	as_operations_init(&ops, 2);
	as_operations_map_create_all(&ops, BIN_NAME, NULL, AS_MAP_UNORDERED, true);
	as_operations_map_put(&ops, BIN_NAME, NULL, NULL, (as_val*)as_integer_new(0), (as_val*)&list);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;

	as_cdt_ctx_init(&ctx, 3);
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)as_integer_new(1), AS_MAP_FLAG_PERSIST_INDEX);
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)as_integer_new(1), AS_MAP_FLAG_PERSIST_INDEX);
	hack_item = as_vector_get(&ctx.list, ctx.list.size - 1);
	hack_item->type |= 0x100; // hack in a persist flag, do not do this normally
	as_cdt_ctx_add_map_key_create(&ctx, (as_val*)as_integer_new(2), AS_MAP_FLAG_PERSIST_INDEX);
	hack_item = as_vector_get(&ctx.list, ctx.list.size - 1);
	hack_item->type |= 0x100; // hack in a persist flag, do not do this normally

	as_operations_init(&ops, 1);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(1));

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_ne(status, AEROSPIKE_OK); // should be rejected
	as_record_destroy(rec);
	rec = NULL;
	as_operations_destroy(&ops);
	as_cdt_ctx_destroy(&ctx);

	// Test set flags.
	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK);

	as_orderedmap m1;
	as_orderedmap_init(&m1, 4);
	as_orderedmap_set(&m1, (as_val*)as_string_new("a1", false), (as_val*)as_integer_new(1));
	as_orderedmap_set(&m1, (as_val*)as_string_new("b1", false), (as_val*)as_integer_new(2));
	as_orderedmap_set(&m1, (as_val*)as_string_new("c1", false), (as_val*)as_integer_new(3));
	as_orderedmap_set(&m1, (as_val*)as_string_new("pk1", false), (as_val*)as_string_new("231108133342353844", false));

	rec = as_record_new(1);
	as_record_set_map(rec, BIN_NAME, (as_map*)&m1);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_true(status == AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, &rp, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	rb = as_record_get_bytes(rec, BIN_NAME);
	uint32_t check_size = as_bytes_size(rb);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	as_map_policy pol;
	as_map_policy_init(&pol);
	as_map_policy_set_all(&pol, AS_MAP_UNORDERED, 0, true);
	as_operations_init(&ops, 1);
	as_operations_add_map_set_policy(&ops, BIN_NAME, &pol);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_true(status == AEROSPIKE_OK);
	as_record_destroy(rec);
	as_operations_destroy(&ops);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, &rp, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	rb = as_record_get_bytes(rec, BIN_NAME);
	assert_int_eq(as_bytes_size(rb), check_size - 4); // -4 for meta header
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(map_basics, "aerospike map basic tests")
{
	suite_add(map_put);
	suite_add(map_put_items);
	suite_add(map_mixed);
	suite_add(map_switch);
	suite_add(map_rank);
	suite_add(map_remove);
	suite_add(map_remove_range);
	suite_add(map_clear);
	suite_add(map_score);
	suite_add(map_remove_non_exist);
	suite_add(map_replace_unfilled);
	suite_add(map_get_by_list);
	suite_add(map_inverted);
	suite_add(map_get_relative);
	suite_add(map_remove_relative);
	suite_add(map_partial);
	suite_add(map_nested);
	suite_add(map_double_nested);
	suite_add(map_ctx_create);
	suite_add(map_simple2);
	suite_add(map_simple3);
	suite_add(map_ctx_create2);
	suite_add(map_ctx_create3);
	suite_add(map_wild);
	suite_add(map_create);
	suite_add(map_exp_mod);
	suite_add(map_exp_read);
	suite_add(map_exp);
	suite_add(map_ordered_result);
	suite_add(ordered_map_eq_exp);
	suite_add(map_inverted_exp);
	suite_add(map_self_correct);
	suite_add(map_persist_index);
}
