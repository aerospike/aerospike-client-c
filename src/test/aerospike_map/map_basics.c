/*
 * Copyright 2008-2018 Aerospike, Inc.
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
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_map.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
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

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

bool
has_cdt_map()
{
	char *res = NULL;
	as_error err;
	int rc = aerospike_info_any(as, &err, NULL, "features", &res);

	if (rc == AEROSPIKE_OK) {
		char *st = strstr(res, "cdt-map");
		free(res);

		if (st) {
			return true;
		}
	}
	return false;
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

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(map_put, "Map put operations")
{
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 2);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 7);

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

	as_record_destroy(rec);
}

TEST(map_mixed, "Map mixed operations")
{
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 1);

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
	if (! has_cdt_map()) {
		info("cdt-map not enabled. skipping map tests.");
		return;
	}

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
}
