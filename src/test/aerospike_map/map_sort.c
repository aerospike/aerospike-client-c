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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>
#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
void example_dump_record(const as_record* p_rec);

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "map_sort"
#define BIN "bin1"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(map_sort_int, "sort map of integer keys")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "k2");

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

    as_hashmap map1;
    as_hashmap_init(&map1, 4);

    as_integer k11,k12,k13,k14;
    as_integer v11,v12,v13,v14;
    as_integer_init(&k11, 2000);
    as_integer_init(&v11, 1);
    as_integer_init(&k12, 1050);
    as_integer_init(&v12, 2);
    as_integer_init(&k13, 2500);
    as_integer_init(&v13, 3);
    as_integer_init(&k14, 1000);
    as_integer_init(&v14, 4);
    as_hashmap_set(&map1, (as_val*)&k11, (as_val*)&v11);
    as_hashmap_set(&map1, (as_val*)&k12, (as_val*)&v12);
    as_hashmap_set(&map1, (as_val*)&k13, (as_val*)&v13);
    as_hashmap_set(&map1, (as_val*)&k14, (as_val*)&v14);

    as_hashmap map2;
    as_hashmap_init(&map2, 4);

    as_integer k21,k22,k23,k24;
    as_integer v21,v22,v23,v24;
    as_integer_init(&k21, 9000);
    as_integer_init(&v21, 1);
    as_integer_init(&k22, 6700);
    as_integer_init(&v22, 2);
    as_integer_init(&k23, 7000);
    as_integer_init(&v23, 3);
    as_integer_init(&k24, 6800);
    as_integer_init(&v24, 4);
    as_hashmap_set(&map2, (as_val*)&k21, (as_val*)&v21);
    as_hashmap_set(&map2, (as_val*)&k22, (as_val*)&v22);
    as_hashmap_set(&map2, (as_val*)&k23, (as_val*)&v23);
    as_hashmap_set(&map2, (as_val*)&k24, (as_val*)&v24);

    as_hashmap map3;
    as_hashmap_init(&map3, 4);

    as_integer k31,k32,k33,k34;
    as_integer v31,v32,v33,v34;
    as_integer_init(&k31, 3000);
    as_integer_init(&v31, 1);
    as_integer_init(&k32, 4000);
    as_integer_init(&v32, 2);
    as_integer_init(&k33, 3999);
    as_integer_init(&v33, 3);
    as_integer_init(&k34, 3500);
    as_integer_init(&v34, 4);
    as_hashmap_set(&map3, (as_val*)&k31, (as_val*)&v31);
    as_hashmap_set(&map3, (as_val*)&k32, (as_val*)&v32);
    as_hashmap_set(&map3, (as_val*)&k33, (as_val*)&v33);
    as_hashmap_set(&map3, (as_val*)&k34, (as_val*)&v34);

    as_map_policy mp;
    as_map_policy_init(&mp);
    as_map_policy_set(&mp, AS_MAP_KEY_ORDERED, 0);

    as_cdt_ctx ctx;
    as_cdt_ctx_init(&ctx, 1);
    as_cdt_ctx_add_list_index(&ctx, -1);

	as_val_reserve(&map2);

    as_operations ops;
    as_operations_inita(&ops, 6);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map1);
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map2); 
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map3); 
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_record* rec = NULL;
    status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);

	as_operations ops2;
    as_operations_inita(&ops2, 1);
	as_hashmap_set_flags(&map2, AS_MAP_KEY_ORDERED);
	as_operations_list_remove_by_value(&ops2, BIN, NULL, (as_val*)&map2, AS_LIST_RETURN_NONE);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops2, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops2);
	as_record_destroy(rec);

	rec = NULL;
    status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);

	as_list* list = as_record_get_list(rec, BIN);
	uint32_t size = as_list_size(list);
	assert_int_eq(size, 2);

	as_record_destroy(rec);
}

TEST(map_sort_string, "sort map of string keys")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "k2");

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

    as_hashmap map1;
    as_hashmap_init(&map1, 4);

    as_string k11,k12,k13,k14;
    as_integer v11,v12,v13,v14;
    as_string_init(&k11, "Bob", false);
    as_integer_init(&v11, 1);
    as_string_init(&k12, "Bob-2", false);
    as_integer_init(&v12, 2);
    as_string_init(&k13, "Bob-3", false);
    as_integer_init(&v13, 3);
    as_string_init(&k14, "Bob-4", false);
    as_integer_init(&v14, 4);
    as_hashmap_set(&map1, (as_val*)&k11, (as_val*)&v11);
    as_hashmap_set(&map1, (as_val*)&k12, (as_val*)&v12);
    as_hashmap_set(&map1, (as_val*)&k13, (as_val*)&v13);
    as_hashmap_set(&map1, (as_val*)&k14, (as_val*)&v14);

    as_hashmap map2;
    as_hashmap_init(&map2, 4);

    as_string k21,k22,k23,k24;
    as_integer v21,v22,v23,v24;
    as_string_init(&k21, "John", false);
    as_integer_init(&v21, 1);
    as_string_init(&k22, "John-2", false);
    as_integer_init(&v22, 2);
    as_string_init(&k23, "John-3", false);
    as_integer_init(&v23, 3);
    as_string_init(&k24, "John-4", false);
    as_integer_init(&v24, 4);
    as_hashmap_set(&map2, (as_val*)&k21, (as_val*)&v21);
    as_hashmap_set(&map2, (as_val*)&k22, (as_val*)&v22);
    as_hashmap_set(&map2, (as_val*)&k23, (as_val*)&v23);
    as_hashmap_set(&map2, (as_val*)&k24, (as_val*)&v24);

    as_hashmap map3;
    as_hashmap_init(&map3, 4);

    as_string k31,k32,k33,k34;
    as_integer v31,v32,v33,v34;
    as_string_init(&k31, "Harry", false);
    as_integer_init(&v31, 1);
    as_string_init(&k32, "Harry-2", false);
    as_integer_init(&v32, 2);
    as_string_init(&k33, "Harry-3", false);
    as_integer_init(&v33, 3);
    as_string_init(&k34, "Harry-4", false);
    as_integer_init(&v34, 4);
    as_hashmap_set(&map3, (as_val*)&k31, (as_val*)&v31);
    as_hashmap_set(&map3, (as_val*)&k32, (as_val*)&v32);
    as_hashmap_set(&map3, (as_val*)&k33, (as_val*)&v33);
    as_hashmap_set(&map3, (as_val*)&k34, (as_val*)&v34);

    as_map_policy mp;
    as_map_policy_init(&mp);
    as_map_policy_set(&mp, AS_MAP_KEY_ORDERED, 0);

    as_cdt_ctx ctx;
    as_cdt_ctx_init(&ctx, 1);
    as_cdt_ctx_add_list_index(&ctx, -1);

	as_val_reserve(&map2);

    as_operations ops;
    as_operations_inita(&ops, 6);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map1);
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map2); 
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map3); 
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_record* rec = NULL;
    status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);

	as_operations ops2;
    as_operations_inita(&ops2, 1);
	as_hashmap_set_flags(&map2, AS_MAP_KEY_ORDERED);
	as_operations_list_remove_by_value(&ops2, BIN, NULL, (as_val*)&map2, AS_LIST_RETURN_NONE);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops2, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops2);
	as_record_destroy(rec);

	rec = NULL;
    status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);

	as_list* list = as_record_get_list(rec, BIN);
	uint32_t size = as_list_size(list);
	assert_int_eq(size, 2);

	as_record_destroy(rec);
}

TEST(map_sort_bool, "sort map of bool keys")
{
	bool b1 = true;
	bool b2 = false;
	bool b3 = 99;

	assert_int_ne(b1, b2);
	assert_int_eq(b1, b3);
	assert_true(b1 - b3 == 0);
	assert_true(b3 - b1 == 0);
	assert_true(b2 - b1 == -1);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "k3");

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

    as_hashmap map1;
    as_hashmap_init(&map1, 4);

    as_boolean k11,k12;
    as_integer v11,v12;
    as_boolean_init(&k11, true);
    as_integer_init(&v11, 1);
    as_boolean_init(&k12, false);
    as_integer_init(&v12, 2);

    as_map_policy mp;
    as_map_policy_init(&mp);
    as_map_policy_set(&mp, AS_MAP_KEY_ORDERED, 0);

    as_cdt_ctx ctx;
    as_cdt_ctx_init(&ctx, 1);
    as_cdt_ctx_add_list_index(&ctx, -1);

	as_val_reserve(&map1);

    as_operations ops;
    as_operations_inita(&ops, 2);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map1);
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_record* rec = NULL;
    status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);

	as_operations ops2;
    as_operations_inita(&ops2, 1);
	as_hashmap_set_flags(&map1, AS_MAP_KEY_ORDERED);
	as_operations_list_remove_by_value(&ops2, BIN, NULL, (as_val*)&map1, AS_LIST_RETURN_NONE);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops2, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops2);
	as_record_destroy(rec);

	rec = NULL;
    status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);

	as_list* list = as_record_get_list(rec, BIN);
	uint32_t size = as_list_size(list);
	assert_int_eq(size, 0);

	as_record_destroy(rec);
}

TEST(map_sort_bytes, "sort map of byte array keys")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "k5");

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

    as_hashmap map1;
    as_hashmap_init(&map1, 6);

    as_bytes k11,k12,k13,k14,k15,k16;
    as_integer v11,v12,v13,v14,v15,v16;
	uint8_t b11[] = {111, 22, 3};
    as_bytes_init_wrap(&k11, b11, sizeof(b11), false);
    as_integer_init(&v11, 1);
	uint8_t b12[] = {111, 22};
    as_bytes_init_wrap(&k12, b12, sizeof(b12), false);
	as_integer_init(&v12, 2);
	uint8_t b13[] = {0};
    as_bytes_init_wrap(&k13, b13, 0, false);  // empty byte array.
    as_integer_init(&v13, 3);
	uint8_t b14[] = {111, 22, 2, 3};
    as_bytes_init_wrap(&k14, b14, sizeof(b14), false);
    as_integer_init(&v14, 4);
	uint8_t b15[] = {111, 21, 1};
    as_bytes_init_wrap(&k15, b15, sizeof(b15), false);
    as_integer_init(&v15, 5);
	uint8_t b16[] = {11, 22, 3};
    as_bytes_init_wrap(&k16, b16, sizeof(b16), false);
    as_integer_init(&v16, 6);
    as_hashmap_set(&map1, (as_val*)&k11, (as_val*)&v11);
    as_hashmap_set(&map1, (as_val*)&k12, (as_val*)&v12);
    as_hashmap_set(&map1, (as_val*)&k13, (as_val*)&v13);
    as_hashmap_set(&map1, (as_val*)&k14, (as_val*)&v14);
    as_hashmap_set(&map1, (as_val*)&k15, (as_val*)&v15);
    as_hashmap_set(&map1, (as_val*)&k16, (as_val*)&v16);

    as_map_policy mp;
    as_map_policy_init(&mp);
    as_map_policy_set(&mp, AS_MAP_KEY_ORDERED, 0);

    as_cdt_ctx ctx;
    as_cdt_ctx_init(&ctx, 1);
    as_cdt_ctx_add_list_index(&ctx, -1);

	as_val_reserve(&map1);

    as_operations ops;
    as_operations_inita(&ops, 2);

    as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map1);
    as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

    as_record* rec = NULL;
    status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);

	as_operations ops2;
    as_operations_inita(&ops2, 1);
	as_hashmap_set_flags(&map1, AS_MAP_KEY_ORDERED);
	as_operations_list_remove_by_value(&ops2, BIN, NULL, (as_val*)&map1, AS_LIST_RETURN_NONE);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops2, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops2);
	as_record_destroy(rec);

	rec = NULL;
    status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);

	as_list* list = as_record_get_list(rec, BIN);
	uint32_t size = as_list_size(list);
	assert_int_eq(size, 0);

	as_record_destroy(rec);
}

TEST(map_sort_mixed, "sort map of mixed type keys")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "k6");

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap map1;
	as_hashmap_init(&map1, 4);

	as_integer k11,k12;
	as_integer_init(&k11, 50);
	as_integer_init(&k12, 25);

	as_string k13,k14;
	as_string_init(&k13, "John", false);
	as_string_init(&k14, "Andrew", false);

	as_integer v11,v12,v13,v14;
	as_integer_init(&v11, 1);
	as_integer_init(&v12, 2);
	as_integer_init(&v13, 3);
	as_integer_init(&v14, 4);

	int r;
	r = as_hashmap_set(&map1, (as_val*)&k11, (as_val*)&v11);
	assert_int_eq(r, 0);
	r = as_hashmap_set(&map1, (as_val*)&k12, (as_val*)&v12);
	assert_int_eq(r, 0);
	r = as_hashmap_set(&map1, (as_val*)&k13, (as_val*)&v13);
	assert_int_eq(r, 0);
	r = as_hashmap_set(&map1, (as_val*)&k14, (as_val*)&v14);
	assert_int_eq(r, 0);

	as_map_policy mp;
	as_map_policy_init(&mp);
	as_map_policy_set(&mp, AS_MAP_KEY_ORDERED, 0);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, -1);

	as_val_reserve(&map1);

	as_operations ops;
	as_operations_inita(&ops, 4);

	as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map1);
	as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

	as_record* rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);

	as_operations ops2;
	as_operations_inita(&ops2, 1);
	as_hashmap_set_flags(&map1, AS_MAP_KEY_ORDERED);
	as_operations_list_remove_by_value(&ops2, BIN, NULL, (as_val*)&map1, AS_LIST_RETURN_NONE);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops2, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops2);
	as_record_destroy(rec);

	rec = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);

	as_list* list = as_record_get_list(rec, BIN);
	uint32_t size = as_list_size(list);
	assert_int_eq(size, 0);

	as_record_destroy(rec);
}

TEST(map_sort_kv, "sort map of mixed type keys and order by key and value")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "k7");

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap map1;
	as_hashmap_init(&map1, 4);

	as_integer k11,k12;
	as_integer_init(&k11, 50);
	as_integer_init(&k12, 25);

	as_string k13,k14;
	as_string_init(&k13, "John", false);
	as_string_init(&k14, "Andrew", false);

	as_integer v11,v12,v13,v14;
	as_integer_init(&v11, 1);
	as_integer_init(&v12, 2);
	as_integer_init(&v13, 3);
	as_integer_init(&v14, 4);

	int r;
	r = as_hashmap_set(&map1, (as_val*)&k11, (as_val*)&v11);
	assert_int_eq(r, 0);
	r = as_hashmap_set(&map1, (as_val*)&k12, (as_val*)&v12);
	assert_int_eq(r, 0);
	r = as_hashmap_set(&map1, (as_val*)&k13, (as_val*)&v13);
	assert_int_eq(r, 0);
	r = as_hashmap_set(&map1, (as_val*)&k14, (as_val*)&v14);
	assert_int_eq(r, 0);

	as_map_policy mp;
	as_map_policy_init(&mp);
	as_map_policy_set(&mp, AS_MAP_KEY_VALUE_ORDERED, 0);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, -1);

	as_val_reserve(&map1);

	as_operations ops;
	as_operations_inita(&ops, 4);

	as_operations_list_append(&ops, BIN, NULL, NULL, (as_val*)&map1);
	as_operations_map_set_policy(&ops, BIN, &ctx, &mp);

	as_record* rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	as_cdt_ctx_destroy(&ctx);

	as_operations ops2;
	as_operations_inita(&ops2, 1);
	as_hashmap_set_flags(&map1, AS_MAP_KEY_VALUE_ORDERED);
	as_operations_list_remove_by_value(&ops2, BIN, NULL, (as_val*)&map1, AS_LIST_RETURN_NONE);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops2, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops2);
	as_record_destroy(rec);

	rec = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);

	as_list* list = as_record_get_list(rec, BIN);
	uint32_t size = as_list_size(list);
	assert_int_eq(size, 0);

	as_record_destroy(rec);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(map_sort, "map sort tests")
{
	suite_add(map_sort_int);
	suite_add(map_sort_string);
	suite_add(map_sort_bool);
	suite_add(map_sort_bytes);
	suite_add(map_sort_mixed);
	suite_add(map_sort_kv);
}
