/*
 * Copyright 2008-2020 Aerospike, Inc.
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
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_record.h>

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
#define SET "test_hll"
#define BIN_NAME "hllbin"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(hll_init, "hll init")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 100);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_init(&ops, BIN_NAME, NULL, NULL, 10);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);
	as_record_destroy(prec);
}

TEST(hll_ops, "hll ops")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 101);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_str(&list, "key1");
	as_arraylist_append_str(&list, "key2");
	as_arraylist_append_str(&list, "key3");

	as_arraylist list2;
	as_arraylist_init(&list2, 1, 0);
	as_arraylist_append_str(&list2, "another val");

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_hll_add(&ops, BIN_NAME, NULL, NULL, (as_list*)&list, 8);
	as_arraylist_destroy(&list);
	as_operations_hll_update(&ops, BIN_NAME, NULL, NULL, (as_list*)&list2);
	as_arraylist_destroy(&list2);
	as_operations_hll_fold(&ops, BIN_NAME, NULL, 6);
	as_operations_hll_refresh_count(&ops, BIN_NAME, NULL);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;
	as_bytes* hllbin = (as_bytes*)results[4].valuep;
	assert_not_null(hllbin);

	// Reserve hllbin for future processing.
	as_val_reserve((as_val*)hllbin);
	uint8_t* bytes = hllbin->value;
	uint32_t len = hllbin->size;

	as_record_destroy(prec);

	// Create hll bytes from reserved hllbin.
	as_bytes hll;
	as_bytes_init_wrap(&hll, bytes, len, true);

	as_arraylist hlls;
	as_arraylist_init(&hlls, 1, 0);
	as_arraylist_append_bytes(&hlls, &hll);

	// Pass in hll list to set union.
	as_operations_inita(&ops, 7);
	as_operations_hll_set_union(&ops, BIN_NAME, NULL, NULL, (as_list*)&hlls);
	as_operations_hll_get_union(&ops, BIN_NAME, NULL, (as_list*)&hlls);
	as_operations_hll_get_union_count(&ops, BIN_NAME, NULL, (as_list*)&hlls);
	as_operations_hll_get_intersect_count(&ops, BIN_NAME, NULL, (as_list*)&hlls);
	as_operations_hll_get_similarity(&ops, BIN_NAME, NULL, (as_list*)&hlls);
	as_operations_hll_describe(&ops, BIN_NAME, NULL);
	as_operations_hll_get_count(&ops, BIN_NAME, NULL);
	as_arraylist_destroy(&hlls);

	prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);
	as_record_destroy(prec);
}

TEST(hll_read_write, "hll read write")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 102);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	// Create HLL bin.
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_str(&list, "key1");
	as_arraylist_append_str(&list, "key2");
	as_arraylist_append_str(&list, "key3");

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_hll_add(&ops, BIN_NAME, NULL, NULL, (as_list*)&list, 8);
	as_arraylist_destroy(&list);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(prec);

	// Read HLL bin.
	prec = 0;
	status = aerospike_key_get(as, &err, NULL, &key, &prec);
    assert_int_eq(status, AEROSPIKE_OK);
	
	as_bytes* bytes_hll = as_record_get_bytes(prec, BIN_NAME);
	assert_not_null(bytes_hll);
	assert_int_eq(bytes_hll->type, AS_BYTES_HLL);

	// Reserve HLL bytes for future processing.
	as_val_reserve((as_val*)bytes_hll);
	uint8_t* bytes = bytes_hll->value;
	uint32_t len = bytes_hll->size;
	as_record_destroy(prec);

	// Write HLL value to another bin
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, "bin2", bytes, len, true);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	// Read both bins.
	prec = 0;
	status = aerospike_key_get(as, &err, NULL, &key, &prec);
    assert_int_eq(status, AEROSPIKE_OK);

	// Compare bins.
	as_bytes* b1 = as_record_get_bytes(prec, BIN_NAME);
	as_bytes* b2 = as_record_get_bytes(prec, "bin2");
	//example_dump_record(prec);
	assert_int_eq(b1->size, b2->size);

	int rv = memcmp(b1->value, b2->value, b1->size);
    assert_int_eq(rv, 0);
	as_record_destroy(prec);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(hll, "HLL tests")
{
	suite_add(hll_init);
	suite_add(hll_ops);
	suite_add(hll_read_write);
}
