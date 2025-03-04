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
#include <aerospike/as_exp.h>
#include <aerospike/as_bit_operations.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_record.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_bit"
#define BIN_NAME "bitbin"

/******************************************************************************
 * TYPES
 *****************************************************************************/

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

void example_dump_record(const as_record* p_rec);

as_key REC_KEY;

static bool
before(atf_suite* suite)
{
	as_node* node = as_node_get_random(as->cluster);

	if (! node) {
		return false;
	}

	as_node_release(node);

	as_key_init_int64(&REC_KEY, NAMESPACE, SET, 117);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &REC_KEY);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		return false;
	}

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};
	as_record rec;
	as_record_inita(&rec, 1);

	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);
	status = aerospike_key_put(as, &err, NULL, &REC_KEY, &rec);
	as_record_destroy(&rec);

	if (status != AEROSPIKE_OK) {
		return false;
	}

	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(bit_resize, "Bit Resize")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 100);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_resize(&ops, BIN_NAME, NULL, NULL, 4, AS_BIT_RESIZE_DEFAULT);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42, 0x00, 0x00};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_insert, "Bit Insert")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 101);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	uint8_t ins[] = {0xFF, 0xC7};
	as_operations_bit_insert(&ops, BIN_NAME, NULL, NULL, 1, sizeof(ins), ins);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0xFF, 0xC7, 0x42, 0x03, 0x04, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_remove, "Bit Remove")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 102);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_remove(&ops, BIN_NAME, NULL, NULL, 2, 3);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_set, "Bit Set")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 103);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	uint8_t bset[] = {0xE0};
	as_operations_bit_set(&ops, BIN_NAME, NULL, NULL, 13, 3, sizeof(bset), bset);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x47, 0x03, 0x04, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_or, "Bit OR")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 104);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	uint8_t bor[] = {0xA8};
	as_operations_bit_or(&ops, BIN_NAME, NULL, NULL, 17, 6, sizeof(bor), bor);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42, 0x57, 0x04, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_xor, "Bit XOR")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 105);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	uint8_t bxor[] = {0xAC};
	as_operations_bit_xor(&ops, BIN_NAME, NULL, NULL, 17, 6, sizeof(bxor), bxor);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42, 0x55, 0x04, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_and, "Bit AND")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 106);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	uint8_t band[] = {0x3C, 0x80};
	as_operations_bit_and(&ops, BIN_NAME, NULL, NULL, 23, 9, sizeof(band), band);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42, 0x02, 0x00, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_not, "Bit Not")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 107);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_not(&ops, BIN_NAME, NULL, NULL, 25, 6);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = { 0x01, 0x42, 0x03, 0x7A, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_lshift, "Bit Left Shift")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 108);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_lshift(&ops, BIN_NAME, NULL, NULL, 32, 8, 3);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42, 0x03, 0x04, 0x28};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_rshift, "Bit Right Shift")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 109);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_rshift(&ops, BIN_NAME, NULL, NULL, 0, 9, 1);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x00, 0xC2, 0x03, 0x04, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_add, "Bit Add")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 110);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_add(&ops, BIN_NAME, NULL, NULL, 24, 16, 128, false, AS_BIT_OVERFLOW_FAIL);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42, 0x03, 0x04, 0x85};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_subtract, "Bit Subtract")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 111);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_subtract(&ops, BIN_NAME, NULL, NULL, 24, 16, 128, false, AS_BIT_OVERFLOW_FAIL);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x01, 0x42, 0x03, 0x03, 0x85};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_set_int, "Bit Set Integer")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 112);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_bit_set_int(&ops, BIN_NAME, NULL, NULL, 1, 8, 127);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[1].valuep;
	uint8_t expected[] = {0x3F, 0xC2, 0x03, 0x04, 0x05};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_get, "Bit Get")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 113);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_get(&ops, BIN_NAME, NULL, 9, 5);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	as_bytes* b = (as_bytes*)results[0].valuep;
	uint8_t expected[] = {0x80};
	assert_bytes_eq(b->value, b->size, expected, sizeof(expected))

	as_record_destroy(prec);
}

TEST(bit_count, "Bit Count")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 114);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_count(&ops, BIN_NAME, NULL, 20, 4);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	int64_t v = results[0].valuep->integer.value;
	assert_int_eq(2, v)

	as_record_destroy(prec);
}

TEST(bit_lscan, "Bit Left Scan")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 115);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_lscan(&ops, BIN_NAME, NULL, 24, 8, true);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	int64_t v = results[0].valuep->integer.value;
	assert_int_eq(5, v)

	as_record_destroy(prec);
}

TEST(bit_rscan, "Bit Right Scan")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 116);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_rscan(&ops, BIN_NAME, NULL, 32, 8, true);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	int64_t v = results[0].valuep->integer.value;
	assert_int_eq(7, v)

	as_record_destroy(prec);
}

TEST(bit_get_int, "Bit Get Integer")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 117);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t bytes[] = {0x01, 0x42, 0x03, 0x04, 0x05};

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_rawp(&rec, BIN_NAME, bytes, sizeof(bytes), false);

	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_get_int(&ops, BIN_NAME, NULL, 8, 16, false);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;

	int64_t v = results[0].valuep->integer.value;
	assert_int_eq(16899, v)

	as_record_destroy(prec);
}

TEST(bit_filter_call_read_get, "Bit filter call read get")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_read_count, "Bit filter call read count")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_count(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_count(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_count(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_count(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_read_lscan, "Bit filter call read lscan")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_int(5),
			as_exp_bit_lscan(as_exp_int(32), as_exp_uint(8),
				as_exp_bool(true), as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_ne(
			as_exp_int(5),
			as_exp_bit_lscan(as_exp_int(0), as_exp_uint(8),
				as_exp_bool(true), as_exp_bit_get(as_exp_int(32),
					as_exp_uint(8), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter3,
		as_exp_cmp_eq(
			as_exp_int(5),
			as_exp_bit_lscan(as_exp_int(0), as_exp_uint(8),
				as_exp_bool(true), as_exp_bit_get(as_exp_int(32),
					as_exp_uint(8), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter3);

	p.base.filter_exp = filter3;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter3);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);

	as_exp_build(filter4,
		as_exp_cmp_eq(
			as_exp_int(5),
			as_exp_bit_lscan(as_exp_int(32), as_exp_uint(8),
				as_exp_bool(true), as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter4);

	p.base.filter_exp = filter4;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter4);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_read_rscan, "Bit filter call read rscan")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_int(7),
			as_exp_bit_rscan(as_exp_int(32), as_exp_uint(8),
				as_exp_bool(true), as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_int(7),
			as_exp_bit_rscan(as_exp_int(32), as_exp_uint(8),
				as_exp_bool(true), as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_read_get_int, "Bit filter call read get int")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_int(0x05),
			as_exp_bit_get_int(as_exp_int(32), as_exp_uint(8), true,
				as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_int(0x05),
			as_exp_bit_get_int(as_exp_int(32), as_exp_uint(8), true,
				as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_resize, "Bit filter call modify resize")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_resize(NULL, as_exp_uint(6), 0,
				as_exp_bin_blob(BIN_NAME)), as_exp_bit_resize(NULL,
					as_exp_uint(6), 0, as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_resize(NULL, as_exp_uint(6), 0,
				as_exp_bin_blob(BIN_NAME)), as_exp_bit_resize(NULL,
					as_exp_uint(6), 0, as_exp_bin_blob(BIN_NAME))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_insert, "Bit filter call modify insert")
{
	uint8_t value = 0xFF;

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_int(value),
			as_exp_bit_get_int(as_exp_int(8), as_exp_uint(8), false,
				as_exp_bit_insert(NULL, as_exp_int(1),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_int(value),
			as_exp_bit_get_int(as_exp_int(8), as_exp_uint(8), false,
				as_exp_bit_insert(NULL, as_exp_int(1),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_remove, "Bit filter call modify remove")
{
	uint8_t value = 0x42;

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_int(value),
			as_exp_bit_get_int(as_exp_int(0), as_exp_uint(8), false,
				as_exp_bit_remove(NULL, as_exp_int(0),
					as_exp_uint(1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);
	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);


	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_int(value),
			as_exp_bit_get_int(as_exp_int(0), as_exp_uint(8), false,
				as_exp_bit_remove(NULL, as_exp_int(0),
					as_exp_uint(1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_set, "Bit filter call modify set")
{
	uint8_t value = 0x80;

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set(NULL, as_exp_int(31), as_exp_uint(1),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set(NULL, as_exp_int(31), as_exp_uint(1),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_set_sub, "Bit filter call modify set sub")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set(NULL, as_exp_int(24), as_exp_uint(8),
					as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
						as_exp_bin_blob(BIN_NAME)),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set(NULL, as_exp_int(24), as_exp_uint(8),
					as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
						as_exp_bin_blob(BIN_NAME)),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_or, "Bit filter call modify or")
{
	uint8_t value = 0x01;

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_or(NULL, as_exp_int(24), as_exp_uint(8),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(32), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_or(NULL, as_exp_int(24), as_exp_uint(8),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_xor, "Bit filter call modify xor")
{
	uint8_t value = 0x02;

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(0), as_exp_uint(8),
				as_exp_bit_xor(NULL, as_exp_int(0), as_exp_uint(8),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(0), as_exp_uint(8),
				as_exp_bit_xor(NULL, as_exp_int(0), as_exp_uint(8),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_and, "Bit filter modify and")
{
	uint8_t value = 0x01;

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(0), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bit_and(NULL, as_exp_int(16), as_exp_uint(8),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(0), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bit_and(NULL, as_exp_int(16), as_exp_uint(8),
					as_exp_bytes(&value, 1), as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_not, "Bit filter call modify not")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(0), as_exp_uint(8),
				as_exp_bit_not(NULL, as_exp_int(6), as_exp_uint(1),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(0), as_exp_uint(8),
				as_exp_bit_not(NULL, as_exp_int(6), as_exp_uint(1),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_lshift, "Bit filter call modify lshift")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(2), as_exp_uint(6),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(0), as_exp_uint(6),
				as_exp_bit_lshift(NULL, as_exp_int(0),
					as_exp_uint(8), as_exp_uint(2),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(2), as_exp_uint(6),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(0), as_exp_uint(6),
				as_exp_bit_lshift(NULL, as_exp_int(0),
					as_exp_uint(8), as_exp_uint(2),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_rshift, "Bit filter call modify rshift")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(24), as_exp_uint(6),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(26), as_exp_uint(6),
				as_exp_bit_rshift(NULL, as_exp_int(24),
					as_exp_uint(8), as_exp_uint(2),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(24), as_exp_uint(6),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(26), as_exp_uint(6),
				as_exp_bit_rshift(NULL, as_exp_int(24),
					as_exp_uint(8), as_exp_uint(2),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_add, "Bit filter call modify add")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bit_add(NULL, as_exp_int(16), as_exp_uint(8),
					as_exp_uint(1), AS_BIT_OVERFLOW_FAIL,
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bit_add_signed(NULL, as_exp_int(16), as_exp_uint(8),
					as_exp_uint(1), false, AS_BIT_OVERFLOW_FAIL,
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_subtract, "Bit filter call modify subtract")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_subtract(NULL, as_exp_int(24),
					as_exp_uint(8), as_exp_uint(1), AS_BIT_OVERFLOW_FAIL,
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(16), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_subtract_signed(NULL, as_exp_int(24),
					as_exp_uint(8), as_exp_uint(1), false, AS_BIT_OVERFLOW_FAIL,
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_set_int, "Bit filter call modify set int")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(8), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set_int(NULL, as_exp_int(24),
					as_exp_uint(8), as_exp_uint(0x42),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(8), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set_int(NULL, as_exp_int(24),
					as_exp_uint(8), as_exp_uint(0x42),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(bit_filter_call_modify_set_int_sub, "Bit filter call modify set int sub")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_bit_get(as_exp_int(8), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set_int(NULL, as_exp_int(24), as_exp_uint(8),
					as_exp_bit_get_int(as_exp_int(8), as_exp_uint(8),
						true, as_exp_bin_blob(BIN_NAME)),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_bit_get(as_exp_int(8), as_exp_uint(8),
				as_exp_bin_blob(BIN_NAME)),
			as_exp_bit_get(as_exp_int(24), as_exp_uint(8),
				as_exp_bit_set_int(NULL, as_exp_int(24), as_exp_uint(8),
					as_exp_bit_get_int(as_exp_int(8), as_exp_uint(8),
						true, as_exp_bin_blob(BIN_NAME)),
					as_exp_bin_blob(BIN_NAME)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &REC_KEY, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(bit, "aerospike bitmap tests")
{
	suite_before(before);

	suite_add(bit_resize);
	suite_add(bit_insert);
	suite_add(bit_remove);
	suite_add(bit_set);
	suite_add(bit_or);
	suite_add(bit_xor);
	suite_add(bit_and);
	suite_add(bit_not);
	suite_add(bit_lshift);
	suite_add(bit_rshift);
	suite_add(bit_add);
	suite_add(bit_subtract);
	suite_add(bit_set_int);
	suite_add(bit_get);
	suite_add(bit_count);
	suite_add(bit_lscan);
	suite_add(bit_rscan);
	suite_add(bit_get_int);
	suite_add(bit_filter_call_read_get);
	suite_add(bit_filter_call_read_count);
	suite_add(bit_filter_call_read_lscan);
	suite_add(bit_filter_call_read_rscan);
	suite_add(bit_filter_call_read_get_int);
	suite_add(bit_filter_call_modify_resize);
	suite_add(bit_filter_call_modify_insert);
	suite_add(bit_filter_call_modify_remove);
	suite_add(bit_filter_call_modify_set);
	suite_add(bit_filter_call_modify_set_sub);
	suite_add(bit_filter_call_modify_or);
	suite_add(bit_filter_call_modify_xor);
	suite_add(bit_filter_call_modify_and);
	suite_add(bit_filter_call_modify_not);
	suite_add(bit_filter_call_modify_lshift);
	suite_add(bit_filter_call_modify_rshift);
	suite_add(bit_filter_call_modify_add);
	suite_add(bit_filter_call_modify_subtract);
	suite_add(bit_filter_call_modify_set_int);
	suite_add(bit_filter_call_modify_set_int_sub);
}
