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
#include <aerospike/as_exp.h>
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_list_operations.h>
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

typedef struct testdata_s {
	const char* bin1;
	const char* bin2;
	const char* bin3;
	const char* lbin;
	as_key key;
	as_arraylist list1;
	as_arraylist list2;
	as_arraylist list3;
	as_bytes hll1;
	as_bytes hll2;
	as_bytes hll3;
} testdata;

testdata DATA = {
		.bin1 = BIN_NAME "_1",
		.bin2 = BIN_NAME "_2",
		.bin3 = BIN_NAME "_3",
		.lbin = BIN_NAME "_l",
};

static bool
before(atf_suite* suite)
{
	as_key_init_int64(&DATA.key, NAMESPACE, SET, 0x5EC7C0DE);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &DATA.key);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		return false;
	}

	as_arraylist_init(&DATA.list1, 3, 0);
	as_arraylist_append_str(&DATA.list1, "Akey1");
	as_arraylist_append_str(&DATA.list1, "Akey2");
	as_arraylist_append_str(&DATA.list1, "Akey3");

	as_arraylist_init(&DATA.list2, 3, 0);
	as_arraylist_append_str(&DATA.list2, "Bkey1");
	as_arraylist_append_str(&DATA.list2, "Bkey2");
	as_arraylist_append_str(&DATA.list2, "Bkey3");

	as_arraylist_init(&DATA.list3, 6, 0);
	as_arraylist_append_str(&DATA.list3, "Akey1");
	as_arraylist_append_str(&DATA.list3, "Akey2");
	as_arraylist_append_str(&DATA.list3, "Bkey1");
	as_arraylist_append_str(&DATA.list3, "Bkey2");
	as_arraylist_append_str(&DATA.list3, "Ckey1");
	as_arraylist_append_str(&DATA.list3, "Ckey2");

	as_operations ops;
	as_operations_inita(&ops, 9);
	as_operations_hll_add(&ops, DATA.bin1, NULL, NULL,
			(as_list*)&DATA.list1, 8);
	as_operations_hll_add(&ops, DATA.bin2, NULL, NULL,
			(as_list*)&DATA.list2, 8);
	as_operations_hll_add(&ops, DATA.bin3, NULL, NULL,
			(as_list*)&DATA.list3, 8);
	as_operations_add_read(&ops, DATA.bin1);
	as_operations_add_read(&ops, DATA.bin2);
	as_operations_add_read(&ops, DATA.bin3);
	as_operations_add_list_append_int64(&ops, DATA.lbin, 1);
	as_operations_add_list_clear(&ops, DATA.lbin);
	as_operations_add_read(&ops, DATA.lbin);

	as_arraylist_destroy(&DATA.list1);
	as_arraylist_destroy(&DATA.list2);
	as_arraylist_destroy(&DATA.list3);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &DATA.key, &ops, &prec);

	if (status != AEROSPIKE_OK) {
		return false;
	}

	as_operations_destroy(&ops);
	// example_dump_record(prec);

	as_bin* results = prec->bins.entries;
	as_bytes* hllbin1 = (as_bytes*)results[3].valuep;
	as_bytes* hllbin2 = (as_bytes*)results[4].valuep;
	as_bytes* hllbin3 = (as_bytes*)results[5].valuep;

	if (hllbin1 == NULL || hllbin2 == NULL || hllbin3 == NULL) {
		return false;
	}

	// Reserve hllbin for future processing.
	as_val_reserve(hllbin1);
	as_val_reserve(hllbin2);
	as_val_reserve(hllbin3);

	uint8_t* bytes1 = hllbin1->value;
	uint32_t len1 = hllbin1->size;
	uint8_t* bytes2 = hllbin2->value;
	uint32_t len2 = hllbin2->size;
	uint8_t* bytes3 = hllbin3->value;
	uint32_t len3 = hllbin3->size;

	// Create hll bytes from reserved hllbin.
	as_bytes_init_wrap(&DATA.hll1, bytes1, len1, true);
	as_bytes_init_wrap(&DATA.hll2, bytes2, len2, true);
	as_bytes_init_wrap(&DATA.hll3, bytes3, len3, true);

	as_record_destroy(prec);
	return true;
}

static bool
after(atf_suite* suite)
{
	as_bytes_destroy(&DATA.hll1);
	as_bytes_destroy(&DATA.hll2);
	as_bytes_destroy(&DATA.hll3);
	return true;
}

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
	as_val_reserve(hllbin);
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
	as_val_reserve(bytes_hll);
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

TEST(hll_filter_call_read_count, "HLL filter call read count")
{
	as_exp_build(filter1,
		as_exp_cmp_eq(
			as_exp_hll_get_count(as_exp_bin_hll(DATA.bin1)), as_exp_int(0)));
	assert_not_null(filter1)

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_gt(
			as_exp_hll_get_count(as_exp_bin_hll(DATA.bin1)), as_exp_int(0)));
	assert_not_null(filter2)

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_union, "HLL filter call read union")
{
	// Reserve for arraylist_append.
	as_val_reserve(&DATA.hll1);
	as_val_reserve(&DATA.hll2);
	as_val_reserve(&DATA.hll3);

	as_arraylist hlls;
	as_arraylist_init(&hlls, 3, 0);
	as_arraylist_append_bytes(&hlls, &DATA.hll1);
	as_arraylist_append_bytes(&hlls, &DATA.hll2);
	as_arraylist_append_bytes(&hlls, &DATA.hll3);

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_hll_get_count(as_exp_hll_get_union(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))),
			as_exp_hll_get_union_count(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter1)

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_hll_get_count(as_exp_hll_get_union(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))),
			as_exp_hll_get_union_count(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter2)

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	as_arraylist_destroy(&hlls);
	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_union_args, "HLL filter call read union args")
{
	// Reserve for arraylist_append.
	as_val_reserve(&DATA.hll1);
	as_val_reserve(&DATA.hll2);
	as_val_reserve(&DATA.hll3);

	as_arraylist hlls;
	as_arraylist_init(&hlls, 3, 0);
	as_arraylist_append_bytes(&hlls, &DATA.hll1);
	as_arraylist_append_bytes(&hlls, &DATA.hll2);
	as_arraylist_append_bytes(&hlls, &DATA.hll3);

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_hll_get_union_count(
				as_exp_hll_get_union(as_exp_bin_hll(DATA.bin2),
					as_exp_bin_hll(DATA.bin1)), as_exp_bin_hll(DATA.bin3)),
			as_exp_hll_get_union_count(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter1)

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);
	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_hll_get_union_count(
				as_exp_hll_get_union(as_exp_bin_hll(DATA.bin2),
					as_exp_bin_hll(DATA.bin1)), as_exp_bin_hll(DATA.bin3)),
			as_exp_hll_get_union_count(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter2)

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	as_arraylist_destroy(&hlls);
	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_union_complex_args, "HLL filter call read union complex args")
{
	// Reserve for arraylist_append.
	as_val_reserve(&DATA.hll1);
	as_val_reserve(&DATA.hll2);
	as_val_reserve(&DATA.hll3);

	as_arraylist hlls;
	as_arraylist_init(&hlls, 3, 0);
	as_arraylist_append_bytes(&hlls, &DATA.hll1);
	as_arraylist_append_bytes(&hlls, &DATA.hll2);
	as_arraylist_append_bytes(&hlls, &DATA.hll3);

	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_hll_get_union_count(
				as_exp_list_append(NULL, NULL, as_exp_bin_hll(DATA.bin3),
					as_exp_list_append(NULL, NULL, as_exp_bin_hll(DATA.bin2),
						as_exp_bin_list(DATA.lbin))), as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_union_count(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter1)

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_hll_get_union_count(
				as_exp_list_append(NULL, NULL, as_exp_bin_hll(DATA.bin3),
					as_exp_list_append(NULL, NULL, as_exp_bin_hll(DATA.bin2),
						as_exp_bin_list(DATA.lbin))), as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_union_count(as_exp_val(&hlls),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter2)

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	as_arraylist_destroy(&hlls);
	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_intersect_count, "HLL filter call read intersect count")
{
	// Reserve for arraylist_append.
	as_val_reserve(&DATA.hll2);
	as_val_reserve(&DATA.hll3);

	as_arraylist hlls2;
	as_arraylist_init(&hlls2, 1, 0);
	as_arraylist_append_bytes(&hlls2, &DATA.hll2);

	as_arraylist hlls3;
	as_arraylist_init(&hlls3, 1, 0);
	as_arraylist_append_bytes(&hlls3, &DATA.hll3);

	as_exp_build(filter1,
		as_exp_cmp_ge(
			as_exp_hll_get_intersect_count(as_exp_val(&hlls2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_intersect_count(as_exp_val(&hlls3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_le(
			as_exp_hll_get_intersect_count(as_exp_val(&hlls2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_intersect_count(as_exp_val(&hlls3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	as_arraylist_destroy(&hlls2);
	as_arraylist_destroy(&hlls3);
	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_intersect_count_args, "HLL filter call read intersect count args")
{
	as_exp_build(filter1,
		as_exp_cmp_ge(
			as_exp_hll_get_intersect_count(as_exp_bin_hll(DATA.bin2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_intersect_count(as_exp_bin_hll(DATA.bin3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_le(
			as_exp_hll_get_intersect_count(as_exp_bin_hll(DATA.bin2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_intersect_count(as_exp_bin_hll(DATA.bin3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_similarity, "HLL filter call read similarity")
{
	// Reserve for arraylist_append.
	as_val_reserve(&DATA.hll2);
	as_val_reserve(&DATA.hll3);

	as_arraylist hlls2;
	as_arraylist_init(&hlls2, 1, 0);
	as_arraylist_append_bytes(&hlls2, &DATA.hll2);

	as_arraylist hlls3;
	as_arraylist_init(&hlls3, 1, 0);
	as_arraylist_append_bytes(&hlls3, &DATA.hll3);

	as_exp_build(filter1,
		as_exp_cmp_ge(
			as_exp_hll_get_similarity(as_exp_val(&hlls2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_similarity(as_exp_val(&hlls3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_le(
			as_exp_hll_get_similarity(as_exp_val(&hlls2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_similarity(as_exp_val(&hlls3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	as_arraylist_destroy(&hlls2);
	as_arraylist_destroy(&hlls3);
	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_similarity_args, "HLL filter call read similarity args")
{
	as_exp_build(filter1,
		as_exp_cmp_ge(
			as_exp_hll_get_similarity(as_exp_bin_hll(DATA.bin2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_similarity(as_exp_bin_hll(DATA.bin3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_le(
			as_exp_hll_get_similarity(as_exp_bin_hll(DATA.bin2),
				as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_similarity(as_exp_bin_hll(DATA.bin3),
				as_exp_bin_hll(DATA.bin1))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_describe, "HLL filter call read describe")
{
	as_exp_build(filter1,
		as_exp_cmp_ne(
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
				as_exp_hll_describe(as_exp_bin_hll(DATA.bin1))),
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
				as_exp_hll_describe(as_exp_bin_hll(DATA.bin2)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
				as_exp_hll_describe(as_exp_bin_hll(DATA.bin1))),
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
				as_exp_hll_describe(as_exp_bin_hll(DATA.bin2)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_read_may_contain, "HLL filter call read may contain")
{
	as_arraylist vals;
	as_arraylist_init(&vals, 1, 0);
	as_arraylist_append_str(&vals, "new_val");

	as_exp_build(filter1,
		as_exp_cmp_eq(
			as_exp_int(1),
			as_exp_hll_may_contain(as_exp_val(&vals),
				as_exp_bin_hll(DATA.bin2))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_ne(
			as_exp_int(1),
			as_exp_hll_may_contain(as_exp_val(&vals),
				as_exp_bin_hll(DATA.bin2))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);
	as_arraylist_destroy(&vals);

	assert_int_eq(status, AEROSPIKE_OK);
}

TEST(hll_filter_call_modify_add, "HLL filter call modify add")
{
	as_arraylist vals;
	as_arraylist_init(&vals, 1, 0);
	as_arraylist_append_str(&vals, "new_val");

	as_exp_build(filter1,
		as_exp_cmp_eq(
			as_exp_hll_get_count(as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_count(
				as_exp_hll_add_mh(NULL, as_exp_val(&vals), -1, -1,
					as_exp_bin_hll(DATA.bin2)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);

	p.base.filter_exp = filter1;

	as_record* prec = NULL;
	as_error err;
	as_status status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter1);
	as_record_destroy(prec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_cmp_lt(
			as_exp_hll_get_count(as_exp_bin_hll(DATA.bin1)),
			as_exp_hll_get_count(as_exp_hll_update(NULL, as_exp_val(&vals),
				as_exp_bin_hll(DATA.bin2)))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	prec = NULL;
	status = aerospike_key_get(as, &err, &p, &DATA.key, &prec);
	as_exp_destroy(filter2);
	as_record_destroy(prec);
	as_arraylist_destroy(&vals);

	assert_int_eq(status, AEROSPIKE_OK);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(hll, "HLL tests")
{
	suite_before(before);
	suite_after(after);

	suite_add(hll_init);
	suite_add(hll_ops);
	suite_add(hll_read_write);

	suite_add(hll_filter_call_read_count);
	suite_add(hll_filter_call_read_union);
	suite_add(hll_filter_call_read_union_args);
	suite_add(hll_filter_call_read_union_complex_args);
	suite_add(hll_filter_call_read_intersect_count);
	suite_add(hll_filter_call_read_intersect_count_args);
	suite_add(hll_filter_call_read_similarity);
	suite_add(hll_filter_call_read_similarity_args);
	suite_add(hll_filter_call_read_describe);
	suite_add(hll_filter_call_read_may_contain);

	suite_add(hll_filter_call_modify_add);

}
