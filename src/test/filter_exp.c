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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_map_operations.h>
#include "test.h"
#include "util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_filter"
#define LUA_FILE AS_START_DIR "src/test/lua/key_apply.lua"
#define UDF_FILE "key_apply"

static char* AString = "A";
static char* BString = "B";
static char* CString = "C";
static char* DString = "D";
static char* EString = "E";

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool
before(atf_suite* suite)
{
	if (! udf_put(LUA_FILE)) {
		error("failure while uploading: %s", LUA_FILE);
		return false;
	}

	if (! udf_exists(LUA_FILE)) {
		error("lua file does not exist: %s", LUA_FILE);
		return false;
	}
	return true;
}

static bool
after(atf_suite* suite)
{
	if (! udf_remove(LUA_FILE)) {
		error("failure while removing: %s", LUA_FILE);
		return false;
	}
	return true;
}

static bool
filter_prepare(as_key* keyA, as_key* keyB)
{
	as_error err;
	as_key_init(keyA, NAMESPACE, SET, AString);
	as_status rc = aerospike_key_remove(as, &err, NULL, keyA);

	if (! (rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND)) {
		return false;
	}

	as_key_init_raw(keyB, NAMESPACE, SET, (const uint8_t*)BString,
			(uint32_t)strlen(BString));
	rc = aerospike_key_remove(as, &err, NULL, keyB);

	if (! (rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND)) {
		return false;
	}

	as_record rec;
	as_record_inita(&rec, 5);
	as_record_set_int64(&rec, AString, 1);
	as_record_set_double(&rec, BString, 1.1);
	as_record_set_strp(&rec, CString, "abcde", false);
	as_record_set_int64(&rec, DString, 1);
	as_record_set_int64(&rec, EString, -1);
	rc = aerospike_key_put(as, &err, NULL, keyA, &rec);

	if (rc != AEROSPIKE_OK) {
		return false;
	}

	as_record_inita(&rec, 5);
	as_record_set_int64(&rec, AString, 2);
	as_record_set_double(&rec, BString, 2.2);
	as_record_set_strp(&rec, CString, "abcdeabcde", false);
	as_record_set_int64(&rec, DString, 1);
	as_record_set_int64(&rec, EString, -2);
	rc = aerospike_key_put(as, &err, NULL, keyB, &rec);

	if (rc != AEROSPIKE_OK) {
		return false;
	}
	return true;
}

static bool
filter_prepare_bitwise(as_key* keyA)
{
	as_error err;
	as_key_init(keyA, NAMESPACE, SET, AString);
	as_status rc = aerospike_key_remove(as, &err, NULL, keyA);

	if (! (rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND)) {
		return false;
	}

	as_record rec;
	as_record_inita(&rec, 3);
	as_record_set_int64(&rec, AString, 0);
	as_record_set_int64(&rec, BString, -1);
	as_record_set_int64(&rec, CString, 1);
	rc = aerospike_key_put(as, &err, NULL, keyA, &rec);

	if (rc != AEROSPIKE_OK) {
		return false;
	}

	return true;
}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(filter_put, "filter put")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)));
	assert_not_null(filter);

	as_policy_write p;
	as_policy_write_init(&p);
	p.base.filter_exp = filter;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, AString, 3);

	as_error err;
	as_status rc = aerospike_key_put(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* prec = NULL;
	rc = aerospike_key_get(as, &err, NULL, &keyA, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(prec, AString, 0);
	assert_int_eq(val, 3);
	as_record_destroy(prec);

	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, AString, 3);

	rc = aerospike_key_put(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	as_record_destroy(&rec);

	prec = NULL;
	rc = aerospike_key_get(as, &err, NULL, &keyB, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	val = as_record_get_int64(prec, AString, 0);
	assert_int_eq(val, 2);
	as_record_destroy(prec);

	as_exp_destroy(filter);
}

TEST(filter_get, "filter get")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* prec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(prec, AString, 0);
	assert_int_eq(val, 1);
	as_record_destroy(prec);

	prec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &prec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_destroy(filter);
}

TEST(filter_batch, "filter batch")
{
	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)));
	assert_not_null(filter);

	as_policy_batch p;
	as_policy_batch_init(&p);
	p.base.filter_exp = filter;

	as_batch_records records;
	as_batch_records_inita(&records, 2);

	as_batch_read_record* recA = as_batch_read_reserve(&records);
	recA->read_all_bins = true;

	as_batch_read_record* recB = as_batch_read_reserve(&records);
	recB->read_all_bins = true;

	bool b = filter_prepare(&recA->key, &recB->key);
	assert_true(b);

	as_error err;
	as_status rc = aerospike_batch_read(as, &err, &p, &records);
	assert_int_eq(rc, AEROSPIKE_OK);

	assert_int_eq(recA->result, AEROSPIKE_OK);
	int64_t val = as_record_get_int64(&recA->record, AString, -1);
	assert_int_eq(val, 1);

	assert_int_eq(recB->result, AEROSPIKE_FILTERED_OUT);

	as_batch_records_destroy(&records);
	as_exp_destroy(filter);
}

TEST(filter_delete, "filter delete")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)));
	assert_not_null(filter);

	as_policy_remove p;
	as_policy_remove_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_status rc = aerospike_key_remove(as, &err, &p, &keyA);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record* prec = NULL;
	rc = aerospike_key_get(as, &err, NULL, &keyA, &prec);
	assert_int_eq(rc, AEROSPIKE_ERR_RECORD_NOT_FOUND);

	rc = aerospike_key_remove(as, &err, &p, &keyB);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	prec = NULL;
	rc = aerospike_key_get(as, &err, NULL, &keyB, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(prec, AString, 0);
	assert_int_eq(val, 2);
	as_record_destroy(prec);

	as_exp_destroy(filter);
}

TEST(filter_operate, "filter operate read")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)));
	assert_not_null(filter);

	as_policy_operate p;
	as_policy_operate_init(&p);
	p.base.filter_exp = filter;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, AString);

	as_record* prec = NULL;
	as_error err;
	as_status rc = aerospike_key_operate(as, &err, &p, &keyA, &ops, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(prec, AString, 0);
	assert_int_eq(val, 1);
	as_record_destroy(prec);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, AString);

	prec = NULL;
	rc = aerospike_key_operate(as, &err, &p, &keyB, &ops, &prec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_destroy(filter);
}

TEST(filter_udf, "filter udf")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)));
	assert_not_null(filter);

	as_policy_apply p;
	as_policy_apply_init(&p);
	p.base.filter_exp = filter;

	as_val* res = NULL;
	as_error err;
	as_status rc = aerospike_key_apply(as, &err, &p, &keyA, UDF_FILE, "one", NULL, &res);
	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(res);
	as_val_destroy(res);

	res = NULL;
	rc = aerospike_key_apply(as, &err, &p, &keyB, UDF_FILE, "one", NULL, &res);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	assert_null(res);

	as_exp_destroy(filter);
}

TEST(filter_call, "filter call")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_arraylist* list = as_arraylist_new(10, 10);
	as_arraylist_append_int64(list, 1);
	as_arraylist_append_int64(list, 2);
	as_arraylist_append_int64(list, 3);
	as_arraylist_append_int64(list, 4);

	as_record rec;
	as_status rc;
	as_error err;
	as_record_inita(&rec, 2);
	as_val_reserve(list);
	as_record_set_list(&rec, AString, (as_list*)list);
	as_record_set_int64(&rec, BString, 1);
	rc = aerospike_key_put(as, &err, NULL, &keyA, &rec);
	assert_true(rc == AEROSPIKE_OK);

	as_arraylist_insert_int64(list, 0, 0);

	as_record_set_list(&rec, AString, (as_list*)list);
	rc = aerospike_key_put(as, &err, NULL, &keyB, &rec);
	as_record_destroy(&rec);
	assert_true(rc == AEROSPIKE_OK);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_int(3),
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(2),
				as_exp_bin_list(AString))));
	assert_not_null(filter);

	as_policy_operate p;
	as_policy_operate_init(&p);
	p.base.filter_exp = filter;

	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BString);

	as_record* prec = NULL;
	rc = aerospike_key_operate(as, &err, &p, &keyA, &ops, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(prec, BString, 0);
	assert_int_eq(val, 1);
	as_record_destroy(prec);
	as_operations_destroy(&ops);

	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, AString);

	prec = NULL;
	rc = aerospike_key_operate(as, &err, &p, &keyB, &ops, &prec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	as_operations_destroy(&ops);

	as_exp_destroy(filter);
}

TEST(filter_call_chain, "filter call chain")
{
	as_error err;
	as_key keyA;

	as_key_init(&keyA, NAMESPACE, SET, AString);
	as_status rc = aerospike_key_remove(as, &err, NULL, &keyA);
	assert(rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	const uint32_t test_count = 100;

	as_hashmap hm;
	as_hashmap_init(&hm, test_count);

	for (uint32_t i = 0; i < test_count; i++) {
		as_hashmap_set(&hm, (as_val*)as_integer_new(i),
				(as_val*)as_integer_new(i % 3));
	}

	as_record test_rec;
	as_record_inita(&test_rec, 2);
	as_record_set_list(&test_rec, AString, (as_list*)&hm);
	as_record_set_int64(&test_rec, BString, 1);
	rc = aerospike_key_put(as, &err, NULL, &keyA, &test_rec);
	assert_int_eq(rc, AEROSPIKE_OK);
	as_record_destroy(&test_rec);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(4),
				as_exp_map_get_by_value(NULL, AS_MAP_RETURN_KEY, as_exp_int(1),
					as_exp_bin_map(AString))),
			as_exp_int(13)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(rec, BString, 0);
	assert_int_eq(val, 1);
	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_call_modify, "filter call modify")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_status rc;
	as_error err;

	as_arraylist listA;
	as_arraylist_init(&listA, 25, 25);

	as_arraylist listB;
	as_arraylist_init(&listB, 25, 25);

	for (uint32_t i = 0; i < 10; i++) {
		as_arraylist_append_int64(&listA, i * 100);
		as_arraylist_append_int64(&listB, i * i);
	}
#if 0
	char *sA = as_val_tostring(&listA);
	char *sB = as_val_tostring(&listB);
	info("A=%s B=%s", sA, sB);
	cf_free(sA);
	cf_free(sB);
#endif
	as_record recA;
	as_record_init(&recA, 1);
	as_record_set(&recA, BString, (as_bin_value*)&listA);
	as_record recB;
	as_record_init(&recB, 1);
	as_record_set(&recB, BString, (as_bin_value*)&listB);

	rc = aerospike_key_put(as, &err, NULL, &keyA, &recA);
	assert_true(rc == AEROSPIKE_OK);
	
	rc = aerospike_key_put(as, &err, NULL, &keyB, &recB);
	assert_true(rc == AEROSPIKE_OK);

	as_operations ops;
	as_record* rec = NULL;
	as_operations_init(&ops, 1);

	as_operations_add_list_set_order(&ops, BString, AS_LIST_ORDERED);
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_list_policy pol;
	as_list_policy_set(&pol, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_NO_FAIL | AS_LIST_WRITE_PARTIAL);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_list_size(NULL,
				as_exp_list_append(NULL, &pol, as_exp_int(100),
					as_exp_list_append_items(NULL, &pol, as_exp_val(&listB),
						as_exp_bin_list(BString)))),
			as_exp_int(19)));

	as_arraylist_destroy(&listB);
	assert_not_null(filter);

	as_policy_operate p;
	as_policy_operate_init(&p);
	p.base.filter_exp = filter;

	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BString);

	rc = aerospike_key_operate(as, &err, &p, &keyA, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_destroy(filter);

	as_exp_build(filter2,
		as_exp_cmp_eq(
			as_exp_list_size(NULL,
				as_exp_list_append(NULL, &pol, as_exp_int(1000),
					as_exp_list_append_items(NULL, &pol, as_exp_val(&listA),
						as_exp_list_append(NULL, &pol, as_exp_int(81),
							as_exp_bin_list(BString))))),
			as_exp_int(20)));

	as_arraylist_destroy(&listA);
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BString);

	rc = aerospike_key_operate(as, &err, &p, &keyB, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_operations_destroy(&ops);
	as_exp_destroy(filter2);
	as_record_destroy(rec);
	as_record_destroy(&recA);
	as_record_destroy(&recB);
}

TEST(filter_call_context, "filter call ctx")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_arraylist* list = as_arraylist_new(10, 10);
	as_arraylist_append_str(list, "a");
	as_arraylist_append_str(list, "b");
	as_arraylist_append_str(list, "c");
	as_arraylist_append_str(list, "d");

	as_arraylist* list0 = as_arraylist_new(10, 10);
	as_arraylist_append_str(list0, "e");
	as_arraylist_append_str(list0, "d");
	as_arraylist_append_str(list0, "c");
	as_arraylist_append_str(list0, "b");
	as_arraylist_append_str(list0, "a");

	as_arraylist_append(list, (as_val*)list0);

	as_record rec;
	as_status rc;
	as_error err;
	as_record_inita(&rec, 2);
	as_val_reserve(list);
	as_record_set_list(&rec, AString, (as_list*)list);
	as_record_set_int64(&rec, BString, 1);
	rc = aerospike_key_put(as, &err, NULL, &keyA, &rec);
	assert_true(rc == AEROSPIKE_OK);

	as_arraylist_insert_int64(list, 0, 0);

	as_record_set_list(&rec, AString, (as_list*)list);
	rc = aerospike_key_put(as, &err, NULL, &keyB, &rec);
	as_record_destroy(&rec);
	assert_true(rc == AEROSPIKE_OK);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, 4);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_list_get_by_index(&ctx, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_STR, as_exp_int(2),
				as_exp_bin_list(AString)),
			as_exp_str("c")));
	assert_not_null(filter);

	as_policy_operate p;
	as_policy_operate_init(&p);
	p.base.filter_exp = filter;

	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BString);

	as_record* prec = NULL;
	rc = aerospike_key_operate(as, &err, &p, &keyA, &ops, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(prec, BString, 0);
	assert_int_eq(val, 1);
	as_record_destroy(prec);
	as_operations_destroy(&ops);

	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, AString);

	prec = NULL;
	rc = aerospike_key_operate(as, &err, &p, &keyB, &ops, &prec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);
	as_operations_destroy(&ops);

	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(filter);
}

TEST(filter_call_context_param, "filter call ctx param")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_arraylist* list = as_arraylist_new(10, 10);
	as_arraylist_append_str(list, "a");
	as_arraylist_append_str(list, "b");
	as_arraylist_append_str(list, "c");
	as_arraylist_append_str(list, "d");

	as_arraylist* list0 = as_arraylist_new(10, 10);
	as_arraylist_append_str(list0, "e");
	as_arraylist_append_str(list0, "d");
	as_arraylist_append_str(list0, "c");
	as_arraylist_append_str(list0, "b");
	as_arraylist_append_str(list0, "a");

	as_arraylist_append(list, (as_val*)list0);

	as_arraylist* list1 = as_arraylist_new(10, 10);
	as_arraylist_append_str(list1, "x");
	as_arraylist_append_str(list1, "y");
	as_arraylist_append_str(list1, "z");

	as_record rec;
	as_status rc;
	as_error err;
	as_record_inita(&rec, 2);
	as_record_set_list(&rec, AString, (as_list*)list);
	as_record_set_list(&rec, BString, (as_list*)list1);
	rc = aerospike_key_put(as, &err, NULL, &keyA, &rec);
	as_record_destroy(&rec);
	assert_true(rc == AEROSPIKE_OK);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, 4);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_list_size(&ctx, as_exp_list_append_items(&ctx, NULL, as_exp_bin_list(BString), as_exp_bin_list(AString))),
			as_exp_int(8)));
	assert_not_null(filter);

	as_policy_operate p;
	as_policy_operate_init(&p);
	p.base.filter_exp = filter;

	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BString);

	as_record* prec = NULL;
	rc = aerospike_key_operate(as, &err, &p, &keyA, &ops, &prec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_list* val = as_record_get_list(prec, BString);
	assert_int_eq(as_list_size(val), 3);
	as_record_destroy(prec);
	as_operations_destroy(&ops);

	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(filter);
}

TEST(filter_rec_key, "filter rec key")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_error err;
	as_status rc = aerospike_key_remove(as, &err, NULL, &keyB);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	rc = aerospike_key_put(as, &err, &wp, &keyB, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter,
		as_exp_and(
			as_exp_cmp_eq(as_exp_bin_type(AString),
				as_exp_int(AS_BYTES_INTEGER)),
			as_exp_key_exist()));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(rec, AString, 0);
	assert_int_eq(val, 3);
	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_float_bin, "filter float bin")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	as_error err;
	as_status rc = aerospike_key_put(as, &err, &wp, &keyB, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter,
		as_exp_cmp_gt(as_exp_bin_float(BString), as_exp_float(2.01)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_blob_key, "filter blob key")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	as_error err;
	as_status rc = aerospike_key_put(as, &err, &wp, &keyB, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_key_blob(),
			as_exp_bytes((uint8_t*)BString, (uint32_t)strlen(BString))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_since_update, "filter since update")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	as_error err;
	as_status rc = aerospike_key_put(as, &err, &wp, &keyB, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter,
		as_exp_cmp_ge(as_exp_since_update(), as_exp_int(0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);
	as_record_destroy(rec);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter);
}

TEST(filter_compare_string_to_unk, "filter compare string to unk")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	as_error err;
	as_status rc = aerospike_key_put(as, &err, &wp, &keyB, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter,
		as_exp_cmp_gt(as_exp_bin_str(CString), as_exp_bin_str("bogus")));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_compare_strings, "filter compare strings")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	as_error err;
	as_status rc = aerospike_key_put(as, &err, &wp, &keyB, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter,
		as_exp_cmp_gt(as_exp_bin_str(CString), as_exp_str("abcde")));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_compare_lists_basic, "filter compare lists basic")
{
	as_key keyA;
	as_key keyB;
	assert_true(filter_prepare(&keyA, &keyB));

	as_arraylist l1;
	as_arraylist_inita(&l1, 3);
	assert_int_eq(as_arraylist_append_int64(&l1, 7), 0);
	assert_int_eq(as_arraylist_append_int64(&l1, 9), 0);
	assert_int_eq(as_arraylist_append_int64(&l1, 5), 0);

	as_record new_rec;
	as_record_inita(&new_rec, 1);

	assert_true(as_record_set_list(&new_rec, AString, (as_list*)&l1));

	as_error err;
	as_status rc = aerospike_key_put(as, &err, NULL, &keyA, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter_eq,
			as_exp_cmp_eq(as_exp_bin_list(AString), as_exp_val(&l1)));
	assert_not_null(filter_eq);
	as_exp_build(filter_ne,
			as_exp_cmp_ne(as_exp_bin_list(AString), as_exp_val(&l1)));
	assert_not_null(filter_ne);
	as_exp_build(filter_ge,
			as_exp_cmp_ge(as_exp_bin_list(AString), as_exp_val(&l1)));
	assert_not_null(filter_ge);
	as_exp_build(filter_gt,
			as_exp_cmp_gt(as_exp_bin_list(AString), as_exp_val(&l1)));
	assert_not_null(filter_gt);
	as_exp_build(filter_le,
			as_exp_cmp_le(as_exp_bin_list(AString), as_exp_val(&l1)));
	assert_not_null(filter_le);
	as_exp_build(filter_lt,
			as_exp_cmp_lt(as_exp_bin_list(AString), as_exp_val(&l1)));
	assert_not_null(filter_lt);


	as_policy_read p;
	assert_not_null(as_policy_read_init(&p));
	as_record* rec = NULL;

	// Equality on identical lists.
	p.base.filter_exp = filter_eq;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Non-equality on identical lists.
	p.base.filter_exp = filter_ne;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Greater-equals on identical lists.
	p.base.filter_exp = filter_ge;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Greater-than on identical lists.
	p.base.filter_exp = filter_gt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Less-equals on identical lists.
	p.base.filter_exp = filter_le;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Less-than on identical lists.
	p.base.filter_exp = filter_lt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_arraylist l2;
	as_arraylist_inita(&l2, 3);
	assert_int_eq(as_arraylist_append_int64(&l2, 7), 0);
	assert_int_eq(as_arraylist_append_int64(&l2, 9), 0);
	assert_int_eq(as_arraylist_append_int64(&l2, 10), 0);

	as_record_inita(&new_rec, 1);
	assert_true(as_record_set_list(&new_rec, AString, (as_list*)&l2));

	rc = aerospike_key_put(as, &err, NULL, &keyA, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	// Equality on non-identical lists.
	p.base.filter_exp = filter_eq;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Non-equality on non-identical lists.
	p.base.filter_exp = filter_ne;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Greater-equals on non-identical lists.
	p.base.filter_exp = filter_ge;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Greater-than on non-identical lists.
	p.base.filter_exp = filter_gt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Less-equals on non-identical lists.
	p.base.filter_exp = filter_le;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Less-than on non-identical lists.
	p.base.filter_exp = filter_lt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_record_destroy(rec);
	rec = NULL;

	as_exp_destroy(filter_lt);
	as_exp_destroy(filter_le);
	as_exp_destroy(filter_gt);
	as_exp_destroy(filter_ge);
	as_exp_destroy(filter_ne);
	as_exp_destroy(filter_eq);
	as_arraylist_destroy(&l2);
	as_arraylist_destroy(&l1);
}

TEST(filter_compare_maps_basic, "filter compare maps basic")
{
	as_key keyA;
	as_key keyB;
	assert_true(filter_prepare(&keyA, &keyB));

	as_orderedmap m1; // { "key1"=1, "key2"=2 }
	assert_not_null(as_orderedmap_init(&m1, 2));
	as_string k1;
	assert_not_null(as_string_init(&k1, "key1", false));
	as_integer v1;
	assert_not_null(as_integer_init(&v1, 1));
	assert_int_eq(as_orderedmap_set(&m1, (as_val*)&k1, (as_val*)&v1), 0);
	as_string k2;
	assert_not_null(as_string_init(&k2, "key2", false));
	as_integer v2;
	assert_not_null(as_integer_init(&v2, 2));
	assert_int_eq(as_orderedmap_set(&m1, (as_val*)&k2, (as_val*)&v2), 0);

	as_record new_rec;
	as_record_inita(&new_rec, 1);

	assert_true(as_record_set_map(&new_rec, AString, (as_map*)&m1));

	as_hashmap hm; // { "key1"=1, "key2"=2 }
	assert_not_null(as_orderedmap_init(&hm, 2));
	assert_not_null(as_string_init(&k1, "key1", false));
	assert_not_null(as_integer_init(&v1, 1));
	assert_int_eq(as_orderedmap_set(&hm, (as_val*)&k1, (as_val*)&v1), 0);
	assert_not_null(as_string_init(&k2, "key2", false));
	assert_not_null(as_integer_init(&v2, 2));
	assert_int_eq(as_orderedmap_set(&hm, (as_val*)&k2, (as_val*)&v2), 0);

	as_error err;
	as_status rc = aerospike_key_put(as, &err, NULL, &keyA, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(filter_eq,
			as_exp_cmp_eq(as_exp_bin_map(AString), as_exp_val(&m1)));
	assert_not_null(filter_eq);
	as_exp_build(filter_ne,
			as_exp_cmp_ne(as_exp_bin_map(AString), as_exp_val(&m1)));
	assert_not_null(filter_ne);
	as_exp_build(filter_ge,
			as_exp_cmp_ge(as_exp_bin_map(AString), as_exp_val(&m1)));
	assert_not_null(filter_ge);
	as_exp_build(filter_gt,
			as_exp_cmp_gt(as_exp_bin_map(AString), as_exp_val(&m1)));
	assert_not_null(filter_gt);
	as_exp_build(filter_le,
			as_exp_cmp_le(as_exp_bin_map(AString), as_exp_val(&m1)));
	assert_not_null(filter_le);
	as_exp_build(filter_lt,
			as_exp_cmp_lt(as_exp_bin_map(AString), as_exp_val(&m1)));
	assert_not_null(filter_lt);
	as_exp_build(filter_bad,
			as_exp_cmp_eq(as_exp_bin_map(AString), as_exp_val(&hm)));
	assert_not_null(filter_bad);


	as_policy_read p;
	assert_not_null(as_policy_read_init(&p));
	as_record* rec = NULL;

	// Equality on identical maps.
	p.base.filter_exp = filter_eq;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Non-equality on identical maps.
	p.base.filter_exp = filter_ne;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Greater-equals on identical maps.
	p.base.filter_exp = filter_ge;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Greater-than on identical maps.
	p.base.filter_exp = filter_gt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Less-equals on identical maps.
	p.base.filter_exp = filter_le;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Less-than on identical maps.
	p.base.filter_exp = filter_lt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	as_orderedmap m2; // { "key1"=9, "key2"=3 }
	assert_not_null(as_orderedmap_init(&m2, 2));
	assert_not_null(as_integer_init(&v1, 9));
	assert_int_eq(as_orderedmap_set(&m2, (as_val*)&k1, (as_val*)&v1), 0);
	as_integer v3;
	assert_not_null(as_integer_init(&v3, 3));
	assert_int_eq(as_orderedmap_set(&m2, (as_val*)&k2, (as_val*)&v3), 0);

	as_record_inita(&new_rec, 1);
	assert_true(as_record_set_map(&new_rec, AString, (as_map*)&m2));

	rc = aerospike_key_put(as, &err, NULL, &keyA, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	// Equality on non-identical maps.
	p.base.filter_exp = filter_eq;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Non-equality on non-identical maps.
	p.base.filter_exp = filter_ne;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Greater-equals on non-identical maps.
	p.base.filter_exp = filter_ge;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Greater-than on non-identical maps.
	p.base.filter_exp = filter_gt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	rec = NULL;

	// Less-equals on non-identical maps.
	p.base.filter_exp = filter_le;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Less-than on non-identical maps.
	p.base.filter_exp = filter_lt;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_record_destroy(rec);
	rec = NULL;

	// Compare ordered map bin to unordered map value.
	p.base.filter_exp = filter_bad;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Compare unordered map bin to ordered map value.
	as_record_inita(&new_rec, 1);
	assert_true(as_record_set_map(&new_rec, AString, (as_map*)&hm));
	rc = aerospike_key_put(as, &err, NULL, &keyA, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	p.base.filter_exp = filter_eq;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	// Compare unordered map bin to unordered map value.
	p.base.filter_exp = filter_bad;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;

	as_exp_destroy(filter_bad);
	as_exp_destroy(filter_lt);
	as_exp_destroy(filter_le);
	as_exp_destroy(filter_gt);
	as_exp_destroy(filter_ge);
	as_exp_destroy(filter_ne);
	as_exp_destroy(filter_eq);
	as_orderedmap_destroy(&hm);
	as_orderedmap_destroy(&m2);
	as_orderedmap_destroy(&m1);
}

TEST(filter_xor, "filter xor")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_exclusive(
			as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)),
			as_exp_cmp_eq(as_exp_bin_int(DString), as_exp_int(1))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_add, "filter add")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_add(as_exp_bin_int(AString),
					as_exp_bin_int(DString), as_exp_int(1)),
			as_exp_int(4)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_add_1, "filter add 1")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_add(as_exp_bin_int(AString)),
			as_exp_int(2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_add_float, "filter add float")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_let(
			as_exp_def("val",
				as_exp_add(as_exp_bin_float(BString), as_exp_float(1.1))),
			as_exp_and(
				as_exp_cmp_ge(as_exp_var("val"), as_exp_float(3.2999)),
				as_exp_cmp_le(as_exp_var("val"), as_exp_float(3.3001)))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_add_float_1, "filter add float 1")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_ge(
			as_exp_add(as_exp_bin_float(BString)),
			as_exp_float(2.2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_sub, "filter sub")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_sub(as_exp_int(1),
					as_exp_bin_int(AString),
					as_exp_bin_int(DString)),
			as_exp_int(-2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_sub_1, "filter sub 1")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_sub(as_exp_bin_int(AString)),
			as_exp_int(-2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_sub_float, "filter sub float")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_let(
			as_exp_def("val",
				as_exp_sub(as_exp_bin_float(BString), as_exp_float(1.1))),
			as_exp_and(
				as_exp_cmp_ge(as_exp_var("val"), as_exp_float(1.0999)),
				as_exp_cmp_le(as_exp_var("val"), as_exp_float(1.1001)))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_sub_float_1, "filter sub float 1")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_let(
			as_exp_def("val", as_exp_sub(as_exp_bin_float(BString))),
			as_exp_and(
				as_exp_cmp_le(as_exp_var("val"), as_exp_float(-2.1999)),
				as_exp_cmp_ge(as_exp_var("val"), as_exp_float(-2.2001)))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_mul, "filter mul")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_mul(as_exp_int(2),
					as_exp_bin_int(AString),
					as_exp_bin_int(DString)),
			as_exp_int(4)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_mul_1, "filter mul 1")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_mul(as_exp_bin_int(AString)),
			as_exp_int(2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_div, "filter div")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_div(as_exp_int(8),
					as_exp_bin_int(AString),
					as_exp_bin_int(DString)),
			as_exp_int(4)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_div_1, "filter div 1")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_div(as_exp_bin_int(AString)),
			as_exp_int(0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_div_float, "filter div float")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_div(as_exp_float(8.8),
				as_exp_bin_float(BString)),
			as_exp_float(4.0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_div_float_1, "filter div float 1")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_let(
			as_exp_def("x", as_exp_div(as_exp_bin_float(BString))),
			as_exp_and(
				as_exp_cmp_ge(as_exp_var("x"), as_exp_float(0.4544)),
				as_exp_cmp_le(as_exp_var("x"), as_exp_float(0.4546)))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_pow, "filter pow")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_let(
			as_exp_def("x", as_exp_pow(
				as_exp_bin_float(BString), as_exp_float(2.0))),
			as_exp_and(
				as_exp_cmp_ge(as_exp_var("x"), as_exp_float(4.8399)),
				as_exp_cmp_le(as_exp_var("x"), as_exp_float(4.8401)))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_log, "filter log")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_let(
			as_exp_def("x", as_exp_log(
				as_exp_bin_float(BString), as_exp_float(2.0))),
			as_exp_and(
				as_exp_cmp_ge(as_exp_var("x"), as_exp_float(1.1374)),
				as_exp_cmp_le(as_exp_var("x"), as_exp_float(1.1376)))));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_mod, "filter mod")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_mod(
			as_exp_bin_int(AString), as_exp_int(2)), as_exp_int(0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_abs, "filter abs")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_abs(as_exp_bin_int(EString)),
			as_exp_int(2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_floor, "filter floor")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_floor(as_exp_bin_float(BString)),
			as_exp_float(2.0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_ceil, "filter ceil")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_ceil(as_exp_bin_float(BString)),
			as_exp_float(3.0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_to_int, "filter to_int")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_to_int(as_exp_bin_float(BString)),
			as_exp_int(2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_to_float, "filter to float")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_to_float(as_exp_bin_int(AString)),
			as_exp_float(2.0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_bitwise_and, "filter bitwise and")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_and(
						as_exp_bin_int(AString),
						as_exp_int(0)),
					as_exp_int(0)),
				as_exp_cmp_eq(
					as_exp_int_and(
						as_exp_bin_int(AString),
						as_exp_int(-1)),
					as_exp_int(0)),
				as_exp_cmp_eq(
					as_exp_int_and(
						as_exp_bin_int(BString),
						as_exp_int(-1)),
					as_exp_int(-1)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_and(
					as_exp_bin_int(AString),
					as_exp_int(0)),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_int_and(
					as_exp_bin_int(AString),
					as_exp_int(-1)),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_int_and(
					as_exp_bin_int(BString),
					as_exp_int(-1)),
				as_exp_int(-1))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_or, "filter bitwise or")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_or(
						as_exp_bin_int(AString),
						as_exp_int(0)),
					as_exp_int(0)),
				as_exp_cmp_eq(
					as_exp_int_or(
						as_exp_bin_int(AString),
						as_exp_int(-1)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_or(
						as_exp_bin_int(BString),
						as_exp_int(-1)),
					as_exp_int(-1)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_or(
					as_exp_bin_int(AString),
					as_exp_int(0)),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_int_or(
					as_exp_bin_int(AString),
					as_exp_int(-1)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_or(
					as_exp_bin_int(BString),
					as_exp_int(-1)),
				as_exp_int(-1))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_xor, "filter bitwise xor")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_xor(
						as_exp_bin_int(AString),
						as_exp_int(0)),
					as_exp_int(0)),
				as_exp_cmp_eq(
					as_exp_int_xor(
						as_exp_bin_int(AString),
						as_exp_int(-1)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_xor(
						as_exp_bin_int(BString),
						as_exp_int(-1)),
					as_exp_int(0)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_xor(
					as_exp_bin_int(AString),
					as_exp_int(0)),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_int_xor(
					as_exp_bin_int(AString),
					as_exp_int(-1)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_xor(
					as_exp_bin_int(BString),
					as_exp_int(-1)),
				as_exp_int(0))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_not, "filter bitwise not")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_not(
						as_exp_bin_int(AString)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_not(
						as_exp_bin_int(BString)),
					as_exp_int(0)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_not(
					as_exp_bin_int(AString)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_not(
					as_exp_bin_int(BString)),
				as_exp_int(0))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_lshift, "filter bitwise lshift")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_lshift(
						as_exp_bin_int(BString),
						as_exp_int(1)),
					as_exp_int(0xFFFFffffFFFFfffe)),
				as_exp_cmp_eq(
					as_exp_int_lshift(
						as_exp_bin_int(CString),
						as_exp_int(1)),
					as_exp_int(2)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_lshift(
					as_exp_bin_int(BString),
					as_exp_int(1)),
				as_exp_int(0xFFFFffffFFFFfffe)),
			as_exp_cmp_eq(
				as_exp_int_lshift(
					as_exp_bin_int(CString),
					as_exp_int(1)),
				as_exp_int(2))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_rshift, "filter bitwise rshift")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_rshift(
						as_exp_bin_int(BString),
						as_exp_int(1)),
					as_exp_int(0x7FFFffffFFFFffff)),
				as_exp_cmp_eq(
					as_exp_int_rshift(
						as_exp_bin_int(CString),
						as_exp_int(1)),
					as_exp_int(0)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_rshift(
					as_exp_bin_int(BString),
					as_exp_int(1)),
				as_exp_int(0x7FFFffffFFFFffff)),
			as_exp_cmp_eq(
				as_exp_int_rshift(
					as_exp_bin_int(CString),
					as_exp_int(1)),
				as_exp_int(0))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_arshift, "filter bitwise arshift")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_arshift(
						as_exp_bin_int(BString),
						as_exp_int(1)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_arshift(
						as_exp_bin_int(CString),
						as_exp_int(1)),
					as_exp_int(0)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_arshift(
					as_exp_bin_int(BString),
					as_exp_int(1)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_arshift(
					as_exp_bin_int(CString),
					as_exp_int(1)),
				as_exp_int(0))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_count, "filter bitwise count")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_count(
						as_exp_bin_int(AString)),
					as_exp_int(0)),
				as_exp_cmp_eq(
					as_exp_int_count(
						as_exp_bin_int(BString)),
					as_exp_int(64)),
				as_exp_cmp_eq(
					as_exp_int_count(
						as_exp_bin_int(CString)),
					as_exp_int(1)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_count(
					as_exp_bin_int(AString)),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_int_count(
					as_exp_bin_int(BString)),
				as_exp_int(64)),
			as_exp_cmp_eq(
				as_exp_int_count(
					as_exp_bin_int(CString)),
				as_exp_int(1))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_lscan, "filter bitwise lscan")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_lscan(
						as_exp_bin_int(AString),
						as_exp_bool(true)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_lscan(
						as_exp_bin_int(BString),
						as_exp_bool(true)),
					as_exp_int(0)),
				as_exp_cmp_eq(
					as_exp_int_lscan(
						as_exp_bin_int(CString),
						as_exp_bool(true)),
					as_exp_int(63)),
				as_exp_cmp_eq(
					as_exp_int_lscan(
						as_exp_bin_int(AString),
						as_exp_bool(false)),
					as_exp_int(0)),
				as_exp_cmp_eq(
					as_exp_int_lscan(
					as_exp_bin_int(BString),
						as_exp_bool(false)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_lscan(
						as_exp_bin_int(CString),
						as_exp_bool(false)),
					as_exp_int(0)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_lscan(
					as_exp_bin_int(AString),
					as_exp_bool(true)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_lscan(
					as_exp_bin_int(BString),
					as_exp_bool(true)),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_int_lscan(
					as_exp_bin_int(CString),
					as_exp_bool(true)),
				as_exp_int(63)),
			as_exp_cmp_eq(
				as_exp_int_lscan(
					as_exp_bin_int(AString),
					as_exp_bool(false)),
				as_exp_int(0)),
			as_exp_cmp_eq(
				as_exp_int_lscan(
				as_exp_bin_int(BString),
					as_exp_bool(false)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_lscan(
				as_exp_bin_int(CString),
					as_exp_bool(false)),
				as_exp_int(0))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_bitwise_rscan, "filter bitwise rscan")
{
	as_key keyA;
	bool b = filter_prepare_bitwise(&keyA);

	assert_true(b);

	as_exp_build(filter1,
		as_exp_not(
			as_exp_and(
				as_exp_cmp_eq(
					as_exp_int_rscan(
						as_exp_bin_int(AString),
						as_exp_bool(true)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_rscan(
						as_exp_bin_int(BString),
						as_exp_bool(true)),
					as_exp_int(63)),
				as_exp_cmp_eq(
					as_exp_int_rscan(
						as_exp_bin_int(CString),
						as_exp_bool(true)),
					as_exp_int(63)),
				as_exp_cmp_eq(
					as_exp_int_rscan(
						as_exp_bin_int(AString),
						as_exp_bool(false)),
					as_exp_int(63)),
				as_exp_cmp_eq(
					as_exp_int_rscan(
						as_exp_bin_int(BString),
						as_exp_bool(false)),
					as_exp_int(-1)),
				as_exp_cmp_eq(
					as_exp_int_rscan(
						as_exp_bin_int(CString),
						as_exp_bool(false)),
					as_exp_int(62)))));
	assert_not_null(filter1);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter1;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	as_exp_build(filter2,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_int_rscan(
					as_exp_bin_int(AString),
					as_exp_bool(true)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_rscan(
					as_exp_bin_int(BString),
					as_exp_bool(true)),
				as_exp_int(63)),
			as_exp_cmp_eq(
				as_exp_int_rscan(
					as_exp_bin_int(CString),
					as_exp_bool(true)),
				as_exp_int(63)),
			as_exp_cmp_eq(
				as_exp_int_rscan(
					as_exp_bin_int(AString),
					as_exp_bool(false)),
				as_exp_int(63)),
			as_exp_cmp_eq(
				as_exp_int_rscan(
					as_exp_bin_int(BString),
					as_exp_bool(false)),
				as_exp_int(-1)),
			as_exp_cmp_eq(
				as_exp_int_rscan(
					as_exp_bin_int(CString),
					as_exp_bool(false)),
				as_exp_int(62))));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter1);
	as_exp_destroy(filter2);
}

TEST(filter_min, "filter min")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_min(
				as_exp_bin_int(AString),
				as_exp_int(2)),
			as_exp_int(2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_max, "filter max")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_max(
				as_exp_bin_int(AString),
				as_exp_int(1)),
			as_exp_int(2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_min_float, "filter min float")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_min(
				as_exp_bin_float(BString),
				as_exp_float(2.3)),
			as_exp_float(2.2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_max_float, "filter max float")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_max(
				as_exp_bin_float(BString),
				as_exp_float(0.9)),
			as_exp_float(2.2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_FILTERED_OUT);

	rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &keyB, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	as_exp_destroy(filter);
}

TEST(filter_let, "filter let")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 0);

	as_error err;
	as_status rc = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	rc = aerospike_key_put(as, &err, &wp, &rkey, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(predexp,
		as_exp_let(
			as_exp_def("x",
				as_exp_cmp_eq(as_exp_bin_type(AString), as_exp_int(1))),
			as_exp_and(
				as_exp_var("x"),
				as_exp_key_exist())));

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = predexp;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(rec, AString, 0);
	assert_int_eq(val, 3);
	as_record_destroy(rec);

	as_exp_destroy(predexp);
}

TEST(filter_cond, "filter cond")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 0);

	as_error err;
	as_status rc = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;

	as_record new_rec;
	as_record_inita(&new_rec, 1);
	as_record_set_int64(&new_rec, AString, 3);
	rc = aerospike_key_put(as, &err, &wp, &rkey, &new_rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_policy_read p;
	as_policy_read_init(&p);

	as_exp_build(predexp1,
		as_exp_cmp_eq(
			as_exp_cond(
				as_exp_cmp_eq(as_exp_int(3), as_exp_bin_int(AString)),
				as_exp_int(10),
				as_exp_cmp_eq(as_exp_int(4), as_exp_bin_int(AString)),
				as_exp_int(20),
				as_exp_int(30)),
			as_exp_int(10)));
	assert_not_null(predexp1);
	p.base.filter_exp = predexp1;

	as_record* rec = NULL;
	rc = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(rec, AString, 0);
	assert_int_eq(val, 3);
	as_record_destroy(rec);
	rec = NULL;

	as_exp_destroy(predexp1);

	as_exp_build(predexp2,
		as_exp_cmp_eq(
			as_exp_cond(
				as_exp_cmp_eq(as_exp_int(1), as_exp_bin_int(AString)),
				as_exp_int(10),
				as_exp_cmp_eq(as_exp_int(3), as_exp_bin_int(AString)),
				as_exp_int(20),
				as_exp_int(30)),
			as_exp_int(20)));
	assert_not_null(predexp2);
	p.base.filter_exp = predexp2;

	rc = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	val = as_record_get_int64(rec, AString, 0);
	assert_int_eq(val, 3);
	as_record_destroy(rec);
	rec = NULL;

	as_exp_destroy(predexp2);

	as_exp_build(predexp3,
		as_exp_cmp_eq(
			as_exp_cond(
				as_exp_cmp_eq(as_exp_int(1), as_exp_bin_int(AString)),
				as_exp_int(10),
				as_exp_cmp_eq(as_exp_int(2), as_exp_bin_int(AString)),
				as_exp_int(20),
				as_exp_int(30)),
			as_exp_int(30)));
	assert_not_null(predexp3);
	p.base.filter_exp = predexp3;

	rc = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	val = as_record_get_int64(rec, AString, 0);
	assert_int_eq(val, 3);
	as_record_destroy(rec);
	rec = NULL;

	as_exp_destroy(predexp3);
}

TEST(filter_list_value_to_bin, "filter list value to bin")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_arraylist list;
	as_arraylist_init(&list, 2, 1);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, -1);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE,
					AS_EXP_TYPE_INT, as_exp_int(1), as_exp_val(&list)),
			as_exp_int(-1)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_arraylist_destroy(&list);
	as_record_destroy(rec);
	as_exp_destroy(filter);
}

TEST(filter_map_value_to_bin, "filter map value to bin")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_arraylist list;
	as_arraylist_init(&list, 2, 1);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, -1);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE,
					AS_EXP_TYPE_INT, as_exp_int(1), as_exp_val(&list)),
			as_exp_int(-1)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_arraylist_destroy(&list);
	as_record_destroy(rec);
	as_exp_destroy(filter);
}

TEST(filter_blob_value_to_bin, "filter blob value to bin")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	uint8_t bytes[] = { 0, 1, 2, 3 };

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_bit_count(as_exp_int(24), as_exp_uint(8),
				as_exp_bytes(bytes, sizeof(bytes))),
			as_exp_int(2)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_exp_destroy(filter);
}

TEST(filter_hll_value_to_bin, "filter hll value to bin")
{
#define HLLBIN "hllbin"
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_error err;
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_init(&ops, HLLBIN, NULL, NULL, 10);

	as_record* rec = NULL;
	as_status rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;

	static const char* bins[] = { HLLBIN, NULL };
	as_record* rec_hll = NULL;

	rc = aerospike_key_select(as, &err, NULL, &keyA, bins, &rec_hll);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_bytes* bytes_hll = as_record_get_bytes(rec_hll, HLLBIN);
	assert_not_null(bytes_hll);
	assert_int_eq(bytes_hll->type, AS_BYTES_HLL);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_hll_get_count(as_exp_val(bytes_hll)),
			as_exp_int(0)));
	assert_not_null(filter);

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.filter_exp = filter;

	rc = aerospike_key_get(as, &err, &p, &keyA, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec_hll);
	as_record_destroy(rec);
	as_exp_destroy(filter);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(filter_exp, "filter expression tests")
{
	suite_before(before);
	suite_after(after);

	suite_add(filter_put);
	suite_add(filter_get);
	suite_add(filter_batch);
	suite_add(filter_delete);
	suite_add(filter_operate);
	suite_add(filter_udf);
	suite_add(filter_call);
	suite_add(filter_call_chain);
	suite_add(filter_call_modify);
	suite_add(filter_call_context);
	suite_add(filter_call_context_param);
	suite_add(filter_rec_key);
	suite_add(filter_float_bin);
	suite_add(filter_blob_key);
	suite_add(filter_since_update);
	suite_add(filter_compare_string_to_unk);
	suite_add(filter_compare_strings);

	// Requires Aerospike 6.3.
	suite_add(filter_compare_lists_basic);
	suite_add(filter_compare_maps_basic);

	// Requires Aerospike 5.6.
	suite_add(filter_xor);
	suite_add(filter_add);
	suite_add(filter_add_1);
	suite_add(filter_add_float);
	suite_add(filter_add_float_1);
	suite_add(filter_sub);
	suite_add(filter_sub_1);
	suite_add(filter_add_float);
	suite_add(filter_add_float_1);
	suite_add(filter_mul);
	suite_add(filter_mul_1);
	suite_add(filter_div);
	suite_add(filter_div_1);
	suite_add(filter_div_float);
	suite_add(filter_div_float_1);
	suite_add(filter_pow);
	suite_add(filter_log);
	suite_add(filter_mod);
	suite_add(filter_abs);
	suite_add(filter_floor);
	suite_add(filter_ceil);
	suite_add(filter_to_int);
	suite_add(filter_to_float);
	suite_add(filter_bitwise_and);
	suite_add(filter_bitwise_or);
	suite_add(filter_bitwise_xor);
	suite_add(filter_bitwise_not);
	suite_add(filter_bitwise_lshift);
	suite_add(filter_bitwise_rshift);
	suite_add(filter_bitwise_arshift);
	suite_add(filter_bitwise_count);
	suite_add(filter_bitwise_lscan);
	suite_add(filter_bitwise_rscan);
	suite_add(filter_min);
	suite_add(filter_max);
	suite_add(filter_min_float);
	suite_add(filter_max_float);
	suite_add(filter_let);
	suite_add(filter_cond);
	// Value to bin promotion tests:
	suite_add(filter_list_value_to_bin);
	suite_add(filter_map_value_to_bin);
	suite_add(filter_blob_value_to_bin);
	suite_add(filter_hll_value_to_bin);
}
