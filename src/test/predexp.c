/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_predexp.h>
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
#define SET "test_predexp"
#define LUA_FILE AS_START_DIR "src/test/lua/key_apply.lua"
#define UDF_FILE "key_apply"

static const char* AString = "A";
static const char* BString = "B";

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
predexp_prepare(as_key* keyA, as_key* keyB)
{
	as_error err;
	as_key_init(keyA, NAMESPACE, SET, AString);
	as_status rc = aerospike_key_remove(as, &err, NULL, keyA);

	if (! (rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND)) {
		return false;
	}

	as_key_init(keyB, NAMESPACE, SET, BString);
	rc = aerospike_key_remove(as, &err, NULL, keyB);

	if (! (rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND)) {
		return false;
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, AString, 1);
	rc = aerospike_key_put(as, &err, NULL, keyA, &rec);

	if (rc != AEROSPIKE_OK) {
		return false;
	}

	as_record_set_int64(&rec, AString, 2);
	rc = aerospike_key_put(as, &err, NULL, keyB, &rec);

	if (rc != AEROSPIKE_OK) {
		return false;
	}
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(predexp_put, "predexp put")
{
	as_key keyA;
	as_key keyB;
	bool b = predexp_prepare(&keyA, &keyB);
	assert_true(b);

	as_predexp_list predexp;
	as_predexp_list_inita(&predexp, 3);
	as_predexp_list_add(&predexp, as_predexp_integer_bin(AString));
	as_predexp_list_add(&predexp, as_predexp_integer_value(1));
	as_predexp_list_add(&predexp, as_predexp_integer_equal());

	as_policy_write p;
	as_policy_write_init(&p);
	p.base.predexp = &predexp;

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

	as_predexp_list_destroy(&predexp);
}

TEST(predexp_get, "predexp get")
{
	as_key keyA;
	as_key keyB;
	bool b = predexp_prepare(&keyA, &keyB);
	assert_true(b);

	as_predexp_list predexp;
	as_predexp_list_inita(&predexp, 3);
	as_predexp_list_add(&predexp, as_predexp_integer_bin(AString));
	as_predexp_list_add(&predexp, as_predexp_integer_value(1));
	as_predexp_list_add(&predexp, as_predexp_integer_equal());

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.predexp = &predexp;

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

	as_predexp_list_destroy(&predexp);
}

TEST(predexp_batch, "predexp batch")
{
	as_key keyA;
	as_key keyB;
	bool b = predexp_prepare(&keyA, &keyB);
	assert_true(b);

	as_predexp_list predexp;
	as_predexp_list_inita(&predexp, 3);
	as_predexp_list_add(&predexp, as_predexp_integer_bin(AString));
	as_predexp_list_add(&predexp, as_predexp_integer_value(1));
	as_predexp_list_add(&predexp, as_predexp_integer_equal());

	as_policy_batch p;
	as_policy_batch_init(&p);
	p.base.predexp = &predexp;

	as_batch_read_records records;
	as_batch_read_inita(&records, 2);

	as_batch_read_record* recA = as_batch_read_reserve(&records);
	as_key_init(&recA->key, NAMESPACE, SET, AString);
	recA->read_all_bins = true;

	as_batch_read_record* recB = as_batch_read_reserve(&records);
	as_key_init(&recB->key, NAMESPACE, SET, BString);
	recB->read_all_bins = true;

	as_error err;
	as_status rc = aerospike_batch_read(as, &err, &p, &records);
	assert_int_eq(rc, AEROSPIKE_OK);

	assert_int_eq(recA->result, AEROSPIKE_OK);
	int64_t val = as_record_get_int64(&recA->record, AString, -1);
	assert_int_eq(val, 1);

	assert_int_eq(recB->result, AEROSPIKE_FILTERED_OUT);

	as_batch_read_destroy(&records);
	as_predexp_list_destroy(&predexp);
}

TEST(predexp_delete, "predexp delete")
{
	as_key keyA;
	as_key keyB;
	bool b = predexp_prepare(&keyA, &keyB);
	assert_true(b);

	as_predexp_list predexp;
	as_predexp_list_inita(&predexp, 3);
	as_predexp_list_add(&predexp, as_predexp_integer_bin(AString));
	as_predexp_list_add(&predexp, as_predexp_integer_value(1));
	as_predexp_list_add(&predexp, as_predexp_integer_equal());

	as_policy_remove p;
	as_policy_remove_init(&p);
	p.base.predexp = &predexp;

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

	as_predexp_list_destroy(&predexp);
}

TEST(predexp_operate, "predexp operate read")
{
	as_key keyA;
	as_key keyB;
	bool b = predexp_prepare(&keyA, &keyB);
	assert_true(b);

	as_predexp_list predexp;
	as_predexp_list_inita(&predexp, 3);
	as_predexp_list_add(&predexp, as_predexp_integer_bin(AString));
	as_predexp_list_add(&predexp, as_predexp_integer_value(1));
	as_predexp_list_add(&predexp, as_predexp_integer_equal());

	as_policy_operate p;
	as_policy_operate_init(&p);
	p.base.predexp = &predexp;

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

	as_predexp_list_destroy(&predexp);
}

TEST(predexp_udf, "predexp udf")
{
	as_key keyA;
	as_key keyB;
	bool b = predexp_prepare(&keyA, &keyB);
	assert_true(b);

	as_predexp_list predexp;
	as_predexp_list_inita(&predexp, 3);
	as_predexp_list_add(&predexp, as_predexp_integer_bin(AString));
	as_predexp_list_add(&predexp, as_predexp_integer_value(1));
	as_predexp_list_add(&predexp, as_predexp_integer_equal());

	as_policy_apply p;
	as_policy_apply_init(&p);
	p.base.predexp = &predexp;

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

	as_predexp_list_destroy(&predexp);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(predexp, "predicate expression tests")
{
    suite_before(before);
    suite_after(after);

	suite_add(predexp_put);
	suite_add(predexp_get);
	suite_add(predexp_batch);
	suite_add(predexp_delete);
	suite_add(predexp_operate);
	suite_add(predexp_udf);
}
