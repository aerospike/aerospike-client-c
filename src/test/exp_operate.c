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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include "test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_expop"

static char* AString = "A";
static char* BString = "B";
static char* CString = "C";
static char* DString = "D";
static char* ExpVar = "EV";
static char* NewString = "New";

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool
before(atf_suite* suite)
{
	return true;
}

static bool
after(atf_suite* suite)
{
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
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, AString, 1);
	as_record_set_int64(&rec, DString, 2);
	rc = aerospike_key_put(as, &err, NULL, keyA, &rec);

	if (rc != AEROSPIKE_OK) {
		return false;
	}

	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, BString, 2);
	as_record_set_int64(&rec, DString, 2);
	rc = aerospike_key_put(as, &err, NULL, keyB, &rec);

	if (rc != AEROSPIKE_OK) {
		return false;
	}
	return true;
}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(exp_read_eval_errors, "exp read eval errors")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(read_exp, as_exp_add(as_exp_bin_int(AString), as_exp_int(4)));
	assert_not_null(read_exp);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, read_exp, AS_EXP_READ_DEFAULT);

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyB, &ops, &rec);
	// Bin AString doesn't exist on keyB.
	assert_int_eq(rc, AEROSPIKE_ERR_OP_NOT_APPLICABLE);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, read_exp, AS_EXP_READ_EVAL_NO_FAIL);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyB, &ops, &rec);
	// Eval failure was ignored.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(read_exp);
}

TEST(exp_read_on_write_eval_errors, "exp read on write eval errors")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(write_exp, as_exp_bin_int(DString));
	assert_not_null(write_exp);

	as_exp_build(read_exp, as_exp_bin_int(AString));
	assert_not_null(read_exp);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_exp_write(&ops, DString, write_exp, AS_EXP_WRITE_DEFAULT);
	as_operations_exp_read(&ops, ExpVar, read_exp, AS_EXP_READ_DEFAULT);

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyB, &ops, &rec);
	// Bin AString doesn't exist on keyB.
	assert_int_eq(rc, AEROSPIKE_ERR_OP_NOT_APPLICABLE);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, read_exp, AS_EXP_READ_EVAL_NO_FAIL);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyB, &ops, &rec);
	// Eval failure was ignored.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(read_exp);
	as_exp_destroy(write_exp);
}

TEST(exp_write_eval_errors, "exp write eval errors")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(write_exp, as_exp_add(as_exp_bin_int(AString), as_exp_int(4)));
	assert_not_null(write_exp);

	as_exp_build(read_exp, as_exp_bin_int(CString));
	assert_not_null(read_exp);

	as_operations ops;
	as_operations_inita(&ops, 3);
	as_operations_exp_write(&ops, CString, write_exp, AS_EXP_WRITE_DEFAULT);
	as_operations_exp_read(&ops, ExpVar, read_exp, AS_EXP_READ_DEFAULT);
	as_operations_add_read(&ops, CString);

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[1]), AS_INTEGER);
	assert_int_eq(as_bin_get_value(&results[1])->integer.value, 5);
	assert_int_eq(as_bin_get_type(&results[2]), AS_INTEGER);
	assert_int_eq(as_bin_get_value(&results[2])->integer.value, 5);

	as_record_destroy(rec);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyB, &ops, &rec);
	// keyB doesn't have bin AString
	assert_int_eq(rc, AEROSPIKE_ERR_OP_NOT_APPLICABLE);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 2);
	as_operations_exp_write(&ops, CString, write_exp, AS_EXP_WRITE_EVAL_NO_FAIL);
	as_operations_exp_read(&ops, ExpVar, read_exp, AS_EXP_READ_DEFAULT);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyB, &ops, &rec);
	// Bin CString shouldn't exist since the write op failed.
	assert_int_eq(rc, AEROSPIKE_ERR_OP_NOT_APPLICABLE);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 2);
	as_operations_exp_write(&ops, CString, write_exp, AS_EXP_WRITE_EVAL_NO_FAIL);
	as_operations_exp_read(&ops, ExpVar, read_exp, AS_EXP_READ_EVAL_NO_FAIL);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyB, &ops, &rec);
	// All failures should have been ignored.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(read_exp);
	as_exp_destroy(write_exp);
}

TEST(exp_write_policy_errors, "exp write policy errors")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(write_exp, as_exp_add(as_exp_bin_int(AString), as_exp_int(4)));
	assert_not_null(write_exp);

	as_error err;
	as_record* rec;
	as_status rc;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, write_exp, AS_EXP_WRITE_UPDATE_ONLY);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Bin CString doesn't exist.
	assert_int_eq(rc, AEROSPIKE_ERR_BIN_NOT_FOUND);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, write_exp,
		AS_EXP_WRITE_UPDATE_ONLY | AS_EXP_WRITE_POLICY_NO_FAIL);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Ignore that bin CString didn't exist.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, write_exp, AS_EXP_WRITE_CREATE_ONLY);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Bin CString created.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, write_exp, AS_EXP_WRITE_CREATE_ONLY);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Bin CString already exists.
	assert_int_eq(rc, AEROSPIKE_ERR_BIN_EXISTS);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, write_exp,
		AS_EXP_WRITE_CREATE_ONLY | AS_EXP_WRITE_POLICY_NO_FAIL);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Ignore that bin CString already exists.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_exp_build(delete_exp, as_exp_nil());
	assert_not_null(write_exp);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, delete_exp, AS_EXP_WRITE_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Cannot delete bin without ALLOW_DELETE policy.
	assert_int_eq(rc, AEROSPIKE_ERR_OP_NOT_APPLICABLE);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, delete_exp, AS_EXP_WRITE_POLICY_NO_FAIL);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Ignore that operation would delete bin.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, delete_exp, AS_EXP_WRITE_ALLOW_DELETE);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Delete allowed.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_write(&ops, CString, write_exp, AS_EXP_WRITE_CREATE_ONLY);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	// Bin CString created - verifying that the bin was deleted.
	assert_int_eq(rc, AEROSPIKE_OK);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(delete_exp);
	as_exp_destroy(write_exp);
}

TEST(exp_returns_unknown, "exp returns unknown")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(expr, as_exp_cond(
		as_exp_cmp_eq(as_exp_bin_int(CString), as_exp_int(5)), as_exp_unknown(),
		as_exp_bin_exists(AString), as_exp_int(5),
		as_exp_unknown()));
	assert_not_null(expr);

	as_operations ops;

	as_operations_inita(&ops, 2);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, CString);

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_ERR_OP_NOT_APPLICABLE);

	as_operations_destroy(&ops);

	as_operations_inita(&ops, 2);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_EVAL_NO_FAIL);
	as_operations_add_read(&ops, CString);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_NIL);
	assert_int_eq(as_bin_get_type(&results[1]), AS_NIL);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_returns_nil, "exp returns nil")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(expr, as_exp_nil());
	assert_not_null(expr);

	as_operations ops;

	as_operations_inita(&ops, 2);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	as_operations_add_read(&ops, CString);

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_NIL);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_returns_int, "exp returns int")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(expr, as_exp_add(as_exp_bin_int(AString), as_exp_int(4)));
	assert_not_null(expr);

	as_error err;
	as_record* rec;
	as_status rc;
	as_operations ops;
	as_bin* results;

	as_operations_inita(&ops, 3);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, CString);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[1]), AS_INTEGER);
	assert_int_eq(as_bin_get_value(&results[1])->integer.value, 5);
	assert_int_eq(as_bin_get_type(&results[2]), AS_INTEGER);
	assert_int_eq(as_bin_get_value(&results[2])->integer.value, 5);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_INTEGER);
	assert_int_eq(as_bin_get_value(&results[0])->integer.value, 5);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_returns_float, "exp returns float")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(expr, as_exp_add(
			as_exp_to_float(as_exp_bin_int(AString)),
			as_exp_float(4.0)));
	assert_not_null(expr);

	as_error err;
	as_record* rec;
	as_status rc;
	as_operations ops;
	as_bin* results;

	as_operations_inita(&ops, 3);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, CString);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[1]), AS_DOUBLE);
	assert_double_eq(as_bin_get_value(&results[1])->dbl.value, 5.0);
	assert_int_eq(as_bin_get_type(&results[2]), AS_DOUBLE);
	assert_double_eq(as_bin_get_value(&results[2])->dbl.value, 5.0);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_DOUBLE);
	assert_double_eq(as_bin_get_value(&results[0])->dbl.value, 5.0);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_returns_string, "exp returns string")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	const char* str = "xxx";

	as_exp_build(expr, as_exp_str(str));
	assert_not_null(expr);

	as_error err;
	as_record* rec;
	as_status rc;
	as_operations ops;
	as_bin* results;

	as_operations_inita(&ops, 3);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, CString);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[1]), AS_STRING);
	assert_string_eq(as_bin_get_value(&results[1])->string.value, str);
	assert_int_eq(as_bin_get_type(&results[2]), AS_STRING);
	assert_string_eq(as_bin_get_value(&results[2])->string.value, str);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_STRING);
	assert_string_eq(as_bin_get_value(&results[0])->string.value, str);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_returns_blob, "exp returns blob")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	uint8_t blob[] = {0x78, 0x78, 0x78};
	as_exp_build(expr, as_exp_bytes(blob, sizeof(blob)));
	assert_not_null(expr);

	as_error err;
	as_record* rec;
	as_status rc;
	as_operations ops;
	as_bin* results;

	as_operations_inita(&ops, 3);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, CString);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[1]), AS_BYTES);
	assert_bytes_eq(as_bin_get_value(&results[1])->bytes.value,
			as_bin_get_value(&results[1])->bytes.size, blob, sizeof(blob));
	assert_int_eq(as_bin_get_type(&results[2]), AS_BYTES);
	assert_bytes_eq(as_bin_get_value(&results[2])->bytes.value,
			as_bin_get_value(&results[2])->bytes.size, blob, sizeof(blob));

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_BYTES);
	assert_bytes_eq(as_bin_get_value(&results[0])->bytes.value,
			as_bin_get_value(&results[0])->bytes.size, blob, sizeof(blob));

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_returns_bool, "exp returns bool")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(expr, as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(1)));
	assert_not_null(expr);

	as_error err;
	as_record* rec;
	as_status rc;
	as_operations ops;
	as_bin* results;

	as_operations_inita(&ops, 3);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, CString);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[1]), AS_BOOLEAN);
	assert_true(as_bin_get_value(&results[1])->boolean.value);
	assert_int_eq(as_bin_get_type(&results[2]), AS_BOOLEAN);
	assert_true(as_bin_get_value(&results[2])->boolean.value);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_BOOLEAN);
	assert_true(as_bin_get_value(&results[0])->boolean.value);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_returns_hll, "exp returns hll")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(expr, as_exp_hll_init(NULL, 4, as_exp_nil()));
	assert_not_null(expr);

	as_error err;
	as_record* rec_hll;
	as_record* rec;
	as_status rc;
	as_operations ops;
	as_bin* results;

	as_operations_inita(&ops, 5);
	as_operations_hll_init(&ops, NewString, NULL, NULL, 4);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, NewString);
	as_operations_add_read(&ops, CString);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	rec_hll = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec_hll);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec_hll->bins.entries;

	assert_int_eq(as_bin_get_type(&results[2]), AS_BYTES);
	uint8_t* hll = as_bin_get_value(&results[2])->bytes.value;
	uint32_t hll_size = as_bin_get_value(&results[2])->bytes.size;

	assert_int_eq(as_bin_get_type(&results[3]), AS_BYTES);
	assert_bytes_eq(as_bin_get_value(&results[3])->bytes.value,
			as_bin_get_value(&results[3])->bytes.size, hll, hll_size);
	assert_int_eq(as_bin_get_type(&results[4]), AS_BYTES);
	assert_bytes_eq(as_bin_get_value(&results[4])->bytes.value,
			as_bin_get_value(&results[4])->bytes.size, hll, hll_size);

	as_operations_destroy(&ops);

	as_operations_inita(&ops, 2);
	as_operations_exp_read(&ops, ExpVar, expr, AS_EXP_READ_DEFAULT);
	as_operations_add_read(&ops, CString);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_BYTES);
	assert_bytes_eq(as_bin_get_value(&results[0])->bytes.value,
			as_bin_get_value(&results[0])->bytes.size, hll, hll_size);

	as_record_destroy(rec_hll);
	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(expr);
}

TEST(exp_merge, "exp merge")
{
	as_key keyA;
	as_key keyB;
	bool b = filter_prepare(&keyA, &keyB);
	assert_true(b);

	as_exp_build(e, as_exp_cmp_eq(as_exp_bin_int(AString), as_exp_int(0)));

	as_exp_build(eand,
 		as_exp_and(
 			as_exp_expr(e),
			as_exp_cmp_eq(as_exp_bin_int(DString), as_exp_int(2))));

	as_exp_build(eor,
 		as_exp_or(
 			as_exp_expr(e),
			as_exp_cmp_eq(as_exp_bin_int(DString), as_exp_int(2))));

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_exp_read(&ops, "res1", eand, AS_EXP_READ_DEFAULT);
	as_operations_exp_read(&ops, "res2", eor, AS_EXP_READ_DEFAULT);

	as_error err;
	as_record* rec = NULL;
	as_status rc = aerospike_key_operate(as, &err, NULL, &keyA, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	bool res1 = as_bin_get_value(&results[0])->boolean.value;
	assert_false(res1);
	bool res2 = as_bin_get_value(&results[1])->boolean.value;
	assert_true(res2);

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_exp_destroy(e);
	as_exp_destroy(eand);
	as_exp_destroy(eor);
}

TEST(exp_base64, "exp base64")
{
	as_exp_build(exp,
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

	char* base64 = as_exp_to_base64(exp);
	assert_not_null(base64);

	as_exp* exp2 = as_exp_from_base64(base64);
	assert_not_null(exp2);
	assert_int_eq(exp2->packed_sz, exp->packed_sz);

	int r = memcmp(exp2->packed, exp->packed, exp->packed_sz);
	assert_int_eq(r, 0);

	as_exp_destroy_base64(base64);
	as_exp_destroy(exp);
	as_exp_destroy(exp2);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(exp_operate, "filter expression tests")
{
	suite_before(before);
	suite_after(after);

	suite_add(exp_read_eval_errors);
	suite_add(exp_read_on_write_eval_errors);
	suite_add(exp_write_eval_errors);
	suite_add(exp_write_policy_errors);
	suite_add(exp_returns_unknown);
	suite_add(exp_returns_nil);
	suite_add(exp_returns_int);
	suite_add(exp_returns_float);
	suite_add(exp_returns_string);
	suite_add(exp_returns_blob);
	suite_add(exp_returns_bool);
	suite_add(exp_returns_hll);
	suite_add(exp_merge);
	suite_add(exp_base64);
}
