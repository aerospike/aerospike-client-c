/*
 * Copyright 2008-2026 Aerospike, Inc.
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
#include <aerospike/aerospike_txn.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_udf.h>
#include "test.h"
#include "util/udf.h"

//---------------------------------
// Global Variables
//---------------------------------

extern aerospike* as;
extern bool g_has_sc;

//---------------------------------
// Macros
//---------------------------------

#define NAMESPACE "test"
#define SET "txn"
#define BIN "a"
#define LUA_FILE AS_START_DIR "src/test/lua/udf_record.lua"
#define UDF_FILE "udf_record"

//---------------------------------
// Static Functions
//---------------------------------

static bool
before(atf_suite* suite)
{
	const char * filename = UDF_FILE".lua";

	as_error err;
	as_bytes content;

	info("reading: %s",LUA_FILE);
	bool b = udf_readfile(LUA_FILE, &content);

	if (! b) {
		return false;
	}

	info("uploading: %s",filename);
	aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

	if (err.code != AEROSPIKE_OK) {
		return false;
	}

	aerospike_udf_put_wait(as, &err, NULL, filename, 100);
	as_bytes_destroy(&content);
	return true;
}

static bool
after(atf_suite* suite)
{
	const char* filename = UDF_FILE".lua";
	as_error err;

	aerospike_udf_remove(as, &err, NULL, filename);

	if (err.code != AEROSPIKE_OK) {
		return false;
	}
	return true;
}

//---------------------------------
// Test Cases
//---------------------------------

TEST(txn_write, "transaction write")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_write");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 2);
	as_record_destroy(recp);
}

TEST(txn_write_twice, "transaction write twice")
{
	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_write_twice");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 2);
	as_record_destroy(recp);
}

TEST(txn_write_conflict, "transaction write conflict")
{
	as_txn txn1;
	as_txn_init(&txn1);

	as_txn txn2;
	as_txn_init(&txn2);

	as_policy_write pw1;
	as_policy_write_default(as, &pw1);
	pw1.base.txn = &txn1;

	as_policy_write pw2;
	as_policy_write_default(as, &pw2);
	pw2.base.txn = &txn2;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_write_conflict");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, &pw1, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw2, &key, &rec);
	assert_int_eq(status, AEROSPIKE_MRT_BLOCKED);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn1, NULL);
	assert_int_eq(status, AEROSPIKE_OK);

	status = aerospike_abort(as, &err, &txn2, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn1);
	as_txn_destroy(&txn2);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1);
	as_record_destroy(recp);
}

TEST(txn_write_block, "transaction write block")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_write_block");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record_set_int64(&rec, BIN, 3);

	// Not run under txn. Should be blocked.
	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_MRT_BLOCKED);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 2);
	as_record_destroy(recp);
}

TEST(txn_write_read, "transaction write read")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_write_read");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1); // Should be original value.
	as_record_destroy(recp);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 2);
	as_record_destroy(recp);
}

TEST(txn_write_abort, "transaction write abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_write_abort");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_policy_read pr;
	as_policy_read_default(as, &pr);
	pr.base.txn = &txn;

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, &pr, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 2);
	as_record_destroy(recp);

	status = aerospike_abort(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1);
	as_record_destroy(recp);
}

TEST(txn_delete, "transaction delete")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_delete");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_remove prem;
	as_policy_remove_default(as, &prem);
	prem.base.txn = &txn;
	prem.durable_delete = true;

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	as_record_destroy(recp);
}

TEST(txn_delete_abort, "transaction delete abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_delete_abort");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_remove prem;
	as_policy_remove_default(as, &prem);
	prem.base.txn = &txn;
	prem.durable_delete = true;

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_abort(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1);
	as_record_destroy(recp);
}

TEST(txn_delete_twice, "transaction delete twice")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_delete_twice");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_remove prem;
	as_policy_remove_default(as, &prem);
	prem.base.txn = &txn;
	prem.durable_delete = true;

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	as_record_destroy(recp);
}

TEST(txn_touch, "transaction touch")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_touch");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_operate po;
	as_policy_operate_default(as, &po);
	po.base.txn = &txn;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);

	as_record* recp = NULL;
	status = aerospike_key_operate(as, &err, &po, &key, &ops, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(recp);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1);
	as_record_destroy(recp);
}

TEST(txn_touch_abort, "transaction touch abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_touch_abort");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_operate po;
	as_policy_operate_default(as, &po);
	po.base.txn = &txn;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);

	as_record* recp = NULL;
	status = aerospike_key_operate(as, &err, &po, &key, &ops, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(recp);

	status = aerospike_abort(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1);
	as_record_destroy(recp);
}

TEST(txn_operate_write, "transaction operate write")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_operate_write");

	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, BIN, 1);
	as_record_set_int64(&rec, "bin2", 1000);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_operate po;
	as_policy_operate_default(as, &po);
	po.base.txn = &txn;

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_write_int64(&ops, BIN, 2);
	as_operations_add_read(&ops, "bin2");

	as_record* recp = NULL;
	status = aerospike_key_operate(as, &err, &po, &key, &ops, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	int64_t val = as_record_get_int64(recp, "bin2", -1);
	assert_int_eq(val, 1000);
	as_record_destroy(recp);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 2);
	as_record_destroy(recp);
}

TEST(txn_operate_write_abort, "transaction operate write abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_operate_write_abort");

	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, BIN, 1);
	as_record_set_int64(&rec, "bin2", 1000);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_operate po;
	as_policy_operate_default(as, &po);
	po.base.txn = &txn;

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_write_int64(&ops, BIN, 2);
	as_operations_add_read(&ops, "bin2");

	as_record* recp = NULL;
	status = aerospike_key_operate(as, &err, &po, &key, &ops, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	int64_t val = as_record_get_int64(recp, "bin2", -1);
	assert_int_eq(val, 1000);
	as_record_destroy(recp);

	status = aerospike_abort(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1);
	as_record_destroy(recp);
}

TEST(txn_udf, "transaction udf")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_udf");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_apply pa;
	as_policy_apply_default(as, &pa);
	pa.base.txn = &txn;

	as_arraylist args;
	as_arraylist_init(&args, 2, 0);
	as_arraylist_append_str(&args, BIN);
	as_arraylist_append_int64(&args, 2);

	as_val* val = NULL;
	status = aerospike_key_apply(as, &err, &pa, &key, "udf_record", "write_bin", (as_list*)&args, &val);
	assert_int_eq(status, AEROSPIKE_OK);
	as_arraylist_destroy(&args);
	as_val_destroy(val);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	int64_t rv = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(rv, 2);
	as_record_destroy(recp);
}

TEST(txn_udf_abort, "transaction udf abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_udf_abort");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_apply pa;
	as_policy_apply_default(as, &pa);
	pa.base.txn = &txn;

	as_arraylist args;
	as_arraylist_init(&args, 2, 0);
	as_arraylist_append_str(&args, BIN);
	as_arraylist_append_int64(&args, 2);

	as_val* val = NULL;
	status = aerospike_key_apply(as, &err, &pa, &key, "udf_record", "write_bin", (as_list*)&args, &val);
	assert_int_eq(status, AEROSPIKE_OK);
	as_arraylist_destroy(&args);
	as_val_destroy(val);

	status = aerospike_abort(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	int64_t rv = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(rv, 1);
	as_record_destroy(recp);
}

typedef struct {
	int64_t expect;
	uint32_t errors;
} batch_data;

static bool
batch_write_cb(const as_batch_result* results, uint32_t n_keys, void* udata)
{
	batch_data* data = udata;

	for (uint32_t i = 0; i < n_keys; i++) {
		const as_batch_result* r = &results[i];

		if (r->result != AEROSPIKE_OK) {
			data->errors++;
		}
	}
	return true;
}

static bool
batch_read_cb(const as_batch_result* results, uint32_t n_keys, void* udata)
{
	batch_data* data = udata;

	for (uint32_t i = 0; i < n_keys; i++) {
		const as_batch_result* r = &results[i];

		if (r->result != AEROSPIKE_OK) {
			data->errors++;
			continue;
		}

		int64_t rv = as_record_get_int64(&r->record, BIN, -1);

		if (rv != data->expect) {
			data->errors++;
		}
	}
	return true;
}

TEST(txn_batch, "transaction batch")
{
	uint32_t n_keys = 4096; // max key count for a transaction

	as_batch batch;
	as_batch_inita(&batch, n_keys);
	
	for (uint32_t i = 0; i < n_keys; i++) {
		as_key_init_int64(as_batch_keyat(&batch, i), NAMESPACE, SET, i);
	}

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_write_int64(&ops, BIN, 1);

	batch_data data = {0};
	as_error err;
	as_status status;

	status = aerospike_batch_operate(as, &err, NULL, NULL, &batch, &ops, batch_write_cb, &data);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.errors, 0);
	as_operations_destroy(&ops);

	as_txn txn;
	as_txn_init_capacity(&txn, n_keys, n_keys);

	as_policy_batch pb;
	as_policy_batch_parent_write_default(as, &pb);
	pb.base.txn = &txn;

	as_operations_inita(&ops, 1);
	as_operations_add_write_int64(&ops, BIN, 2);

	status = aerospike_batch_operate(as, &err, &pb, NULL, &batch, &ops, batch_write_cb, &data);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.errors, 0);
	as_operations_destroy(&ops);

	status = aerospike_commit(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	data.expect = 2;

	status = aerospike_batch_get(as, &err, NULL, &batch, batch_read_cb, &data);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.errors, 0);

	as_batch_destroy(&batch);
}

TEST(txn_batch_too_many_keys, "transaction batch too many keys")
{
	uint32_t n_keys = 4097;

	as_batch batch;
	as_batch_inita(&batch, n_keys);
	
	for (uint32_t i = 0; i < n_keys; i++) {
		as_key_init_int64(as_batch_keyat(&batch, i), NAMESPACE, SET, i);
	}

	as_txn txn;
	as_txn_init_capacity(&txn, n_keys, n_keys);

	batch_data data = {0};
	as_error err;
	as_status status;

	as_policy_batch pb;
	as_policy_batch_parent_write_default(as, &pb);
	pb.base.txn = &txn;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_write_int64(&ops, BIN, 3);

	status = aerospike_batch_operate(as, &err, &pb, NULL, &batch, &ops, batch_write_cb, &data);
	assert_int_eq(status, AEROSPIKE_MRT_TOO_MANY_WRITES);
	assert_int_eq(data.errors, 0);
	as_operations_destroy(&ops);

	status = aerospike_abort(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	as_batch_destroy(&batch);
}

TEST(txn_batch_abort, "transaction batch abort")
{
	uint32_t n_keys = 10;

	as_batch batch;
	as_batch_inita(&batch, n_keys);
	
	for (uint32_t i = 0; i < n_keys; i++) {
		as_key_init_int64(as_batch_keyat(&batch, i), NAMESPACE, SET, i);
	}

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_write_int64(&ops, BIN, 1);

	batch_data data = {0};
	as_error err;
	as_status status;

	status = aerospike_batch_operate(as, &err, NULL, NULL, &batch, &ops, batch_write_cb, &data);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.errors, 0);
	as_operations_destroy(&ops);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_batch pb;
	as_policy_batch_parent_write_default(as, &pb);
	pb.base.txn = &txn;

	as_operations_inita(&ops, 1);
	as_operations_add_write_int64(&ops, BIN, 2);

	status = aerospike_batch_operate(as, &err, &pb, NULL, &batch, &ops, batch_write_cb, &data);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.errors, 0);
	as_operations_destroy(&ops);

	status = aerospike_abort(as, &err, &txn, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_txn_destroy(&txn);

	data.expect = 1;

	status = aerospike_batch_get(as, &err, NULL, &batch, batch_read_cb, &data);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.errors, 0);

	as_batch_destroy(&batch);
}

TEST(txn_commit_fail_safe_abortable, "commit fail (not in doubt) leaves txn abortable")
{
	// A clean, not-in-doubt commit failure at mark roll forward must NOT block a
	// subsequent abort. Sabotage: durable-delete the transaction monitor record so
	// mark roll forward fails with MRT_EXPIRED (in_doubt=false).
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_commit_fail_safe_abortable");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);
	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	// Sabotage: durable-delete the monitor record for this transaction.
	as_key mkey;
	as_key_init_int64(&mkey, NAMESPACE, "<ERO~MRT", (int64_t)txn.id);

	as_policy_remove prem;
	as_policy_remove_default(as, &prem);
	prem.durable_delete = true;

	status = aerospike_key_remove(as, &err, &prem, &mkey);
	assert_int_eq(status, AEROSPIKE_OK);

	// Commit fails at mark roll forward, but the failure is not in doubt, so the
	// txn stays VERIFIED (abortable) rather than transitioning to COMMIT_FAILED.
	as_commit_status commit_status = -1;
	status = aerospike_commit(as, &err, &txn, &commit_status);
	assert_int_ne(status, AEROSPIKE_OK);
	assert_int_eq(commit_status, AS_COMMIT_MARK_ROLL_FORWARD_ABANDONED);
	assert_int_eq(txn.in_doubt, false);
	assert_int_eq(txn.state, AS_TXN_STATE_VERIFIED);

	// Abort must be allowed (not blocked) so the still-locked records are released.
	as_abort_status abort_status = -1;
	status = aerospike_abort(as, &err, &txn, &abort_status);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_ne(abort_status, AS_ABORT_COMMIT_FAILED);
	assert_int_eq(txn.state, AS_TXN_STATE_ABORTED);

	as_txn_destroy(&txn);
}

TEST(txn_commit_fail_abort_blocked, "abort blocked after in-doubt commit failure")
{
	// An in-doubt commit failure at mark roll forward must block a subsequent abort
	// (the commit may still advance, so a roll back could discard committed
	// writes), while the commit remains retryable.
	// The true in-doubt path requires a transport fault to trigger
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_commit_fail_abort_blocked");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);
	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	// Simulate an in-doubt mark roll forward failure.
	txn.state = AS_TXN_STATE_COMMIT_FAILED;
	txn.in_doubt = true;

	// Abort must be blocked without sending a roll back.
	as_abort_status abort_status = -1;
	status = aerospike_abort(as, &err, &txn, &abort_status);
	assert_int_eq(status, AEROSPIKE_TXN_FAILED);
	assert_int_eq(abort_status, AS_ABORT_COMMIT_FAILED);
	assert_int_eq(txn.state, AS_TXN_STATE_COMMIT_FAILED);

	// Commit must still be dispatched (retryable) from COMMIT_FAILED. The retry
	// resolves the transaction and cleans up.
	as_commit_status commit_status = -1;
	status = aerospike_commit(as, &err, &txn, &commit_status);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn_destroy(&txn);
}

TEST(txn_commit_fail_command_blocked, "commands blocked while txn in commit-failed state")
{
	// While a txn is in the in-doubt COMMIT_FAILED state, issuing further commands
	// on it must be rejected by as_txn_verify_command rather than proceeding.
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_commit_fail_command_blocked");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, 1);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_default(as, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);
	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	// Simulate an in-doubt mark roll forward failure.
	txn.state = AS_TXN_STATE_COMMIT_FAILED;
	txn.in_doubt = true;

	// A further command on the txn must be rejected (not sent to the server).
	as_record_set_int64(&rec, BIN, 3);
	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_ERR_PARAM);
	as_record_destroy(&rec);

	// Retry commit to resolve the transaction and clean up.
	as_commit_status commit_status = -1;
	status = aerospike_commit(as, &err, &txn, &commit_status);
	assert_int_eq(status, AEROSPIKE_OK);

	as_txn_destroy(&txn);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(transaction, "Transaction tests")
{
	// Transactions require strong consistency namespaces.
	if (!g_has_sc) {
		return;
	}

	suite_before(before);
	suite_after(after);

	suite_add(txn_write);
	suite_add(txn_write_twice);
	suite_add(txn_write_conflict);
	suite_add(txn_write_block);
	suite_add(txn_write_read);
	suite_add(txn_write_abort);
	suite_add(txn_delete);
	suite_add(txn_delete_abort);
	suite_add(txn_delete_twice);
	suite_add(txn_touch);
	suite_add(txn_touch_abort);
	suite_add(txn_operate_write);
	suite_add(txn_operate_write_abort);
	suite_add(txn_udf);
	suite_add(txn_udf_abort);
	suite_add(txn_batch);
	suite_add(txn_batch_too_many_keys);
	suite_add(txn_batch_abort);
	suite_add(txn_commit_fail_safe_abortable);
	suite_add(txn_commit_fail_abort_blocked);
	suite_add(txn_commit_fail_command_blocked);
}
