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
#include <aerospike/aerospike_txn.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include "test.h"

//---------------------------------
// Global Variables
//---------------------------------

extern aerospike* as;

//---------------------------------
// Macros
//---------------------------------

#define NAMESPACE "test"
#define SET "txn"
#define BIN "a"

//---------------------------------
// Static Functions
//---------------------------------

/*
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
*/

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
	as_policy_write_copy(&as->config.policies.write, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_write_copy(&as->config.policies.write, &pw);
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

	status = aerospike_commit(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_write_copy(&as->config.policies.write, &pw1);
	pw1.base.txn = &txn1;

	as_policy_write pw2;
	as_policy_write_copy(&as->config.policies.write, &pw2);
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

	status = aerospike_commit(as, &err, &txn1);
	assert_int_eq(status, AEROSPIKE_OK);

	status = aerospike_abort(as, &err, &txn2);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_write_copy(&as->config.policies.write, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record_set_int64(&rec, BIN, 3);

	// Not run under txn. Should be blocked.
	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_MRT_BLOCKED);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_write_copy(&as->config.policies.write, &pw);
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

	status = aerospike_commit(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_write_copy(&as->config.policies.write, &pw);
	pw.base.txn = &txn;

	as_record_set_int64(&rec, BIN, 2);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_policy_read pr;
	as_policy_read_copy(&as->config.policies.read, &pr);
	pr.base.txn = &txn;

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, &pr, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 2);
	as_record_destroy(recp);

	status = aerospike_abort(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_remove_copy(&as->config.policies.remove, &prem);
	prem.base.txn = &txn;
	prem.durable_delete = true;

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_remove_copy(&as->config.policies.remove, &prem);
	prem.base.txn = &txn;
	prem.durable_delete = true;

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_abort(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_remove_copy(&as->config.policies.remove, &prem);
	prem.base.txn = &txn;
	prem.durable_delete = true;

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	status = aerospike_key_remove(as, &err, &prem, &key);
	assert_int_eq(status, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	as_record_destroy(&rec);

	status = aerospike_commit(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_operate_copy(&as->config.policies.operate, &po);
	po.base.txn = &txn;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);

	as_record* recp = NULL;
	status = aerospike_key_operate(as, &err, &po, &key, &ops, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(recp);

	status = aerospike_commit(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

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
	as_policy_operate_copy(&as->config.policies.operate, &po);
	po.base.txn = &txn;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);

	as_record* recp = NULL;
	status = aerospike_key_operate(as, &err, &po, &key, &ops, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(recp);

	status = aerospike_abort(as, &err, &txn);
	assert_int_eq(status, AEROSPIKE_OK);

	recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);

	int64_t val = as_record_get_int64(recp, BIN, -1);
	assert_int_eq(val, 1);
	as_record_destroy(recp);
}


//---------------------------------
// Test Suite
//---------------------------------

SUITE(transaction, "Transaction tests")
{
	//suite_before(before);
	//suite_after(after);

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
}
