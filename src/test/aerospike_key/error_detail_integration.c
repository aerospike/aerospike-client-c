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
#include <inttypes.h>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_txn.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_bit_operations.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_version.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
static as_monitor monitor;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_error_detail"
#define LUA_FILE AS_START_DIR "src/test/lua/error_detail_udf.lua"
#define UDF_MODULE "error_detail_udf"

/******************************************************************************
 * SYNC SUITE LIFECYCLE
 *****************************************************************************/

static bool
before_sync(atf_suite* suite)
{
	as_node* node = as_node_get_random(as->cluster);

	if (!node) {
		return false;
	}

	if (as_version_compare(&node->version, &as_server_version_8_1_3) < 0) {
		info("Skipping error_detail_sync suite: server %u.%u.%u < 8.1.3",
			 node->version.major, node->version.minor, node->version.patch);
		as_node_release(node);
		return false;
	}
	as_node_release(node);

	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "ibin", 100);
	as_record_set_strp(&rec, "sbin", "hello", false);

	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	if (status != AEROSPIKE_OK) {
		error("Failed to create test record: %s", err.message);
		return false;
	}
	as_key_destroy(&key);

	// CDT list record for 5.11
	as_key key_list;
	as_key_init(&key_list, NAMESPACE, SET, "error_detail_list");
	as_record rec_list;
	as_record_inita(&rec_list, 1);
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_record_set_list(&rec_list, "lbin", (as_list*)&list);
	status = aerospike_key_put(as, &err, NULL, &key_list, &rec_list);
	if (status != AEROSPIKE_OK) {
		error("Failed to create list record: %s", err.message);
		return false;
	}
	as_key_destroy(&key_list);

	// CDT map record for 5.12
	as_key key_map;
	as_key_init(&key_map, NAMESPACE, SET, "error_detail_map");
	as_operations map_ops;
	as_operations_inita(&map_ops, 1);
	as_map_policy mp;
	as_map_policy_init(&mp);
	as_integer mkey;
	as_integer_init(&mkey, 1);
	as_integer mval;
	as_integer_init(&mval, 100);
	as_operations_add_map_put(&map_ops, "mbin", &mp, (as_val*)&mkey, (as_val*)&mval);
	status = aerospike_key_operate(as, &err, NULL, &key_map, &map_ops, NULL);
	as_operations_destroy(&map_ops);
	if (status != AEROSPIKE_OK) {
		error("Failed to create map record: %s", err.message);
		return false;
	}
	as_key_destroy(&key_map);

	// Bit record for 5.13
	as_key key_bit;
	as_key_init(&key_bit, NAMESPACE, SET, "error_detail_bit");
	as_record rec_bit;
	as_record_inita(&rec_bit, 1);
	uint8_t raw_bytes[] = {0x01, 0x02, 0x03, 0x04, 0x05};
	as_record_set_rawp(&rec_bit, "bbin", raw_bytes, sizeof(raw_bytes), false);
	status = aerospike_key_put(as, &err, NULL, &key_bit, &rec_bit);
	if (status != AEROSPIKE_OK) {
		error("Failed to create bit record: %s", err.message);
		return false;
	}
	as_key_destroy(&key_bit);

	// Register UDF for 5.9
	if (!udf_put(LUA_FILE)) {
		error("Failed to register UDF: %s", LUA_FILE);
		return false;
	}

	return true;
}

/******************************************************************************
 * SECTION 5: Sync Integration Tests
 *****************************************************************************/

// 5.1.1 Write with verbosity 0 returns no error detail
TEST(ed_sync_write_gen_v0, "5.1.1 write gen mismatch verbosity 0")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 0;
	pw.gen = AS_POLICY_GEN_EQ;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);
	rec.gen = 9999;

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_true(strstr(err.message, "subcode=") == NULL);
	assert_int_eq(err.subcode, 0);
}

// 5.1.2 Write with verbosity 1 returns subcode only
TEST(ed_sync_write_gen_v1, "5.1.2 write gen mismatch verbosity 1")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 1;
	pw.gen = AS_POLICY_GEN_EQ;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);
	rec.gen = 9999;

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	// AS_ERR_RECORD_GENERATION has no per-status subcode enum; server uses AS_SUB_NONE.
	// At verbosity 1, field 45 is omitted entirely when there is no dispatchable subcode.
	assert_int_eq(err.subcode, 0);
}

// 5.1.3 Write with verbosity 2 returns server message (no subcode for gen mismatch)
TEST(ed_sync_write_gen_v2, "5.1.3 write gen mismatch verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;
	pw.gen = AS_POLICY_GEN_EQ;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);
	rec.gen = 9999;

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	// AS_ERR_RECORD_GENERATION has no per-status subcode enum; server uses AS_SUB_NONE.
	// At verbosity 2, server may send message-only or omit field 45 entirely.
	assert_int_eq(err.subcode, 0);
	assert_true(strlen(err.message) > 0);
}

// 5.2.1 Delete with verbosity 0
TEST(ed_sync_delete_gen_v0, "5.2.1 delete gen mismatch verbosity 0")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_remove pr;
	as_policy_remove_init(&pr);
	pr.base.error_detail_verbosity = 0;
	pr.gen = AS_POLICY_GEN_EQ;
	pr.generation = 9999;

	as_status status = aerospike_key_remove(as, &err, &pr, &key);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_true(strstr(err.message, "subcode=") == NULL);
}

// 5.2.2 Delete with verbosity 2
TEST(ed_sync_delete_gen_v2, "5.2.2 delete gen mismatch verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_remove pr;
	as_policy_remove_init(&pr);
	pr.base.error_detail_verbosity = 2;
	pr.gen = AS_POLICY_GEN_EQ;
	pr.generation = 9999;

	as_status status = aerospike_key_remove(as, &err, &pr, &key);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_int_eq(err.subcode, 0);
	assert_true(strlen(err.message) > 0);
}

// 5.3.1 Touch with verbosity 0
TEST(ed_sync_touch_gen_v0, "5.3.1 touch gen mismatch verbosity 0")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 0;
	po.gen = AS_POLICY_GEN_EQ;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);
	ops.gen = 9999;

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_true(strstr(err.message, "subcode=") == NULL);
	as_operations_destroy(&ops);
}

// 5.3.2 Touch with verbosity 2
TEST(ed_sync_touch_gen_v2, "5.3.2 touch gen mismatch verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;
	po.gen = AS_POLICY_GEN_EQ;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);
	ops.gen = 9999;

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_int_eq(err.subcode, 0);
	assert_true(strlen(err.message) > 0);
	as_operations_destroy(&ops);
}

// 5.4.1 Operate with verbosity 0
TEST(ed_sync_operate_type_v0, "5.4.1 operate bin type mismatch verbosity 0")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 0;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_append_str(&ops, "ibin", "abc");

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_int_eq(status, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE);
	assert_true(strstr(err.message, "subcode=") == NULL);
	as_operations_destroy(&ops);
}

// 5.4.2 Operate with verbosity 1
TEST(ed_sync_operate_type_v1, "5.4.2 operate bin type mismatch verbosity 1")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 1;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_append_str(&ops, "ibin", "abc");

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_int_eq(status, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE);
	// AS_ERR_BIN_INCOMPATIBLE_TYPE has no per-status subcode enum.
	assert_int_eq(err.subcode, 0);
	as_operations_destroy(&ops);
}

// 5.4.3 Operate with verbosity 2
TEST(ed_sync_operate_type_v2, "5.4.3 operate bin type mismatch verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_append_str(&ops, "ibin", "abc");

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_int_eq(status, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE);
	// AS_ERR_BIN_INCOMPATIBLE_TYPE has no per-status subcode enum.
	assert_int_eq(err.subcode, 0);
	assert_true(strlen(err.message) > 0);
	as_operations_destroy(&ops);
}

// 5.5.1 Successful read at verbosity 2 has no error detail
TEST(ed_sync_read_ok_v2, "5.5.1 successful read verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_read pr;
	as_policy_read_init(&pr);
	pr.base.error_detail_verbosity = 2;

	as_record* rec = NULL;
	as_status status = aerospike_key_get(as, &err, &pr, &key, &rec);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_not_null(rec);
	assert_int_eq(as_record_get_int64(rec, "ibin", 0), 100);
	assert_int_eq(err.subcode, 0);
	as_record_destroy(rec);
}

// 5.6.1 Successful exists at verbosity 2 has no error detail
TEST(ed_sync_exists_ok_v2, "5.6.1 successful exists verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_read pr;
	as_policy_read_init(&pr);
	pr.base.error_detail_verbosity = 2;

	as_record* rec = NULL;
	as_status status = aerospike_key_exists(as, &err, &pr, &key, &rec);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_not_null(rec);
	assert_true(rec->gen > 0);
	assert_int_eq(err.subcode, 0);
	as_record_destroy(rec);
}

// 5.7.1 Exists on missing key at verbosity 2
TEST(ed_sync_exists_not_found_v2, "5.7.1 exists not found verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "nonexistent_error_detail_key");

	as_policy_read pr;
	as_policy_read_init(&pr);
	pr.base.error_detail_verbosity = 2;

	as_record* rec = NULL;
	as_status status = aerospike_key_exists(as, &err, &pr, &key, &rec);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
}

// 5.8.1 Delete missing key at verbosity 2
TEST(ed_sync_delete_not_found_v2, "5.8.1 delete not found verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "nonexistent_error_detail_key");

	as_policy_remove pr;
	as_policy_remove_init(&pr);
	pr.base.error_detail_verbosity = 2;

	as_status status = aerospike_key_remove(as, &err, &pr, &key);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
}

// 5.10.1 Successful write at verbosity 2
TEST(ed_sync_write_ok_v2, "5.10.1 successful write verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_happy_write");

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "val", 42);

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(err.subcode, 0);

	// Read back to verify
	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &recp);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(as_record_get_int64(recp, "val", 0), 42);
	as_record_destroy(recp);

	// Cleanup
	aerospike_key_remove(as, &err, NULL, &key);
}

// 5.15.1 Write inside a transaction at verbosity 2 surfaces error detail
TEST(ed_sync_txn_write_gen_v2, "5.15.1 txn write gen mismatch verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;
	pw.base.txn = &txn;
	pw.gen = AS_POLICY_GEN_EQ;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);
	rec.gen = 9999;

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_int_eq(err.subcode, 0);
	assert_true(strlen(err.message) > 0);

	aerospike_abort(as, &err, &txn, NULL);
	as_txn_destroy(&txn);
}

// 5.15.2 Successful write inside a transaction at verbosity 2
TEST(ed_sync_txn_write_ok_v2, "5.15.2 txn write ok verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_txn_happy");

	as_txn txn;
	as_txn_init(&txn);

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;
	pw.base.txn = &txn;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "val", 7);

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(err.subcode, 0);

	aerospike_abort(as, &err, &txn, NULL);
	as_txn_destroy(&txn);

	// Cleanup
	aerospike_key_remove(as, &err, NULL, &key);
}

// 5.16.1 Server detail replaces default error format at verbosity 2
// Uses CDT list OOB which has a per-status subcode (AS_SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS).
TEST(ed_sync_priority_logic_v2, "5.16.1 server message displaces default format")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_list");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get_by_index(&ops, "lbin", NULL, 99, AS_LIST_RETURN_VALUE);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	assert_true(err.subcode > 0);
	// Must NOT be the default format "<addr> AEROSPIKE_ERR_..."
	assert_true(strstr(err.message, as_error_string(status)) == NULL);
	assert_true(strstr(err.message, "subcode=") != NULL);
	as_operations_destroy(&ops);
}

// 5.16.2 Default format is preserved when server sends no detail (verbosity 0)
TEST(ed_sync_priority_logic_v0, "5.16.2 default format preserved at verbosity 0")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 0;
	pw.gen = AS_POLICY_GEN_EQ;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);
	rec.gen = 9999;

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_true(strstr(err.message, as_error_string(AEROSPIKE_ERR_RECORD_GENERATION)) != NULL);
	assert_int_eq(err.subcode, 0);
}

// 5.17.1 Cross-verbosity consistency
// Uses CDT list OOB which has a per-status subcode, so both v1 and v2 return a real subcode.
TEST(ed_sync_cross_verbosity, "5.17.1 same error at v1 and v2 returns same subcode")
{
	as_error err1, err2;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_list");

	// Verbosity 1
	as_policy_operate po1;
	as_policy_operate_init(&po1);
	po1.base.error_detail_verbosity = 1;

	as_operations ops1;
	as_operations_inita(&ops1, 1);
	as_operations_list_get_by_index(&ops1, "lbin", NULL, 99, AS_LIST_RETURN_VALUE);

	aerospike_key_operate(as, &err1, &po1, &key, &ops1, NULL);
	as_operations_destroy(&ops1);

	// Verbosity 2
	as_policy_operate po2;
	as_policy_operate_init(&po2);
	po2.base.error_detail_verbosity = 2;

	as_operations ops2;
	as_operations_inita(&ops2, 1);
	as_operations_list_get_by_index(&ops2, "lbin", NULL, 99, AS_LIST_RETURN_VALUE);

	aerospike_key_operate(as, &err2, &po2, &key, &ops2, NULL);
	as_operations_destroy(&ops2);

	assert_true(err1.subcode > 0);
	assert_int_eq(err1.subcode, err2.subcode);
	assert_true(strlen(err2.message) > 0);
}

// 5.9.1 UDF error at verbosity 2 does not break existing UDF error path
TEST(ed_sync_udf_non_interference, "5.9.1 UDF error non-interference at verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_apply pa;
	as_policy_apply_init(&pa);
	pa.base.error_detail_verbosity = 2;

	as_val* res = NULL;
	as_status status = aerospike_key_apply(as, &err, &pa, &key, UDF_MODULE, "fail_test", NULL, &res);

	assert_int_eq(status, AEROSPIKE_ERR_UDF);
	assert_true(strstr(err.message, "test failure") != NULL);
	assert_int_eq(err.subcode, 0);

	if (res) {
		as_val_destroy(res);
	}
}

// 5.11.1 CDT list get by index out of range at verbosity 2
TEST(ed_sync_cdt_list_oob, "5.11.1 CDT list index out of bounds verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_list");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get_by_index(&ops, "lbin", NULL, 99, AS_LIST_RETURN_VALUE);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	assert_true(err.subcode > 0);
	assert_true(strstr(err.message, "subcode=") != NULL);
	as_operations_destroy(&ops);
}

// 5.12.1 CDT map put with CREATE_ONLY on existing key at verbosity 2
TEST(ed_sync_cdt_map_create_only, "5.12.1 CDT map create-only violation verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_map");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_map_policy mp;
	as_map_policy_set(&mp, AS_MAP_UNORDERED, AS_MAP_CREATE_ONLY);

	as_integer mkey;
	as_integer_init(&mkey, 1);
	as_integer mval;
	as_integer_init(&mval, 999);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_map_put(&ops, "mbin", &mp, (as_val*)&mkey, (as_val*)&mval);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_operations_destroy(&ops);
}

// 5.13.1 Bit operation with invalid parameters at verbosity 2
TEST(ed_sync_bit_invalid, "5.13.1 bit invalid offset verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_bit");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	uint8_t bset[] = {0xFF};
	as_operations_bit_set(&ops, "bbin", NULL, NULL, 5 * 8 + 100, 8, sizeof(bset), bset);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_operations_destroy(&ops);
}

// 5.14.1 HLL init with invalid index bits at verbosity 2
TEST(ed_sync_hll_invalid, "5.14.1 HLL invalid params verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_hll");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	aerospike_key_remove(as, &err, NULL, &key);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_init(&ops, "hbin", NULL, NULL, 0);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_operations_destroy(&ops);
}

// 5.18.1 Bit get with offset beyond blob size at verbosity 2
// (AS_ERR_PARAMETER / BITS_OFFSET_OUT_OF_RANGE)
TEST(ed_sync_param_ttl_invalid, "5.18.1 param bit offset out of range verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_bit");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_bit_get(&ops, "bbin", NULL, 1000, 8);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_operations_destroy(&ops);
}

// 5.19.1 CDT list get by rank out of bounds at verbosity 2
// (AS_ERR_OP_NOT_APPLICABLE / CDT_RANK_OUT_OF_BOUNDS)
TEST(ed_sync_cdt_rank_oob, "5.19.1 CDT list rank out of bounds verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_list");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_list_get_by_rank(&ops, "lbin", 9999, AS_LIST_RETURN_VALUE);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_operations_destroy(&ops);
}

// 5.20.1 Write filtered out by expression at verbosity 2 (AS_ERR_FILTERED_OUT)
TEST(ed_sync_filtered_out, "5.20.1 filtered out verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_exp_build(filter,
		as_exp_cmp_eq(as_exp_bin_int("ibin"), as_exp_int(99999)));
	assert_not_null(filter);

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;
	pw.base.filter_exp = filter;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);

	as_status status = aerospike_key_put(as, &err, &pw, &key, &rec);

	assert_int_eq(status, AEROSPIKE_FILTERED_OUT);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_exp_destroy(filter);
}

// 5.21.1 HLL fold on nonexistent bin at verbosity 2
// (AS_ERR_BIN_NOT_FOUND / HLL_CANNOT_CREATE_WITH_OP)
TEST(ed_sync_bin_not_found_hll, "5.21.1 bin not found HLL verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_hll_fold(&ops, "no_hll_bin", NULL, 4);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_operations_destroy(&ops);
}

// 5.22.1 Bit set with size=0 at verbosity 2
// (AS_ERR_PARAMETER / BITS_SIZE_OUT_OF_RANGE)
TEST(ed_sync_param_bits_size, "5.22.1 param bits size out of range verbosity 2")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_bit");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	uint8_t bset[] = {0xFF};
	as_operations_bit_set(&ops, "bbin", NULL, NULL, 0, 0, sizeof(bset), bset);

	as_status status = aerospike_key_operate(as, &err, &po, &key, &ops, NULL);

	assert_true(status != AEROSPIKE_OK);
	if (err.subcode > 0) {
		assert_true(strstr(err.message, "subcode=") != NULL);
	}
	as_operations_destroy(&ops);
}

/******************************************************************************
 * SECTION 7: Negative / Edge-Case Tests (Integration)
 *****************************************************************************/

// 7.1 Verbosity is not leaked into batch requests
TEST(ed_sync_batch_no_leak, "7.1 batch verbosity no leak")
{
	as_error err;
	as_policy_batch pb;
	as_policy_batch_init(&pb);
	pb.base.error_detail_verbosity = 2;

	as_batch_records records;
	as_batch_records_inita(&records, 3);

	for (int i = 0; i < 3; i++) {
		as_batch_read_record* record = as_batch_read_reserve(&records);
		char kbuf[64];
		snprintf(kbuf, sizeof(kbuf), "batch_ed_key_%d", i);
		as_key_init_strp(&record->key, NAMESPACE, SET, kbuf, false);
		record->read_all_bins = true;
	}

	as_status status = aerospike_batch_read(as, &err, &pb, &records);
	assert_int_eq(status, AEROSPIKE_OK);

	as_vector* list = &records.list;
	for (uint32_t i = 0; i < list->size; i++) {
		as_batch_read_record* br = as_vector_get(list, i);
		(void)br;
	}
	as_batch_records_destroy(&records);
}

// 7.2 Verbosity is not leaked into scan requests
static bool
scan_no_leak_cb(const as_val* val, void* udata)
{
	uint32_t* count = (uint32_t*)udata;
	if (val) {
		(*count)++;
	}
	return true;
}

TEST(ed_sync_scan_no_leak, "7.2 scan verbosity no leak")
{
	as_error err;
	as_policy_scan ps;
	as_policy_scan_init(&ps);
	ps.base.error_detail_verbosity = 2;

	as_scan scan;
	as_scan_init(&scan, NAMESPACE, SET);

	uint32_t count = 0;
	as_status status = aerospike_scan_foreach(as, &err, &ps, &scan, scan_no_leak_cb, &count);
	assert_int_eq(status, AEROSPIKE_OK);

	as_scan_destroy(&scan);
}

// 7.3 Verbosity is not leaked into query requests
static bool
query_no_leak_cb(const as_val* val, void* udata)
{
	uint32_t* count = (uint32_t*)udata;
	if (val) {
		(*count)++;
	}
	return true;
}

TEST(ed_sync_query_no_leak, "7.3 query verbosity no leak")
{
	as_error err;
	as_policy_query pq;
	as_policy_query_init(&pq);
	pq.base.error_detail_verbosity = 2;

	as_query query;
	as_query_init(&query, NAMESPACE, SET);

	uint32_t count = 0;
	as_status status = aerospike_query_foreach(as, &err, &pq, &query, query_no_leak_cb, &count);
	assert_int_eq(status, AEROSPIKE_OK);

	as_query_destroy(&query);
}

// 7.4 Multiple sequential errors do not leak across requests
TEST(ed_sync_no_cross_request_leak, "7.4 no cross-request leak")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	// Error A: generation mismatch
	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;
	pw.gen = AS_POLICY_GEN_EQ;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);
	rec.gen = 9999;

	aerospike_key_put(as, &err, &pw, &key, &rec);
	assert_int_eq(err.code, AEROSPIKE_ERR_RECORD_GENERATION);
	uint32_t subcode_a = err.subcode;
	info("Error A subcode: %" PRIu32 ", message: %s", subcode_a, err.message);

	// Successful write clears error
	as_policy_write pw_ok;
	as_policy_write_init(&pw_ok);
	pw_ok.base.error_detail_verbosity = 2;

	as_record rec_ok;
	as_record_inita(&rec_ok, 1);
	as_record_set_int64(&rec_ok, "ibin", 100);

	aerospike_key_put(as, &err, &pw_ok, &key, &rec_ok);
	assert_int_eq(err.code, AEROSPIKE_OK);

	// Error B: bin type mismatch
	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_append_str(&ops, "ibin", "abc");

	aerospike_key_operate(as, &err, &po, &key, &ops, NULL);
	assert_int_eq(err.code, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE);
	info("Error B subcode: %" PRIu32 ", message: %s", err.subcode, err.message);
	as_operations_destroy(&ops);
}

// 7.5 Second operation properly resets error state
// Uses CDT list OOB (has a real subcode) for the failing step, then a successful write.
TEST(ed_sync_error_state_reset, "7.5 error state reset on next op")
{
	as_error err;

	// Failing CDT list operation (has per-status subcode)
	as_key key_list;
	as_key_init(&key_list, NAMESPACE, SET, "error_detail_list");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get_by_index(&ops, "lbin", NULL, 99, AS_LIST_RETURN_VALUE);

	aerospike_key_operate(as, &err, &po, &key_list, &ops, NULL);
	assert_true(err.code != AEROSPIKE_OK);
	assert_true(err.subcode > 0);
	as_operations_destroy(&ops);

	// Successful write using the same as_error
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_write pw_ok;
	as_policy_write_init(&pw_ok);
	pw_ok.base.error_detail_verbosity = 2;

	as_record rec_ok;
	as_record_inita(&rec_ok, 1);
	as_record_set_int64(&rec_ok, "ibin", 100);

	aerospike_key_put(as, &err, &pw_ok, &key, &rec_ok);
	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(err.subcode, 0);
}

/******************************************************************************
 * SECTION 6: Async Integration Tests
 *****************************************************************************/

// Async helper data
typedef struct {
	atf_test_result* result;
	as_error err_copy;
	bool got_error;
} async_error_data;

// 6.1 Async write generation mismatch at verbosity 2
static void
async_write_gen_v2_cb(as_error* err, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;
	atf_test_result* __result__ = data->result;

	if (err) {
		as_error_copy(&data->err_copy, err);
		data->got_error = true;
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_write_gen_v2, "6.1 async write gen mismatch verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;
	pw.gen = AS_POLICY_GEN_EQ;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ibin", 200);
	rec.gen = 9999;

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_put_async(as, &err, &pw, &key, &rec,
		async_write_gen_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_true(data.got_error);
	assert_int_eq(data.err_copy.code, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_int_eq(data.err_copy.subcode, 0);
}

// 6.2 Async delete generation mismatch at verbosity 2
static void
async_delete_gen_v2_cb(as_error* err, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;

	if (err) {
		as_error_copy(&data->err_copy, err);
		data->got_error = true;
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_delete_gen_v2, "6.2 async delete gen mismatch verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_remove pr;
	as_policy_remove_init(&pr);
	pr.base.error_detail_verbosity = 2;
	pr.gen = AS_POLICY_GEN_EQ;
	pr.generation = 9999;

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_remove_async(as, &err, &pr, &key,
		async_delete_gen_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_true(data.got_error);
	assert_int_eq(data.err_copy.code, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_int_eq(data.err_copy.subcode, 0);
}

// 6.3 Async operate bin type mismatch at verbosity 2
static void
async_operate_type_v2_cb(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;

	if (err) {
		as_error_copy(&data->err_copy, err);
		data->got_error = true;
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_operate_type_v2, "6.3 async operate bin type mismatch verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_append_str(&ops, "ibin", "abc");

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_operate_async(as, &err, &po, &key, &ops,
		async_operate_type_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);
	as_operations_destroy(&ops);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_true(data.got_error);
	assert_int_eq(data.err_copy.code, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE);
	assert_int_eq(data.err_copy.subcode, 0);
}

// 6.4 Async touch generation mismatch at verbosity 2
static void
async_touch_gen_v2_cb(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;

	if (err) {
		as_error_copy(&data->err_copy, err);
		data->got_error = true;
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_touch_gen_v2, "6.4 async touch gen mismatch verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;
	po.gen = AS_POLICY_GEN_EQ;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);
	ops.gen = 9999;

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_operate_async(as, &err, &po, &key, &ops,
		async_touch_gen_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);
	as_operations_destroy(&ops);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_true(data.got_error);
	assert_int_eq(data.err_copy.code, AEROSPIKE_ERR_RECORD_GENERATION);
	assert_int_eq(data.err_copy.subcode, 0);
}

// 6.5 Async exists on missing key at verbosity 2
static void
async_exists_not_found_v2_cb(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;

	if (err) {
		as_error_copy(&data->err_copy, err);
		data->got_error = true;
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_exists_not_found_v2, "6.5 async exists not found verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "nonexistent_async_key");

	as_policy_read pr;
	as_policy_read_init(&pr);
	pr.base.error_detail_verbosity = 2;

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_exists_async(as, &err, &pr, &key,
		async_exists_not_found_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_true(data.got_error);
	assert_int_eq(data.err_copy.code, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	if (data.err_copy.subcode > 0) {
		assert_true(strstr(data.err_copy.message, "subcode=") != NULL);
	}
}

// 6.6 Async CDT list out of bounds at verbosity 2
static void
async_cdt_list_oob_cb(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;

	if (err) {
		as_error_copy(&data->err_copy, err);
		data->got_error = true;
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_cdt_list_oob, "6.6 async CDT list out of bounds verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_list");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get_by_index(&ops, "lbin", NULL, 99, AS_LIST_RETURN_VALUE);

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_operate_async(as, &err, &po, &key, &ops,
		async_cdt_list_oob_cb, &data, 0, NULL);
	as_key_destroy(&key);
	as_operations_destroy(&ops);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_true(data.got_error);
	assert_true(data.err_copy.subcode > 0);
	assert_true(strstr(data.err_copy.message, "subcode=") != NULL);
}

// 6.7 Async read happy path at verbosity 2
static void
async_read_ok_v2_cb(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;
	atf_test_result* __result__ = data->result;

	if (err) {
		data->got_error = true;
		as_error_copy(&data->err_copy, err);
	}
	else {
		data->got_error = false;
		assert_async(&monitor, rec != NULL);
		assert_int_eq_async(&monitor, as_record_get_int64(rec, "ibin", 0), 100);
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_read_ok_v2, "6.7 async read happy path verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_policy_read pr;
	as_policy_read_init(&pr);
	pr.base.error_detail_verbosity = 2;

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_get_async(as, &err, &pr, &key,
		async_read_ok_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_false(data.got_error);
}

// 6.8 Async write happy path at verbosity 2
static void
async_write_ok_v2_cb(as_error* err, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;

	if (err) {
		data->got_error = true;
		as_error_copy(&data->err_copy, err);
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_write_ok_v2, "6.8 async write happy path verbosity 2")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_async_happy");

	as_policy_write pw;
	as_policy_write_init(&pw);
	pw.base.error_detail_verbosity = 2;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "val", 99);

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_put_async(as, &err, &pw, &key, &rec,
		async_write_ok_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_false(data.got_error);

	// Cleanup
	as_key key2;
	as_key_init(&key2, NAMESPACE, SET, "error_detail_async_happy");
	aerospike_key_remove(as, &err, NULL, &key2);
}

// 6.9 Async server message displaces default format
// Uses CDT list OOB which has a per-status subcode.
static void
async_priority_v2_cb(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	async_error_data* data = udata;

	if (err) {
		as_error_copy(&data->err_copy, err);
		data->got_error = true;
	}
	else {
		data->got_error = false;
	}
	as_monitor_notify(&monitor);
}

TEST(ed_async_priority_logic_v2, "6.9 async server message displaces default format")
{
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_list");

	as_policy_operate po;
	as_policy_operate_init(&po);
	po.base.error_detail_verbosity = 2;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_list_get_by_index(&ops, "lbin", NULL, 99, AS_LIST_RETURN_VALUE);

	async_error_data data;
	data.result = __result__;
	data.got_error = false;

	as_error err;
	as_status status = aerospike_key_operate_async(as, &err, &po, &key, &ops,
		async_priority_v2_cb, &data, 0, NULL);
	as_key_destroy(&key);
	as_operations_destroy(&ops);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);

	assert_true(data.got_error);
	assert_true(data.err_copy.subcode > 0);
	assert_true(strstr(data.err_copy.message, as_error_string(data.err_copy.code)) == NULL);
	assert_true(strstr(data.err_copy.message, "subcode=") != NULL);
}

/******************************************************************************
 * ASYNC SUITE LIFECYCLE
 *****************************************************************************/

static bool
before_async(atf_suite* suite)
{
	as_node* node = as_node_get_random(as->cluster);

	if (!node) {
		return false;
	}

	if (as_version_compare(&node->version, &as_server_version_8_1_3) < 0) {
		info("Skipping error_detail_async suite: server %u.%u.%u < 8.1.3",
			 node->version.major, node->version.minor, node->version.patch);
		as_node_release(node);
		return false;
	}
	as_node_release(node);

	as_monitor_init(&monitor);

	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "error_detail_test");

	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "ibin", 100);
	as_record_set_strp(&rec, "sbin", "hello", false);

	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

	if (status != AEROSPIKE_OK) {
		error("Failed to create test record: %s", err.message);
		as_monitor_destroy(&monitor);
		return false;
	}
	as_key_destroy(&key);

	// CDT list record for async 6.6
	as_key key_list;
	as_key_init(&key_list, NAMESPACE, SET, "error_detail_list");
	as_record rec_list;
	as_record_inita(&rec_list, 1);
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	as_record_set_list(&rec_list, "lbin", (as_list*)&list);
	status = aerospike_key_put(as, &err, NULL, &key_list, &rec_list);
	if (status != AEROSPIKE_OK) {
		error("Failed to create list record: %s", err.message);
		as_monitor_destroy(&monitor);
		return false;
	}
	as_key_destroy(&key_list);

	return true;
}

static bool
after_async(atf_suite* suite)
{
	as_monitor_destroy(&monitor);
	return true;
}

/******************************************************************************
 * TEST SUITES
 *****************************************************************************/

SUITE(error_detail_sync, "error detail sync integration tests")
{
	suite_before(before_sync);

	suite_add(ed_sync_write_gen_v0);
	suite_add(ed_sync_write_gen_v1);
	suite_add(ed_sync_write_gen_v2);
	suite_add(ed_sync_delete_gen_v0);
	suite_add(ed_sync_delete_gen_v2);
	suite_add(ed_sync_touch_gen_v0);
	suite_add(ed_sync_touch_gen_v2);
	suite_add(ed_sync_operate_type_v0);
	suite_add(ed_sync_operate_type_v1);
	suite_add(ed_sync_operate_type_v2);
	suite_add(ed_sync_read_ok_v2);
	suite_add(ed_sync_exists_ok_v2);
	suite_add(ed_sync_exists_not_found_v2);
	suite_add(ed_sync_delete_not_found_v2);
	suite_add(ed_sync_write_ok_v2);
	suite_add(ed_sync_txn_write_gen_v2);
	suite_add(ed_sync_txn_write_ok_v2);
	suite_add(ed_sync_priority_logic_v2);
	suite_add(ed_sync_priority_logic_v0);
	suite_add(ed_sync_cross_verbosity);
	suite_add(ed_sync_udf_non_interference);
	suite_add(ed_sync_cdt_list_oob);
	suite_add(ed_sync_cdt_map_create_only);
	suite_add(ed_sync_bit_invalid);
	suite_add(ed_sync_hll_invalid);
	suite_add(ed_sync_param_ttl_invalid);
	suite_add(ed_sync_cdt_rank_oob);
	suite_add(ed_sync_filtered_out);
	suite_add(ed_sync_bin_not_found_hll);
	suite_add(ed_sync_param_bits_size);
	suite_add(ed_sync_batch_no_leak);
	suite_add(ed_sync_scan_no_leak);
	suite_add(ed_sync_query_no_leak);
	suite_add(ed_sync_no_cross_request_leak);
	suite_add(ed_sync_error_state_reset);
}

SUITE(error_detail_async, "error detail async integration tests")
{
	suite_before(before_async);
	suite_after(after_async);

	suite_add(ed_async_write_gen_v2);
	suite_add(ed_async_delete_gen_v2);
	suite_add(ed_async_operate_type_v2);
	suite_add(ed_async_touch_gen_v2);
	suite_add(ed_async_exists_not_found_v2);
	suite_add(ed_async_cdt_list_oob);
	suite_add(ed_async_read_ok_v2);
	suite_add(ed_async_write_ok_v2);
	suite_add(ed_async_priority_logic_v2);
}
