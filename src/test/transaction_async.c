/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_monitor.h>
#include <aerospike/as_udf.h>
#include <aerospike/as_vector.h>
#include "test.h"
#include "util/udf.h"

//---------------------------------
// Global Variables
//---------------------------------

extern aerospike* as;
extern bool g_has_sc;
static as_monitor monitor;

//---------------------------------
// Macros
//---------------------------------

#define NAMESPACE "test"
#define SET "txn"
#define BIN "a"
#define LUA_FILE AS_START_DIR "src/test/lua/udf_record.lua"
#define UDF_FILE "udf_record"

//---------------------------------
// Async Commander Implementation
//---------------------------------

typedef enum {
	CMD_PUT,
	CMD_GET,
	CMD_OPERATE,
	CMD_TOUCH,
	CMD_UDF,
	CMD_DELETE,
	CMD_BATCH_READ,
	CMD_BATCH_WRITE,
	CMD_COMMIT,
	CMD_ABORT
} cmd_type;

typedef struct {
	as_txn* txn;
	as_key* key;
	int64_t val;
	uint32_t batch_size;
	cmd_type type;
	as_status status;
} command;

typedef struct {
	as_vector* cmds;
	command* cmd;
	atf_test_result* result;
	int idx;
} commander;

static void
commander_run_next(commander* cmdr);

static void
commander_fail(commander* cmdr, as_error* err)
{
	atf_test_result* __result__ = cmdr->result;
	fail_async(&monitor, "Error %d: %s", err->code, err->message);
}

//---------------------------------
// Put
//---------------------------------

static void
put_add(as_vector* cmds, as_txn* txn, as_key* key, int64_t val)
{
	command cmd = {.txn = txn, .key = key, .val = val, .type = CMD_PUT};
	as_vector_append(cmds, &cmd);
}

static void
put_add_error(as_vector* cmds, as_txn* txn, as_key* key, int64_t val, as_status status)
{
	command cmd = {.txn = txn, .key = key, .val = val, .type = CMD_PUT, .status = status};
	as_vector_append(cmds, &cmd);
}

static void
put_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			commander_run_next(cmdr);
		}
		else {
			commander_fail(cmdr, err);
		}
		return;
	}

	if (cmdr->cmd->status != AEROSPIKE_OK) {
		atf_test_result* __result__ = cmdr->result;
		fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		return;
	}

	commander_run_next(cmdr);
}

static as_status
put_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_write p;
	as_policy_write* policy = &as->config.policies.write;

	if (cmd->txn) {
		as_policy_write_copy(policy, &p);
		p.base.txn = cmd->txn;
		policy = &p;
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN, cmd->val);

	return aerospike_key_put_async(as, err, policy, cmd->key, &rec, put_listener, cmdr, NULL, NULL);
}

//---------------------------------
// Get
//---------------------------------

static void
get_add(as_vector* cmds, as_txn* txn, as_key* key, int64_t val)
{
	command cmd = {.txn = txn, .key = key, .val = val, .type = CMD_GET};
	as_vector_append(cmds, &cmd);
}

static void
get_add_error(as_vector* cmds, as_txn* txn, as_key* key, as_status status)
{
	command cmd = {.txn = txn, .key = key, .type = CMD_GET, .status = status};
	as_vector_append(cmds, &cmd);
}

static void
get_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			commander_run_next(cmdr);
		}
		else {
			commander_fail(cmdr, err);
		}
		return;
	}

	if (cmdr->cmd->status != AEROSPIKE_OK) {
		atf_test_result* __result__ = cmdr->result;
		fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		return;
	}

	int64_t val = as_record_get_int64(record, BIN, -1);
 	atf_test_result* __result__ = cmdr->result;
   	assert_int_eq_async(&monitor, val, cmdr->cmd->val);

	commander_run_next(cmdr);
}

static as_status
get_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_read p;
	as_policy_read* policy = &as->config.policies.read;

	if (cmd->txn) {
		as_policy_read_copy(policy, &p);
		p.base.txn = cmd->txn;
		policy = &p;
	}

	return aerospike_key_get_async(as, err, policy, cmd->key, get_listener, cmdr, NULL, NULL);
}

//---------------------------------
// Operate
//---------------------------------

static void
operate_add(as_vector* cmds, as_txn* txn, as_key* key, int64_t val)
{
	command cmd = {.txn = txn, .key = key, .val = val, .type = CMD_OPERATE};
	as_vector_append(cmds, &cmd);
}

static void
operate_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			commander_run_next(cmdr);
		}
		else {
			commander_fail(cmdr, err);
		}
		return;
	}

	if (cmdr->cmd->status != AEROSPIKE_OK) {
		atf_test_result* __result__ = cmdr->result;
		fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		return;
	}

	int64_t val = as_record_get_int64(record, BIN, -1);
 	atf_test_result* __result__ = cmdr->result;
   	assert_int_eq_async(&monitor, val, cmdr->cmd->val);

	commander_run_next(cmdr);
}

static as_status
operate_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_operate p;
	as_policy_operate* policy = &as->config.policies.operate;

	if (cmd->txn) {
		as_policy_operate_copy(policy, &p);
		p.base.txn = cmd->txn;
		policy = &p;
	}

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_write_int64(&ops, BIN, 2);
	as_operations_add_read(&ops, BIN);

	return aerospike_key_operate_async(as, err, policy, cmd->key, &ops, operate_listener, cmdr, NULL, NULL);
}

//---------------------------------
// Touch
//---------------------------------

static void
touch_add(as_vector* cmds, as_txn* txn, as_key* key)
{
	command cmd = {.txn = txn, .key = key, .type = CMD_TOUCH};
	as_vector_append(cmds, &cmd);
}

static void
touch_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			commander_run_next(cmdr);
		}
		else {
			commander_fail(cmdr, err);
		}
		return;
	}

	if (cmdr->cmd->status != AEROSPIKE_OK) {
		atf_test_result* __result__ = cmdr->result;
		fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		return;
	}

	commander_run_next(cmdr);
}

static as_status
touch_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_operate p;
	as_policy_operate* policy = &as->config.policies.operate;

	if (cmd->txn) {
		as_policy_operate_copy(policy, &p);
		p.base.txn = cmd->txn;
		policy = &p;
	}

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_touch(&ops);

	return aerospike_key_operate_async(as, err, policy, cmd->key, &ops, touch_listener, cmdr, NULL, NULL);
}

//---------------------------------
// UDF
//---------------------------------

static void
udf_add(as_vector* cmds, as_txn* txn, as_key* key, int64_t val)
{
	command cmd = {.txn = txn, .key = key, .val = val, .type = CMD_UDF};
	as_vector_append(cmds, &cmd);
}

static void
udf_listener(as_error* err, as_val* val, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			commander_run_next(cmdr);
		}
		else {
			commander_fail(cmdr, err);
		}
		return;
	}

	if (cmdr->cmd->status != AEROSPIKE_OK) {
		atf_test_result* __result__ = cmdr->result;
		fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		return;
	}

	commander_run_next(cmdr);
}

static as_status
udf_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_apply p;
	as_policy_apply* policy = &as->config.policies.apply;

	if (cmd->txn) {
		as_policy_apply_copy(policy, &p);
		p.base.txn = cmd->txn;
		policy = &p;
	}

	as_arraylist args;
	as_arraylist_inita(&args, 2);
	as_arraylist_append_str(&args, BIN);
	as_arraylist_append_int64(&args, cmd->val);

	as_status status = aerospike_key_apply_async(as, err, policy, cmd->key, "udf_record", "write_bin",
		(as_list*)&args, udf_listener, cmdr, NULL, NULL);

	as_arraylist_destroy(&args);
	return status;
}

//---------------------------------
// Delete
//---------------------------------

static void
delete_add(as_vector* cmds, as_txn* txn, as_key* key)
{
	command cmd = {.txn = txn, .key = key, .type = CMD_DELETE};
	as_vector_append(cmds, &cmd);
}

static void
delete_add_error(as_vector* cmds, as_txn* txn, as_key* key, as_status status)
{
	command cmd = {.txn = txn, .key = key, .type = CMD_DELETE, .status = status};
	as_vector_append(cmds, &cmd);
}

static void
delete_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			commander_run_next(cmdr);
		}
		else {
			commander_fail(cmdr, err);
		}
		return;
	}

	if (cmdr->cmd->status != AEROSPIKE_OK) {
		atf_test_result* __result__ = cmdr->result;
		fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		return;
	}

	commander_run_next(cmdr);
}

static as_status
delete_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_remove p;
	as_policy_remove* policy = &as->config.policies.remove;

	if (cmd->txn) {
		as_policy_remove_copy(policy, &p);
		p.base.txn = cmd->txn;
		p.durable_delete = true;
		policy = &p;
	}

	return aerospike_key_remove_async(as, err, policy, cmd->key, delete_listener, cmdr, NULL, NULL);
}

//---------------------------------
// Batch Read
//---------------------------------

static void
batch_read_add(as_vector* cmds, as_txn* txn, uint32_t batch_size, int64_t val)
{
	command cmd = {.txn = txn, .batch_size = batch_size, .val = val, .type = CMD_BATCH_READ};
	as_vector_append(cmds, &cmd);
}

static void
batch_read_listener(
	as_error* err, as_batch_records* recs, void* udata, as_event_loop* event_loop
	)
{
	commander* cmdr = udata;
	bool success = false;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			success = true;
		}
		else {
			commander_fail(cmdr, err);
		}
	}
	else {
		atf_test_result* __result__ = cmdr->result;

		if (cmdr->cmd->status == AEROSPIKE_OK) {
			for (uint32_t i = 0; i < recs->list.size; i++) {
				const as_batch_read_record* rec = as_vector_get(&recs->list, i);
				assert_int_eq_async(&monitor, rec->result, AEROSPIKE_OK);

				int64_t val = as_record_get_int64(&rec->record, BIN, -1);
				assert_int_eq_async(&monitor, val, cmdr->cmd->val);
			}
			success = true;
		}
		else {
			fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		}
	}

	as_batch_records_destroy(recs);

	if (success) {
		commander_run_next(cmdr);
	}
}

static as_status
batch_read_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_batch p;
	as_policy_batch* policy = &as->config.policies.batch;

	if (cmd->txn) {
		as_policy_batch_copy(policy, &p);
		p.base.txn = cmd->txn;
		policy = &p;
	}

	as_batch_records* recs = as_batch_records_create(cmd->batch_size);

	for (uint32_t i = 0; i < cmd->batch_size; i++) {
		as_batch_read_record* rec = as_batch_read_reserve(recs);
		as_key_init_int64(&rec->key, NAMESPACE, SET, i);
		rec->read_all_bins = true;
	}

	as_status status = aerospike_batch_read_async(as, err, policy, recs, batch_read_listener, cmdr, NULL);

	if (status != AEROSPIKE_OK) {
		as_batch_records_destroy(recs);
	}
	return status;
}

//---------------------------------
// Batch Write
//---------------------------------

static void
batch_write_add(as_vector* cmds, as_txn* txn, uint32_t batch_size, int64_t val)
{
	command cmd = {.txn = txn, .batch_size = batch_size, .val = val, .type = CMD_BATCH_WRITE};
	as_vector_append(cmds, &cmd);
}

static void
batch_write_listener(
	as_error* err, as_batch_records* recs, void* udata, as_event_loop* event_loop
	)
{
	commander* cmdr = udata;
	bool success = false;

	if (err) {
		if (err->code == cmdr->cmd->status) {
			success = true;
		}
		else {
			commander_fail(cmdr, err);
		}
	}
	else {
		atf_test_result* __result__ = cmdr->result;

		if (cmdr->cmd->status == AEROSPIKE_OK) {
			for (uint32_t i = 0; i < recs->list.size; i++) {
				const as_batch_write_record* rec = as_vector_get(&recs->list, i);
				assert_int_eq_async(&monitor, rec->result, AEROSPIKE_OK);
			}
			success = true;
		}
		else {
			fail_async(&monitor, "Unexpected success. Expected %d", cmdr->cmd->status);
		}
	}

	if (recs->list.size > 0) {
		// Destroy heap allocated ops that is used on every batch record.
		const as_batch_write_record* rec = as_vector_get(&recs->list, 0);
		as_operations_destroy(rec->ops);
	}
	as_batch_records_destroy(recs);

	if (success) {
		commander_run_next(cmdr);
	}
}

static as_status
batch_write_exec(commander* cmdr, command* cmd, as_error* err)
{
	as_policy_batch p;
	as_policy_batch* policy = &as->config.policies.batch_parent_write;

	if (cmd->txn) {
		as_policy_batch_copy(policy, &p);
		p.base.txn = cmd->txn;
		policy = &p;
	}

	as_batch_records* recs = as_batch_records_create(cmd->batch_size);

	// Must allocate ops on the heap when a transaction is involved.
	as_operations* ops = as_operations_new(1);
	as_operations_add_write_int64(ops, BIN, cmd->val);

	for (uint32_t i = 0; i < cmd->batch_size; i++) {
		as_batch_write_record* rec = as_batch_write_reserve(recs);
		as_key_init_int64(&rec->key, NAMESPACE, SET, i);
		rec->ops = ops;
	}

	as_status status = aerospike_batch_write_async(as, err, policy, recs, batch_write_listener, cmdr, NULL);

	if (status != AEROSPIKE_OK) {
		as_batch_records_destroy(recs);
		as_operations_destroy(ops);
	}
	return status;
}

//---------------------------------
// Commit
//---------------------------------

static void
commit_add(as_vector* cmds, as_txn* txn)
{
	command cmd = {.txn = txn, .type = CMD_COMMIT};
	as_vector_append(cmds, &cmd);
}

static void
commit_listener(
	as_error* err, as_commit_status status, void* udata, struct as_event_loop* event_loop
	)
{
	commander* cmdr = udata;

	if (err) {
		commander_fail(cmdr, err);
		return;
	}

	commander_run_next(cmdr);
}

static as_status
commit_exec(commander* cmdr, command* cmd, as_error* err)
{
	return aerospike_commit_async(as, err, cmd->txn, commit_listener, cmdr, NULL);
}

//---------------------------------
// Abort
//---------------------------------

static void
abort_add(as_vector* cmds, as_txn* txn)
{
	command cmd = {.txn = txn, .type = CMD_ABORT};
	as_vector_append(cmds, &cmd);
}

static void
abort_listener(
	as_error* err, as_abort_status status, void* udata, struct as_event_loop* event_loop
	)
{
	commander* cmdr = udata;

	if (err) {
		commander_fail(cmdr, err);
		return;
	}

	commander_run_next(cmdr);
}

static as_status
abort_exec(commander* cmdr, command* cmd, as_error* err)
{
	return aerospike_abort_async(as, err, cmd->txn, abort_listener, cmdr, NULL);
}

//---------------------------------
// Run
//---------------------------------

static void
commander_run_next(commander* cmdr)
{
	cmdr->idx++;

	if (cmdr->idx == cmdr->cmds->size) {
		as_monitor_notify(&monitor);
		return;
	}

	as_error err;
	as_status status;
	command* cmd = cmdr->cmd = as_vector_get(cmdr->cmds, cmdr->idx);

	switch (cmd->type) {
		case CMD_PUT:
			status = put_exec(cmdr, cmd, &err);
			break;

		case CMD_GET:
			status = get_exec(cmdr, cmd, &err);
			break;

		case CMD_OPERATE:
			status = operate_exec(cmdr, cmd, &err);
			break;

		case CMD_TOUCH:
			status = touch_exec(cmdr, cmd, &err);
			break;

		case CMD_UDF:
			status = udf_exec(cmdr, cmd, &err);
			break;

		case CMD_DELETE:
			status = delete_exec(cmdr, cmd, &err);
			break;

		case CMD_BATCH_READ:
			status = batch_read_exec(cmdr, cmd, &err);
			break;

		case CMD_BATCH_WRITE:
			status = batch_write_exec(cmdr, cmd, &err);
			break;

		case CMD_COMMIT:
			status = commit_exec(cmdr, cmd, &err);
			break;

		case CMD_ABORT:
			status = abort_exec(cmdr, cmd, &err);
			break;

		default:
			status = AEROSPIKE_ERR_CLIENT;
			break;
	}

	atf_test_result* __result__ = cmdr->result;
	assert_status_async(&monitor, status, &err);
}

static void
commander_execute(as_vector* cmds, atf_test_result* result)
{
	as_monitor_begin(&monitor);

	commander c = {.cmds = cmds, .result = result, .idx = -1};
	commander_run_next(&c);

	as_monitor_wait(&monitor);
}

//---------------------------------
// Before/After Suite
//---------------------------------

static bool
before(atf_suite* suite)
{
	as_monitor_init(&monitor);

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
	as_monitor_destroy(&monitor);

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

TEST(txn_async_write, "transaction async write")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_write");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	put_add(&cmds, &txn, &key, 2);
	commit_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 2);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_write_twice, "transaction async write twice")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_write_twice");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, &txn, &key, 1);
	put_add(&cmds, &txn, &key, 2);
	commit_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 2);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_write_block, "transaction async write block")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_write_block");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 5);

	put_add(&cmds, NULL, &key, 1);
	put_add(&cmds, &txn, &key, 2);
	put_add_error(&cmds, NULL, &key, 3, AEROSPIKE_MRT_BLOCKED);
	commit_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 2);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_write_read, "transaction async write read")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_write_read");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 5);

	put_add(&cmds, NULL, &key, 1);
	put_add(&cmds, &txn, &key, 2);
	get_add(&cmds, NULL, &key, 1);
	commit_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 2);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_write_abort, "transaction async write abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_write_abort");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 5);

	put_add(&cmds, NULL, &key, 1);
	put_add(&cmds, &txn, &key, 2);
	get_add(&cmds, &txn, &key, 2);
	abort_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 1);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_delete, "transaction async delete")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_delete");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	delete_add(&cmds, &txn, &key);
	commit_add(&cmds, &txn);
	get_add_error(&cmds, NULL, &key, AEROSPIKE_ERR_RECORD_NOT_FOUND);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_delete_abort, "transaction async delete abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_delete_abort");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	delete_add(&cmds, &txn, &key);
	abort_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 1);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_delete_twice, "transaction async delete twice")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_delete_twice");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 5);

	put_add(&cmds, NULL, &key, 1);
	delete_add(&cmds, &txn, &key);
	delete_add_error(&cmds, &txn, &key, AEROSPIKE_ERR_RECORD_NOT_FOUND);
	commit_add(&cmds, &txn);
	get_add_error(&cmds, NULL, &key, AEROSPIKE_ERR_RECORD_NOT_FOUND);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_touch, "transaction async touch")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_touch");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	touch_add(&cmds, &txn, &key);
	commit_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 1);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_touch_abort, "transaction async touch abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_touch_abort");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	touch_add(&cmds, &txn, &key);
	abort_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 1);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_operate_write, "transaction async operate write")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_operate_write");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	operate_add(&cmds, &txn, &key, 2);
	commit_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 2);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_operate_write_abort, "transaction async operate write abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_operate_write_abort");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	operate_add(&cmds, &txn, &key, 2);
	abort_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 1);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_udf, "transaction async udf")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_udf");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	udf_add(&cmds, &txn, &key, 2);
	commit_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 2);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_udf_abort, "transaction async udf abort")
{
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "txn_async_udf_abort");

	as_txn txn;
	as_txn_init(&txn);

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	put_add(&cmds, NULL, &key, 1);
	udf_add(&cmds, &txn, &key, 2);
	abort_add(&cmds, &txn);
	get_add(&cmds, NULL, &key, 1);

	commander_execute(&cmds, __result__);
	as_txn_destroy(&txn);
}

TEST(txn_async_batch, "transaction async batch")
{
	uint32_t batch_size = 10;

	as_txn* txn = as_txn_create();

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	batch_write_add(&cmds, NULL, batch_size, 1);
	batch_write_add(&cmds, txn, batch_size, 2);
	commit_add(&cmds, txn);
	batch_read_add(&cmds, NULL, batch_size, 2);

	commander_execute(&cmds, __result__);
	as_txn_destroy(txn);
}

TEST(txn_async_batch_abort, "transaction async batch abort")
{
	uint32_t batch_size = 10;

	as_txn* txn = as_txn_create();

	as_vector cmds;
	as_vector_inita(&cmds, sizeof(command), 4);

	batch_write_add(&cmds, NULL, batch_size, 1);
	batch_write_add(&cmds, txn, batch_size, 2);
	abort_add(&cmds, txn);
	batch_read_add(&cmds, NULL, batch_size, 1);

	commander_execute(&cmds, __result__);
	as_txn_destroy(txn);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(transaction_async, "Async transaction tests")
{
	// Transactions require strong consistency namespaces.
	if (!g_has_sc) {
		return;
	}

	suite_before(before);
	suite_after(after);

	suite_add(txn_async_write);
	suite_add(txn_async_write_twice);
	suite_add(txn_async_write_block);
	suite_add(txn_async_write_read);
	suite_add(txn_async_write_abort);
	suite_add(txn_async_delete);
	suite_add(txn_async_delete_abort);
	suite_add(txn_async_delete_twice);
	suite_add(txn_async_touch);
	suite_add(txn_async_touch_abort);
	suite_add(txn_async_operate_write);
	suite_add(txn_async_operate_write_abort);
	suite_add(txn_async_udf);
	suite_add(txn_async_udf_abort);
	suite_add(txn_async_batch);
	suite_add(txn_async_batch_abort);
}
