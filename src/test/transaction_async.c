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
	PUT,
	GET,
	COMMIT
} cmd_type;

typedef struct {
	as_txn* txn;
	as_key* key;
	int64_t val;
	cmd_type type;
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

static void
put_add(as_vector* cmds, as_txn* txn, as_key* key, int64_t val)
{
	command cmd = {.txn = txn, .key = key, .val = val, .type = PUT};
	as_vector_append(cmds, &cmd);
}

static void
put_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		commander_fail(cmdr, err);
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

static void
get_add(as_vector* cmds, as_txn* txn, as_key* key, int64_t val)
{
	command cmd = {.txn = txn, .key = key, .val = val, .type = GET};
	as_vector_append(cmds, &cmd);
}

static void
get_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	commander* cmdr = udata;

	if (err) {
		commander_fail(cmdr, err);
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

static void
commit_add(as_vector* cmds, as_txn* txn)
{
	command cmd = {.txn = txn, .type = COMMIT};
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
		case PUT:
			//printf("Run put\n");
			status = put_exec(cmdr, cmd, &err);
			break;

		case GET:
			//printf("Run get\n");
			status = get_exec(cmdr, cmd, &err);
			break;

		case COMMIT:
			//printf("Run commit\n");
			status = commit_exec(cmdr, cmd, &err);
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
// Static Functions
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
	get_add(&cmds, &txn, &key, 2);

	commander_execute(&cmds, __result__);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(transaction_async, "Async transaction tests")
{
	suite_before(before);
	suite_after(after);

	suite_add(txn_async_write);
}
