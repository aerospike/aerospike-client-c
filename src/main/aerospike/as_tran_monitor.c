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
#include <aerospike/as_tran_monitor.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_async.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_tran.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_random.h>

//---------------------------------
// Constants
//---------------------------------

#define BIN_NAME_ID "id"
#define BIN_NAME_DIGESTS "keyds"

//---------------------------------
// Common Functions
//---------------------------------

static void
as_tran_get_ops_single(as_tran* tran, const as_key* key, as_operations* ops)
{
	if (tran->deadline == 0) {
		// No existing monitor record.
		as_operations_add_write_int64(ops, BIN_NAME_ID, tran->id);
	}

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_NO_FAIL | AS_LIST_WRITE_PARTIAL);

	as_bytes bytes;
	as_bytes_init_wrap(&bytes, (uint8_t*)key->digest.value, AS_DIGEST_VALUE_SIZE, false);

	as_operations_list_append(ops, BIN_NAME_DIGESTS, NULL, &lp, (as_val*)&bytes);
}

static as_status
as_tran_get_ops_keys(as_tran* tran, const as_batch* batch, as_operations* ops, as_error* err)
{
	uint32_t n_keys = batch->keys.size;

	as_arraylist digests;

	if (n_keys <= 5000) {
		// All keys should fit on stack.
		as_arraylist_inita(&digests, n_keys);
	}
	else {
		// Allocate list on heap to avoid stack overflow.
		as_arraylist_init(&digests, n_keys, 0);
	}

	as_bytes bytes;

	for (uint32_t i = 0; i < n_keys; i++) {
		as_key* key = &batch->keys.entries[i];
		as_status status = as_tran_set_ns(tran, key->ns, err);
		
		if (status != AEROSPIKE_OK) {
			as_arraylist_destroy(&digests);
			return status;
		}

		as_bytes_init_wrap(&bytes, key->digest.value, AS_DIGEST_VALUE_SIZE, false);
		as_arraylist_append_bytes(&digests, &bytes);
	}

	if (tran->deadline == 0) {
		// No existing monitor record.
		as_operations_add_write_int64(ops, BIN_NAME_ID, tran->id);
	}

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_NO_FAIL | AS_LIST_WRITE_PARTIAL);

	as_operations_list_append_items(ops, BIN_NAME_DIGESTS, NULL, &lp, (as_list*)&digests);

	as_arraylist_destroy(&digests);
	return AEROSPIKE_OK;
}

static as_status
as_tran_get_ops_records(as_tran* tran, as_batch_records* records, as_operations* ops, as_error* err)
{
	as_vector* records_list = &records->list;
	uint32_t n_keys = records->list.size;

	as_arraylist digests;

	if (n_keys <= 5000) {
		// All keys should fit on stack.
		as_arraylist_inita(&digests, n_keys);
	}
	else {
		// Allocate list on heap to avoid stack overflow.
		as_arraylist_init(&digests, n_keys, 0);
	}

	as_bytes bytes;

	for (uint32_t i = 0; i < n_keys; i++) {
		as_batch_base_record* rec = as_vector_get(records_list, i);
		as_key* key = &rec->key;
		as_status status = as_tran_set_ns(tran, key->ns, err);

		if (status != AEROSPIKE_OK) {
			as_arraylist_destroy(&digests);
			return status;
		}

		if (rec->has_write) {
			as_bytes_init_wrap(&bytes, key->digest.value, AS_DIGEST_VALUE_SIZE, false);
			as_arraylist_append_bytes(&digests, &bytes);
		}
	}

	if (digests.size == 0) {
		as_arraylist_destroy(&digests);
		return AEROSPIKE_OK;
	}

	if (tran->deadline == 0) {
		// No existing monitor record.
		as_operations_add_write_int64(ops, BIN_NAME_ID, tran->id);
	}

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_NO_FAIL | AS_LIST_WRITE_PARTIAL);

	as_operations_list_append_items(ops, BIN_NAME_DIGESTS, NULL, &lp, (as_list*)&digests);

	// Do not destroy digests array because
	as_arraylist_destroy(&digests);
	return AEROSPIKE_OK;
}

static void
as_tran_policy_copy(const as_policy_base* src, as_policy_operate* trg)
{
	as_policy_operate_init(trg);

	trg->base.tran = src->tran;
	trg->base.socket_timeout = src->socket_timeout;
	trg->base.total_timeout = src->total_timeout;
	trg->base.max_retries = src->max_retries;
	trg->base.sleep_between_retries = src->sleep_between_retries;
	trg->base.compress = src->compress;
}

//---------------------------------
// Sync Functions
//---------------------------------

static as_status
as_tran_monitor_add_keys(
	 aerospike* as, as_tran* tran, const as_policy_base* cmd_policy, as_operations* ops, as_error* err
	 )
{
	as_key key;
	as_key_init_int64(&key, tran->ns, "<ERO~MRT", tran->id);

	as_policy_operate tran_policy;
	as_tran_policy_copy(cmd_policy, &tran_policy);

	as_record* rec = NULL;
	as_status status = aerospike_key_operate(as, err, &tran_policy, &key, ops, &rec);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_record_destroy(rec);
	return status;
}

as_status
as_tran_monitor_add_key(aerospike* as, const as_policy_base* cmd_policy, const as_key* cmd_key, as_error* err)
{
	as_tran* tran = cmd_policy->tran;

	if (as_tran_writes_contain(tran, cmd_key)) {
		// Transaction monitor already contains this key.
		return AEROSPIKE_OK;
	}

	as_status status = as_tran_set_ns(tran, cmd_key->ns, err);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_tran_get_ops_single(tran, cmd_key, &ops);

	status = as_tran_monitor_add_keys(as, tran, cmd_policy, &ops, err);
	as_operations_destroy(&ops);
	return status;
}

as_status
as_tran_monitor_add_keys_batch(aerospike* as, const as_policy_base* cmd_policy, const as_batch* batch, as_error* err)
{
	as_tran* tran = cmd_policy->tran;

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_status status = as_tran_get_ops_keys(tran, batch, &ops, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_tran_monitor_add_keys(as, tran, cmd_policy, &ops, err);
	as_operations_destroy(&ops);
	return status;
}

as_status
as_tran_monitor_add_keys_records(aerospike* as, const as_policy_base* cmd_policy, as_batch_records* records, as_error* err)
{
	as_tran* tran = cmd_policy->tran;

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_status status = as_tran_get_ops_records(tran, records, &ops, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (ops.binops.size == 0) {
		// Readonly batch
		as_operations_destroy(&ops);
		return AEROSPIKE_OK;
	}

	status = as_tran_monitor_add_keys(as, tran, cmd_policy, &ops, err);
	as_operations_destroy(&ops);
	return status;
}

//---------------------------------
// Async Functions
//---------------------------------

static void
as_tran_monitor_notify_error(as_error* err, as_event_command* cmd, as_event_loop* event_loop)
{
	switch (cmd->type) {
		case AS_ASYNC_TYPE_WRITE:
			((as_async_write_command*)cmd)->listener(err, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_RECORD:
			((as_async_record_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_VALUE:
			((as_async_value_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		// TOOD: HANDLE!
		case AS_ASYNC_TYPE_BATCH:
			//as_async_batch_error(cmd, err);
			//as_event_executor_error(cmd->udata, err, 1);
			break;
		default:
			as_log_error("Invalid command type: %u", cmd->type);
			break;
	}
}

static void
as_tran_monitor_add_keys_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	as_event_command* cmd = udata;

	if (err) {
		as_tran_monitor_notify_error(err, cmd, event_loop);
		as_event_command_destroy(cmd);
		return;
	}

	// Add tran monitor keys succeeded. Run original command.
	as_status status = as_event_command_execute(cmd, err);

	if (status != AEROSPIKE_OK) {
		as_tran_monitor_notify_error(err, cmd, event_loop);
	}
}

static as_status
as_tran_monitor_add_keys_async(
	aerospike* as, as_error* err, as_tran* tran, const as_policy_base* cmd_policy, as_operations* ops,
	as_event_command* cmd
	)
{
	as_key key;
	as_key_init_int64(&key, tran->ns, "<ERO~MRT", tran->id);

	as_policy_operate tran_policy;
	as_tran_policy_copy(cmd_policy, &tran_policy);

	return aerospike_key_operate_async(as, err, &tran_policy, &key, ops, as_tran_monitor_add_keys_callback, cmd, cmd->event_loop, NULL);
}

as_status
as_async_execute(aerospike* as, as_error* err, const as_policy_base* cmd_policy, const as_key* cmd_key, as_event_command* cmd)
{
	if (! cmd_policy->tran) {
		// Command is not run under a MRT monitor. Run original command.
		return as_event_command_execute(cmd, err);
	}

	as_tran* tran = cmd_policy->tran;

	if (as_tran_writes_contain(tran, cmd_key)) {
		// Transaction monitor already contains this key. Run original command.
		return as_event_command_execute(cmd, err);
	}

	as_status status = as_tran_set_ns(tran, cmd_key->ns, err);

	if (status != AEROSPIKE_OK) {
		as_event_command_destroy(cmd);
		return status;
	}

	// Add key to MRT monitor and then run original command.
	as_operations ops;
	as_operations_inita(&ops, 2);
	as_tran_get_ops_single(tran, cmd_key, &ops);

	status = as_tran_monitor_add_keys_async(as, err, tran, cmd_policy, &ops, cmd);
	as_operations_destroy(&ops);

	if (status != AEROSPIKE_OK) {
		as_event_command_destroy(cmd);
	}

	return status;
}
