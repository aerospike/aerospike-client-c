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
#include <aerospike/aerospike_txn.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_txn.h>
#include <aerospike/as_txn_monitor.h>

//---------------------------------
// Function Declarations
//---------------------------------

as_status
as_txn_verify(aerospike* as, as_error* err, as_txn* txn);

as_status
as_txn_roll(
	aerospike* as, as_error* err, as_policy_txn_roll* policy, as_txn* txn, uint8_t txn_attr
	);

as_status
as_txn_verify_async(
	aerospike* as, as_error* err, as_txn* txn, as_async_batch_listener listener, void* udata,
	as_event_loop* event_loop
	);

as_status
as_txn_roll_async(
	aerospike* as, as_error* err, as_policy_txn_roll* policy, as_txn* txn, uint8_t txn_attr,
	as_async_batch_listener listener, void* udata, as_event_loop* event_loop
	);

//---------------------------------
// Common Functions
//---------------------------------

static void
as_error_copy_fields(as_error* trg, as_error* src)
{
	trg->func = src->func;
	trg->file = src->file;
	trg->line = src->line;
	trg->in_doubt = src->in_doubt;
}

//---------------------------------
// Sync Commit
//---------------------------------

as_status
aerospike_commit(aerospike* as, as_error* err, as_txn* txn)
{
	if (! as_txn_set_roll_attempted(txn)) {
		return as_error_set_message(err, AEROSPIKE_ROLL_ALREADY_ATTEMPTED,
			"Commit or abort already attempted");
	}

	as_error_reset(err);

	as_policy_txn_roll* roll_policy = &as->config.policies.txn_roll;

	as_key key;
	as_txn_monitor_init_key(txn, &key);

	as_error verify_err;
	as_status status = as_txn_verify(as, &verify_err, txn);

	if (status != AEROSPIKE_OK) {
		// Verify failed. Abort.
		as_error roll_err;
		as_status roll_status = as_txn_roll(as, &roll_err, roll_policy, txn, AS_MSG_INFO4_MRT_ROLL_BACK);

		if (roll_status != AEROSPIKE_OK) {
			as_error_update(err, status, "Txn aborted:\nVerify failed: %s\nRollback abandoned: %s",
				verify_err.message, roll_err.message);
			as_error_copy_fields(err, &verify_err);
			return status;
		}

		if (as_txn_monitor_might_exist(txn)) {
			roll_status = as_txn_monitor_remove(as, &roll_err, &roll_policy->base, &key);

			if (roll_status != AEROSPIKE_OK) {
				as_error_update(err, status, "Txn aborted:\nVerify failed: %s\nClose abandoned: %s",
					verify_err.message, roll_err.message);
				as_error_copy_fields(err, &verify_err);
				return status;
			}
		}

		as_error_update(err, status, "Txn aborted:\nVerify failed: %s", verify_err.message);
		as_error_copy_fields(err, &verify_err);
		return status;
	}

	if (as_txn_monitor_exists(txn)) {
		status = as_txn_monitor_mark_roll_forward(as, &verify_err, &roll_policy->base, &key);

		if (status != AEROSPIKE_OK) {
			as_error_update(err, status, "Txn aborted:\nMark roll forward abandoned: %s",
				verify_err.message);
			as_error_copy_fields(err, &verify_err);
			return status;
		}
	}

	status = as_txn_roll(as, err, roll_policy, txn, AS_MSG_INFO4_MRT_ROLL_FORWARD);

	if (status != AEROSPIKE_OK) {
		// The client roll has error. The server will eventually roll forward the transaction
		// after as_txn_monitor_mark_roll_forward() succeeds. Therefore, return success and
		// leave the error message for debug/log purposes.
		err->code = AEROSPIKE_OK;
		return AEROSPIKE_OK;
	}

	if (as_txn_monitor_might_exist(txn)) {
		status = as_txn_monitor_remove(as, err, &roll_policy->base, &key);

		if (status != AEROSPIKE_OK) {
			// The client transaction monitor remove has error. The server will eventually remove the
			// monitor record after as_txn_monitor_mark_roll_forward() succeeds. Therefore, return
			// success and leave the error message for debug/log purposes.
			err->code = AEROSPIKE_OK;
			return AEROSPIKE_OK;
		}
	}

	as_txn_clear(txn);
	return AEROSPIKE_OK;
}

//---------------------------------
// Sync Abort
//---------------------------------

as_status
aerospike_abort(aerospike* as, as_error* err, as_txn* txn)
{
	if (! as_txn_set_roll_attempted(txn)) {
		return as_error_set_message(err, AEROSPIKE_ROLL_ALREADY_ATTEMPTED,
			"Abort or commit already attempted");
	}

	as_error_reset(err);

	as_policy_txn_roll* roll_policy = &as->config.policies.txn_roll;

	as_status status = as_txn_roll(as, err, roll_policy, txn, AS_MSG_INFO4_MRT_ROLL_BACK);

	if (status != AEROSPIKE_OK) {
		// The client roll has error. The server will eventually abort the transaction.
		// Therefore, return success, but leave the error message for debug/log purposes.
		err->code = AEROSPIKE_OK;
		return AEROSPIKE_OK;
	}

	if (as_txn_monitor_might_exist(txn)) {
		as_key key;
		as_txn_monitor_init_key(txn, &key);

		status = as_txn_monitor_remove(as, err, &roll_policy->base, &key);

		if (status != AEROSPIKE_OK) {
			// The client transaction monitor remove has error. The server will eventually remove the
			// monitor record. Therefore, return success and leave the error message for debug/log purposes.
			err->code = AEROSPIKE_OK;
			return AEROSPIKE_OK;
		}
	}
	return AEROSPIKE_OK;
}

//---------------------------------
// Async Commit
//---------------------------------

typedef struct {
	aerospike* as;
	as_txn* txn;
	as_policy_txn_roll* roll_policy;
	as_commit_listener listener;
	void* udata;
	as_error* verify_err;
	bool verified;
} as_commit_data;

static inline void
as_commit_data_destroy(as_commit_data* data)
{
	if (data->verify_err) {
		cf_free(data->verify_err);
	}
	cf_free(data);
}

static inline void
as_commit_notify_success(as_commit_status status, as_commit_data* data, as_event_loop* event_loop)
{
	data->listener(NULL, status, data->udata, event_loop);
	as_commit_data_destroy(data);
}

static inline void
as_commit_notify_error(as_error* err, as_commit_status status, as_commit_data* data, as_event_loop* event_loop)
{
	data->listener(err, status, data->udata, event_loop);
	as_commit_data_destroy(data);
}

static void
as_commit_notify_error_mark(as_error* err, as_commit_data* data, as_event_loop* event_loop)
{
	as_error commit_err;
	as_error_update(&commit_err, err->code, "Txn aborted:\nMark roll forward abandoned: %s",
		err->message);

	as_error_copy_fields(&commit_err, err);
	as_commit_notify_error(&commit_err, AS_COMMIT_ROLL_FORWARD_ABANDONED, data, event_loop);
}

static void
as_commit_notify_error_verify_rollback(
	as_error* roll_err, as_commit_data* data, as_event_loop* event_loop
	)
{
	as_error commit_err;
	as_error* verify_err = data->verify_err;

	as_error_update(&commit_err, verify_err->code,
		"Txn aborted:\nVerify failed: %s\nRollback abandoned: %s",
		verify_err->message, roll_err->message);

	as_error_copy_fields(&commit_err, verify_err);
	as_commit_notify_error(&commit_err, AS_COMMIT_ROLL_FORWARD_ABANDONED, data, event_loop);
}

static void
as_commit_notify_error_verify_close(
	as_error* close_err, as_commit_data* data, as_event_loop* event_loop
	)
{
	as_error commit_err;
	as_error* verify_err = data->verify_err;

	as_error_update(&commit_err, verify_err->code,
		"Txn aborted:\nVerify failed: %s\nClose abandoned: %s",
		verify_err->message, close_err->message);

	as_error_copy_fields(&commit_err, verify_err);
	as_commit_notify_error(&commit_err, AS_COMMIT_CLOSE_ABANDONED, data, event_loop);
}

static void
as_commit_notify_error_verify(as_commit_data* data, as_event_loop* event_loop)
{
	as_error commit_err;
	as_error* verify_err = data->verify_err;

	as_error_update(&commit_err, verify_err->code,
		"Txn aborted:\nVerify failed: %s", verify_err->message);

	as_error_copy_fields(&commit_err, verify_err);
	as_commit_notify_error(&commit_err, AS_COMMIT_ROLL_FORWARD_ABANDONED, data, event_loop);
}

static void
as_commit_remove_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	as_commit_data* data = udata;

	if (err) {
		if (data->verified) {
			// The client transaction monitor remove has error. The server will eventually remove the
			// monitor record. Therefore, notify success.
			as_commit_notify_success(AS_COMMIT_CLOSE_ABANDONED, data, event_loop);
		}
		else {
			as_commit_notify_error_verify_close(err, data, event_loop);
		}
		return;
	}

	if (data->verified) {
		as_commit_notify_success(AS_COMMIT_OK, data, event_loop);
	}
	else {
		as_commit_notify_error_verify(data, event_loop);
	}
}

static void
as_commit_roll_listener(
	as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop
	)
{
	if (records) {
		as_batch_records_destroy(records);
	}

	as_commit_data* data = udata;

	if (err) {
		if (data->verified) {
			// The client roll has error. The server will eventually roll forward the transaction
			// after as_txn_monitor_mark_roll_forward_async() succeeds. Therefore, notify success.
			as_commit_notify_success(AS_COMMIT_ROLL_FORWARD_ABANDONED, data, event_loop);
		}
		else {
			as_commit_notify_error_verify_rollback(err, data, event_loop);
		}
		return;
	}

	as_key key;
	as_txn_monitor_init_key(data->txn, &key);

	as_error close_err;
	as_status status = as_txn_monitor_remove_async(data->as, &close_err, &data->roll_policy->base,
		&key, as_commit_remove_listener, data, event_loop);

	if (status != AEROSPIKE_OK) {
		if (data->verified) {
			// The client transaction monitor remove has error. The server will eventually remove the
			// monitor record after as_txn_monitor_mark_roll_forward_async() succeeds. Therefore,
			// notify success.
			as_commit_notify_success(AS_COMMIT_CLOSE_ABANDONED, data, event_loop);
		}
		else {
			as_commit_notify_error_verify_close(&close_err, data, event_loop);
		}
	}
}

static void
as_commit_mark_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	as_commit_data* data = udata;

	if (err) {
		as_commit_notify_error_mark(err, data, event_loop);
		return;
	}

	as_error roll_err;
	as_status status = as_txn_roll_async(data->as, &roll_err, data->roll_policy, data->txn,
	 	AS_MSG_INFO4_MRT_ROLL_FORWARD, as_commit_roll_listener, data, event_loop);

	if (status != AEROSPIKE_OK) {
		// The client roll has error. The server will eventually roll forward the transaction
		// after as_txn_monitor_mark_roll_forward_async() succeeds. Therefore, notify success.
		as_commit_notify_success(AS_COMMIT_ROLL_FORWARD_ABANDONED, data, event_loop);
	}
}

static void
as_commit_verify_listener(
	as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop
	)
{
	if (records) {
		as_batch_records_destroy(records);
	}

	as_commit_data* data = udata;

	if (err) {
		// Verify failed. Rollback transaction.
		data->verify_err = cf_malloc(sizeof(as_error));
		as_error_copy(data->verify_err, err);

		as_error roll_err;
		as_status status = as_txn_roll_async(data->as, &roll_err, data->roll_policy, data->txn,
			AS_MSG_INFO4_MRT_ROLL_BACK, as_commit_roll_listener, data, event_loop);

		if (status != AEROSPIKE_OK) {
			as_commit_notify_error_verify_rollback(&roll_err, data, event_loop);
		}
		return;
	}

	data->verified = true;

	if (! as_txn_monitor_exists(data->txn)) {
		as_commit_mark_listener(NULL, data, event_loop);
		return;
	}

	as_key key;
	as_txn_monitor_init_key(data->txn, &key);

	as_error mark_err;
	as_status status = as_txn_monitor_mark_roll_forward_async(data->as, &mark_err,
		&data->roll_policy->base, &key, as_commit_mark_listener, data, event_loop);

	if (status != AEROSPIKE_OK) {
		as_commit_notify_error_mark(err, data, event_loop);
	}
}

as_status
aerospike_commit_async(
	aerospike* as, as_error* err, as_txn* txn, as_commit_listener listener, void* udata,
	as_event_loop* event_loop
	)
{
	if (! as_txn_set_roll_attempted(txn)) {
		return as_error_set_message(err, AEROSPIKE_ROLL_ALREADY_ATTEMPTED,
			"Commit or abort already attempted");
	}

	event_loop = as_event_assign(event_loop);

	as_error_reset(err);

	as_commit_data* data = cf_malloc(sizeof(as_commit_data));
	data->as = as;
	data->txn = txn;
	data->roll_policy = &as->config.policies.txn_roll;
	data->listener = listener;
	data->udata = udata;
	data->verify_err = NULL;
	data->verified = false;

	as_status status = as_txn_verify_async(as, err, txn, as_commit_verify_listener, data, event_loop);

	if (status != AEROSPIKE_OK) {
		as_commit_data_destroy(data);
	}
	return status;
}

//---------------------------------
// Async Abort
//---------------------------------

typedef struct {
	aerospike* as;
	as_txn* txn;
	as_policy_txn_roll* roll_policy;
	as_abort_listener listener;
	void* udata;
} as_abort_data;

static inline void
as_abort_data_destroy(as_abort_data* data)
{
	cf_free(data);
}

static inline void
as_abort_notify_success(as_abort_status status, as_abort_data* data, as_event_loop* event_loop)
{
	data->listener(NULL, status, data->udata, event_loop);
	as_abort_data_destroy(data);
}

static void
as_abort_remove_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	as_abort_data* data = udata;

	if (err) {
		// The client transaction monitor remove has error. The server will eventually remove the
		// monitor record. Therefore, notify success.
		as_abort_notify_success(AS_ABORT_CLOSE_ABANDONED, data, event_loop);
		return;
	}

	as_abort_notify_success(AS_ABORT_OK, data, event_loop);
}

static void
as_abort_roll_listener(
	as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop
	)
{
	if (records) {
		as_batch_records_destroy(records);
	}

	as_abort_data* data = udata;

	if (err) {
		// The client roll has error. The server will eventually roll back the transaction.
		// Therefore, notify success.
		as_abort_notify_success(AS_ABORT_ROLL_BACK_ABANDONED, data, event_loop);
		return;
	}

	as_key key;
	as_txn_monitor_init_key(data->txn, &key);

	as_error close_err;
	as_status status = as_txn_monitor_remove_async(data->as, &close_err, &data->roll_policy->base,
		&key, as_abort_remove_listener, data, event_loop);

	if (status != AEROSPIKE_OK) {
		// The client transaction monitor remove has error. The server will eventually remove the
		// monitor record. Therefore, notify success.
		as_abort_notify_success(AS_ABORT_CLOSE_ABANDONED, data, event_loop);
	}
}

as_status
aerospike_abort_async(
	aerospike* as, as_error* err, as_txn* txn, as_abort_listener listener, void* udata,
	as_event_loop* event_loop
	)
{
	if (! as_txn_set_roll_attempted(txn)) {
		return as_error_set_message(err, AEROSPIKE_ROLL_ALREADY_ATTEMPTED,
			"Commit or abort already attempted");
	}

	event_loop = as_event_assign(event_loop);

	as_error_reset(err);

	as_abort_data* data = cf_malloc(sizeof(as_commit_data));
	data->as = as;
	data->txn = txn;
	data->roll_policy = &as->config.policies.txn_roll;
	data->listener = listener;
	data->udata = udata;

	as_status status = as_txn_roll_async(data->as, err, data->roll_policy, data->txn,
		AS_MSG_INFO4_MRT_ROLL_BACK, as_abort_roll_listener, data, event_loop);

	if (status != AEROSPIKE_OK) {
		as_abort_data_destroy(data);
	}
	return status;
}
