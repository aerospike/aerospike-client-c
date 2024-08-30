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
as_txn_roll(aerospike* as, as_error* err, as_policy_txn_roll* policy, as_txn* txn, uint8_t txn_attr);

//---------------------------------
// Static Functions
//---------------------------------

static void
as_error_copy_fields(as_error* err, as_error* verify_err)
{
	err->func = verify_err->func;
	err->file = verify_err->file;
	err->line = verify_err->line;
	err->in_doubt = verify_err->in_doubt;
}

//---------------------------------
// Functions
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
		// The client roll has error, but the server always eventually rolls forward the MRT
		// after as_txn_monitor_mark_roll_forward() succeeds. Therefore, return success,
		// but leave the error message for debug/log purposes.
		err->code = AEROSPIKE_OK;
		return AEROSPIKE_OK;
	}

	if (as_txn_monitor_might_exist(txn)) {
		status = as_txn_monitor_remove(as, err, &roll_policy->base, &key);

		if (status != AEROSPIKE_OK) {
			// The client remove has error, but the server always eventually removes the MRT
			// after as_txn_monitor_mark_roll_forward() succeeds. Therefore, return success,
			// but leave the error message for debug/log purposes.
			err->code = AEROSPIKE_OK;
			return AEROSPIKE_OK;
		}
	}

	as_txn_clear(txn);
	return AEROSPIKE_OK;
}

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
		// The client roll has error, but the server always eventually aborts the MRT.
		// Therefore, return success, but leave the error message for debug/log purposes.
		err->code = AEROSPIKE_OK;
		return AEROSPIKE_OK;
	}

	if (as_txn_monitor_might_exist(txn)) {
		as_key key;
		as_txn_monitor_init_key(txn, &key);

		status = as_txn_monitor_remove(as, err, &roll_policy->base, &key);

		if (status != AEROSPIKE_OK) {
			// The client remove has error, but the server always eventually removes the MRT.
			// Therefore, return success, but leave the error message for debug/log purposes.
			err->code = AEROSPIKE_OK;
			return AEROSPIKE_OK;
		}
	}
	return AEROSPIKE_OK;
}
