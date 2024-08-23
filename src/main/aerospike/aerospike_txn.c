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
#include <aerospike/as_txn.h>

//---------------------------------
// Types
//---------------------------------

typedef struct {
	as_cluster* cluster;
	as_txn* txn;
} as_roll;

//---------------------------------
// Static Functions
//---------------------------------

static inline bool
khash_is_empty(const as_khash* h)
{
	// TODO: Should this be done under lock?
	return h->n_eles == 0;
}

//---------------------------------
// Functions
//---------------------------------

as_status
as_txn_verify(aerospike* as, as_error* err, as_txn* txn, as_batch_listener listener, void* udata);

as_status
aerospike_commit(aerospike* as, as_error* err, as_txn* txn)
{
	if (! as_txn_set_roll_attempted(txn)) {
		return as_error_set_message(err, AEROSPIKE_ROLL_ALREADY_ATTEMPTED,
			"Commit or abort already attempted");
	}

	as_error_reset(err);

	// TODO: Create listener.
	/*
	as_status status = as_txn_verify(as, err, txn, listener, udata);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_policy_txn_roll* ptr = &as->config.policies.txn_roll;
	*/
	return AEROSPIKE_OK;
}

as_status
aerospike_abort(aerospike* as, as_error* err, as_txn* txn)
{
	if (! as_txn_set_roll_attempted(txn)) {
		return as_error_set_message(err, AEROSPIKE_ROLL_ALREADY_ATTEMPTED,
			"Abort or commit already attempted");
	}
	return AEROSPIKE_OK;
}
