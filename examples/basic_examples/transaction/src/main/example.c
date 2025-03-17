/*******************************************************************************
 * Copyright 2008-2024 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
#include <stddef.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_txn.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"

static as_status run_commands(aerospike* as, as_txn* txn);

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		return -1;
	}

	// Connect to cluster.
	as_error err;
	as_status status;
	aerospike as;
	example_connect_to_aerospike(&as);

	// Initialize transaction.
	as_txn txn;
	as_txn_init(&txn);
	printf("Initialize transaction: %" PRIu64 "\n", txn.id);

	// Run commands in a transaction. The commands must use a single namespace and
	// the namespace must be configured as strong-consistency.
	status = run_commands(&as, &txn);

	if (status == AEROSPIKE_OK) {
		printf("Commit transaction: %" PRIu64 "\n", txn.id);
		as_commit_status commit_status;
		status = aerospike_commit(&as, &err, &txn, &commit_status);

		if (status != AEROSPIKE_OK) {
			printf("aerospike_commit() returned %d - %s\n", err.code, err.message);
			// Do not call aerospike_abort() if the commit fails.
			if (commit_status == AS_COMMIT_MARK_ROLL_FORWARD_ABANDONED) {
				// The commit read-verify step succeeded, but the transaction monitor
				// could not be marked for roll-forward. In this case, the transaction
				// could be re-committed.
				printf("Transaction can be re-committed\n");
			}
			else {
				// The commit read-verify step failed. The transaction has been
				// permanently aborted.
				printf("Transaction aborted\n");
			}
		}
	}
	else {
		printf("Abort transaction: %" PRIu64 "\n", txn.id);
		as_status s = aerospike_abort(&as, &err, &txn, NULL);

		if (s != AEROSPIKE_OK) {
			printf("aerospike_abort() returned %d - %s\n", err.code, err.message);
		}
	}

	// Cleanup.
	as_txn_destroy(&txn);
	aerospike_close(&as, &err);
	aerospike_destroy(&as);
	return status == AEROSPIKE_OK? 0 : -1;
}

static bool batch_write_cb(const as_batch_result* results, uint32_t n_keys, void* udata);

static as_status
run_commands(aerospike* as, as_txn* txn)
{
	as_error err;
	as_status status;

	printf("Write record\n");

	as_policy_write pw;
	as_policy_write_copy(&as->config.policies.write, &pw);
	pw.base.txn = txn;

	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, 1);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 1234);

	status = aerospike_key_put(as, &err, &pw, &key, &rec);

	if (status != AEROSPIKE_OK) {
		printf("aerospike_key_put() 1 returned %d - %s\n", err.code, err.message);
		return status;
	}
	as_record_destroy(&rec);

	printf("Write more records in a batch\n");

	as_policy_batch pb;
	as_policy_batch_copy(&as->config.policies.batch_parent_write, &pb);
	pb.base.txn = txn;

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_write_int64(&ops, "c", 9999);

	as_batch batch;
	as_batch_inita(&batch, 2);

	for (uint32_t i = 0; i < 2; i++) {
		as_key_init_int64(as_batch_keyat(&batch, i), g_namespace, g_set, i);
	}

	status = aerospike_batch_operate(as, &err, &pb, NULL, &batch, &ops, batch_write_cb, NULL);
	as_operations_destroy(&ops);

	if (status != AEROSPIKE_OK) {
		printf("aerospike_batch_operate() returned %d - %s\n", err.code, err.message);
		return status;
	}
 	as_batch_destroy(&batch);

	printf("Read record\n");

	as_policy_read pr;
	as_policy_read_copy(&as->config.policies.read, &pr);
	pr.base.txn = txn;

	as_key_init_int64(&key, g_namespace, g_set, 3);

	as_record* recp = NULL;
	status = aerospike_key_get(as, &err, &pr, &key, &recp);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		printf("aerospike_key_get() returned %d - %s\n", err.code, err.message);
		return status;
	}
	as_record_destroy(recp);

	printf("Delete record\n");

	as_policy_remove prem;
	as_policy_remove_copy(&as->config.policies.remove, &prem);
	prem.base.txn = txn;
	prem.durable_delete = true; // Required when deleting records in a transaction.

	status = aerospike_key_remove(as, &err, &prem, &key);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		printf("aerospike_key_remove() returned %d - %s\n", err.code, err.message);
		return status;
	}

	return AEROSPIKE_OK;
}

static bool
batch_write_cb(const as_batch_result* results, uint32_t n_keys, void* udata)
{
	for (uint32_t i = 0; i < n_keys; i++) {
		const as_batch_result* r = &results[i];

		if (r->result != AEROSPIKE_OK) {
			printf("batch row[%u] returned %d\n", i, r->result);
		}
	}
	return true;
}
