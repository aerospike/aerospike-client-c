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
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_txn.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"

static void
abort_txn(aerospike* as, as_txn* txn)
{
	as_error err;
	as_status status = aerospike_abort(as, &err, txn);

	if (status != AEROSPIKE_OK) {
		LOG("aerospike_abort() returned %d - %s", err.code, err.message);
	}
	example_cleanup(as);
}

int
main(int argc, char* argv[])
{
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	aerospike as;

	example_connect_to_aerospike(&as);
	example_remove_test_record(&as);

	// Initialize transaction.
	as_txn txn;
	as_txn_init(&txn);
	LOG("Initialize transaction: %" PRId64, (int64_t)txn.id);

	// Perform commands in a transaction.
	as_policy_write pw;
	as_policy_write_copy(&as.config.policies.write, &pw);
	pw.base.txn = &txn;

	LOG("aerospike_key_put()");

	as_key key;
	as_key_init_int64(&key, "test", "demoset", 1);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 1234);

	as_error err;

	if (aerospike_key_put(&as, &err, &pw, &key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		abort_txn(&as, &txn);
		exit(-1);
	}

	LOG("aerospike_key_put()");

	as_key_init_int64(&key, "test", "demoset", 2);
	as_record_set_int64(&rec, "b", 5678);

	if (aerospike_key_put(&as, &err, &pw, &key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		abort_txn(&as, &txn);
		exit(-1);
	}
	as_record_destroy(&rec);

	LOG("aerospike_key_get()");

	as_policy_read pr;
	as_policy_read_copy(&as.config.policies.read, &pr);
	pr.base.txn = &txn;

	as_key_init_int64(&key, "test", "demoset", 3);

	as_record* recp = NULL;
	as_status status = aerospike_key_get(&as, &err, &pr, &key, &recp);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
		abort_txn(&as, &txn);
		exit(-1);
	}
	as_record_destroy(recp);

	LOG("aerospike_key_remove()");

	as_policy_remove prem;
	as_policy_remove_copy(&as.config.policies.remove, &prem);
	prem.base.txn = &txn;
	prem.durable_delete = true;

	status = aerospike_key_remove(&as, &err, &prem, &key);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		LOG("aerospike_key_remove() returned %d - %s", err.code, err.message);
		abort_txn(&as, &txn);
		exit(-1);
	}

	LOG("Commit transaction: %" PRId64, (int64_t)txn.id);

	if (aerospike_commit(&as, &err, &txn) != AEROSPIKE_OK) {
		LOG("aerospike_commit() returned %d - %s", err.code, err.message);
		// Do not call aerospike_abort() when commit fails.
		// aerospike_commit() will attempt an abort on failure.
		example_cleanup(&as);
		exit(-1);
	}

	// Cleanup transaction.
	as_txn_destroy(&txn);
	example_cleanup(&as);
	return 0;
}
