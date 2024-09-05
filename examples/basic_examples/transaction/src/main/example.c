/*******************************************************************************
 * Copyright 2008-2018 by Aerospike.
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


//==========================================================
// Includes
//

#include <stddef.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_txn.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"

int
main(int argc, char* argv[])
{
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	aerospike as;

	example_connect_to_aerospike(&as);
	example_remove_test_record(&as);

	LOG("as_txn_init");

	as_txn txn;
	as_txn_init(&txn);

	// Write record in a transaction.
	as_policy_write pw;
	as_policy_write_copy(&as.config.policies.write, &pw);
	pw.base.txn = &txn;

	as_key key;
	as_key_init_int64(&key, "test", "demoset", 1);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 1234);

	as_error err;

	LOG("aerospike_key_put: %" PRId64, (int64_t)txn.id);

	if (aerospike_key_put(&as, &err, &pw, &key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("aerospike_commit");
	// Commit transaction.
	// TODO: Change to as_txn_commit(&as, &err, &txn)
	if (aerospike_commit(&as, &err, &txn) != AEROSPIKE_OK) {
		LOG("aerospike_commit() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("as_txn_destroy");
	as_record_destroy(&rec);
	as_txn_destroy(&txn);

	example_cleanup(&as);
	LOG("txn example completed");
	return 0;
}
