/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

bool scan_cb(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);


//==========================================================
// STANDARD SCAN Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_MULTI_KEY_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_records(&as);

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	if (! example_read_test_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Specify the namespace and set to use during the scan.
	as_scan scan;
	as_scan_init(&scan, g_namespace, g_set);

	LOG("starting scan all ...");

	// Do the scan. This call blocks while the scan is running - callbacks are
	// made in the scope of this call.
	if (aerospike_scan_foreach(&as, &err, NULL, &scan, scan_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_scan_foreach() returned %d - %s", err.code, err.message);
		as_scan_destroy(&scan);
		cleanup(&as);
		exit(-1);
	}

	LOG("... scan all completed");

	// Now specify that only two bins are to be returned by the scan. The first
	// ten records do not have these two bins, so they should not be returned by
	// the scan. The remaining records should be returned without test-bin-1.
	as_scan_select_inita(&scan, 2);
	as_scan_select(&scan, "test-bin-2");
	as_scan_select(&scan, "test-bin-3");

	LOG("starting scan with select ...");

	// Do the scan. This call blocks while the scan is running - callbacks are
	// made in the scope of this call.
	if (aerospike_scan_foreach(&as, &err, NULL, &scan, scan_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_scan_foreach() returned %d - %s", err.code, err.message);
		as_scan_destroy(&scan);
		cleanup(&as);
		exit(-1);
	}

	LOG("... scan with select completed");

	// Destroy the as_scan object.
	as_scan_destroy(&scan);

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("standard scan example successfully completed");

	return 0;
}


//==========================================================
// Scan Callback
//

bool
scan_cb(const as_val* p_val, void* udata)
{
	if (! p_val) {
		LOG("scan callback returned null - scan is complete");
		return true;
	}

	// The scan didn't use a UDF, so the as_val object should be an as_record.
	as_record* p_rec = as_record_fromval(p_val);

	if (! p_rec) {
		LOG("scan callback returned non-as_record object");
		return true;
	}

	LOG("scan callback returned record:");
	example_dump_record(p_rec);

	return true;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	// Create an as_record object with up to three integer value bins. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 3);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// Set up a default as_policy_write object.
		as_policy_write wpol;
		as_policy_write_init(&wpol);

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		// In general it's ok to reset a bin value - all as_record_set_... calls
		// destroy any previous value.

		if (i < 10) {
			// Only write one bin in the first ten records.
			as_record_set_int64(&rec, "test-bin-1", (int64_t)i);

			// By default, we don't store the key with the record in the
			// database. For these records, the key will not be returned in the
			// scan callback.
		}
		else {
			// Write three bins in all remaining records.
			as_record_set_int64(&rec, "test-bin-1", (int64_t)i);
			as_record_set_int64(&rec, "test-bin-2", (int64_t)(100 + i));
			as_record_set_int64(&rec, "test-bin-3", (int64_t)(1000 + i));

			// If we want the key to be returned in the scan callback, we must
			// store it with the record in the database. AS_POLICY_KEY_SEND
			// causes the key to be stored.
			wpol.key = AS_POLICY_KEY_SEND;
		}

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, &wpol, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}
