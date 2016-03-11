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
#include <inttypes.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// Constants
//

#define UDF_MODULE "bg_scan_udf"
const char UDF_FILE_PATH[] = "src/lua/" UDF_MODULE ".lua";


//==========================================================
// Forward Declarations
//

void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);


//==========================================================
// BACKGROUND SCAN Example
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

	// Register the UDF in the database cluster.
	if (! example_register_udf(&as, UDF_FILE_PATH)) {
		example_cleanup(&as);
		exit(-1);
	}

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	if (! example_read_test_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Specify the namespace, set, and the UDF to apply during the scan.
	as_scan scan;
	as_scan_init(&scan, g_namespace, g_set);
	as_scan_apply_each(&scan, UDF_MODULE, "test_bin_add_1000", NULL);

	// Using a scan ID of 0 tells the client to generate one.
	uint64_t scan_id = 0;

	// Start the scan. This call does NOT block while the scan is running.
	if (aerospike_scan_background(&as, &err, NULL, &scan, &scan_id) !=
			AEROSPIKE_OK) {
		LOG("aerospike_scan_background() returned %d - %s", err.code,
				err.message);
		as_scan_destroy(&scan);
		cleanup(&as);
		exit(-1);
	}

	// Destroy the as_scan object.
	as_scan_destroy(&scan);

	LOG("started background scan %" PRIu64 " ...", scan_id);

	as_scan_info info;

	// Poll to see when scan is done.
	do {
		usleep(1000 * 500);

		if (aerospike_scan_info(&as, &err, NULL, scan_id, &info) !=
				AEROSPIKE_OK) {
			LOG("aerospike_scan_info() returned %d - %s", err.code,
					err.message);
			cleanup(&as);
			exit(-1);
		}

		LOG("scan status: %d, progress pct: %u, records scanned: %u",
				info.status, info.progress_pct, info.records_scanned);

	} while (info.status == AS_SCAN_STATUS_INPROGRESS);

	LOG("... finished background scan");

	// Read everything back and show the changes done by the scan.
	if (! example_read_test_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("background scan example successfully completed");

	return 0;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_remove_udf(p_as, UDF_FILE_PATH);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	// Create an as_record object with one (integer value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 1);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		// In general it's ok to reset a bin value - all as_record_set_... calls
		// destroy any previous value.
		as_record_set_int64(&rec, "test-bin", (int64_t)i);

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}
