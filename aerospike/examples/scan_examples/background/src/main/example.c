/*******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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
#include <unistd.h> // temp, for sleep

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// Constants
//

#define UDF_MODULE "bg_scan_udf"
const char UDF_FILE_PATH[] = "src/lua/" UDF_MODULE ".lua";
const char UDF_FUNCTION_NAME[] = "test_bin_add_1000";


//==========================================================
// Forward Declarations
//

void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);
bool read_records(aerospike* p_as);
void remove_records(aerospike* p_as);


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

	if (! example_register_udf(&as, UDF_FILE_PATH)) {
		example_cleanup(&as);
		exit(-1);
	}

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	if (! read_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	// Specify the namespace, set, and the UDF to apply during the scan.
	as_scan scan;
	as_scan_init(&scan, g_namespace, g_set);
	as_scan_foreach(&scan, UDF_MODULE, UDF_FUNCTION_NAME, NULL);

	as_error err;

	// Using a scan ID of 0 tells the client to generate one.
	uint64_t scan_id = 0;

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

	LOG("started background scan %lu ...", scan_id);

	// TODO - poll to see when it's actually done.
	sleep(3);

	LOG("... finished background scan");

	// Read everything back and show the changes done by the scan.
	if (! read_records(&as)) {
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
	remove_records(p_as);
	example_remove_udf(p_as, UDF_FILE_PATH);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	as_key key;
	as_record rec;

	// Create an as_record object with one (integer value) bin.
	as_record_inita(&rec, 1);

	// Re-using key and rec, write records into the database such that each
	// record's key and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);
		as_record_set_int64(&rec, "test-bin", (int64_t)i);

		as_error err;

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}

bool
read_records(aerospike* p_as)
{
	as_key key;

	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		as_error err;
		as_record* p_rec = NULL;

		// Read a test record from the database.
		if (aerospike_key_get(p_as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
			return false;
		}

		// If we didn't get an as_record object back, something's wrong.
		if (! p_rec) {
			LOG("aerospike_key_get() retrieved null as_record object");
			return false;
		}

		// Log the result.
		LOG("read key %u from database:", i);
		example_dump_record(p_rec);

		// Destroy the as_record object.
		as_record_destroy(p_rec);
	}

	return true;
}

void
remove_records(aerospike* p_as)
{
	as_key key;

	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		as_error err;

		// Ignore errors - just trying to leave the database as we found it.
		aerospike_key_remove(p_as, &err, NULL, &key);
	}
}
