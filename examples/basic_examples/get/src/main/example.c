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
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

bool write_record(aerospike* p_as);


//==========================================================
// GET Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_record(&as);

	as_error err;
	as_record* p_rec = NULL;

	// Try to read the test record from the database. This should fail since the
	// record is not there.
	if (aerospike_key_get(&as, &err, NULL, &g_key, &p_rec) !=
			AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		LOG("aerospike_key_get() returned %d - %s, expected "
				"AEROSPIKE_ERR_RECORD_NOT_FOUND", err.code, err.message);
		as_record_destroy(p_rec);
		example_cleanup(&as);
		exit(-1);
	}

	// Note that p_rec will still be NULL here.
	LOG("get (non-existent record) failed as expected");

	// Write a record to the database so we can demonstrate read success.
	if (! write_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Read the (whole) test record from the database.
	if (aerospike_key_get(&as, &err, NULL, &g_key, &p_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Log the result and recycle the as_record object.
	LOG("record was successfully read from database:");
	example_dump_record(p_rec);
	as_record_destroy(p_rec);
	p_rec = NULL;

	// Select bins 1 and 3 to read.
	static const char* bins_1_3[] = { "test-bin-1", "test-bin-3", NULL };

	// Read only these two bins of the test record from the database.
	if (aerospike_key_select(&as, &err, NULL, &g_key, bins_1_3, &p_rec) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_select() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Log the result and recycle the as_record object.
	LOG("bins 1 and 3 were read from database:");
	example_dump_record(p_rec);
	as_record_destroy(p_rec);
	p_rec = NULL;

	// Select non-existent bin 5 to read.
	static const char* bins_5[] = { "test-bin-5", NULL };

	// Read only this bin from the database. This call should return an
	// as_record object with one bin, with null as_bin_value.
	if (aerospike_key_select(&as, &err, NULL, &g_key, bins_5, &p_rec) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_select() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Log the result and destroy the as_record object.
	LOG("non-existent bin 5 was read from database:");
	example_dump_record(p_rec);
	as_record_destroy(p_rec);
	p_rec = NULL;

	// Sleep 2 seconds, just to show the TTL decrease.
	LOG("waiting 2 seconds ...");
	sleep(2);

	// Use aerospike_key_exists() to get only record metadata.
	if (aerospike_key_exists(&as, &err, NULL, &g_key, &p_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_exists() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	// Log the result, which will only have metadata.
	LOG("existence check found record metadata:");
	example_dump_record(p_rec);
	as_record_destroy(p_rec);

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("get example successfully completed");

	return 0;
}


//==========================================================
// Helpers
//

bool
write_record(aerospike* p_as)
{
	as_error err;

	// Create an as_record object with four bins with different value types. By
	// using as_record_inita(), we won't need to destroy the record if we only
	// set bins using as_record_set_int64(), as_record_set_str(), and
	// as_record_set_raw().
	as_record rec;
	as_record_inita(&rec, 4);
	as_record_set_int64(&rec, "test-bin-1", 1111);
	as_record_set_int64(&rec, "test-bin-2", 2222);
	as_record_set_str(&rec, "test-bin-3", "test-bin-3-data");

	static const uint8_t bytes[] = { 1, 2, 3 };
	as_record_set_raw(&rec, "test-bin-4", bytes, 3);

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database.
	if (aerospike_key_put(p_as, &err, NULL, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		return false;
	}

	LOG("write succeeded");

	return true;
}
