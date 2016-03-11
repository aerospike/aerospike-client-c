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
// Constants
//

const uint32_t TEST_TTL = 5;


//==========================================================
// EXPIRE Example
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

	// Create an as_record object with one (integer value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "test-bin", 1234);

	// Set the TTL of the record so that it won't last very long.
	rec.ttl = TEST_TTL;

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database.
	if (aerospike_key_put(&as, &err, NULL, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("write succeeded");

	as_record* p_rec = NULL;

	// Check that the record is in the database.
	if (aerospike_key_exists(&as, &err, NULL, &g_key, &p_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_exists() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified record is in database");
	as_record_destroy(p_rec);
	p_rec = NULL;

	// Wait until past its specified expiration.
	LOG("waiting %u seconds for record to expire ...", TEST_TTL + 1);
	sleep(TEST_TTL + 1);

	// Check that it's no longer in the database.
	if (aerospike_key_exists(&as, &err, NULL, &g_key, &p_rec) !=
			AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		LOG("aerospike_key_exists() returned %d - %s, expected "
				"AEROSPIKE_ERR_RECORD_NOT_FOUND", err.code, err.message);
		as_record_destroy(p_rec);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("verified record successfully expired");

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("expire example successfully completed");

	return 0;
}
