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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// PUT Example
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

	// Create an as_record object with two bins with different value types. By
	// using as_record_inita(), we won't need to destroy the record if we only
	// set bins using as_record_set_int64(), as_record_set_str(), and
	// as_record_set_nil().
	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "test-bin-1", 1234);
	as_record_set_str(&rec, "test-bin-2", "test-bin-2-data");

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

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Generate a different as_record object to write. In general it's ok to
	// reuse the stack object by calling as_record_inita() again, as long as the
	// previous contents are destroyed if necessary.
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "test-bin-2", 2222);
	as_record_set_str(&rec, "test-bin-3", "test-bin-3-data");

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database. This will change the type and value of
	// test-bin-2, will add test-bin-3, and will leave test-bin-one unchanged.
	if (aerospike_key_put(&as, &err, NULL, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("write succeeded");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Generate another as_record object to write.
	as_record_inita(&rec, 1);
	as_record_set_nil(&rec, "test-bin-3");

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database. This will remove test-bin-3 and
	// will leave test-bin-1 and test-bin-2 unchanged.
	if (aerospike_key_put(&as, &err, NULL, &g_key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("write succeeded");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Generate another as_record object to write.
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "test-bin-1", 1111);

	// Require that the write succeeds only if the record doesn't exist.
	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.exists = AS_POLICY_EXISTS_CREATE;

	// Log its contents.
	LOG("as_record object to create in database:");
	example_dump_record(&rec);

	// Try to create the record. This should fail since the record already
	// exists in the database.
	if (aerospike_key_put(&as, &err, &wpol, &g_key, &rec) !=
			AEROSPIKE_ERR_RECORD_EXISTS) {
		LOG("aerospike_key_put() returned %d - %s, expected "
				"AEROSPIKE_ERR_RECORD_EXISTS", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("create failed as expected");

	// Remove the record from the database so we can demonstrate create success.
	if (aerospike_key_remove(&as, &err, NULL, &g_key) != AEROSPIKE_OK) {
		LOG("aerospike_key_remove() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("record removed from database, trying create again");

	// Try to create the record again. This should succeed since the record is
	// not currently in the database.
	if (aerospike_key_put(&as, &err, &wpol, &g_key, &rec) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("create succeeded");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("put example successfully completed");

	return 0;
}
