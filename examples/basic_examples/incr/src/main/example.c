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
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// INCR Example
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

	// Create an as_operations object with a pair of bin arithmetic operations.
	// Generally, if using as_operations_inita(), we won't need to destroy the
	// object unless we call as_operations_add_write() with an externally
	// allocated as_bin_value.
	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_incr(&ops, "test-bin-1", 1001);
	as_operations_add_incr(&ops, "test-bin-2", 1002);

	// Log the operations.
	LOG("as_operations object to apply to database:");
	example_dump_operations(&ops);

	// Apply the operations. Since the record does not exist, it will be created
	// and the bins initialized with the ops' integer values.
	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("operations succeeded");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Generate a different set of arithmetic operations.
	as_operations_inita(&ops, 3);
	as_operations_add_incr(&ops, "test-bin-1", 1);
	as_operations_add_incr(&ops, "test-bin-2", -2);
	as_operations_add_incr(&ops, "test-bin-3", 3);

	// Log the operations.
	LOG("as_operations object to apply to database:");
	example_dump_operations(&ops);

	// Apply the operations. The first two bins exist, so those ops' values will
	// be added to the existing values. The third (non-existent) bin will be
	// created and initialized with the op's integer value.
	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("operations succeeded");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Create an as_record object with one string value bin.
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, "test-bin-1", "test-bin-1-data");

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database, to change the value type of the bin.
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

	// Log the operations. (Same operations as last time.)
	LOG("as_operations object to apply to database:");
	example_dump_operations(&ops);

	// Try to apply the three arithmetic operations again. This will fail, since
	// we can't increment the string value. Note that if any operation in the
	// transaction is rejected, none will be applied.
	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, NULL) !=
			AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE) {
		LOG("aerospike_key_operate() returned %d - %s, expected "
				"AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("operations failed as expected");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Generate a pair of operations to do an atomic increment and read.
	as_operations_inita(&ops, 2);
	as_operations_add_incr(&ops, "test-bin-3", 1);
	as_operations_add_read(&ops, "test-bin-3");

	// Log the operations.
	LOG("as_operations object to apply to database:");
	example_dump_operations(&ops);

	as_record* p_rec = NULL;

	// Apply the operations. The first will add the op's value to the existing
	// value, and the second will return the result. The pair of operations will
	// be atomic on the server.
	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, &p_rec) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		exit(-1);
	}

	LOG("operations succeeded");

	example_dump_record(p_rec);
	as_record_destroy(p_rec);

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("incr example successfully completed");

	return 0;
}
