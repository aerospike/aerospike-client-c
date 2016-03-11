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
// APPEND Example
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

	// Create an as_operations object with three concatenation operations.
	// Generally, if using as_operations_inita(), we won't need to destroy the
	// object unless we call as_operations_add_write() with an externally
	// allocated as_bin_value.
	as_operations ops;
	as_operations_inita(&ops, 3);
	as_operations_add_append_str(&ops, "test-bin-1", "John");
	as_operations_add_prepend_str(&ops, "test-bin-2", "Washington");
	as_operations_add_append_raw(&ops, "test-bin-3", (const uint8_t*)"123", 3);

	// Log the operations.
	LOG("as_operations object to apply to database:");
	example_dump_operations(&ops);

	// Apply the operations. Since the record does not exist, it will be created
	// and the bins initialized with the ops' string values.
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

	// Generate a different set of concatenation operations.
	as_operations_inita(&ops, 3);
	as_operations_add_append_str(&ops, "test-bin-1", " Hancock");
	as_operations_add_prepend_str(&ops, "test-bin-2", "George ");
	as_operations_add_append_raw(&ops, "test-bin-3", (const uint8_t*)"456", 3);

	// Log the operations.
	LOG("as_operations object to apply to database:");
	example_dump_operations(&ops);

	// Apply the operations. The bins exist, so the ops' values will be applied
	// to the existing values.
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

	// Generate a different set of concatenation operations.
	as_operations_inita(&ops, 2);
	as_operations_add_prepend_str(&ops, "test-bin-2", "President ");
	as_operations_add_append_str(&ops, "test-bin-3", "789");

	// Log the operations.
	LOG("as_operations object to apply to database:");
	example_dump_operations(&ops);

	// Try to apply the operations. This will fail, since we can't append a
	// string value to an existing bin with "raw" value. Note that if any
	// operation in the transaction is rejected, none will be applied.
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

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("append example successfully completed");

	return 0;
}
