/*******************************************************************************
 * Copyright 2015 by Aerospike.
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
#include <aerospike/as_error.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// List Example
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
	as_operations ops;

	LOG("append 10 values from 0 to 9");

	// The first append will create the record and bin.
	for (int i = 0; i < 10; i++) {
		as_operations_inita(&ops, 1);
		as_operations_add_list_append_int64(&ops, "test-bin-1", i);

		if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, NULL) !=
				AEROSPIKE_OK) {
			LOG("aerospike_key_operate() returned %d - %s", err.code,
					err.message);
			example_cleanup(&as);
			exit(-1);
		}

		as_operations_destroy(&ops);
	}

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	LOG("append operations succeeded");
	LOG("pop from the tail (index -1)");

	as_operations_inita(&ops, 1);
	as_operations_add_list_pop(&ops, "test-bin-1", -1);

	as_record *p_rec = NULL;

	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, &p_rec) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		as_operations_destroy(&ops);
		example_cleanup(&as);
		exit(-1);
	}

	as_operations_destroy(&ops);

	LOG("pop operation succeeded");
	LOG("insert popped value at the head (index 0)");

	as_val *val = (as_val *)as_record_get(p_rec, "test-bin-1");

	as_operations_inita(&ops, 1);
	as_operations_add_list_insert(&ops, "test-bin-1", 0, val);

	if (aerospike_key_operate(&as, &err, NULL, &g_key, &ops, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		as_operations_destroy(&ops);
		example_cleanup(&as);
		exit(-1);
	}

	as_record_destroy(p_rec);
	as_operations_destroy(&ops);

	LOG("insert operation succeeded");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("list example successfully completed");

	return 0;
}
