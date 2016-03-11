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
#include <inttypes.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Constants
//

#define UDF_MODULE "basic_udf"
const char UDF_FILE_PATH[] = "src/lua/" UDF_MODULE ".lua";


//==========================================================
// Forward Declarations
//

void cleanup(aerospike* p_as);
bool write_record(aerospike* p_as);


//==========================================================
// UDF Example
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

	// Register the UDF in the database cluster.
	if (! example_register_udf(&as, UDF_FILE_PATH)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Write a record to the database.
	if (! write_record(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Apply a simple UDF, with no arguments and no return value.
	if (aerospike_key_apply(&as, &err, NULL, &g_key, UDF_MODULE,
			"test_bin_1_add_1000", NULL, NULL) != AEROSPIKE_OK) {
		LOG("aerospike_key_apply() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	LOG("test_bin_1_add_1000() was successfully applied");

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Create an argument list for a (different) UDF. By using
	// as_arraylist_inita() we avoid some but not all internal heap usage, so we
	// must call as_arraylist_destroy().
	as_arraylist args;
	as_arraylist_inita(&args, 3);
	as_arraylist_append_str(&args, "test-bin-2");
	as_arraylist_append_int64(&args, 4);
	as_arraylist_append_int64(&args, 400);

	// Expect an integer return value.
	as_val* p_return_val = NULL;

	// Apply a UDF with arguments and a return value.
	if (aerospike_key_apply(&as, &err, NULL, &g_key, UDF_MODULE,
			"bin_transform", (as_list*)&args, &p_return_val) != AEROSPIKE_OK) {
		LOG("aerospike_key_apply() returned %d - %s", err.code, err.message);
		as_arraylist_destroy(&args);
		cleanup(&as);
		exit(-1);
	}

	as_arraylist_destroy(&args);

	if (! p_return_val) {
		LOG("aerospike_key_apply() retrieved null as_val object");
		cleanup(&as);
		exit(-1);
	}

	// Extract an integer from the as_val returned.
	int64_t i_val = as_integer_getorelse(as_integer_fromval(p_return_val), -1);

	// Caller's responsibility to destroy as_val returned.
	as_val_destroy(p_return_val);

	if (i_val == -1) {
		LOG("aerospike_key_apply() retrieved non-as_integer object");
		cleanup(&as);
		exit(-1);
	}

	LOG("bin_transform() was successfully applied - returned %" PRId64, i_val);

	if (! example_read_test_record(&as)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("udf example successfully completed");

	return 0;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_udf(p_as, UDF_FILE_PATH);
	example_cleanup(p_as);
}

bool
write_record(aerospike* p_as)
{
	as_error err;

	// Create an as_record object with two (integer type) bins. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "test-bin-1", 1000);
	as_record_set_int64(&rec, "test-bin-2", 1000);

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
