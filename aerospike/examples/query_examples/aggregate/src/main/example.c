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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Constants
//

#define UDF_MODULE "query_udf"
const char UDF_FILE_PATH[] = "src/lua/" UDF_MODULE ".lua";

const char TEST_INDEX_NAME[] = "test-bin-index";


//==========================================================
// Forward Declarations
//

bool query_cb(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);
void remove_test_index(aerospike* p_as);


//==========================================================
// AGGREGATE QUERY Example
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
	remove_test_index(&as);

	// Register the UDF in the database cluster.
	if (! example_register_udf(&as, UDF_FILE_PATH)) {
		example_cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Create a numeric secondary index on test-bin.
	if (aerospike_index_integer_create(&as, &err, NULL, g_namespace, g_set,
			"test-bin", TEST_INDEX_NAME) != AEROSPIKE_OK) {
		LOG("aerospike_index_integer_create() returned %d - %s", err.code,
				err.message);
		cleanup(&as);
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

	// Create an as_query object.
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	// Generate an as_query.where condition. Note that as_query_destroy() takes
	// care of destroying all the query's member objects if necessary. However
	// using as_query_where_inita() does avoid internal heap usage.
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin", integer_range(1, 4));
	as_query_apply(&query, UDF_MODULE, "sum_test_bin", NULL);

	LOG("executing query: where test-bin = 1 ... 4");

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		cleanup(&as);
	}

	LOG("query executed");

	as_query_destroy(&query);

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("aggregate query example successfully completed");

	return 0;
}


//==========================================================
// Query Callback
//

bool
query_cb(const as_val* p_val, void* udata)
{
	// Because of the UDF used, we expect an as_integer to be returned.
	int64_t i_val = as_integer_getorelse(as_integer_fromval(p_val), -1);

	// Caller's responsibility to destroy as_val returned.
	as_val_destroy(p_val);

	if (i_val == -1) {
		LOG("query callback returned non-as_integer object");
	}

	LOG("query callback returned %ld", i_val);

	return true;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	remove_test_index(p_as);
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

void
remove_test_index(aerospike* p_as)
{
	as_error err;

	// Ignore errors - just trying to leave the database as we found it.
	aerospike_index_remove(p_as, &err, NULL, g_namespace, TEST_INDEX_NAME);
}
