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
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <inttypes.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_map.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Constants
//

#define UDF_MODULE "query_udf"
#define UDF_USER_PATH "src/lua/"
const char UDF_FILE_PATH[] =  UDF_USER_PATH UDF_MODULE ".lua";

const char TEST_INDEX_NAME[] = "test-bin-index";

const int TOKENS_PER_BIN = 5;
const int MAX_TOKEN = 10; // don't exceed 2 digits, i.e. 99


//==========================================================
// Forward Declarations
//

bool query_cb(const as_val* p_val, void* udata);
bool query_cb_map(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);
char* generate_numbers(char* numbers);


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
	example_connect_to_aerospike_with_udf_config(&as, UDF_USER_PATH);

	// Start clean.
	example_remove_test_records(&as);
	example_remove_index(&as, TEST_INDEX_NAME);

	// Register the UDF in the database cluster.
	if (! example_register_udf(&as, UDF_FILE_PATH)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Create a numeric secondary index on test-bin.
	if (! example_create_integer_index(&as, "test-bin", TEST_INDEX_NAME)) {
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

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	// Generate an as_query.where condition. Note that as_query_destroy() takes
	// care of destroying all the query's member objects if necessary. However
	// using as_query_where_inita() does avoid internal heap usage.
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin", as_integer_range(1, 10));

	// Specify the UDF to use on the resulting stream.
	as_query_apply(&query, UDF_MODULE, "sum_test_bin", NULL);

	LOG("executing map-reduce query: where test-bin = 1 ... 10");

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		cleanup(&as);
		exit(-1);
	}

	LOG("map-reduce query executed");

	// Reuse the as_query object for another query.
	as_query_destroy(&query);
	as_query_init(&query, g_namespace, g_set);

	// Generate an as_query.where condition.
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin", as_integer_range(1, 10));

	// Specify another UDF to use on the resulting stream. Like the previous UDF
	// it sums the test-bin values that satisfy the where condition, but does so
	// in a different, more efficient manner (see query_udf.lua).
	as_query_apply(&query, UDF_MODULE, "sum_test_bin_2", NULL);

	LOG("executing aggregate-reduce query: where test-bin = 1 ... 10");

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		cleanup(&as);
		exit(-1);
	}

	LOG("aggregate-reduce query executed");

	// Reuse the as_query object for another query.
	as_query_destroy(&query);
	as_query_init(&query, g_namespace, g_set);

	// Generate an as_query.where condition.
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin", as_integer_range(1, 10));

	// Specify another UDF to use on the resulting stream. Like the previous
	// UDFs it sums test-bin values that satisfy the where condition, but first
	// applies a filter to sum only even values (see query_udf.lua).
	as_query_apply(&query, UDF_MODULE, "sum_test_bin_even", NULL);

	LOG("executing filter-aggregate-reduce query: where test-bin = 1 ... 10");

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		cleanup(&as);
		exit(-1);
	}

	LOG("filter-aggregate-reduce query executed");

	// Reuse the as_query object for another query.
	as_query_destroy(&query);
	as_query_init(&query, g_namespace, g_set);

	// No as_query.where condition in this case, so we include everything.

	// Specify another UDF to use on the resulting stream. This UDF operates on
	// the numbers-bin (string) values, and demonstrates a case where the value
	// returned by the query callback is an as_map (instead of an as_integer).
	as_query_apply(&query, UDF_MODULE, "count_numbers", NULL);

	LOG("executing numbers aggregate-reduce query: all records");

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb_map, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		cleanup(&as);
		exit(-1);
	}

	LOG("numbers aggregate-reduce query executed");

	as_query_destroy(&query);

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("aggregate query example successfully completed");

	return 0;
}


//==========================================================
// Query Callbacks
//

bool
query_cb(const as_val* p_val, void* udata)
{
	if (! p_val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	// Because of the UDF used, we expect an as_integer to be returned.
	as_integer* p_integer = as_integer_fromval(p_val);

	if (! p_integer) {
		LOG("query callback returned non-as_integer object");
		return true;
	}

	LOG("query callback returned %" PRId64, as_integer_get(p_integer));

	return true;
}

bool
query_cb_map(const as_val* p_val, void* udata)
{
	if (! p_val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	// Because of the UDF used, we expect an as_map to be returned.
	if (! as_map_fromval(p_val)) {
		LOG("query callback returned non-as_map object");
		return true;
	}

	// The map keys are number tokens ("1" to "10") and each value is the total
	// number of occurrences of the token in the records aggregated.
	char* val_as_str = as_val_tostring(p_val);

	LOG("query callback returned %s", val_as_str);
	free(val_as_str);

	return true;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_remove_index(p_as, TEST_INDEX_NAME);
	example_remove_udf(p_as, UDF_FILE_PATH);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	srand((unsigned int)time(0));

	// Create an as_record object with an integer value bin, and a string value
	// bin, where the string is a list of comma-separated numbers. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64() and as_record_set_str().
	as_record rec;
	as_record_inita(&rec, 2);

	// Re-using rec, write records into the database such that each record's key
	// and test-bin value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		// In general it's ok to reset a bin value - all as_record_set_... calls
		// destroy any previous value.
		as_record_set_int64(&rec, "test-bin", (int64_t)i);

		char numbers[(TOKENS_PER_BIN * 3) + 1];
		as_record_set_str(&rec, "numbers-bin", generate_numbers(numbers));

		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}

char*
generate_numbers(char* numbers)
{
	char* p_write = numbers;

	// Generate a comma-separated string of number tokens.
	for (int i = 0; i < TOKENS_PER_BIN; i++) {
		p_write += sprintf(p_write, "%d,", (rand() % MAX_TOKEN) + 1);
	}

	// Truncate the last comma.
	*(p_write - 1) = '\0';

	return numbers;
}
