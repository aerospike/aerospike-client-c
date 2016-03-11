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
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"


//==========================================================
// Forward Declarations
//

bool batch_read_cb(const as_batch_read* results, uint32_t n, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);


//==========================================================
// BATCH GET Example
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

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Make a batch of all the keys we inserted.
	as_batch batch;
	as_batch_inita(&batch, g_n_keys);

	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_key_init_int64(as_batch_keyat(&batch, i), g_namespace, g_set,
				(int64_t)i);
	}

	// Check existence of these keys - they should all be there.
	if (aerospike_batch_exists(&as, &err, NULL, &batch, batch_read_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_batch_exists() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	LOG("batch exists call completed");

	// Get all of these keys - they should all be there.
	if (aerospike_batch_get(&as, &err, NULL, &batch, batch_read_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_batch_get() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	LOG("batch get call completed");

	// Delete some records in the middle.
	uint32_t n_to_delete = g_n_keys / 5;

	if (n_to_delete == 0) {
		n_to_delete = 1;
	}

	uint32_t n_start = g_n_keys / 2;

	if (n_start + n_to_delete > g_n_keys) {
		n_start = 0;
	}

	uint32_t n_end = n_start + n_to_delete;

	for (uint32_t i = n_start; i < n_end; i++) {
		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		if (aerospike_key_remove(&as, &err, NULL, &key) != AEROSPIKE_OK) {
			LOG("aerospike_key_remove() returned %d - %s", err.code,
					err.message);
			cleanup(&as);
			exit(-1);
		}
	}

	LOG("deleted records %u ... %u", n_start, n_end - 1);

	// Check existence of these keys - some should not be found.
	if (aerospike_batch_exists(&as, &err, NULL, &batch, batch_read_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_batch_exists() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	LOG("second batch exists call completed");

	// Get all of these keys - some should not be found.
	if (aerospike_batch_get(&as, &err, NULL, &batch, batch_read_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_batch_get() returned %d - %s", err.code, err.message);
		cleanup(&as);
		exit(-1);
	}

	LOG("second batch get call completed");

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	LOG("batch get example successfully completed");

	return 0;
}


//==========================================================
// Batch Callback
//

bool
batch_read_cb(const as_batch_read* results, uint32_t n, void* udata)
{
	LOG("batch read callback returned %u/%u record results:", n, g_n_keys);

	uint32_t n_found = 0;

	for (uint32_t i = 0; i < n; i++) {
		LOG("index %u, key %" PRId64 ":", i,
				as_integer_getorelse((as_integer*)results[i].key->valuep, -1));

		if (results[i].result == AEROSPIKE_OK) {
			LOG("  AEROSPIKE_OK");
			// For aerospike_batch_exists() calls, there should be record
			// metadata but no bins.
			example_dump_record(&results[i].record);
			n_found++;
		}
		else if (results[i].result == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			// The transaction succeeded but the record doesn't exist.
			LOG("  AEROSPIKE_ERR_RECORD_NOT_FOUND");
		}
		else {
			// The transaction didn't succeed.
			LOG("  error %d", results[i].result);
		}
	}

	LOG("... found %u/%u records", n_found, n);

	return true;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
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
