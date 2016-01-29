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
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

//==========================================================
// Globals
//

static aerospike as;
static as_monitor monitor;
static uint32_t max_commands = 100;
const char TEST_INDEX_NAME[] = "test-bin-index";

//==========================================================
// Forward Declarations
//

void insert_records();
bool insert_record(as_event_loop* event_loop, void* udata, uint32_t index);
void insert_listener(as_error* err, void* udata, as_event_loop* event_loop);
void run_query(as_event_loop* event_loop);
bool query_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop);

//==========================================================
// SIMPLE QUERY Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_MULTI_KEY_OPTS)) {
		exit(-1);
	}

	// Initialize monitor.
	as_monitor_init(&monitor);
	as_monitor_begin(&monitor);

	// Create an asynchronous event loop.
	if (! example_create_event_loop()) {
		return 0;
	}

	// Connect to the aerospike database cluster.
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_records(&as);
	example_remove_index(&as, TEST_INDEX_NAME);

	// Create a numeric secondary index on test-bin.
	if (! example_create_integer_index(&as, "test-bin", TEST_INDEX_NAME)) {
		example_cleanup(&as);
		return -1;
	}

	// Counter can be placed on stack because main() will not end until batch read is finished.
	uint32_t counter = 0;

	// Start inserting records.  Query will be made when insertions are complete.
	insert_records(&counter);

	// Wait till commands have completed before shutting down.
	as_monitor_wait(&monitor);
	
	// Cleanup and shutdown.
	example_remove_test_records(&as);
	example_remove_index(&as, TEST_INDEX_NAME);
	example_cleanup(&as);
	as_event_close_loops();
	return 0;
}

void
insert_records(uint32_t* counter)
{
	// Insert all records on same event loop.
	as_event_loop* event_loop = as_event_loop_get();	
		
	// Put up to max_commands on the async queue at a time.
	int block_size = g_n_keys >= max_commands ? max_commands : g_n_keys;

	for (uint32_t i = 0; i < block_size; i++) {
		if (! insert_record(event_loop, counter, i)) {
			break;
		}
	}
}

bool
insert_record(as_event_loop* event_loop, void* udata, uint32_t index)
{
	// No need to destroy a stack as_key object, if we only use as_key_init_int64().
	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, (int64_t)index);

	// Create an as_record object with one (integer value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_int64().
	as_record rec;
	as_record_inita(&rec, 1);

	// In general it's ok to reset a bin value - all as_record_set_... calls
	// destroy any previous value.
	as_record_set_int64(&rec, "test-bin", (int64_t)index);

	// Write a record to the database.
	as_error err;
	if (aerospike_key_put_async(&as, &err, NULL, &key, &rec, insert_listener, udata, event_loop, NULL) != AEROSPIKE_OK) {
		insert_listener(&err, udata, event_loop);
		return false;
	}
	return true;
}

void
insert_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	uint32_t* counter = udata;

	if (err) {
		LOG("aerospike_key_put_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return;
	}

	// Atomic increment is not necessary since only one event loop is used.
	if (++(*counter) == g_n_keys) {
		// We have reached max number of records.
		LOG("inserted %u keys", *counter);
		run_query(event_loop);
		return;
	}

	// Check if we need to insert more records.
	uint32_t next = *counter + max_commands - 1;

	if (next < g_n_keys) {
		insert_record(event_loop, udata, next);
	}
}

void
run_query(as_event_loop* event_loop)
{
	// Create an as_query object.
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	// Generate an as_query.where condition. Note that as_query_destroy() takes
	// care of destroying all the query's member objects if necessary. However
	// using as_query_where_inita() does avoid internal heap usage.
	as_query_where_inita(&query, 1);
	as_query_where(&query, "test-bin", as_integer_equals(7));

	LOG("executing query: where test-bin = 7");

	// Execute the query.
	as_error err;
	if (aerospike_query_async(&as, &err, NULL, &query, query_listener, NULL, event_loop) != AEROSPIKE_OK) {
		query_listener(&err, NULL, NULL, event_loop);
	}

	as_query_destroy(&query);
}

bool
query_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	if (err) {
		LOG("aerospike_query_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return false;	
	}

	if (! record) {
		LOG("query is complete");
		as_monitor_notify(&monitor);
		return false;
	}

	LOG("query callback returned record:");
	example_dump_record(record);
	return true;
}
