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
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"

//==========================================================
// Globals
//

static aerospike as;
static as_monitor monitor;

//==========================================================
// Forward Declarations
//

void expect_not_found(as_error* err, as_record* record, void* udata, as_event_loop* event_loop);
void write_record(as_event_loop* event_loop);
void write_listener(as_error* err, void* udata, as_event_loop* event_loop);
void read_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop);

//==========================================================
// GET Example
//

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		return -1;
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
	example_remove_test_record(&as);

	// Try to read the test record from the database. This should fail since the record is not there.
	as_error err;
	if (aerospike_key_get_async(&as, &err, NULL, &g_key, expect_not_found, NULL, NULL, NULL) != AEROSPIKE_OK) {
		LOG("aerospike_key_get_async() returned %d - %s", err.code, err.message);
		example_cleanup(&as);
		as_event_close_loops();
		return -1;
	}

	// Wait till commands have completed before shutting down.
	as_monitor_wait(&monitor);
	
	// Cleanup and shutdown.
	example_cleanup(&as);
	as_event_close_loops();
	return 0;
}

void
expect_not_found(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	as_status status = err? err->code : AEROSPIKE_OK;

	if (status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		LOG("aerospike_key_get() returned %d - %s, expected "
				"AEROSPIKE_ERR_RECORD_NOT_FOUND", err->code, err->message);
		as_monitor_notify(&monitor);
		return;
 	}

	LOG("get (non-existent record) failed as expected");

	// Write a record to the database so we can demonstrate read success.
	write_record(event_loop);
}

void
write_record(as_event_loop* event_loop)
{
	as_error err;

	// Create an as_record object with four bins with different value types. By
	// using as_record_inita(), we won't need to destroy the record if we only
	// set bins using as_record_set_int64(), as_record_set_str(), and
	// as_record_set_raw().
	as_record rec;
	as_record_inita(&rec, 4);
	as_record_set_int64(&rec, "test-bin-1", 1111);
	as_record_set_int64(&rec, "test-bin-2", 2222);
	as_record_set_str(&rec, "test-bin-3", "test-bin-3-data");

	static const uint8_t bytes[] = { 1, 2, 3 };
	as_record_set_raw(&rec, "test-bin-4", bytes, 3);

	// Log its contents.
	LOG("as_record object to write to database:");
	example_dump_record(&rec);

	// Write the record to the database.
	if (aerospike_key_put_async(&as, &err, NULL, &g_key, &rec, write_listener, NULL, event_loop, NULL) != AEROSPIKE_OK) {
		write_listener(&err, NULL, event_loop);
	}
	as_record_destroy(&rec);
}

void
write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	if (err) {
		LOG("aerospike_key_put_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return;
	}

	LOG("write succeeded");

	as_error errloc;

	// Read the (whole) test record from the database.
	if (aerospike_key_get_async(&as, &errloc, NULL, &g_key, read_listener, NULL, event_loop, NULL) != AEROSPIKE_OK) {
		read_listener(&errloc, NULL, NULL, event_loop);
	}
}

void
read_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	if (err) {
		LOG("aerospike_key_get_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return;
	}

	// Log the result and recycle the as_record object.
	LOG("record was successfully read from database:");
	example_dump_record(record);
	as_monitor_notify(&monitor);
}
