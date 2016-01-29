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
#include <inttypes.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"

//==========================================================
// Globals
//

static aerospike as;
static as_monitor monitor;
static uint32_t max_commands = 100;

//==========================================================
// Forward Declarations
//

void insert_records();
bool insert_record(as_event_loop* event_loop, void* udata, uint32_t index);
void insert_listener(as_error* err, void* udata, as_event_loop* event_loop);
void batch_read(as_event_loop* event_loop);
void batch_listener(as_error* err, as_batch_read_records* records, void* udata, as_event_loop* event_loop);

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

	// Counter can be placed on stack because main() will not end until batch read is finished.
	uint32_t counter = 0;
	
	// Start inserting records.  Batch read will be made when insertions are complete.
	insert_records(&counter);

	// Wait till commands have completed before shutting down.
	as_monitor_wait(&monitor);
	
	// Cleanup and shutdown.
	example_remove_test_records(&as);
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
		batch_read(event_loop);
		return;
	}

	// Check if we need to insert more records.
	uint32_t next = *counter + max_commands - 1;

	if (next < g_n_keys) {
		insert_record(event_loop, udata, next);
	}
}

void
batch_read(as_event_loop* event_loop)
{
	// Make a batch of all the keys we inserted.
	as_batch_read_records* records = as_batch_read_create(g_n_keys);

	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_batch_read_record* record = as_batch_read_reserve(records);
		as_key_init_int64(&record->key, g_namespace, g_set, (int64_t)i);
		record->read_all_bins = true;
	}

	// Read these keys.
	as_error err;
	if (aerospike_batch_read_async(&as, &err, NULL, records, batch_listener, NULL, event_loop) != AEROSPIKE_OK) {
		batch_listener(&err, records, NULL, event_loop);
	}
}

void
batch_listener(as_error* err, as_batch_read_records* records, void* udata, as_event_loop* event_loop)
{
	if (err) {
		LOG("aerospike_batch_read_async() returned %d - %s", err->code, err->message);
		as_batch_read_destroy(records);
		as_monitor_notify(&monitor);
		return;	
	}

	as_vector* list = &records->list;
	LOG("batch read callback returned %u/%u record results:", list->size, g_n_keys);

	uint32_t n_found = 0;

	for (uint32_t i = 0; i < list->size; i++) {
		as_batch_read_record* record = as_vector_get(list, i);

		LOG("index %u, key %" PRId64 ":", i, as_integer_getorelse((as_integer*)record->key.valuep, -1));

		if (record->result == AEROSPIKE_OK) {
			LOG("  AEROSPIKE_OK");
			example_dump_record(&record->record);
			n_found++;
		}
		else if (record->result == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			// The transaction succeeded but the record doesn't exist.
			LOG("  AEROSPIKE_ERR_RECORD_NOT_FOUND");
		}
		else {
			// The transaction didn't succeed.
			LOG("  error %d", record->result);
		}
	}

	LOG("... found %u/%u records", n_found, list->size);
	as_batch_read_destroy(records);
	as_monitor_notify(&monitor);	
}
