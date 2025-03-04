/*
 * Copyright 2008-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <stddef.h>
#include <stdlib.h>

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

void insert_records(uint32_t* counter);
bool insert_record(as_event_loop* event_loop, void* udata, uint32_t index);
void insert_listener(as_error* err, void* udata, as_event_loop* event_loop);
void batch_read(as_event_loop* event_loop);
void batch_read_listener(as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop);
void batch_exists(as_event_loop* event_loop);
void batch_exists_listener(as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop);

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
	
	// Start inserting records. Batch read/exists calls will be made when insertions are complete.
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
	uint32_t block_size = g_n_keys >= max_commands ? max_commands : g_n_keys;

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
	if (aerospike_key_put_async(&as, &err, NULL, &key, &rec, insert_listener, udata, event_loop, NULL)
		!= AEROSPIKE_OK) {
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
	as_batch_records* records = as_batch_records_create(g_n_keys);

	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_batch_read_record* record = as_batch_read_reserve(records);
		as_key_init_int64(&record->key, g_namespace, g_set, (int64_t)i);
		record->read_all_bins = true;
	}

	// Read these keys.
	as_error err;
	if (aerospike_batch_read_async(&as, &err, NULL, records, batch_read_listener, NULL, event_loop)
		!= AEROSPIKE_OK) {
		batch_read_listener(&err, records, NULL, event_loop);
	}
}

void
batch_read_listener(as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop)
{
	if (err) {
		LOG("aerospike_batch_read_async() returned %d - %s", err->code, err->message);
		as_batch_records_destroy(records);
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
			// The command succeeded but the record doesn't exist.
			LOG("  AEROSPIKE_ERR_RECORD_NOT_FOUND");
		}
		else {
			// The command didn't succeed.
			LOG("  error %d", record->result);
		}
	}

	LOG("... found %u/%u records", n_found, list->size);
	as_batch_records_destroy(records);

	// Call batch exists example.
	batch_exists(event_loop);
}

void
batch_exists(as_event_loop* event_loop)
{
	// Add 5 keys that will be not found.
	uint32_t max = g_n_keys + 5;

	as_batch_records* records = as_batch_records_create(max);

	for (uint32_t i = 0; i < max; i++) {
		// as_batch_read_reserve() reserves a slot in the batch and initializes all
		// as_batch_read_record fields to zero/false, including n_bin_names and read_all_bins.
		// This default indicates an exists operation should be performed.
		as_batch_read_record* record = as_batch_read_reserve(records);
		as_key_init_int64(&record->key, g_namespace, g_set, i);
	}

	as_error err;

	if (aerospike_batch_read_async(&as, &err, NULL, records, batch_exists_listener, NULL, event_loop)
		!= AEROSPIKE_OK) {
		batch_exists_listener(&err, records, NULL, event_loop);
	}
}

void
batch_exists_listener(as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop)
{
	if (err) {
		LOG("batch_exists() error %d - %s", err->code, err->message);
		as_batch_records_destroy(records);
		as_monitor_notify(&monitor);
		return;
	}

	as_vector* list = &records->list;
	LOG("batch_exists() returned %u results", list->size);

	for (uint32_t i = 0; i < list->size; i++) {
		as_batch_read_record* record = as_vector_get(list, i);

		if (record->result == AEROSPIKE_OK) {
			LOG("exists[%u]=true", i);
		}
		else if (record->result == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			LOG("exists[%u]=false", i);
		}
		else {
			LOG("exists[%u]=error %d", i, record->result);
		}
	}
	as_batch_records_destroy(records);
	as_monitor_notify(&monitor);	
}
