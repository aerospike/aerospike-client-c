/*******************************************************************************
 * Copyright 2008-2023 by Aerospike.
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
#include <stddef.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"

//---------------------------------
// Types
//---------------------------------

struct counter {
	as_scan* scan;
	uint32_t page;
	uint32_t count;
	uint32_t max; // Only used in terminate/resume
};

//---------------------------------
// Globals
//---------------------------------

static aerospike as;
static as_monitor monitor;
static uint32_t max_commands = 100;

//---------------------------------
// Forward Declarations
//---------------------------------

void insert_records(uint32_t* counter);
bool insert_record(as_event_loop* event_loop, void* udata, uint32_t index);
void insert_listener(as_error* err, void* udata, as_event_loop* event_loop);
void run_scan(as_event_loop* event_loop);
void run_first_page_scan(as_event_loop* event_loop);
void run_page_scan(as_event_loop* event_loop, struct counter* c);
void scan_terminate_with_serialization(as_event_loop* event_loop);
void scan_resume_with_serialization(as_event_loop* event_loop, uint8_t* bytes, uint32_t bytes_size);

//---------------------------------
// Main
//---------------------------------

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

	// Start inserting records.  Scan will be made when insertions are complete.
	insert_records(&counter);

	// Wait till commands have completed before shutting down.
	as_monitor_wait(&monitor);
	
	// Cleanup and shutdown.
	example_remove_test_records(&as);
	example_cleanup(&as);
	as_event_close_loops();
	return 0;
}

//---------------------------------
// Insert Records
//---------------------------------

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
		LOG("Inserted %u keys", *counter);
		run_scan(event_loop);
		return;
	}

	// Check if we need to insert more records.
	uint32_t next = *counter + max_commands - 1;

	if (next < g_n_keys) {
		insert_record(event_loop, udata, next);
	}
}

//---------------------------------
// Async Scan Regular
//---------------------------------

static bool
scan_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	if (err) {
		LOG("aerospike_scan_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return false;	
	}

	if (! record) {
		LOG("Scan is complete");
		run_first_page_scan(event_loop);
		return false;
	}

	LOG("Scan returned record:");
	example_dump_record(record);
	return true;
}

void
run_scan(as_event_loop* event_loop)
{
	// Create an as_scan object.
	as_scan scan;
	as_scan_init(&scan, g_namespace, g_set);

	LOG("Execute scan");

	as_policy_scan p;
	as_policy_scan_init(&p);
	p.base.socket_timeout = 5000;

	// Execute the scan.
	as_error err;
	if (aerospike_scan_async(&as, &err, &p, &scan, NULL, scan_listener, NULL, event_loop)
		!= AEROSPIKE_OK) {
		scan_listener(&err, NULL, NULL, event_loop);
	}

	as_scan_destroy(&scan);
}

//---------------------------------
// Async Scan Page
//---------------------------------

static bool
scan_page_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	struct counter* c = udata;

	if (err) {
		LOG("scan page returned %d - %s", err->code, err->message);
		as_scan_destroy(c->scan);
		cf_free(c);
		as_monitor_notify(&monitor);
		return false;	
	}

	if (! record) {
		LOG("Scan page %u complete: count=%u", c->page, c->count);

		if (c->page < 2) {
			c->count = 0;
			c->page++;
			run_page_scan(event_loop, c);
		}
		else {
			as_scan_destroy(c->scan);
			cf_free(c);

			scan_terminate_with_serialization(event_loop);
		}
		return false;
	}

	c->count++;
	//LOG("Scan returned record:");
	//example_dump_record(record);
	return true;
}

void
run_first_page_scan(as_event_loop* event_loop)
{
	// Must allocate as_scan on heap when paginate is used because
	// the scan's as_partitions_status is stored/written during the
	// scan.
	as_scan* scan = as_scan_new(g_namespace, g_set);
	as_scan_set_paginate(scan, true);

	// Must allocate counter on heap too.
	struct counter* c = cf_malloc(sizeof(struct counter));
	c->scan = scan;
	c->page = 1;
	c->count = 0;

	run_page_scan(event_loop, c);
}

void
run_page_scan(as_event_loop* event_loop, struct counter* c)
{
	LOG("Scan page %u", c->page);

	as_policy_scan p;
	as_policy_scan_init(&p);
	p.base.socket_timeout = 5000;
	p.max_records = 11;

	// Execute the scan.
	as_error err;
	if (aerospike_scan_async(&as, &err, &p, c->scan, NULL, scan_page_listener, c, event_loop)
		!= AEROSPIKE_OK) {
		scan_page_listener(&err, NULL, c, event_loop);
	}
}

//---------------------------------
// Async Scan Terminate/Resume
//---------------------------------

static bool
scan_terminate_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	struct counter* c = udata;

	if (err) {
		LOG("Scan terminate returned %d - %s", err->code, err->message);
		as_scan_destroy(c->scan);
		cf_free(c);
		as_monitor_notify(&monitor);
		return false;	
	}

	if (! record) {
		LOG("Scan terminate unexpectedly complete: count=%u", c->count);
		as_scan_destroy(c->scan);
		cf_free(c);
		as_monitor_notify(&monitor);
		return false;
	}

	if (c->count >= c->max) {
		// Since we are terminating the scan here, the scan last digest
		// will not be set and the current record will be returned again
		// if the scan resumes at a later time.
		LOG("Terminate scan after %u records", c->count);
		uint32_t bytes_size = 0;
		uint8_t* bytes = NULL;
		as_scan_to_bytes(c->scan, &bytes, &bytes_size);
		as_scan_destroy(c->scan);
		cf_free(c);

		LOG("Resume scan");
		scan_resume_with_serialization(event_loop, bytes, bytes_size);
		return false;
	}

	c->count++;
	//LOG("Scan returned record:");
	//example_dump_record(record);
	return true;
}

void
scan_terminate_with_serialization(as_event_loop* event_loop)
{
	LOG("Scan to be terminated");

	// Must allocate as_scan on heap when paginate is used.
	as_scan* scan = as_scan_new(g_namespace, g_set);
	as_scan_set_paginate(scan, true);

	// Must allocate counter on heap too.
	struct counter* c = cf_malloc(sizeof(struct counter));
	c->scan = scan;
	c->page = 1;
	c->count = 0;
	c->max = 11;

	// Execute the scan.
	as_error err;

	if (aerospike_scan_async(&as, &err, NULL, c->scan, NULL, scan_terminate_listener, c, event_loop)
		!= AEROSPIKE_OK) {
		scan_terminate_listener(&err, NULL, NULL, event_loop);
	}
}

static bool
scan_resume_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	struct counter* c = udata;

	if (err) {
		LOG("Scan resume returned %d - %s", err->code, err->message);
		as_scan_destroy(c->scan);
		cf_free(c);
		as_monitor_notify(&monitor);
		return false;	
	}

	if (! record) {
		LOG("Scan resume complete: count=%u", c->count);
		as_scan_destroy(c->scan);
		cf_free(c);
		as_monitor_notify(&monitor);
		return false;
	}

	c->count++;
	//LOG("Scan returned record:");
	//example_dump_record(record);
	return true;
}

void
scan_resume_with_serialization(as_event_loop* event_loop, uint8_t* bytes, uint32_t bytes_size)
{
	struct counter* c = cf_malloc(sizeof(struct counter));
	c->scan = as_scan_from_bytes_new(bytes, bytes_size);
	c->page = 2;
	c->count = 0;
	c->max = 0;

	cf_free(bytes);

	// Execute the scan.
	as_error err;

	if (aerospike_scan_async(&as, &err, NULL, c->scan, NULL, scan_resume_listener, c, event_loop)
		!= AEROSPIKE_OK) {
		scan_resume_listener(&err, NULL, NULL, event_loop);
	}
}
