/*******************************************************************************
 * Copyright 2008-2024 by Aerospike.
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

//==========================================================
// Forward Declarations
//

bool create_event_loop_with_delay_queue();
void insert_records(uint32_t* counter);
bool insert_record(as_event_loop* event_loop, void* udata, uint32_t index);
void insert_listener(as_error* err, void* udata, as_event_loop* event_loop);

//==========================================================
// Async Delay Queue Example
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

	if (! create_event_loop_with_delay_queue()) {
		return 0;
	}

	// Connect to the aerospike database cluster.
	example_connect_to_aerospike(&as);

	// Start clean.
	example_remove_test_records(&as);

	// Counter can be placed on stack because main() will not end until writes are finished.
	uint32_t counter = 0;
	
	// Start inserting records.
	insert_records(&counter);

	// Wait till commands have completed before shutting down.
	as_monitor_wait(&monitor);
	
	// Cleanup and shutdown.
	example_remove_test_records(&as);
	example_cleanup(&as);
	as_event_close_loops();
	return 0;
}

bool
create_event_loop_with_delay_queue()
{
#if AS_EVENT_LIB_DEFINED
	as_policy_event p;
	as_policy_event_init(&p);

	// Default g_n_keys written is 20 and the event loop will be limited to
	// processing 10 commands at a time.
	p.max_commands_in_process = 10;

	// Place a hard limit on delay queue.  If this limit is reached, the command
	// will be rejected (not placed on delay queue).
	p.max_commands_in_queue = 5000;

	as_error err;

	if (as_create_event_loops(&err, &p, 1, NULL) == AEROSPIKE_OK) {
		return true;
	}
	LOG("Failed to create event loop: %s", err.message);
	return false;
#endif
	LOG("Event library not defined. Skip async example.");
	return false;
}

void
insert_records(uint32_t* counter)
{
	// Write all g_n_keys at once on the same event loop. Commands that can't be executed
	// immediately will be placed on the delay queue.
	//
	// Note that g_n_keys can't be infinite because the delay queue would run out of memory.
	// The delay queue is good for managing socket usage for short bursts of commands.
	// A sustained imbalance of commands placed on the queue over the command processing rate
	// will result in the delay queue becoming excessively large.
	as_event_loop* event_loop = as_event_loop_get();
		
	for (uint32_t i = 0; i < g_n_keys; i++) {
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
		as_monitor_notify(&monitor);
		return;
	}
}
