/*******************************************************************************
 * Copyright 2008-2025 by Aerospike.
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

void write_record(as_event_loop* event_loop);
void write_listener(as_error* err, void* udata, as_event_loop* event_loop);

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

	write_record(NULL);

	// Wait till commands have completed before shutting down.
	as_monitor_wait(&monitor);

	// Cleanup and shutdown.
	example_cleanup(&as);
	as_event_close_loops();
	return 0;
}

void
write_record(as_event_loop* event_loop)
{
	size_t size = 100 * 1024;
	char* payload = malloc(size);
	memset(payload, 'X', size);
	payload[size-1] = 0;

	as_error err;

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_strp(&rec, "payload", payload, false);

	if (aerospike_key_put_async(&as, &err, NULL, &g_key, &rec, write_listener, payload, event_loop, NULL) != AEROSPIKE_OK) {
		write_listener(&err, NULL, event_loop);
	}
	as_record_destroy(&rec);
}

void
write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	free(udata);

	if (err) {
		LOG("aerospike_key_put_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return;
	}

	LOG("write succeeded");
	as_monitor_notify(&monitor);
}
