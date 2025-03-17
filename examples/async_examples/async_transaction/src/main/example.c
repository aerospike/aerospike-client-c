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
#include <aerospike/aerospike_txn.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include "example_utils.h"

//---------------------------------
// Types
//---------------------------------

typedef struct {
	as_event_loop* event_loop;
	as_txn* txn;
} user_data;

//---------------------------------
// Globals
//---------------------------------

static aerospike as;
static as_monitor monitor;

//---------------------------------
// Forward Declarations
//---------------------------------

static void run_transaction(user_data* data);

//---------------------------------
// Main
//---------------------------------

int
main(int argc, char* argv[])
{
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
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

	// Create transaction.
	as_txn* txn = as_txn_create();
	LOG("Initialize transaction: %" PRId64, (int64_t)txn->id);

	user_data* data = malloc(sizeof(user_data));
	data->event_loop = as_event_loop_get();
	data->txn = txn;

	// Run transaction.
	run_transaction(data);

	// Wait till commands have completed before shutting down.
	as_monitor_wait(&monitor);

	// Cleanup and shutdown.
	free(data);
	as_txn_destroy(txn);
	example_cleanup(&as);
	as_event_close_loops();
	return 0;
}

//---------------------------------
// Commit
//---------------------------------

static void
commit_listener(
	as_error* err, as_commit_status status, void* udata, struct as_event_loop* event_loop
	)
{
	if (err) {
		LOG("aerospike_commit_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return;
	}

	LOG("transaction committed");
	as_monitor_notify(&monitor);
}

static void
commmit_txn(user_data* data)
{
	as_error err;
	as_status status = aerospike_commit_async(&as, &err, data->txn, commit_listener, data, data->event_loop);

	if (status != AEROSPIKE_OK) {
		LOG("aerospike_commit_async() returned %d - %s", err.code, err.message);
		as_monitor_notify(&monitor);
	}
}

//---------------------------------
// Abort
//---------------------------------

static void
abort_listener(
	as_error* err, as_abort_status status, void* udata, struct as_event_loop* event_loop
	)
{
	if (err) {
		LOG("aerospike_abort_async() returned %d - %s", err->code, err->message);
		as_monitor_notify(&monitor);
		return;
	}

	LOG("transaction aborted");
	as_monitor_notify(&monitor);
}

static void
abort_txn(user_data* data)
{
	as_error err;
	as_status status = aerospike_abort_async(&as, &err, data->txn, abort_listener, data, data->event_loop);

	if (status != AEROSPIKE_OK) {
		LOG("aerospike_abort_async() returned %d - %s", err.code, err.message);
		as_monitor_notify(&monitor);
	}
}

//---------------------------------
// Remove
//---------------------------------

static void
remove_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	user_data* data = udata;

	if (err && err->code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		LOG("aerospike_key_remove_async() returned %d - %s", err->code, err->message);
		abort_txn(data);
		return;
	}

	commmit_txn(data);
}

static void
run_remove(user_data* data)
{
	LOG("aerospike_key_remove_async()");

	as_policy_remove prem;
	as_policy_remove_copy(&as.config.policies.remove, &prem);
	prem.base.txn = data->txn;
	prem.durable_delete = true;

	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, 3);

	as_error err;
	as_status status = aerospike_key_remove_async(&as, &err, &prem, &key, remove_listener, data, data->event_loop, NULL);

	if (status != AEROSPIKE_OK) {
		LOG("aerospike_key_remove_async() returned %d - %s", err.code, err.message);
		abort_txn(data);
		return;
	}
}

//---------------------------------
// Get
//---------------------------------

static void
get_listener(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	user_data* data = udata;

	if (err && err->code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		LOG("aerospike_key_get_async() returned %d - %s", err->code, err->message);
		abort_txn(data);
		return;
	}

	run_remove(data);
}

static void
run_get(user_data* data)
{
	LOG("aerospike_key_get_async()");

	as_policy_read pr;
	as_policy_read_copy(&as.config.policies.read, &pr);
	pr.base.txn = data->txn;

	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, 3);

	as_error err;
	as_status status = aerospike_key_get_async(&as, &err, &pr, &key, get_listener, data, data->event_loop, NULL);

	if (status != AEROSPIKE_OK) {
		LOG("aerospike_key_get_async() returned %d - %s", err.code, err.message);
		abort_txn(data);
		return;
	}
}

//---------------------------------
// Put
//---------------------------------

static void
put_listener2(as_error* err, void* udata, as_event_loop* event_loop)
{
	user_data* data = udata;

	if (err) {
		LOG("aerospike_key_put_async() returned %d - %s", err->code, err->message);
		abort_txn(data);
		return;
	}

	run_get(data);
}

static void
run_put(user_data* data)
{
	LOG("aerospike_key_put_async()");

	as_policy_write pw;
	as_policy_write_copy(&as.config.policies.write, &pw);
	pw.base.txn = data->txn;

	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, 2);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 5678);

	as_error err;
	as_status status = aerospike_key_put_async(&as, &err, &pw, &key, &rec, put_listener2, data, data->event_loop, NULL);

	if (status != AEROSPIKE_OK) {
		LOG("aerospike_key_put_async() returned %d - %s", err.code, err.message);
		abort_txn(data);
		return;
	}
}

static void
put_listener1(as_error* err, void* udata, as_event_loop* event_loop)
{
	user_data* data = udata;

	if (err) {
		LOG("aerospike_key_put_async() returned %d - %s", err->code, err->message);
		abort_txn(data);
		return;
	}

	// Run another put with a different key.
	run_put(data);
}

static void
run_transaction(user_data* data)
{
	LOG("aerospike_key_put_async()");

	as_policy_write pw;
	as_policy_write_copy(&as.config.policies.write, &pw);
	pw.base.txn = data->txn;

	as_key key;
	as_key_init_int64(&key, g_namespace, g_set, 1);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 1234);

	as_error err;
	as_status status = aerospike_key_put_async(&as, &err, &pw, &key, &rec, put_listener1, data, data->event_loop, NULL);

	if (status != AEROSPIKE_OK) {
		LOG("aerospike_key_put_async() returned %d - %s", err.code, err.message);
		abort_txn(data);
		return;
	}
}
