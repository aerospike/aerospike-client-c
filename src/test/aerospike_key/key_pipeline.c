/*
 * Copyright 2008-2018 Aerospike, Inc.
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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_map.h>
#include <aerospike/as_msgpack_serializer.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_event.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
static as_monitor monitor;
static int responses[10];

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "pipe"

#define set_error_message(result, fmt, ...) \
	if (! result->message[0]) {\
		atf_assert_log(result, "", __FILE__, __LINE__, fmt, ##__VA_ARGS__);\
	}

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct {
	atf_test_result* result;
	uint32_t queue_size;  // Maximum records allowed inflight (in async queue).
	uint32_t max;
	uint32_t started;     // Key of next record to write.
	uint32_t completed;
	uint32_t pipe_count;
} counter;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool
before(atf_suite* suite)
{
	as_monitor_init(&monitor);
	return true;
}

static bool
after(atf_suite* suite)
{
	as_monitor_destroy(&monitor);
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static void
stop_pipeline(counter* ctr)
{
	free(ctr);
	as_monitor_notify(&monitor);
}

static bool
has_error(atf_test_result* result)
{
	return result->message[0];
}

static void
set_error(as_error* err, atf_test_result* result)
{
	if (! has_error(result)) {
		atf_assert_log(result, "", __FILE__, __LINE__, "Error %d: %s", err->code, err->message);
	}
}

static void
pipeline_noop(void* udata, as_event_loop* event_loop)
{
}

static void
get_listener(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	counter* ctr = udata;
	ctr->completed++;

	if (err) {
		set_error(err, ctr->result);
	}
	else {
		if (as_record_numbins(rec) == 1) {
			int64_t v = as_record_get_int64(rec, "a", -1);

			if (v < 0 || v >= 10) {
				set_error_message(ctr->result, "Response value invalid %" PRIi64, v);
			}
			else {
				responses[v] = 1;
			}
		}
		else {
			set_error(err, ctr->result);
		}
	}

	if (ctr->completed != ctr->max) {
		return;
	}

	if (! has_error(ctr->result)) {
		as_log_info("Pipeline reads complete. Validating.");
		for (int i = 0; i < 10; i++) {
			if (! responses[i]) {
				set_error_message(ctr->result, "Response value invalid %d", i);
			}
		}
	}

	stop_pipeline(ctr);
}

static void
read_all(as_event_loop* event_loop, counter* ctr)
{
	// Read and verify all records with pipeline.
	ctr->started = 0;
	ctr->completed = 0;

	as_key key;
	char key_buf[64];

	as_policy_read p;
	as_policy_read_init(&p);
	p.base.total_timeout = 1000;
	p.base.socket_timeout = 200;

	as_error e;

	for (uint32_t i = 0; i < ctr->max; i++) {
		as_log_trace("Read rec %d", i);
		sprintf(key_buf, "pipe%d", i);
		as_key_init(&key, NAMESPACE, SET, key_buf);

		if (aerospike_key_get_async(as, &e, &p, &key, get_listener, ctr, event_loop, pipeline_noop) != AEROSPIKE_OK) {
			get_listener(&e, NULL, ctr, event_loop);
		}
	}
}

static bool
write_record(as_event_loop* event_loop, counter* ctr);

static void
write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	counter* ctr = udata;
	ctr->completed++;

	if (err) {
		set_error(err, ctr->result);
	}

	// Stop early if error and no more commands in pipeline.
	if (has_error(ctr->result) && ctr->completed == ctr->started) {
		stop_pipeline(ctr);
		return;
	}

	// Check if written all records.
	if (ctr->completed == ctr->max) {
		// We have written all records.
		// Read those records back.
		read_all(event_loop, ctr);
		return;
	}

	// Check if need to write another record.
	if (ctr->started < ctr->max) {
		write_record(event_loop, ctr);
	}
	else {
		// There's one fewer command in the pipeline.
		ctr->pipe_count--;
	}
}

static void
pipeline_listener(void* udata, as_event_loop* event_loop)
{
	counter* ctr = udata;

	// Check if pipeline has space.
	if (ctr->pipe_count < ctr->queue_size && ctr->started < ctr->max) {
		// Issue another write.
		ctr->pipe_count++;
		write_record(event_loop, ctr);
	}
}

static bool
write_record(as_event_loop* event_loop, counter* ctr)
{
	uint32_t id = ctr->started++;
	char key_buf[64];
	sprintf(key_buf, "pipe%u", id);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, key_buf);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", id);

	as_error err;

	if (aerospike_key_put_async(as, &err, NULL, &key, &rec, write_listener, ctr, event_loop, pipeline_listener) != AEROSPIKE_OK) {
		write_listener(&err, ctr, event_loop);
		return false;
	}
	return true;
}

TEST(key_pipeline_put, "pipeline puts")
{
	as_monitor_begin(&monitor);
	
	counter* ctr = malloc(sizeof(counter));
	memset(ctr, 0, sizeof(counter));
	ctr->result = __result__;
	ctr->queue_size = 100;
	ctr->max = 10;

	// Write a single record to start pipeline.
	// More records will be written in pipeline_listener to fill pipeline queue.
	// A NULL event_loop indicates that an event loop will be chosen round-robin.
	ctr->pipe_count++;
	write_record(NULL, ctr);

	// Wait for tasks to finish.
	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(key_pipeline, "aerospike pipeline tests")
{	
	suite_before(before);
	suite_after(after);

    suite_add(key_pipeline_put);
}
