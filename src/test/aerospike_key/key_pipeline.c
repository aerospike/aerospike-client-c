/*
 * Copyright 2008-2016 Aerospike, Inc.
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

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "pipe"

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct {
	atf_test_result* __result__;
	uint32_t counter;
} counter_data;

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

static int responses[11];

static void
as_get_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	assert_int_eq_async(&monitor, as_record_numbins(rec), 1);

	int64_t v = as_record_get_int64(rec, "a", -1);
	counter_data* cdata = udata;
	cdata->counter++;
	
	//as_log_trace("Read Response %p %u %" PRIi64, cdata, cdata->counter, v);
	
	if (v < 1 || v > 11) {
		fail_async(&monitor, "Response value invalid %" PRIi64, v);
	}
	
	responses[v-1] = 1;
	
	if (cdata->counter == 11) {
		as_log_info("Pipeline reads complete. Validating.");
		for (int i = 0; i < 11; i++) {
			if (! responses[i]) {
				fail_async(&monitor, "Response value invalid %d", i);
			}
		}
		free(cdata);
		as_monitor_notify(&monitor);
	}
}

static void
as_put_callback2(as_error* err, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	counter_data* cdata = udata;
	cdata->counter++;
	
	if (cdata->counter == 10) {
		// All writes complete, now read and verify all records with pipeline.
		cdata->counter = 0;
				
		as_key key;
		char key_buf[64];

		as_error e;
		as_status status;

		for (int i = 1; i <= 11; i++) {
			//as_log_trace("Read rec %d", i);
			sprintf(key_buf, "pipe%d", i);
			as_key_init(&key, NAMESPACE, SET, key_buf);
			
			status = aerospike_key_get_async(as, &e, NULL, &key, as_get_callback, cdata, event_loop, true);
			assert_status_async(&monitor, status, &e);
		}
	}
}

static void
as_put_callback1(as_error* err, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	// Issue 10 more puts with pipeline set on same event loop.
	counter_data* cdata = malloc(sizeof(counter_data));
	cdata->__result__ = udata;
	cdata->counter = 0;
	
	as_key key;
	char key_buf[64];
	
	as_record rec;
	as_record_inita(&rec, 1);

	as_error e;
	as_status status;

	for (int i = 2; i <= 11; i++) {
		//as_log_trace("Write rec %d", i);
		sprintf(key_buf, "pipe%d", i);
		as_key_init(&key, NAMESPACE, SET, key_buf);
		as_record_set_int64(&rec, "a", i);

		status = aerospike_key_put_async(as, &e, NULL, &key, &rec, as_put_callback2, cdata, event_loop, true);
		assert_status_async(&monitor, status, &e);
	}
}

TEST(key_pipeline_put, "pipeline puts")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pipe1");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 1);
	
	// Execute put with pipeline set on any event loop and wait for callback.
	as_error err;
	as_status status = aerospike_key_put_async(as, &err, NULL, &key, &rec, as_put_callback1, __result__, NULL, true);
    assert_int_eq(status, AEROSPIKE_OK);

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
