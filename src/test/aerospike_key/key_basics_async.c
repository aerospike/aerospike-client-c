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
#define SET "test_basics"

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct {
	atf_test_result* result;
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

static void
as_get_callback1(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
    assert_int_eq_async(&monitor, as_record_numbins(rec), 1);
    assert_int_eq_async(&monitor, as_record_get_int64(rec, "a", 0), 123);
	as_monitor_notify(&monitor);
}

static void
as_put_callback1(as_error* err, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa1");
	
	as_error e;
	as_status status = aerospike_key_get_async(as, &e, NULL, &key, as_get_callback1, __result__, event_loop, NULL);
	assert_status_async(&monitor, status, &e);
}

TEST(key_basics_async_get, "async get")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa1");

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "a", 123);
		
	as_error err;
	as_status status = aerospike_key_put_async(as, &err, NULL, &key, &rec, as_put_callback1, __result__, 0, NULL);
	as_key_destroy(&key);
	as_record_destroy(&rec);
	
    assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

static void
as_get_callback2(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
    assert_int_eq_async(&monitor, as_record_numbins(rec), 1);
    assert_string_eq_async(&monitor, as_record_get_str(rec, "bbb"), "pa2 value");
	as_monitor_notify(&monitor);
}

static void
as_put_callback2(as_error* err, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa2");
	const char* select[] = {"bbb", NULL};
	
	as_error e;
	as_status status = aerospike_key_select_async(as, &e, NULL, &key, select, as_get_callback2, __result__, event_loop, NULL);
	assert_status_async(&monitor, status, &e);
}

TEST(key_basics_async_select, "async select")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa2");
	
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_strp(&rec, "bbb", "pa2 value", false);
	
	as_error err;
	as_status status = aerospike_key_put_async(as, &err, NULL, &key, &rec, as_put_callback2, __result__, 0, NULL);
	as_key_destroy(&key);
	as_record_destroy(&rec);
	
    assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

static void
as_get_callback_found(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	counter_data* cdata = udata;
	assert_success_async(&monitor, err, cdata->result);
	
    assert_async(&monitor, rec);
    assert_int_eq_async(&monitor, as_record_numbins(rec), 0);
    assert_async(&monitor, rec->gen > 0);
	
	cdata->counter++;
	if (cdata->counter == 2) {
		as_monitor_notify(&monitor);
	}
}

static void
as_get_callback_not_found(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	counter_data* cdata = udata;
	atf_test_result* __result__ = cdata->result;
    assert_async(&monitor, err && err->code == AEROSPIKE_ERR_RECORD_NOT_FOUND);
    assert_async(&monitor, !rec);
	
	cdata->counter++;
	if (cdata->counter == 2) {
		as_monitor_notify(&monitor);
	}
}

static void
as_put_callback3(as_error* err, void* udata, as_event_loop* event_loop)
{
	counter_data* cdata = udata;
	assert_success_async(&monitor, err, cdata->result);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa3");
	
	as_error e;
	as_status status = aerospike_key_exists_async(as, &e, NULL, &key, as_get_callback_found, udata, event_loop, NULL);
	assert_status_async(&monitor, status, &e);
	
	as_key_init(&key, NAMESPACE, SET, "notfound");
	
	status = aerospike_key_exists_async(as, &e, NULL, &key, as_get_callback_not_found, udata, event_loop, NULL);
	assert_status_async(&monitor, status, &e);
}

TEST(key_basics_async_exists, "async exists")
{
	as_monitor_begin(&monitor);
	
	// udata can exist on stack only because this function doesn't exit until the test is completed.
	counter_data udata;
	udata.result = __result__;
	udata.counter = 0;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa3");
	
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "c", 55);
	
	as_error err;
	as_status status = aerospike_key_put_async(as, &err, NULL, &key, &rec, as_put_callback3, &udata, 0, NULL);
	as_key_destroy(&key);
	as_record_destroy(&rec);
	
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

static void
as_remove_callback(as_error* err, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	as_monitor_notify(&monitor);
}

static void
as_put_callback4(as_error* err, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa4");
	
	as_error e;
	as_status status = aerospike_key_remove_async(as, &e, NULL, &key, as_remove_callback, __result__, event_loop, NULL);
	assert_status_async(&monitor, status, &e);
}

TEST(key_basics_async_remove, "async remove")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa4");
	
	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "c", 55);
	
	as_error err;
	as_status status = aerospike_key_put_async(as, &err, NULL, &key, &rec, as_put_callback4, __result__, 0, NULL);
	as_key_destroy(&key);
	as_record_destroy(&rec);
	
    assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

static void
as_operate_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
    assert_async(&monitor, rec);
    assert_int_eq_async(&monitor, as_record_numbins(rec), 2);
    assert_int_eq_async(&monitor, as_record_get_int64(rec, "a", 0), 316);
    assert_string_eq_async(&monitor, as_record_get_str(rec, "b"), "abcmiddef");
	as_monitor_notify(&monitor);
}

static void
as_put_operate_callback(as_error* err, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa5");

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_add_incr(&ops, "a", -5);
	as_operations_add_append_str(&ops, "b", "def");
	as_operations_add_prepend_str(&ops, "b", "abc");
	as_operations_add_read(&ops, "a");
	as_operations_add_read(&ops, "b");
	
	as_error e;
	as_status status = aerospike_key_operate_async(as, &e, NULL, &key, &ops, as_operate_callback, __result__, event_loop, NULL);
	assert_status_async(&monitor, status, &e);

	as_key_destroy(&key);
	as_operations_destroy(&ops);
}

TEST(key_basics_async_operate, "async operate")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "pa5");
	
	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "a", 321);
	as_record_set_strp(&rec, "b", "mid", false);

	as_error err;
	as_status status = aerospike_key_put_async(as, &err, NULL, &key, &rec, as_put_operate_callback, __result__, 0, NULL);
	as_key_destroy(&key);
	
    assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(key_basics_async, "aerospike_key basic tests")
{	
	suite_before(before);
	suite_after(after);

    suite_add(key_basics_async_get);
	suite_add(key_basics_async_select);
	suite_add(key_basics_async_exists);
	suite_add(key_basics_async_remove);
	suite_add(key_basics_async_operate);
}
