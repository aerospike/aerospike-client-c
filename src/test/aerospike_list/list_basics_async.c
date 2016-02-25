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
#define SET "asynclist"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

bool has_cdt_list();

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
pop_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
    assert_async(&monitor, rec);
    assert_int_eq_async(&monitor, as_record_numbins(rec), 1);
	
	as_list* list = as_record_get_list(rec, "list");
    assert_async(&monitor, list);
    assert_int_eq_async(&monitor, as_list_size(list), 1);
    assert_int_eq_async(&monitor, as_list_get_int64(list, 0), 8);
	as_monitor_notify(&monitor);
}

static void
write_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "alkey");
	
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_list_pop_range(&ops, "list", -2, 1);
	
	as_error e;
	as_status status = aerospike_key_operate_async(as, &e, NULL, &key, &ops, pop_callback, __result__, event_loop, NULL);
	as_operations_destroy(&ops);
	assert_status_async(&monitor, status, &e);
}

TEST(async_list_op, "async list operate")
{
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}
	
	as_monitor_begin(&monitor);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "alkey");

	as_arraylist list;
	as_arraylist_inita(&list, 3);
	as_arraylist_append_int64(&list, 7);
	as_arraylist_append_int64(&list, 8);
	as_arraylist_append_int64(&list, 9);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_write(&ops, "list", (as_bin_value*)&list);
	
	as_error err;
	as_status status = aerospike_key_operate_async(as, &err, NULL, &key, &ops, write_callback, __result__, NULL, NULL);
	
	as_arraylist_destroy(&list);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(list_basics_async, "async list tests")
{
	suite_before(before);
	suite_after(after);
	
    suite_add(async_list_op);
}
