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
#include <aerospike/as_monitor.h>
#include <aerospike/as_msgpack_serializer.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_event.h>

#include "../test.h"
#include "../util/udf.h"

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
#define LUA_FILE "src/test/lua/key_apply.lua"
#define UDF_FILE "key_apply"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool
before(atf_suite* suite)
{
	as_monitor_init(&monitor);
	
    if (! udf_put(LUA_FILE)) {
        error("failure while uploading: %s", LUA_FILE);
        return false;
    }
	
    if (! udf_exists(LUA_FILE)) {
        error("lua file does not exist: %s", LUA_FILE);
        return false;
    }
	return true;
}

static bool
after(atf_suite* suite)
{
    if (! udf_remove(LUA_FILE)) {
        error("failure while removing: %s", LUA_FILE);
        return false;
    }
	as_monitor_destroy(&monitor);
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static void
as_apply_callback(as_error* err, as_val* val, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
    assert_async(&monitor, val);
    as_integer* i = as_integer_fromval(val);
    assert_async(&monitor, i);
    assert_int_eq_async(&monitor, as_integer_toint(i), 3);
	as_monitor_notify(&monitor);
}

TEST(key_apply_async1, "async key apply")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "aa1");
	
	as_arraylist arglist;
	as_arraylist_init(&arglist, 3, 0);
	as_arraylist_append_int64(&arglist, 1);
	as_arraylist_append_int64(&arglist, 2);

	as_error err;
	as_status status = aerospike_key_apply_async(as, &err, NULL, &key, UDF_FILE, "add", (as_list *) &arglist, as_apply_callback, __result__, 0, NULL);
	as_key_destroy(&key);
    as_val_destroy(&arglist);
    assert_int_eq(status, AEROSPIKE_OK);

	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(key_apply_async, "aerospike_key apply tests")
{
	suite_before(before);
	suite_after(after);

    suite_add(key_apply_async1);
}
