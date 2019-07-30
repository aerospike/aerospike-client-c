/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_map_operations.h>
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
#define SET "asyncmap"
#define BIN_NAME "testmap"

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
top_score_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
    assert_async(&monitor, rec);
    assert_int_eq_async(&monitor, as_record_numbins(rec), 1);
	
	as_bin* results = rec->bins.entries;
	uint32_t i = 0;
	
	as_list* list = &results[i++].valuep->list;
	assert_int_eq_async(&monitor, as_list_size(list), 1 * 2);
	
	const char* name = as_list_get_str(list, 0);
	assert_string_eq_async(&monitor, name, "Jim");

	int64_t score = as_list_get_int64(list, 1);
	assert_int_eq_async(&monitor, score, 98);

	as_monitor_notify(&monitor);
}

static void
write_callback(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	assert_success_async(&monitor, err, udata);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "amkey");

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_map_get_by_rank_range(&ops, BIN_NAME, -1, 1, AS_MAP_RETURN_KEY_VALUE);
		
	as_error e;
	as_status status = aerospike_key_operate_async(as, &e, NULL, &key, &ops, top_score_callback, __result__, event_loop, NULL);
	as_operations_destroy(&ops);
	assert_status_async(&monitor, status, &e);
}

TEST(async_map_op, "async map operate")
{
	as_monitor_begin(&monitor);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "amkey");

	as_map_policy mode;
	as_map_policy_init(&mode);
	
	as_hashmap item_map;
	as_hashmap_init(&item_map, 4);
	as_string  mkey1;
	as_integer mval1;
	as_string_init(&mkey1, "Charlie", false);
	as_integer_init(&mval1, 55);
	as_hashmap_set(&item_map, (as_val*)&mkey1, (as_val*)&mval1);
	as_string  mkey2;
	as_integer mval2;
	as_string_init(&mkey2, "Jim", false);
	as_integer_init(&mval2, 98);
	as_hashmap_set(&item_map, (as_val*)&mkey2, (as_val*)&mval2);
	as_string  mkey3;
	as_integer mval3;
	as_string_init(&mkey3, "John", false);
	as_integer_init(&mval3, 76);
	as_hashmap_set(&item_map, (as_val*)&mkey3, (as_val*)&mval3);
	as_string  mkey4;
	as_integer mval4;
	as_string_init(&mkey4, "Harry", false);
	as_integer_init(&mval4, 82);
	as_hashmap_set(&item_map, (as_val*)&mkey4, (as_val*)&mval4);
	
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_map_put_items(&ops, BIN_NAME, &mode, (as_map*)&item_map);
	
	as_error err;
	as_status status = aerospike_key_operate_async(as, &err, NULL, &key, &ops, write_callback, __result__, NULL, NULL);
	
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(map_basics_async, "async map tests")
{
	suite_before(before);
	suite_after(after);
	
    suite_add(async_map_op);
}
