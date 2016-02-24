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
#include <aerospike/aerospike_query.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_stringmap.h>

#include "../test.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "query_foreach"

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct {
	atf_test_result* result;
	uint32_t counter;
	bool valid;
} query_data;

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
static as_monitor monitor;
static query_data qdata;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

extern bool query_foreach_create();
extern bool query_foreach_destroy();

static bool
before(atf_suite* suite)
{
	as_monitor_init(&monitor);
	return query_foreach_create();
}

static bool
after(atf_suite* suite)
{
	as_monitor_destroy(&monitor);
	return query_foreach_destroy();
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static bool
query_handler(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	query_data* qdata = udata;
	atf_test_result* __result__ = qdata->result;
	
	if (err) {
		fail_async(&monitor, "Error %d: %s", err->code, err->message);
		return false;
	}
	
	if (! record) {
		// Query ended.  Validate.
		if (qdata->counter == 100) {
			info("count: %u", qdata->counter);
			as_monitor_notify(&monitor);
		}
		else {
			fail_async(&monitor, "count: %u != 100", qdata->counter);
		}
		return false;
	}
	
	qdata->counter++;
	return true;
}

TEST(query_async_foreach_1, "count(*) where a == 'abc'")
{
	as_monitor_begin(&monitor);

	as_error err;
	as_error_reset(&err);
	
	as_query q;
	as_query_init(&q, NAMESPACE, SET);
	
	as_query_select_inita(&q, 1);
	as_query_select(&q, "c");
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));
	
	qdata.result = __result__;
	qdata.counter = 0;
	qdata.valid = true;
	
	as_status status = aerospike_query_async(as, &err, NULL, &q, query_handler, &qdata, NULL);
	
	as_query_destroy(&q);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
	qdata.valid = false;
}

static bool
query_quit_early_handler(as_error* err, as_record* record, void* udata, as_event_loop* event_loop)
{
	if (! qdata.valid) {
		error("Query callback called after returning false. Exit test framework.");
		exit(-1);
	}

	atf_test_result* __result__ = qdata.result;
	
	if (err) {
		fail_async(&monitor, "Error %d: %s", err->code, err->message);
		return false;
	}
	
	qdata.counter++;
	
	if (! record) {
		fail_async(&monitor, "Query should not have ended %u", qdata.counter);
		return false;
	}
	
	qdata.valid = false;
	as_monitor_notify(&monitor);
	return false;
}

TEST(query_async_quit_early, "normal query and quit early")
{
	as_monitor_begin(&monitor);
	
	qdata.result = __result__;
	qdata.counter = 0;
	qdata.valid = true;
		
	as_error err;
	as_error_reset(&err);
		
	as_query q;
	as_query_init(&q, NAMESPACE, SET);
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "a", as_string_equals("abc"));
	
	as_status status = aerospike_query_async(as, &err, NULL, &q, query_quit_early_handler, NULL, NULL);
	
	as_query_destroy(&q);
	
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(query_async, "Query Async Tests")
{
	suite_before(before);
	suite_after(after);

	suite_add(query_async_foreach_1);
	suite_add(query_async_quit_early);
}
