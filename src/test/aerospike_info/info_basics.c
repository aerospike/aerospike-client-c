/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/aerospike_info.h>

#include <aerospike/as_node.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../aerospike_test.h"

//---------------------------------
// Globals
//---------------------------------

extern aerospike * as;

//---------------------------------
// Types
//---------------------------------

struct info_data_s {
	char* 	actual;
	uint8_t matches;
	uint8_t count;
};

typedef struct info_data_s info_data;

//---------------------------------
// Static Functions
//---------------------------------

static bool
info_compare(const as_error* err, const as_node* node, const char* req, char* res, void* udata)
{
	info_data * data = (info_data *) udata;
	
	// count results
	data->count++;

	// if actual is NULL, then set it
	if ( data->actual == NULL ) {
		data->actual = strdup(res);
		data->matches++;
	}
	else {
		// else compare it
		data->matches += strcmp(data->actual, res) == 0 ? 1 : 0;
	}

	return true;
}

//---------------------------------
// Tests
//---------------------------------

TEST(info_basics_help, "help")
{
	as_error err;
	as_error_reset(&err);

	info_data data = {
		.actual = NULL,
		.matches = 0,
		.count = 0
	};

	as_status rc = AEROSPIKE_OK;

	rc = aerospike_info_foreach(as, &err, NULL, "help", info_compare, &data);
	
	assert_int_eq(rc, AEROSPIKE_OK);
	assert(data.count > 0);
	assert(data.matches > 0);
	assert(data.count == data.matches);

	char* res = NULL;

	rc = aerospike_info_any(as, &err, NULL, "help", &res);
	
	assert_not_null(res);
	assert_string_eq(res, data.actual);

	free(res);
	res = NULL;

	if (data.actual) {
		free(data.actual);
	}
}

TEST(info_basics_features, "build")
{
	as_error err;
	as_error_reset(&err);

	info_data data = {
		.actual = NULL,
		.matches = 0,
		.count = 0
	};

	as_status rc = AEROSPIKE_OK;

	rc = aerospike_info_foreach(as, &err, NULL, "build", info_compare, &data);

	assert_int_eq(rc, AEROSPIKE_OK);
	assert(data.count > 0);
	assert(data.matches > 0);
	assert_int_eq(data.count, data.matches);

	char* res = NULL;

	rc = aerospike_info_any(as, &err, NULL, "build", &res);
	
	assert_not_null(res);
	assert_string_eq(res, data.actual);

	free(res);
	res = NULL;
	
	if (data.actual) {
		free(data.actual);
	}
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(info_basics, "aerospike_info basic tests")
{
	suite_add(info_basics_help);
	suite_add(info_basics_features);
}
