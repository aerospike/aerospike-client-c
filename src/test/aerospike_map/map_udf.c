/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <aerospike/aerospike_udf.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_record.h>
#include <aerospike/as_map.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike *as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "map_udf"

#define LUA_FILE AS_START_DIR "src/test/lua/udf_record.lua"
#define UDF_FILE "udf_record"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( map_udf_pre , "upload udf_record.lua" )
{
	const char *filename = UDF_FILE".lua";

	as_error err;
	as_bytes content;

	info("reading: %s",LUA_FILE);
	bool b = udf_readfile(LUA_FILE, &content);
	assert_true(b);

	info("uploading: %s",filename);
	aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

	assert_int_eq( err.code, AEROSPIKE_OK );

	aerospike_udf_put_wait(as, &err, NULL, filename, 100);

	as_bytes_destroy(&content);
}

TEST( map_udf_post , "remove udf_record.lua" )
{
	const char *filename = UDF_FILE".lua";

	as_error err;

	aerospike_udf_remove(as, &err, NULL, filename);

	assert_int_eq( err.code, AEROSPIKE_OK );

	as_sleep(100);
}

TEST( map_udf_update_map, "udf_record.update_map()" )
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	// Start clean.
	aerospike_key_remove(as, &err, NULL, &key);
	assert_true(err.code == AEROSPIKE_OK || err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	// Create map in a UDF.
	as_arraylist args;
	as_arraylist_init(&args, 2, 0);
	as_arraylist_append_str(&args, "a");
	as_arraylist_append_int64(&args, 2);

	as_val *val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_record", "update_map", (as_list *)&args, &val);

	assert_int_eq( err.code, AEROSPIKE_OK );
	assert_int_eq( as_val_type(val), AS_STRING );

	as_record *rec = NULL;
	aerospike_key_get(as, &err, NULL, &key, &rec);
	//example_dump_record(rec);
	as_map *map = as_record_get_map(rec, "m");
	// As of server 6.1, maps created in UDF will default to being sorted maps.
	// Disable this unsorted map assertion to support new reality.
	// assert_int_eq( map->flags, 0 );
	as_record_destroy(rec);

	as_val_destroy(val);
	val = NULL;

	// Test udf call on key + value ordered map.
	as_operations ops;
	as_map_policy policy;

	as_operations_inita(&ops, 2);
	as_map_policy_set(&policy, AS_MAP_KEY_VALUE_ORDERED, 0);
	as_operations_add_map_set_policy(&ops, "m", &policy);
	as_operations_add_map_put(&ops, "m", &policy, (as_val *)as_string_new_strdup("c"), (as_val *)as_integer_new(100));
	aerospike_key_operate(as, &err, NULL, &key, &ops, NULL);

	assert_int_eq( err.code, AEROSPIKE_OK );
	as_operations_destroy(&ops);

	as_arraylist_set_str(&args, 0, "c");
	aerospike_key_apply(as, &err, NULL, &key, "udf_record", "update_map", (as_list *)&args, &val);

	rec = NULL;
	aerospike_key_get(as, &err, NULL, &key, &rec);
	//example_dump_record(rec);
	map = as_record_get_map(rec, "m");

	assert_int_eq( as_map_size(map), 3 );
	assert_int_eq( map->flags, AS_MAP_KEY_VALUE_ORDERED );
	assert_int_eq( as_stringmap_get_int64(map, "a"), 102 );
	assert_int_eq( as_stringmap_get_int64(map, "b"), 103 );
	assert_int_eq( as_stringmap_get_int64(map, "c"), 104 );

	as_record_destroy(rec);

	as_val_destroy(val);
	as_val_destroy(&args);
	as_key_destroy(&key);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( map_udf, "aerospike_map udf tests" ) {
	suite_add( map_udf_pre );
	suite_add( map_udf_update_map );
	suite_add( map_udf_post );
}
