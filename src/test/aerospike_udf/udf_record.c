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
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_double.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/as_udf.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "query_bg"

#define LUA_FILE AS_START_DIR "src/test/lua/udf_record.lua"
#define UDF_FILE "udf_record"

static char bin1[] = "bin1";

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static bool
before(atf_suite* suite)
{
	const char * filename = UDF_FILE".lua";

	as_error err;
	as_bytes content;

	info("reading: %s",LUA_FILE);
	bool b = udf_readfile(LUA_FILE, &content);

	if (! b) {
		return false;
	}

	info("uploading: %s",filename);
	aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

	if (err.code != AEROSPIKE_OK) {
		return false;
	}

	aerospike_udf_put_wait(as, &err, NULL, filename, 100);
	as_bytes_destroy(&content);

	for (int i = 20000; i <= 20003; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, i);

		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, bin1, i);

		as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);

		if (status != AEROSPIKE_OK) {
			info("error(%d): %s", err.code, err.message);
			as_record_destroy(&rec);
			return false;
		}
		as_record_destroy(&rec);
	}
	return true;
}

static bool
after(atf_suite* suite)
{
	const char* filename = UDF_FILE".lua";
	as_error err;

	aerospike_udf_remove(as, &err, NULL, filename);

	if (err.code != AEROSPIKE_OK) {
		return false;
	}
	as_sleep(100);
	return true;
}

TEST(udf_record_update_map, "udf_record.update_map()")
{
	as_error err;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "test");

	as_arraylist args;
	as_arraylist_init(&args,2,0);
	as_arraylist_append_str(&args, "a");
	as_arraylist_append_int64(&args, 2);

	as_val* val = NULL;

	aerospike_key_apply(as, &err, NULL, &key, "udf_record", "update_map", (as_list*) &args, &val);

	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(as_val_type(val), AS_STRING);

	char* s = as_val_tostring(val);
	info(s);
	free(s);

	as_arraylist_destroy(&args);
	as_val_destroy(val);
	as_key_destroy(&key);
}

static bool
result_cb(const as_batch_result* results, uint32_t n, void* udata)
{
	uint32_t* errors = udata;

	for (uint32_t i = 0; i < n; i++) {
		const as_batch_result* r = &results[i];

		if (r->result == AEROSPIKE_OK) {
			char* v = as_record_get_str(&r->record, "B5");

			if (strcmp(v, "value5") != 0) {
				(*errors)++;
			}
		}
		else {
			(*errors)++;
		}
	}
	return true;
}

TEST(batch_udf, "Batch UDF Apply")
{
	as_error err;
	as_status status;

	// Define keys
	as_batch batch;
	as_batch_inita(&batch, 2);
	as_key_init_int64(as_batch_keyat(&batch, 0), NAMESPACE, SET, 20000);
	as_key_init_int64(as_batch_keyat(&batch, 1), NAMESPACE, SET, 20001);

	// Delete keys
	status = aerospike_batch_remove(as, &err, NULL, NULL, &batch, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_OK);

	// Apply UDF
	as_arraylist args;
	as_arraylist_init(&args, 2, 0);
	as_arraylist_append_str(&args, "B5");
	as_arraylist_append_str(&args, "value5");

	status = aerospike_batch_apply(as, &err, NULL, NULL, &batch, "udf_record", "write_bin",
		(as_list*)&args, NULL, NULL);

	as_arraylist_destroy(&args);
	assert_int_eq(status, AEROSPIKE_OK);

	// Validate records
	uint32_t errors = 0;
	status = aerospike_batch_get(as, &err, NULL, &batch, result_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);
}

static bool
invalid_cb(const as_batch_result* results, uint32_t n, void* udata)
{
	uint32_t* errors = udata;

	for (uint32_t i = 0; i < n; i++) {
		const as_batch_result* r = &results[i];

		if (r->result == AEROSPIKE_ERR_UDF) {
			char* v = as_record_get_udf_error(&r->record);

			if (!v) {
				(*errors)++;
				continue;
			}

			size_t len = strlen(v);
			char e[] = "Invalid value";
			size_t s = sizeof(e);

			if (s >= len) {
				(*errors)++;
				continue;
			}

			char* p = v + len - s + 1;

			if (strcmp(p, e) != 0) {
				(*errors)++;
			}
		}
		else {
			(*errors)++;
		}
	}
	return true;
}

TEST(batch_udf_error, "Batch UDF Error")
{
	as_error err;
	as_status status;

	// Define keys
	as_batch batch;
	as_batch_inita(&batch, 2);
	as_key_init_int64(as_batch_keyat(&batch, 0), NAMESPACE, SET, 20002);
	as_key_init_int64(as_batch_keyat(&batch, 1), NAMESPACE, SET, 20003);

	// Delete keys
	status = aerospike_batch_remove(as, &err, NULL, NULL, &batch, NULL, NULL);
	assert_int_eq(status, AEROSPIKE_OK);

	// Apply UDF
	as_arraylist args;
	as_arraylist_init(&args, 2, 0);
	as_arraylist_append_str(&args, "B5");
	as_arraylist_append_int64(&args, 999);

	uint32_t errors = 0;
	status = aerospike_batch_apply(as, &err, NULL, NULL, &batch, "udf_record", "write_bin_validate",
		(as_list*)&args, invalid_cb, &errors);

	as_arraylist_destroy(&args);
	assert_int_eq(status, AEROSPIKE_BATCH_FAILED);
	assert_int_eq(errors, 0);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(udf_record, "aerospike udf record tests")
{
	suite_before(before);
	suite_after(after);
	suite_add(udf_record_update_map);
	suite_add(batch_udf);
	suite_add(batch_udf_error);
}
