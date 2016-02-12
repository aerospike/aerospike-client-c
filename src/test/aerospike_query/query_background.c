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
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_job.h>
#include <aerospike/as_list.h>
#include <aerospike/as_query.h>
#include <aerospike/as_map.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>
#include <aerospike/mod_lua.h>

#include "../test.h"
#include "../util/udf.h"
#include "../util/consumer_stream.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
static bool server_has_double = false;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/query_background.lua"
#define UDF_FILE "query_background"

#define NAMESPACE "test"
#define SET "test_query"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool
before(atf_suite * suite)
{
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
after(atf_suite * suite)
{
	as_error err;
	as_error_reset(&err);

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "qeindex9");
	if ( err.code != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}

	if (! udf_remove(LUA_FILE)) {
		error("failure while removing: %s", LUA_FILE);
		return false;
	}
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(query_background_create, "create records and indices")
{
	as_error err;
	as_error_reset(&err);

	as_index_task task;
	as_status status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "qebin1", "qeindex9", AS_INDEX_NUMERIC);

	if (status != AEROSPIKE_OK) {
		error("error(%d): %s", err.code, err.message);
	}
	
	aerospike_index_create_wait(&err, &task, 0);

	as_key key;
	char keystr[64];
		
	for (int i = 1; i <= 10; i++) {
		sprintf(keystr, "qekey%d", i);
		as_key_init(&key, NAMESPACE, SET, keystr);

		as_record r;
		as_record_init(&r, 2);
		as_record_set_int64(&r, "qebin1", i);
		as_record_set_int64(&r, "qebin2", i);

		status = aerospike_key_put(as, &err, NULL, &key, &r);
		assert_int_eq(status, AEROSPIKE_OK);

		as_record_destroy(&r);
	}
}

TEST(query_background1, "query background1")
{
	as_error err;
	as_error_reset(&err);
	
	as_query q;
	as_query_init(&q, NAMESPACE, SET);
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "qebin1", as_integer_range(3, 9));
	
	as_arraylist args;
	as_arraylist_inita(&args, 3);
	as_string s1;
	as_string_init(&s1, "qebin1", false);
	as_arraylist_append_string(&args, &s1);
	as_string s2;
	as_string_init(&s2, "qebin2", false);
	as_arraylist_append_string(&args, &s2);
	as_arraylist_append_int64(&args, 100);
	
	as_query_apply(&q, UDF_FILE, "process_test_rec", (as_list*)&args);
	
	uint64_t query_id = 0;
	
	if (aerospike_query_background(as, &err, NULL, &q, &query_id) == AEROSPIKE_OK) {
		aerospike_query_wait(as, &err, NULL, &q, query_id, 0);
	}
	else {
		error("%s (%d) [%s:%d]", err.message, err.code, err.file, err.line);
	}
	
	assert_int_eq(err.code, 0);
	as_query_destroy(&q);
}

static int expected_list[10] = {1,2,3,104,5,106,7,108,-1,10};

typedef struct qdata_s {
	int count;
	bool abort;
} qdata;

static bool
as_query_callback(const as_val* v, void* udata)
{
	if (! v) {
		return true;
	}
	qdata* data = udata;
	data->count++;
	
	as_record* rec = (as_record*)v;
	int v1 = (int)as_record_get_int64(rec, "qebin1", 0);
	int v2 = (int)as_record_get_int64(rec, "qebin2", 0);
	//info("v1=%d v2=%d", v1, v2);

	if (v1 == 9) {
		error("Data mismatch. value1 %d should not exist", v1);
		data->abort = true;
		return false;
	}
	
	if (v1 == 5) {
		if (v2 != 0) {
			error("Data mismatch. value2 %d should be null", v2);
			data->abort = true;
			return false;
		}
	}
	else if (v2 == 0) {
		error("v2 should not be zero");
		data->abort = true;
		return false;
	}
	else if (v1 != expected_list[v2-1]) {
		error("Data mismatch. Expected %d Received %d", expected_list[v2-1], v1);
		data->abort = true;
		return false;
	}
	return true;
}

TEST(query_validate1, "query validate1")
{
	as_error err;
	as_error_reset(&err);
	
	as_query q;
	as_query_init(&q, NAMESPACE, SET);
	
	as_query_where_inita(&q, 1);
	as_query_where(&q, "qebin1", as_integer_range(1, 110));
	
	qdata data = {.count = 0, .abort = false};
	aerospike_query_foreach(as, &err, NULL, &q, as_query_callback, &data);
	
	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_false(data.abort);
	assert_int_eq(data.count, 9);
	
	as_query_destroy(&q);
}

bool
as_query_aggr_cb(const as_val* p_val, void* udata)
{
	if (p_val) {
		double * sum = (double *) udata;

		if (as_val_type(p_val) == AS_DOUBLE) {
			as_double* res = as_double_fromval(p_val);
			*sum = res ? res->value : 0;
		} else if (as_val_type(p_val) == AS_INTEGER) {
			as_integer* res = as_integer_fromval(p_val);
			*sum = res ? res->value : 0;
		} else {
			warn("unexpected return type %d",as_val_type(p_val));
		}
	}
	return true;
}

TEST(query_aggregation_double, "query aggregation validate")
{
	as_error  err;
	as_error_reset(&err);

	int n_recs = 1000;
	int start_range = 1, end_range = 99;
	char *int_bin = "a_int_bin";
	char *double_bin = "a_double_bin";

	as_status status;
	as_index_task task;

	//create index on "a_int_bin"
	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, int_bin, "idx_test_a_int_bin", AS_INDEX_NUMERIC);
	if ( status == AEROSPIKE_OK ) {
		aerospike_index_create_wait(&err, &task, 0);
	}
	else {
		info("error(%d): %s", err.code, err.message);
	}

	as_record r;
	as_record_init(&r, 2);

	// insert records
	for ( int i = 1; i <= n_recs; i++ ) {

		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i);

		as_record_set_int64(&r, int_bin, i);
		as_record_set_double(&r, double_bin, i/(double)10);

		aerospike_key_put(as, &err, NULL, &key, &r);

	}

	as_record_destroy(&r);

	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	as_query_where_inita(&q, 1);
	as_query_where(&q, "a_int_bin", as_integer_range(start_range, end_range));

	as_query_apply(&q, UDF_FILE, "sum_bin", NULL);

	double expected_sum = 0;
	double received_sum = 0;
	aerospike_query_foreach(as, &err, NULL, &q, as_query_aggr_cb, &received_sum);

	assert_int_eq(err.code, AEROSPIKE_OK);

	for ( int j = start_range; j <= end_range; j++ ) {
		expected_sum += j/(double)10;
	}

	bool isInboundry = (expected_sum > (received_sum - 0.01)) && (expected_sum < (received_sum + 0.01)) ? true : false;
	assert_true( isInboundry);

	as_query_destroy(&q);

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_a_int_bin");
	if ( err.code != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq( err.code, AEROSPIKE_OK );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(query_background, "aerospike_query_background tests")
{
	server_has_double = aerospike_has_double(as);

	suite_before(before);
	suite_after(after);

	suite_add(query_background_create);
	suite_add(query_background1);
	suite_add(query_validate1);

	if (server_has_double) {
		suite_add( query_aggregation_double );
	}

}
