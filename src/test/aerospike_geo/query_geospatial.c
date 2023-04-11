/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#include <aerospike/as_exp.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_query.h>
#include <aerospike/as_map.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/consumer_stream.h"
#include "../util/index_util.h"
#include "../util/log_helper.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;
static int g_n_keys = 20;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "query_geo"
#define SET2 "query_geo_points"
#define SET3 "query_geo_regions"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool before(atf_suite * suite) {
	as_error err;
	as_error_reset(&err);

	// create index on "a"
	as_index_task task;
	as_status status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "geobin", "idx_test_geo", AS_INDEX_GEO2DSPHERE);
	return index_process_return_code(status, &err, &task);
}

static bool after(atf_suite * suite) {
	as_error err;
	as_error_reset(&err);

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_geo");

	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( invalid_geojson, "various geojson formats supported" ) {
	as_error err;
	as_error_reset(&err);

	as_key key;

	as_record rec;

	char buff[1024];
	as_status rc;

	// Invalid GeoJson - Lower cased 'point'
	as_key_init(&key, NAMESPACE, SET, "lower_point");
	as_record_inita(&rec, 1);
	snprintf(buff, sizeof(buff),
			 "{ \"type\": \"point\", \"coordinates\": [%f, %f] }", 0.0, 0.0);
	as_record_set_geojson_str(&rec, "geobin", buff);
	rc = aerospike_key_put(as, &err, NULL, &key, &rec);

	assert_int_eq( rc, AEROSPIKE_ERR_GEO_INVALID_GEOJSON );
	as_record_destroy(&rec);
	as_key_destroy(&key);

	// Invalid GeoJson - MultiPoint
	as_key_init(&key, NAMESPACE, SET, "mult_point");
	as_record_inita(&rec, 1);
	snprintf(buff, sizeof(buff),
			 "{ \"type\": \"MultiPoint\", \"coordinates\": [ [0.0, 0.0] , [0.0, 0.0] ] }");
	as_record_set_geojson_str(&rec, "geobin", buff);
	rc = aerospike_key_put(as, &err, NULL, &key, &rec);

	assert_int_eq( rc, AEROSPIKE_ERR_GEO_INVALID_GEOJSON );
	as_record_destroy(&rec);
	as_key_destroy(&key);

	// Invalid GeoJson - LineString
	as_key_init(&key, NAMESPACE, SET, "line_string");
	as_record_inita(&rec, 1);
	snprintf(buff, sizeof(buff),
			 "{ \"type\": \"LineString\", \"coordinates\": [ [100.0, 0.0], [101.0, 1.0] ] }");
	as_record_set_geojson_str(&rec, "geobin", buff);
	rc = aerospike_key_put(as, &err, NULL, &key, &rec);

	assert_int_eq( rc, AEROSPIKE_ERR_GEO_INVALID_GEOJSON );
	as_record_destroy(&rec);
	as_key_destroy(&key);

}

TEST( valid_geojson, "valid geojson formats" ) {
	as_error err;
	as_error_reset(&err);

	as_key key;

	as_record rec;

	char buff[1024];
	as_status rc;

	// valid GeoJson - Feature
	as_key_init(&key, NAMESPACE, SET, "feature");
	as_record_inita(&rec, 1);
	snprintf(buff, sizeof(buff),
			 "{ \"type\": \"Feature\", \"geometry\": { \"type\": \"Polygon\", \"coordinates\": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ], [ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ] ] } }");
	as_record_set_geojson_str(&rec, "geobin", buff);
	rc = aerospike_key_put(as, &err, NULL, &key, &rec);

	assert_int_eq( rc, AEROSPIKE_OK );
	as_record_destroy(&rec);
	as_key_destroy(&key);

	// valid GeoJson - Polygon
	as_key_init(&key, NAMESPACE, SET, "polygon");
	as_record_inita(&rec, 1);
	snprintf(buff, sizeof(buff),
			 "{ \"type\": \"Polygon\", \"coordinates\":[ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] }");
	as_record_set_geojson_str(&rec, "geobin", buff);
	rc = aerospike_key_put(as, &err, NULL, &key, &rec);

	assert_int_eq( rc, AEROSPIKE_OK );
	as_record_destroy(&rec);
	as_key_destroy(&key);

}

static bool
insert_points(char const * set)
{
	// Create an as_record object with one (GeoJSON value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_geojson().
	as_record rec;
	as_record_inita(&rec, 1);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (int i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, NAMESPACE, set, (int64_t)i);

		double lng = -122 + (0.1 * i);
		double lat = 37.5 + (0.1 * i);
		char buff[1024];
		snprintf(buff, sizeof(buff),
				 "{ \"type\": \"Point\", \"coordinates\": [%f, %f] }", lng, lat);
		as_record_set_geojson_str(&rec, "loc", buff);

		// Write a record to the database.
		if (aerospike_key_put(as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			info("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}
	return true;
}

static void
remove_points(char const * set)
{
	for ( int i = 0; i < g_n_keys; i++ ) {
		as_error err;
		as_key key;
		as_key_init_int64(&key, NAMESPACE, set, (int64_t)i);
		aerospike_key_remove(as, &err, NULL, &key);
	}
}

#define NSTARBUCKS	15

static bool
insert_regions(char const * set)
{
	double starbucks[NSTARBUCKS][2] = {
		{ -122.1708441, 37.4241193 },
		{ -122.1492040, 37.4273569 },
		{ -122.1441078, 37.4268202 },
		{ -122.1251714, 37.4130590 },
		{ -122.0964289, 37.4218102 },
		{ -122.0776641, 37.4158199 },
		{ -122.0943475, 37.4114654 },
		{ -122.1122861, 37.4028493 },
		{ -122.0947230, 37.3909250 },
		{ -122.0831037, 37.3876090 },
		{ -122.0707119, 37.3787855 },
		{ -122.0303178, 37.3882739 },
		{ -122.0464861, 37.3786236 },
		{ -122.0582128, 37.3726980 },
		{ -122.0365083, 37.3676930 }
	};

	// Create an as_record object with one (GeoJSON value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_geojson().
	as_record rec;
	as_record_inita(&rec, 1);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < NSTARBUCKS; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, NAMESPACE, set, (int64_t)i);

		char buff[1024];
		snprintf(buff, sizeof(buff),
				 "{ \"type\": \"AeroCircle\", "
				 "  \"coordinates\": [[%f, %f], 3000.0 ] }",
				 starbucks[i][0], starbucks[i][1]);

		as_record_set_geojson_str(&rec, "loc", buff);

		// Write a record to the database.
		if (aerospike_key_put(as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			info("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}
	return true;
}

static void
remove_regions(char const * set)
{
	for ( int i = 0; i < NSTARBUCKS; i++ ) {
		as_error err;
		as_key key;
		as_key_init_int64(&key, NAMESPACE, set, (int64_t)i);
		aerospike_key_remove(as, &err, NULL, &key);
	}
}

typedef struct filter_points_within_region_udata_s {
	uint64_t count;
	pthread_mutex_t lock;
} filter_points_within_region_udata;

static bool filter_points_within_region_callback(const as_val * v, void * udata)
{
	if (v) {
		// as_record* rec = as_record_fromval(v);
		filter_points_within_region_udata *d = (filter_points_within_region_udata *)udata;
		pthread_mutex_lock(&d->lock);
		d->count++;
		pthread_mutex_unlock(&d->lock);
	}

	return true;
}

TEST( filter_points_within_region, "filter_points_within_region" ) {

	if (! insert_points(SET2)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET2);

	// Our query region:
	char * region =
		"{ "
		"    \"type\": \"Polygon\", "
		"    \"coordinates\": [ "
		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
		"         [-122.500000, 37.000000]] "
		"    ] "
		" } ";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_geo(region)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_points_within_region_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_points_within_region_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_OK );

	assert_int_eq( udata.count, 6 );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_points(SET2);
}

TEST( filter_pir_rchild_wrong_type, "filter_pir_rchild_wrong_type" ) {

	if (! insert_points(SET2)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET2);

	// Our query region:
	char* region =
		"{ "
		"    \"type\": \"Polygon\", "
		"    \"coordinates\": [ "
		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
		"         [-122.500000, 37.000000]] "
		"    ] "
		" } ";

	as_exp_build(filter,
		as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_str(region)));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_points_within_region_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_points_within_region_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_ERR_REQUEST_INVALID );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_points(SET2);
}

TEST( filter_pir_lchild_wrong_type, "filter_pir_lchild_wrong_type" ) {

	if (! insert_points(SET2)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET2);

	// Our query region:
	char* region =
		"{ "
		"    \"type\": \"Polygon\", "
		"    \"coordinates\": [ "
		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
		"         [-122.500000, 37.000000]] "
		"    ] "
		" } ";

	as_exp_build(filter,
		as_exp_cmp_geo(as_exp_str(region), as_exp_bin_geo("loc")));

	as_policy_query p;
	as_policy_query_init(&p);
	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_points_within_region_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_points_within_region_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_ERR_REQUEST_INVALID );

	as_exp_destroy(filter);
	as_query_destroy(&query);
	remove_points(SET2);
}

TEST( filter_pir_rchild_not_immed, "filter_pir_rchild_not_immed" ) {

	if (! insert_points(SET2)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET2);

	as_exp_build(filter,
		as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_bin_geo("loc")));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_points_within_region_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_points_within_region_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_OK ); // allowed in filter2

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_points(SET2);
}

TEST( filter_pir_parse_failed, "filter_pir_parse_failed" ) {

	if (! insert_points(SET2)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET2);

	// Our query region:
	char * region =
		"{ "
		"    \"type\": \"XPolygon\", "		/* !Polygon */
		"    \"coordinates\": [ "
		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
		"         [-122.500000, 37.000000]] "
		"    ] "
		" } ";

	as_exp_build(filter,
		as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_geo(region)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_points_within_region_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_points_within_region_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_ERR_REQUEST_INVALID );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_points(SET2);
}

TEST( filter_pir_on_regions, "filter_pir_on_regions" ) {

	if (! insert_regions(SET2)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET2);

	// Our query region:
	char * region =
		"{ "
		"    \"type\": \"Polygon\", "
		"    \"coordinates\": [ "
		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
		"         [-122.500000, 37.000000]] "
		"    ] "
		" } ";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_geo(region)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_points_within_region_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_points_within_region_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_OK );

	// Should succeed but match nothing.
	assert_int_eq( udata.count, 0 );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_regions(SET2);
}

TEST( filter_geojson_crash_aer_5650, "filter_geojson_crash_aer_5650" ) {

	if (! insert_points(SET2)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET2);

	// Our query region:
	char * region =
		"{ "
		"    \"type\": \"AeroCircle\", "
		"    \"coordinates\": [[-122.0, 37.5], 50000.0] }";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_geo(region)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_points_within_region_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_points_within_region_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_OK );

	assert_int_eq( udata.count, 4 );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_points(SET2);
}

typedef struct filter_regions_containing_point_udata_s {
	uint64_t count;
	pthread_mutex_t lock;
} filter_regions_containing_point_udata;

static bool filter_regions_containing_point_callback(const as_val * v, void * udata) {
	if (v) {
		// as_record* rec = as_record_fromval(v);
		filter_regions_containing_point_udata *d = (filter_regions_containing_point_udata *)udata;
		pthread_mutex_lock(&d->lock);
		d->count++;
		pthread_mutex_unlock(&d->lock);
	}

	return true;
}

TEST( filter_regions_containing_point, "filter_regions_containing_point" ) {

	if (! insert_regions(SET3)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET3);

	// Our query point:
	char * point =
		"{ "
		"    \"type\": \"Point\", "
		"    \"coordinates\": [ -122.0986857, 37.4214209 ] "
		"} ";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_geo(point)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_regions_containing_point_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_regions_containing_point_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_OK );

	// Look at ./starbucks_3k.png for insight that 5 is the correct answer.
	assert_int_eq( udata.count, 5 );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_regions(SET3);
}

TEST( filter_rcp_rchild_wrong_type, "filter_rcp_rchild_wrong_type" ) {

	if (! insert_regions(SET3)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET3);

	// Our query point:
	char * point =
		"{ "
		"    \"type\": \"Point\", "
		"    \"coordinates\": [ -122.0986857, 37.4214209 ] "
		"} ";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_str(point)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_regions_containing_point_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_regions_containing_point_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_ERR_REQUEST_INVALID );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_regions(SET3);
}

TEST( filter_rcp_lchild_wrong_type, "filter_rcp_lchild_wrong_type" ) {

	if (! insert_regions(SET3)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET3);

	// Our query point:
	char * point =
		"{ "
		"    \"type\": \"Point\", "
		"    \"coordinates\": [ -122.0986857, 37.4214209 ] "
		"} ";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_str(point)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_regions_containing_point_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_regions_containing_point_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_ERR_REQUEST_INVALID );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_regions(SET3);
}

TEST( filter_rcp_rchild_not_immed, "filter_rcp_rchild_not_immed" ) {

	if (! insert_regions(SET3)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET3);

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_bin_geo("loc")));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_regions_containing_point_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_regions_containing_point_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_OK ); // allowed in filter2

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_regions(SET3);
}

TEST( filter_rcp_parse_failed, "filter_rcp_parse_failed" ) {

	if (! insert_regions(SET3)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET3);

	// Our query point:
	char * point =
		"{ "
		"    \"type\": \"XPoint\", "	/* !Point */
		"    \"coordinates\": [ -122.0986857, 37.4214209 ] "
		"} ";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_geo(point)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_regions_containing_point_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_regions_containing_point_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_ERR_REQUEST_INVALID );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_regions(SET3);
}

TEST( filter_rcp_on_points, "filter_rcp_on_points" ) {

	if (! insert_points(SET3)) {
		assert_true(false);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, NAMESPACE, SET3);

	// Our query point:
	char * point =
		"{ "
		"    \"type\": \"Point\", "
		"    \"coordinates\": [ -122.0986857, 37.4214209 ] "
		"} ";

	as_exp_build(filter,
			as_exp_cmp_geo(as_exp_bin_geo("loc"), as_exp_geo(point)));

	as_policy_query p;

	as_policy_query_init(&p);

	p.base.filter_exp = filter;

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	filter_regions_containing_point_udata udata = { 0, PTHREAD_MUTEX_INITIALIZER };
	aerospike_query_foreach(as, &err, &p, &query,
			filter_regions_containing_point_callback, &udata);
	assert_int_eq( err.code, AEROSPIKE_OK );

	// Should succeed but match nothing.
	assert_int_eq( udata.count, 0 );

	as_exp_destroy(filter);
	as_query_destroy(&query);

	remove_points(SET3);
}

typedef struct foreach_udata_s {
	pthread_mutex_t lock;
	uint64_t count;
	char binname[128];
	as_hashmap *hm;
} foreach_udata;


static bool query_foreach_count_callback(const as_val * v, void * udata) {
	foreach_udata * m = (foreach_udata *) udata;
	if ( v == NULL ) {
		pthread_mutex_lock(&m->lock);
		uint64_t count = m->count;
		pthread_mutex_unlock(&m->lock);
		info("count: %d", count);
	}
	else {
		// dump_record((as_record *)v);
		char * uniq = as_record_get_str((as_record *)v, m->binname);
		pthread_mutex_lock(&m->lock);
		as_map_set((as_map*)m->hm, (as_val *) as_string_new(strdup(uniq),true), (as_val *) as_integer_new(1));
		m->count++;
		pthread_mutex_unlock(&m->lock);
	}
	return true;
}

TEST( query_geojson_in_list, "IN LIST count(*) where p in <rectangle>" ) {

	// create complex index on a list of geojson regions
	char *index_name = "idx_test_list_p";
	char *indexed_bin_name = "geolistbin";

	as_error err;
	as_error_reset(&err);

	as_index_task task;

	as_status status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET,
			indexed_bin_name, index_name, AS_INDEX_TYPE_LIST, AS_INDEX_GEO2DSPHERE);

	index_process_return_code(status, &err, &task);

	// insert records
	int n_recs = 1000;

	for ( int i = 0; i < n_recs; i++ ) {

		// Make a list of points and regions
		as_arraylist mylist;
		as_arraylist_init(&mylist, 20, 0);


		for ( int jj = 0; jj < 10; ++jj ) {
			//
			// This creates a grid of points:
			// [0.00, 0.00], [0.00, 0.10], ... [0.00, 0.90]
			// [0.01, 0.00], [0.01, 0.10], ... [0.01, 0.90]
			// ...
			// [0.99, 0.00], [0.99, 0.10], ... [0.99, 0.90]
			//
			double plat = 0.0 + (0.01 * i);
			double plng = 0.0 + (0.10 * jj);
			char pntbuf[1024];
			snprintf(pntbuf, sizeof(pntbuf),
					 "{ \"type\": \"Point\", \"coordinates\": [%f, %f] }",
					 plng, plat);

			as_arraylist_append(&mylist, (as_val *) as_geojson_new(strdup(pntbuf), true));

			//
			// This creates a grid of regions centered around the following points
			// [0.00, 0.00], [0.00, -0.10], ... [0.00, -0.90]
			// [0.01, 0.00], [0.01, -0.10], ... [0.01, -0.90]
			// ...
			// [0.99, 0.00], [0.99, -0.10], ... [0.99, -0.90]
			//
			double rlat = 0.0 + (0.01 * i);
			double rlng = 0.0 - (0.10 * jj);
			char rgnbuf[1024];
			snprintf(rgnbuf, sizeof(rgnbuf),
					 "{ \"type\": \"Polygon\", "
					 "\"coordinates\": ["
					 "[[%f, %f], [%f, %f], [%f, %f], [%f, %f], [%f, %f]] "
					 "] }",
					 rlng - 0.001, rlat - 0.001,
					 rlng + 0.001, rlat - 0.001,
					 rlng + 0.001, rlat + 0.001,
					 rlng - 0.001, rlat + 0.001,
					 rlng - 0.001, rlat - 0.001);

			as_arraylist_append(&mylist, (as_val *) as_geojson_new(strdup(rgnbuf), true));
		}

		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i+1000);
		as_record r;
		as_record_init(&r, 2);

		char binval[128];
		snprintf(binval, sizeof(binval), "other_bin_value_%d", i);

		as_record_set_str(&r, "a", binval);
		as_record_set_list(&r, indexed_bin_name, (as_list *) &mylist);

		aerospike_key_put(as, &err, NULL, &key, &r);

		as_record_destroy(&r);
	}

	// Query
	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	char const * region =
		"{ "
		"    \"type\": \"Polygon\", "
		"    \"coordinates\": [["
		"        [-0.202, -0.202], "
		"        [ 0.202, -0.202], "
		"        [ 0.202,  0.202], "
		"        [-0.202,  0.202], "
		"        [-0.202, -0.202] "
		"    ]]"
		" } ";

	as_query_where_inita(&q, 1);
	as_query_where(&q, indexed_bin_name, AS_PREDICATE_RANGE, AS_INDEX_TYPE_LIST, AS_INDEX_GEO2DSPHERE, region);

	foreach_udata udata;
	memset(&udata, 0, sizeof(udata));
	pthread_mutex_init(&udata.lock, NULL);
	udata.hm = as_hashmap_new(32);
	strncpy (udata.binname, "a", sizeof(udata.binname));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &udata);

	if ( err.code != AEROSPIKE_OK ) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}
	assert_int_eq( err.code, AEROSPIKE_OK );

	// We should find only points.
	// The first 21 records have lat from 0.00 to 0.20.
	// Each record has 3 points with lng 0.00, 0.10, 0.20

	assert_int_eq( udata.hm->count, 21 );

	// currently we may return duplicates
	assert( udata.count >= 45 && udata.count <= 697 );

	as_hashmap_destroy(udata.hm);
	pthread_mutex_destroy(&udata.lock);

	as_query_destroy(&q);

	// Cleanup
	aerospike_index_remove(as, &err, NULL, NAMESPACE, index_name);

	for ( int i = 0; i < n_recs; i++ ) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i+1000);

		aerospike_key_remove(as, &err, NULL, &key);
	}
}


TEST( query_geojson_in_mapvalue, "IN MAPVALUES count(*) where p in <rectangle>" ) {

	// create complex index on a map with values of geojson regions
	char *index_name = "idx_test_map_p";
	char *indexed_bin_name = "geomapbin";

	as_error err;
	as_error_reset(&err);

	as_index_task task;

	as_status status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, indexed_bin_name, index_name, AS_INDEX_TYPE_MAPVALUES, AS_INDEX_GEO2DSPHERE);
	index_process_return_code(status, &err, &task);

	// insert records
	int n_recs = 1000;

	for ( int i = 0; i < n_recs; i++ ) {
		// Make a map of points and regions

		as_hashmap mymap;
		as_hashmap_init(&mymap, 20);
		for ( int jj = 0; jj < 10; ++jj ) {
			//
			// This creates a grid of points:
			// [0.00, 0.00], [0.00, 0.10], ... [0.00, 0.90]
			// [0.01, 0.00], [0.01, 0.10], ... [0.01, 0.90]
			// ...
			// [0.99, 0.00], [0.99, 0.10], ... [0.99, 0.90]
			//
			double plat = 0.0 + (0.01 * i);
			double plng = 0.0 + (0.10 * jj);
			char pntbuf[1024];
			snprintf(pntbuf, sizeof(pntbuf),
					 "{ \"type\": \"Point\", \"coordinates\": [%f, %f] }",
					 plng, plat);

			char mkey[128];
			snprintf(mkey, sizeof(mkey), "pointkey_%d_%d", i, jj);
			as_stringmap_set((as_map *) &mymap, mkey, (as_val *)as_geojson_new(strdup(pntbuf), true));

			//
			// This creates a grid of regions centered around the following points
			// [0.00, 0.00], [0.00, -0.10], ... [0.00, -0.90]
			// [0.01, 0.00], [0.01, -0.10], ... [0.01, -0.90]
			// ...
			// [0.99, 0.00], [0.99, -0.10], ... [0.99, -0.90]
			//
			double rlat = 0.0 + (0.01 * i);
			double rlng = 0.0 - (0.10 * jj);
			char rgnbuf[1024];
			snprintf(rgnbuf, sizeof(rgnbuf),
					 "{ \"type\": \"Polygon\", "
					 "\"coordinates\": ["
					 "[[%f, %f], [%f, %f], [%f, %f], [%f, %f], [%f, %f]] "
					 "] }",
					 rlng - 0.001, rlat - 0.001,
					 rlng + 0.001, rlat - 0.001,
					 rlng + 0.001, rlat + 0.001,
					 rlng - 0.001, rlat + 0.001,
					 rlng - 0.001, rlat - 0.001);

			snprintf(mkey, sizeof(mkey), "regionkey_%d_%d", i,jj);
			as_stringmap_set((as_map *) &mymap, mkey, (as_val *)as_geojson_new(strdup(rgnbuf), true));

		}

		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i+1000);
		as_record r;
		as_record_init(&r, 2);

		char binval[128];
		snprintf(binval, sizeof(binval), "other_bin_value_%d", i);

		as_record_set_str(&r, "a", binval);
		as_record_set_map(&r, indexed_bin_name, (as_map *) &mymap);

		aerospike_key_put(as, &err, NULL, &key, &r);

		as_record_destroy(&r);
	}

	// Query
	as_query q;
	as_query_init(&q, NAMESPACE, SET);

	char const * region =
		"{ "
		"    \"type\": \"Polygon\", "
		"    \"coordinates\": [["
		"        [-0.202, -0.202], "
		"        [ 0.202, -0.202], "
		"        [ 0.202,  0.202], "
		"        [-0.202,  0.202], "
		"        [-0.202, -0.202] "
		"    ]]"
		" } ";

	as_query_where_inita(&q, 1);
	as_query_where(&q, indexed_bin_name, AS_PREDICATE_RANGE, AS_INDEX_TYPE_MAPVALUES, AS_INDEX_GEO2DSPHERE, region);

	foreach_udata udata;
	memset(&udata, 0, sizeof(udata));
	pthread_mutex_init(&udata.lock, NULL);
	udata.hm = as_hashmap_new(32);
	strncpy (udata.binname, "a", sizeof(udata.binname));

	aerospike_query_foreach(as, &err, NULL, &q, query_foreach_count_callback, &udata);

	if ( err.code != AEROSPIKE_OK ) {
		 fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}
	assert_int_eq( err.code, AEROSPIKE_OK );

	// We should find only points.
	// The first 21 records have lat from 0.00 to 0.20.
	// Each record has 3 points with lng 0.00, 0.10, 0.20

	assert_int_eq( udata.hm->count, 21 );

	// currently we may return duplicates
	assert( udata.count >= 45 && udata.count <= 697 );

	as_hashmap_destroy(udata.hm);
	pthread_mutex_destroy(&udata.lock);

	as_query_destroy(&q);

	// Cleanup
	aerospike_index_remove(as, &err, NULL, NAMESPACE, index_name);

	for ( int i = 0; i < n_recs; i++ ) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, (int64_t)i+1000);

		aerospike_key_remove(as, &err, NULL, &key);
	}
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( query_geospatial, "aerospike_query_geospatial tests" ) {

	suite_before( before );
	suite_after( after   );

	suite_add( invalid_geojson );
	suite_add( valid_geojson );
	suite_add( filter_points_within_region );
	suite_add( filter_pir_rchild_wrong_type );
	suite_add( filter_pir_lchild_wrong_type );
	suite_add( filter_pir_rchild_not_immed );
	suite_add( filter_pir_parse_failed );
	suite_add( filter_pir_on_regions );
	suite_add( filter_geojson_crash_aer_5650 );
	suite_add( filter_regions_containing_point );
	suite_add( filter_rcp_rchild_wrong_type );
	suite_add( filter_rcp_lchild_wrong_type );
	suite_add( filter_rcp_rchild_not_immed );
	suite_add( filter_rcp_on_points );
	suite_add( query_geojson_in_list );
	suite_add( query_geojson_in_mapvalue );
}
