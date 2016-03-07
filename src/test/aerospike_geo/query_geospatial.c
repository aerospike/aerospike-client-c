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
#include "../util/consumer_stream.h"
#include "../util/log_helper.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;
static bool server_has_geo = false;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "query_geo"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool before(atf_suite * suite) {

	as_error err;
	as_error_reset(&err);

	// create index on "a"
	as_status status;
	as_index_task task;

	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "geobin", "idx_test_geo", AS_INDEX_GEO2DSPHERE);
	if ( status == AEROSPIKE_OK ) {
		aerospike_index_create_wait(&err, &task, 0);
	}
	else {
		info("error(%d): %s", err.code, err.message);
	}

	return true;
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

typedef struct foreach_udata_s {
	uint64_t count;
	char binname[128];
	as_hashmap *hm;
} foreach_udata;


static bool query_foreach_count_callback(const as_val * v, void * udata) {
	foreach_udata * m = (foreach_udata *) udata;
	if ( v == NULL ) {
		info("count: %d", m->count);
	}
	else {
		// dump_record((as_record *)v);
		char * uniq = as_record_get_str((as_record *)v, m->binname);
		as_map_set((as_map*)m->hm, (as_val *) as_string_new(strdup(uniq),true), (as_val *) as_integer_new(1));

		m->count += 1;
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
	if ( status == AEROSPIKE_OK ) {
		aerospike_index_create_wait(&err, &task, 0);
	}
	else {
		info("error(%d): %s", err.code, err.message);
	}

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

	foreach_udata udata = {0};
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

	// currently we return duplicates
	assert_int_eq( udata.count,697);

	as_hashmap_destroy(udata.hm);

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
	if ( status == AEROSPIKE_OK ) {
		aerospike_index_create_wait(&err, &task, 0);
	}
	else {
		info("error(%d): %s", err.code, err.message);
	}

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
			snprintf(mkey, sizeof(mkey), "pointkey_%d", jj);
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

			snprintf(mkey, sizeof(mkey), "regionkey_%d", jj);
			as_stringmap_set((as_map *) &mymap, mkey, (as_val *)as_geojson_new(strdup(pntbuf), true));

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

	foreach_udata udata = {0};
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

	// currently we return duplicates
	assert_int_eq( udata.count,63);

	as_hashmap_destroy(udata.hm);

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

	server_has_geo = aerospike_has_geo(as);

	if (!server_has_geo) {
		info("geospatial tests skipped");
		return;
	}

	suite_before( before );
	suite_after( after   );
	
	suite_add( invalid_geojson );
	suite_add( valid_geojson );
	suite_add( query_geojson_in_list );
	suite_add( query_geojson_in_mapvalue );

}
