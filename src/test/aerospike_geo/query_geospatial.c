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
#include "../util/udf.h"
#include "../util/consumer_stream.h"

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

	status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "geobin", "idx_test_a", AS_INDEX_GEO2DSPHERE);
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

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_a");

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
}
