/*******************************************************************************
 * Copyright 2015 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/


//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// Constants
//

#define UDF_MODULE "geo_filter_amen"
#define UDF_USER_PATH "src/lua/"
const char UDF_FILE_PATH[] =  UDF_USER_PATH UDF_MODULE ".lua";

const char TEST_BIN_NAME1[] = "geofilterloc";
const char TEST_BIN_NAME2[] = "geofilteramen";
const char TEST_INDEX_NAME[] = "filter-loc-index";


//==========================================================
// Forward Declarations
//

bool query_cb(const as_val* p_val, void* udata);
void cleanup(aerospike* p_as);
bool insert_records(aerospike* p_as);


//==========================================================
// FILTER QUERY Example
//

int g_nschools = 0;

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_MULTI_KEY_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike_with_udf_config(&as, UDF_USER_PATH);

	// Ensure the server supports geospatial queries.
	if (! aerospike_has_geo(&as)) {
		fprintf(stderr, "server does not support geospatial\n");
		exit(0);
	}
	
	// Start clean.
	example_remove_test_records(&as);
	example_remove_index(&as, TEST_INDEX_NAME);

	// Register the UDF in the database cluster.
	if (! example_register_udf(&as, UDF_FILE_PATH)) {
		example_cleanup(&as);
		exit(-1);
	}

	// Create a numeric secondary index on test-bin.
	if (! example_create_2dsphere_index(&as, TEST_BIN_NAME1, TEST_INDEX_NAME)) {
		cleanup(&as);
		exit(-1);
	}

	if (! insert_records(&as)) {
		cleanup(&as);
		exit(-1);
	}

	as_error err;

	// Create an as_query object.
	as_query query;
	as_query_init(&query, g_namespace, g_set);

	// Our query region:
	char const * region =
		"{ "
		"    \"type\": \"Polygon\", "
		"    \"coordinates\": [ "
		"        [[-122.500000, 37.000000],[-121.000000, 37.000000], "
		"         [-121.000000, 38.080000],[-122.500000, 38.080000], "
		"         [-122.500000, 37.000000]] "
		"    ] "
		" } ";

	// Generate an as_query.where condition. Note that as_query_destroy() takes
	// care of destroying all the query's member objects if necessary. However
	// using as_query_where_inita() does avoid internal heap usage.
	as_query_where_inita(&query, 1);
	as_query_where(&query, TEST_BIN_NAME1, as_geo_within(region));

	as_arraylist args;
	as_arraylist_init(&args, 1, 0);
	as_arraylist_append_str(&args, "school");
	as_query_apply(&query, UDF_MODULE, "match_amen", (as_list *) &args);

	LOG("executing query: within <rect>");

	// Execute the query. This call blocks - callbacks are made in the scope of
	// this call.
	if (aerospike_query_foreach(&as, &err, NULL, &query, query_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_query_foreach() returned %d - %s", err.code,
				err.message);
		as_query_destroy(&query);
		cleanup(&as);
		exit(-1);
	}

	LOG("query executed");

	as_query_destroy(&query);

	// Cleanup and disconnect from the database cluster.
	cleanup(&as);

	if (g_nschools == 2) {
		LOG("filter query example successfully completed");
	}
	else {
		LOG("wrong number of schools found, should be 2, found %d", g_nschools);
	}

	return 0;
}


//==========================================================
// Query Callback
//

bool
query_cb(const as_val* p_val, void* udata)
{
	if (! p_val) {
		LOG("query callback returned null - query is complete");
		return true;
	}

	as_geojson * gp = as_geojson_fromval(p_val);
	const char * locstr = as_geojson_get(gp);

	LOG("matched: %s", locstr);

	++g_nschools;

	return true;
}


//==========================================================
// Helpers
//

void
cleanup(aerospike* p_as)
{
	example_remove_test_records(p_as);
	example_remove_index(p_as, TEST_INDEX_NAME);
	example_cleanup(p_as);
}

bool
insert_records(aerospike* p_as)
{
	// Create an as_record object with one (GeoJSON value) bin. By using
	// as_record_inita(), we won't need to destroy the record if we only set
	// bins using as_record_set_geojson().
	as_record rec;
	as_record_inita(&rec, 2);

	// Re-using rec, write records into the database such that each record's key
	// and (test-bin) value is based on the loop index.
	for (uint32_t i = 0; i < g_n_keys; i++) {
		as_error err;

		// No need to destroy a stack as_key object, if we only use
		// as_key_init_int64().
		as_key key;
		as_key_init_int64(&key, g_namespace, g_set, (int64_t)i);

		double lng = -122 + (0.1 * i);
		double lat = 37.5 + (0.1 * i);
		char buff[1024];
		snprintf(buff, sizeof(buff),
				 "{ \"type\": \"Point\", \"coordinates\": [%f, %f] }", lng, lat);
		as_record_set_geojson_str(&rec, TEST_BIN_NAME1, buff);

		if (i % 7 == 0) {
			as_record_set_str(&rec, TEST_BIN_NAME2, "hospital");
		}
		else if (i % 2 == 0) {
			as_record_set_str(&rec, TEST_BIN_NAME2, "school");
		}
		else {
			as_record_set_str(&rec, TEST_BIN_NAME2, "store");
		}
		
		// Write a record to the database.
		if (aerospike_key_put(p_as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
			return false;
		}
	}

	LOG("insert succeeded");

	return true;
}
