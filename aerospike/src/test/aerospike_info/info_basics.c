
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
#include "../unittest.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * TYPES
 *****************************************************************************/

struct info_data_s {
	char * 	actual;
	uint8_t matches;
	uint8_t count;
};

typedef struct info_data_s info_data;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool info_compare(const as_error * err, const as_node * node, const char * req, const char * res, void * udata) {
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

	if(res) {
		free(res);
		res =  NULL;
	}

	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( info_basics_help , "help" ) {

	as_error err;
	as_error_reset(&err);

	info_data data = {
		.actual = NULL,
		.matches = 0,
		.count = 0
	};

	as_status rc = AEROSPIKE_OK;

	rc = aerospike_info_foreach(as, &err, NULL, "help", info_compare, &data);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert( data.count > 0 );
	assert( data.matches > 0 );
	assert( data.count == data.matches );

	char * res = NULL;

	rc = aerospike_info_host(as, &err, NULL, as->config.hosts[0].addr, 3000, "help", &res);
	
	assert_not_null( res );
	assert_string_eq(res, data.actual);

	free(res);
	res = NULL;

	if ( data.actual ) {
		free(data.actual);
	}
}

TEST( info_basics_features , "features" ) {

	as_error err;
	as_error_reset(&err);

	info_data data = {
		.actual = "features\tas_msg;replicas-read;replicas-prole;replicas-write;replicas-master;cluster-generation;partition-info;partition-generation;udf\n",
		.matches = 0,
		.count = 0
	};

	as_status rc = AEROSPIKE_OK;

	rc = aerospike_info_foreach(as, &err, NULL, "features", info_compare, &data);
	
	assert_int_eq( rc, AEROSPIKE_OK );
	assert( data.count > 0 );
	assert( data.matches > 0 );
	assert_int_eq( data.count, data.matches );

	char * res = NULL;

	rc = aerospike_info_host(as, &err, NULL, as->config.hosts[0].addr, 3000, "features", &res);
	
	assert_not_null( res );
	assert_string_eq(res, data.actual);

	free(res);
	res = NULL;
}

TEST( info_basics_help_bad_params_foreach , "help with bad parameters foreach" ) {

	as_error err;
	as_error_reset(&err);

	info_data data = {
		.actual = NULL,
		.matches = 0,
		.count = 0
	};

	as_status rc = AEROSPIKE_OK;

	rc = aerospike_info_foreach(as, &err, NULL, 99999, info_compare, &data);
	assert_int_ne( rc, AEROSPIKE_OK );

	char * res = NULL;

//	rc = aerospike_info_host(as, &err, NULL, "127.0.0.1", 3000, "help", &res);

	rc = aerospike_info_host(as, &err, NULL, as->config.hosts[0].addr, 3000, "help", &res);

	assert_null( res );
	assert_string_ne(res, data.actual);

	free(res);
	res = NULL;

	if ( data.actual ) {
		free(data.actual);
	}
}

TEST( info_basics_help_bad_params_info_host , "help with bad parameters info_host" ) {

	as_error err;
	as_error_reset(&err);

	info_data data = {
		.actual = NULL,
		.matches = 0,
		.count = 0
	};

	as_status rc = AEROSPIKE_OK;

	rc = aerospike_info_foreach(as, &err, NULL, "help", info_compare, &data);
	assert_int_ne( rc, AEROSPIKE_OK );

	char * res = NULL;

//	rc = aerospike_info_host(as, &err, NULL, "127.0.0.1", 3000, "help", &res);

	rc = aerospike_info_host(as, &err, NULL, as->config.hosts[0].addr, 3000, 99999, &res);

	assert_null( res );
	assert_string_ne(res, data.actual);

	free(res);
	res = NULL;

	if ( data.actual ) {
		free(data.actual);
	}
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( info_basics, "aerospike_info basic tests" ) {
	suite_add( info_basics_help );
	suite_add( info_basics_features );

//	suite_add( info_basics_help_bad_params_foreach );
//	suite_add( info_basics_help_bad_params_info_host );
}
