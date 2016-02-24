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
#include <aerospike/aerospike_index.h>

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

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/
#define NAMESPACE "test"
#define SET "test_index"

/******************************************************************************
 * TYPES
 *****************************************************************************/


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( index_basics_create , "Create index on bin" ) {

	as_error err;
	as_error_reset(&err);

	as_index_task task;

	// DEFAULT type index
	as_status status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "new_bin", "idx_test_new_bin", AS_INDEX_STRING);

	if ( status == AEROSPIKE_OK ) {
		aerospike_index_create_wait(&err, &task, 0);
	}
	else {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq( status , AEROSPIKE_OK );

	// LIST type index
	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE, SET, "new_bin[0]", "idx_test_listbin", AS_INDEX_TYPE_LIST, AS_INDEX_STRING);

	if ( status == AEROSPIKE_OK ) {
		aerospike_index_create_wait(&err, &task, 0);
	}
	else {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq( status , AEROSPIKE_OK );
}

TEST( index_basics_drop , "Drop index" ) {

	as_error err;
	as_error_reset(&err);

	// DEFAULT type index
	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_new_bin");
	if ( err.code != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq( err.code, AEROSPIKE_OK );

	// LIST type index
	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_listbin");
	if ( err.code != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq( err.code, AEROSPIKE_OK );
}


/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( index_basics, "aerospike_sindex basic tests" ) {
	suite_add( index_basics_create );
	suite_add( index_basics_drop );
}
