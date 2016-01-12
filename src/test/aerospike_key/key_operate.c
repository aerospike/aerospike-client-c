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

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_val.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;
static bool server_has_double = false;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_operate"


/******************************************************************************
 * TYPES
 *****************************************************************************/


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( key_operate_touchget , "operate: (test,test,key2) = {touch, get}" ) {

	as_error err;
	as_error_reset(&err);

	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);

	as_record r, * rec = &r;
	as_record_init(rec, 3);
	as_record_set_int64(rec, "a", 123);
	as_record_set_str(rec, "b", "abc");
	as_record_set_list(rec, "e", (as_list *) &list);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "key2");

	as_status rc = aerospike_key_remove(as, &err, NULL, &key);

	rc = aerospike_key_put(as, &err, NULL, &key, rec);
	assert_int_eq( rc, AEROSPIKE_OK );

	as_record_destroy(rec);
	as_record_init(rec, 1);

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_touch(&ops);
	as_operations_add_read(&ops, "e");
	ops.ttl = 120;

	// Apply the operation.
	rc = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	assert_int_eq( rc, AEROSPIKE_OK );

	as_list * rlist = as_record_get_list(rec, "e");
	assert_not_null( rlist );
	assert_int_eq( as_list_size(rlist), 3 );

	as_record_destroy(rec);

}

TEST( key_operate_9 , "operate: (test,test,key3) = {append, read, write, read, incr, read, prepend}" ) {

	as_error err;
	as_error_reset(&err);

	as_key key;
    as_operations asops;
    as_operations *ops = &asops;
    as_map *map = NULL;
    as_record *rec = NULL;
    int rc;

    as_key_init( &key, NAMESPACE, SET, "test-key1" );
	rc = aerospike_key_remove(as, &err, NULL, &key);
	assert_true( rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND);

    as_operations_init( ops, 8);

    as_operations_add_append_strp( ops, "app", "append str", 0 );
    as_operations_add_read( ops, "app" );

    map = (as_map*)as_hashmap_new(1);
    as_stringmap_set_str( map, "hello", "world" );
    as_operations_add_write( ops, "map", (void*)map );
    as_operations_add_read( ops, "map" );

    as_operations_add_incr( ops, "incr", 1900 );
    as_operations_add_read( ops, "incr" );

    as_operations_add_prepend_strp( ops, "pp", "prepend str", false );
    as_operations_add_read( ops, "pp" );

    rc = aerospike_key_operate(as, &err, NULL, &key, ops, &rec );
	assert_int_eq( rc, AEROSPIKE_OK );

	assert_string_eq( as_record_get_str(rec, "app"), "append str" );
    assert_int_eq( as_record_get_int64(rec, "incr", 0), 1900 );
	assert_string_eq( as_record_get_str(rec, "pp"), "prepend str" );

    as_record_destroy( rec );
    as_operations_destroy( ops );
}

TEST( key_operate_gen_equal , "operate: only if expected generation " )
{
	as_error err;
	as_error_reset(&err);
	
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	
	as_record r, * rec = &r;
	as_record_init(rec, 3);
	as_record_set_int64(rec, "a", 123);
	as_record_set_str(rec, "b", "abc");
	as_record_set_list(rec, "e", (as_list *) &list);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "key2");
	
	as_status rc = aerospike_key_remove(as, &err, NULL, &key);
	
	rc = aerospike_key_put(as, &err, NULL, &key, rec);
	assert_int_eq( rc, AEROSPIKE_OK );
	
	as_record_destroy(rec);
	as_record_init(rec, 1);
	
	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_touch(&ops);
	as_operations_add_read(&ops, "e");
	ops.gen = 2;
	ops.ttl = 120;
	
	as_policy_operate policy;
	as_policy_operate_init(&policy);
	policy.gen = AS_POLICY_GEN_EQ;
	
	// Expect failure.
	rc = aerospike_key_operate(as, &err, &policy, &key, &ops, &rec);
	assert_int_eq( rc, AEROSPIKE_ERR_RECORD_GENERATION );

	// Expect success.
	ops.gen = 1;
	rc = aerospike_key_operate(as, &err, &policy, &key, &ops, &rec);
	assert_int_eq( rc, AEROSPIKE_OK );

	as_record_destroy(rec);
}

TEST( key_operate_float , "operate: (test,test,opfloat) = {append, read, write, read, incr, read, prepend}" ) {

	if (server_has_double) {
		as_error err;
		as_error_reset(&err);

		as_key key;
		as_operations asops;
		as_operations *ops = &asops;
		as_record *rec = NULL;
		int rc;

		as_key_init( &key, NAMESPACE, SET, "opfloat" );
		rc = aerospike_key_remove(as, &err, NULL, &key);
		assert_true( rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND);

		// Make sure we can write and read double
		as_operations_init( ops, 2);

		as_double dbl;
		as_double_init(&dbl, 3.45);

		as_operations_add_write( ops, "incr", (as_bin_value*) &dbl );
		as_operations_add_read( ops, "incr" );
		rc = aerospike_key_operate(as, &err, NULL, &key, ops, &rec );
		assert_int_eq( rc, AEROSPIKE_OK );

		assert_double_eq( as_record_get_double(rec,"incr",0.0), 3.45);

		as_record_destroy( rec );
		as_operations_destroy( ops );
		rec = NULL;

		// Make sure we can increment double
		as_operations_init( ops, 3);

		as_operations_add_incr_double( ops, "incr", 19.03 );
		as_operations_add_incr_double( ops, "incr", -5.03 );
		as_operations_add_read( ops, "incr" );
		rc = aerospike_key_operate(as, &err, NULL, &key, ops, &rec );
		assert_int_eq( rc, AEROSPIKE_OK );

		assert_double_eq( as_record_get_double(rec,"incr",0.0), 17.45);

		as_record_destroy( rec );
		as_operations_destroy( ops );
		rec = NULL;

		// Make sure append string fails
		as_operations_init( ops, 1);
		as_operations_add_append_str( ops, "incr", "my string" );
		rc = aerospike_key_operate( as, &err, NULL, &key, ops, &rec );
		assert_int_eq( rc, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE );

		as_record_destroy( rec );
		as_operations_destroy( ops );
	}
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( key_operate, "aerospike_key_operate tests" ) {
	server_has_double = aerospike_has_double(as);

	suite_add( key_operate_touchget );
	suite_add( key_operate_9 );
	suite_add( key_operate_gen_equal );
	suite_add( key_operate_float );
}
