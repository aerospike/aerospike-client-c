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
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_lmap.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_record.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_status.h>

#include "../test.h"
#include "../aerospike_test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_ldt"
#define INFO_CALL "namespace/test"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/
static bool is_ldt_enabled()
{
	char* res = NULL;
	as_error err;
	int rc = aerospike_info_host(as, &err, NULL, g_host, 3000, INFO_CALL, &res);

	if (rc == AEROSPIKE_OK) {
		char *st = strstr(res, "ldt-enabled=true");
		free(res);

		if (st) {
			return true;
		}
	}
	return false;
}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( lmap_put , "put: (test,test,t1) = {bin:1}" ) {

	if (!is_ldt_enabled()) {
		fprintf(stderr, "ldt not enabled. skipping test");
		return;
	}

	as_error err;
	as_error_reset(&err);

	as_ldt lmap;

	assert_not_null( as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL) );

	// No need to destroy as_integer if using as_integer_init() on stack object.
	as_key skey;
	as_key_init_str(&skey, NAMESPACE, SET, "lmap_put");

	as_integer iname;
	as_integer_init(&iname, 12);
	as_integer ival;
	as_integer_init(&ival, 34);

	as_status rc = aerospike_lmap_put(as, &err, NULL, &skey, &lmap, (as_val* )&iname, (as_val* )&ival);
	assert_int_eq( rc, AEROSPIKE_OK );

	// Make sure we can read it one back
	as_map* p_map = NULL;

	// Make sure we cannot get the value any more.
	rc = aerospike_lmap_get(as, &err, NULL, &skey, &lmap,(const as_val*)&iname, (as_val**)&p_map);
	assert_not_null(p_map);

	assert_int_eq(as_val_type(p_map), AS_MAP);

	as_val* p_val = as_map_get(p_map,(const as_val*)&iname);

	assert_not_null(p_val);
	assert_int_eq(as_val_type(p_val),AS_INTEGER);
	assert_int_eq(as_integer_get((as_integer *)p_val),34);

	as_val_destroy(p_map);

	// Destroy the lmap.
	rc = aerospike_lmap_destroy(as, &err, NULL, &skey, &lmap);
	assert_int_eq( rc, AEROSPIKE_OK );

}

TEST( lmap_put_all , "putall: (test,test,1) = {x:1,y:2,z:3}" ) {

	as_error err;
	as_error_reset(&err);

	as_ldt lmap;

	if (! as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL)) {
	}

	// No need to destroy as_integer if using as_integer_init() on stack object.
	as_key skey;
	as_key_init_str(&skey, NAMESPACE, SET, "lmap_put_all");

	// creating hashmap
	as_hashmap map;
	as_hashmap_init(&map, 3);
	as_stringmap_set_int64((as_map *) &map, "x", 0);
	as_stringmap_set_int64((as_map *) &map, "y", 1);
	as_stringmap_set_int64((as_map *) &map, "z", 2);

	// Put all map entry to the lmap.
	as_status rc = (aerospike_lmap_put_all(as, &err, NULL, &skey, &lmap, (as_map*)&map));
	assert_int_eq( rc, AEROSPIKE_OK );

	// Make sure we can read each one back
	as_map* p_map = NULL;
	rc = aerospike_lmap_get_all(as, &err, NULL, &skey, &lmap, &p_map);
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null(p_map);
	assert_int_eq(as_val_type(p_map), AS_MAP);

	// See if the elements match what we expect.
	as_hashmap_iterator it;
	as_hashmap_iterator_init(&it, (const as_hashmap*)p_map);

	assert_int_eq(as_hashmap_size((as_hashmap*)p_map),3);
	int loop = 0;
	while (as_hashmap_iterator_has_next(&it)) {
		const as_val* p_val = as_hashmap_iterator_next(&it);
		assert_int_eq(as_val_type(p_val), AS_PAIR);
		if (loop == 0) {
			assert_string_eq("(\"x\", 0)",as_val_tostring(p_val));
		} else if (loop == 1) {
			assert_string_eq("(\"y\", 1)",as_val_tostring(p_val));
		} else if (loop == 2) {
			assert_string_eq("(\"z\", 2)",as_val_tostring(p_val));
		}

		loop++;
	}
	assert_int_eq(loop, 3);

	// Destroy the lmap.
	rc = aerospike_lmap_destroy(as, &err, NULL, &skey, &lmap);
	assert_int_eq( rc, AEROSPIKE_OK );
}


/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( ldt_lmap, "aerospike_lmap basic tests" ) {
    suite_add( lmap_put );
    //suite_add( lmap_put_all );
}
