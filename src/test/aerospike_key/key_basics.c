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
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_msgpack_serializer.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
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
#define SET "test_basics"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


static bool key_basics_print_bins(const char * name, const as_val * value, void * udata)
{
	char * sval = as_val_tostring(value);
	info("bin: name=%s, value=%s", name, sval);
	free(sval);
	return true;
}

static bool key_basics_count_bins(const char * name, const as_val * value, void * udata)
{
	uint64_t * counter = (uint64_t *) udata;
	*counter += 1;
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( key_basics_put , "put: (test,test,foo) = {a: 123, b: 'abc', c: 456, d: 'def', e: [1,2,3], f: {x: 7, y: 8, z: 9} g: 67.43}" ) {

	as_error err;
	as_error_reset(&err);
	
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);
	
	as_hashmap map;
	as_hashmap_init(&map, 32);
	as_stringmap_set_int64((as_map *) &map, "x", 7);
	as_stringmap_set_int64((as_map *) &map, "y", 8);
	as_stringmap_set_int64((as_map *) &map, "z", 9);

	as_record r, * rec = &r;
	as_record_init(rec, 7);
	as_record_set_int64(rec, "a", 123);
	as_record_set_str(rec, "b", "abc");
	as_record_set_integer(rec, "c", as_integer_new(456));
	as_record_set_string(rec, "d", as_string_new("def",false));
	as_record_set_list(rec, "e", (as_list *) &list);
	as_record_set_map(rec, "f", (as_map *) &map);
	
	if (server_has_double) {
		as_record_set_double(rec, "g", 67.43);
	}
	else {
		as_record_set_int64(rec, "g", 67);
	}

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_put(as, &err, NULL, &key, rec);

	as_key_destroy(&key);

    info("bins: ");
    as_record_foreach(rec, key_basics_print_bins, NULL);

	as_record_destroy(rec);

	assert_int_eq( rc, AEROSPIKE_OK );
}

TEST( key_basics_put_key , "put_with_key: (test,test,foo_key) = <bytes>" ) {

	as_error err;
	as_error_reset(&err);

	int count = 20000;
	uint8_t *mybytes = alloca (count);
	memset(mybytes, count, count);

	as_record r, * rec = &r;
	as_record_init(rec, 1);
	as_record_set_rawp(rec, "a", mybytes, count, false);

	// Set up a as_policy_write object with SEND_KEY
	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.key = AS_POLICY_KEY_SEND;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo_key");

	as_status rc = aerospike_key_put(as, &err, &wpol, &key, rec);
	assert_int_eq( rc, AEROSPIKE_OK );
	as_record_destroy(rec);

	as_error_reset(&err);
	as_record * rrec=NULL;
	rc = aerospike_key_get(as, &err, NULL, &key, &rrec);
	assert_int_eq( rc, AEROSPIKE_OK );

	as_record_foreach(rrec, key_basics_print_bins, NULL);

	as_key_destroy(&key);
	as_record_destroy(rrec);
}

TEST( key_basics_get , "get: (test,test,foo) = {a: 123, b: 'abc', c: 456, d: 'def', e: [1,2,3], f: {x: 7, y: 8, z: 9}}" ) {

	as_error err;
	as_error_reset(&err);

	as_record r, *rec = &r;
	as_record_init(&r, 0);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_get(as, &err, NULL, &key, &rec);

	as_key_destroy(&key);

    info("bins: ");
    as_record_foreach(rec, key_basics_print_bins, NULL);
	
    assert_int_eq( rc, AEROSPIKE_OK );
    assert_int_eq( as_record_numbins(rec), 7 );
    
    assert_int_eq( as_record_get_int64(rec, "a", 0), 123 );
    assert_not_null( as_record_get_integer(rec, "a") );
	assert_int_eq( as_integer_toint(as_record_get_integer(rec, "a")), 123 );

    assert_string_eq( as_record_get_str(rec, "b"), "abc" );
	assert_not_null( as_record_get_string(rec, "b") );
	assert_string_eq( as_string_tostring(as_record_get_string(rec, "b")), "abc" );
    
    assert_int_eq( as_record_get_int64(rec, "c", 0), 456 );
    assert_not_null( as_record_get_integer(rec, "c") );
	assert_int_eq( as_integer_toint(as_record_get_integer(rec, "c")), 456 );

	assert_string_eq( as_record_get_str(rec, "d"), "def" );
	assert_not_null( as_record_get_string(rec, "d") );
	assert_string_eq( as_string_tostring(as_record_get_string(rec, "d")), "def" );

    as_list * list = as_record_get_list(rec, "e");
    assert_not_null( list );
    assert_int_eq( as_list_size(list), 3 );

    as_map * map = as_record_get_map(rec, "f");
    assert_not_null( map );
    assert_int_eq( as_map_size(map), 3 );

	if (server_has_double) {
		assert_double_eq( as_record_get_double(rec, "g", 0), 67.43);
		assert_not_null( as_record_get_as_double(rec, "g") );
	}
	else {
		assert_int_eq( as_record_get_int64(rec, "g", 0), 67 );
		assert_not_null( as_record_get_integer(rec, "g") );
	}
    as_record_destroy(rec);
}

TEST( key_basics_select , "select: (test,test,foo) = {a: 123, b: 'abc'}" ) {

	as_error err;
	as_error_reset(&err);

	as_record r, *rec = &r;
	as_record_init(&r, 0);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	const char * bins[4] = { "dontexist","b", "a", NULL };

	as_status rc = aerospike_key_select(as, &err, NULL, &key, bins, &rec);

	as_key_destroy(&key);
    
    assert_int_eq( rc, AEROSPIKE_OK );
    assert_int_eq( as_record_numbins(rec), 2 );
    
    assert_int_eq( as_record_get_int64(rec, "a", 0), 123 );
    assert_not_null( as_record_get_integer(rec, "a") );
	assert_int_eq( as_integer_toint(as_record_get_integer(rec, "a")), 123 );

    assert_string_eq( as_record_get_str(rec, "b"), "abc" );
	assert_not_null( as_record_get_string(rec, "b") );
	assert_string_eq( as_string_tostring(as_record_get_string(rec, "b")), "abc" );
    
    assert_int_eq( as_record_get_int64(rec, "c", 0), 0 );
    assert_null( as_record_get_integer(rec, "c") );
	assert_null( as_record_get_str(rec, "d") );
	assert_null( as_record_get_string(rec, "d") );
    assert_null( as_record_get_list(rec, "e") );
    assert_null( as_record_get_map(rec, "f") );

    uint64_t counter = 0;
    as_record_foreach(rec, key_basics_count_bins, &counter);
    assert_int_eq( counter, 2 );

    as_record_destroy(rec);
}

TEST( key_basics_exists , "exists: (test,test,foo)" ) {

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_record * rec = NULL;
	as_status rc = aerospike_key_exists(as, &err, NULL, &key, &rec);

	as_key_destroy(&key);

    assert_int_eq( rc, AEROSPIKE_OK );
	assert_not_null( rec );
	assert_true ( rec->gen != 0 );
	assert_true ( rec->ttl != 0 );
	
	as_record_destroy(rec);
}

TEST( key_basics_notexists , "not exists: (test,test,foozoo)" ) {

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foozoo");

	
	as_record * rec = NULL;
	as_status rc = aerospike_key_exists(as, &err, NULL, &key, &rec);

	as_key_destroy(&key);

    assert_int_eq( rc, AEROSPIKE_ERR_RECORD_NOT_FOUND );
	assert_null( rec );
	
	as_record_destroy(rec);
}

TEST( key_basics_remove , "remove: (test,test,foo)" ) {

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_remove(as, &err, NULL, &key);

	as_key_destroy(&key);

    assert_true( rc == AEROSPIKE_OK || rc == AEROSPIKE_ERR_RECORD_NOT_FOUND );
}

TEST( key_basics_remove_generation , "remove generation: (test,test,foo)" ) {

	as_error err;
	as_error_reset(&err);

	as_policy_remove dpol;
	as_policy_remove_init(&dpol);
	dpol.gen = AS_POLICY_GEN_EQ;
	dpol.generation = 2;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_remove(as, &err, &dpol, &key);

	assert_true( rc == AEROSPIKE_ERR_RECORD_GENERATION );

	dpol.generation = 1;
	rc = aerospike_key_remove(as, &err, &dpol, &key);

    assert_true( rc == AEROSPIKE_OK );

    as_key_destroy(&key);
}

TEST( key_basics_put_generation , "put generation: (test,test,foo)" ) {

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_record r, * rec = &r;
	as_record_init(rec, 1);
	as_record_set_int64(rec, "a", 123);

	as_status rc = aerospike_key_put(as, &err, NULL, &key, rec);

	assert_true( rc == AEROSPIKE_OK );

	as_record_destroy(rec);
	as_record_init(rec, 1);
	as_record_set_int64(rec, "a", 456);

	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.gen = AS_POLICY_GEN_EQ; // perform generation equality check on writes

	rec->gen = 2; // generation in database should be 1, so this should fail

	rc = aerospike_key_put(as, &err, &wpol, &key, rec);

	assert_true( rc == AEROSPIKE_ERR_RECORD_GENERATION );

	rec->gen = 1; // ... but this should succeed

	rc = aerospike_key_put(as, &err, &wpol, &key, rec);

	assert_true( rc == AEROSPIKE_OK );

	as_record_destroy(rec);
	as_record_init(rec, 1);
	as_record_set_nil(rec, "a"); // remove bin 'a' - causes record deletion

	rec->gen = 2; // generation in database should be 2, so this should succeed

	rc = aerospike_key_put(as, &err, &wpol, &key, rec);

	assert_true( rc == AEROSPIKE_OK );

	as_record_destroy(rec);

	// Verify the record is gone.
	rec = NULL;
	rc = aerospike_key_exists(as, &err, NULL, &key, &rec);

	assert_int_eq( rc, AEROSPIKE_ERR_RECORD_NOT_FOUND );
	assert_null( rec );

	as_key_destroy(&key);
}

TEST( key_basics_remove_notexists , "remove not exists: (test,test,foozoo)" ) {
	
	as_error err;
	as_error_reset(&err);
	
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foozoo");
	
	as_status rc = aerospike_key_remove(as, &err, NULL, &key);
	
	as_key_destroy(&key);
	
    assert_true( rc == AEROSPIKE_ERR_RECORD_NOT_FOUND );
	assert_true( err.message[0] != 0 );
}

TEST( key_basics_operate , "operate: (test,test,foo) => {a: incr(321), b: append('def'), d: prepend('abc')}" ) {

	as_error err;
	as_error_reset(&err);

	as_operations ops;
	as_operations_inita(&ops, 3);
	as_operations_add_incr(&ops, "a", 321);
	as_operations_add_append_str(&ops, "b", "def");
	as_operations_add_prepend_str(&ops, "d", "abc");

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_status rc = aerospike_key_operate(as, &err, NULL, &key, &ops, NULL);

	as_key_destroy(&key);

    assert_int_eq( rc, AEROSPIKE_OK );
}

TEST( key_basics_get2 , "get: (test,test,foo) = {a: 444, b: 'abcdef', d: 'abcdef'}" ) {

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_record r, *rec = &r;
	as_record_init(&r, 0);

	as_status rc = aerospike_key_get(as, &err, NULL, &key, &rec);

	as_key_destroy(&key);
    
    info("bins: ");
    as_record_foreach(rec, key_basics_print_bins, NULL);
	
    assert_int_eq( rc, AEROSPIKE_OK );
    assert_int_eq( as_record_numbins(rec), 7 );
    
    assert_int_eq( as_record_get_int64(rec, "a", 0), 444 );
    assert_not_null( as_record_get_integer(rec, "a") );
	assert_int_eq( as_integer_toint(as_record_get_integer(rec, "a")), 444 );

    assert_string_eq( as_record_get_str(rec, "b"), "abcdef" );
	assert_not_null( as_record_get_string(rec, "b") );
	assert_string_eq( as_string_tostring(as_record_get_string(rec, "b")), "abcdef" );
    
    assert_int_eq( as_record_get_int64(rec, "c", 0), 456 );
    assert_not_null( as_record_get_integer(rec, "c") );
	assert_int_eq( as_integer_toint(as_record_get_integer(rec, "c")), 456 );

	assert_string_eq( as_record_get_str(rec, "d"), "abcdef" );
	assert_not_null( as_record_get_string(rec, "d") );
	assert_string_eq( as_string_tostring(as_record_get_string(rec, "d")), "abcdef" );

    as_list * list = as_record_get_list(rec, "e");
    assert_not_null( list );
    assert_int_eq( as_list_size(list), 3 );

    as_map * map = as_record_get_map(rec, "f");
    assert_not_null( map );
    assert_int_eq( as_map_size(map), 3 );

	if (server_has_double) {
		assert_double_eq( as_record_get_double(rec, "g", 0), 67.43);
		assert_not_null( as_record_get_as_double(rec, "g") );
	}
	else {
		assert_int_eq( as_record_get_int64(rec, "g", 0), 67 );
		assert_not_null( as_record_get_integer(rec, "g") );
	}

    as_record_destroy(rec);
}

TEST( key_basics_write_preserialized_list , "write pre-serialized list" ) {
	as_error err;
	as_error_reset(&err);
	
	// Create list.
	as_arraylist list;
	as_arraylist_init(&list, sizeof(int), 3);
	as_arraylist_append_int64(&list, 7);
	as_arraylist_append_int64(&list, 3);
	as_arraylist_append_int64(&list, -86);

	// Serialize list.
	as_buffer buffer;
	as_serializer ser;
	as_msgpack_init(&ser);
	as_serializer_serialize(&ser, (as_val*)&list, &buffer);
	as_serializer_destroy(&ser);
	as_arraylist_destroy(&list);
	
	// Write pre-serialized list.
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");
	
	as_record rec;
	as_record_init(&rec, 1);
	as_record_set_raw_typep(&rec, "a", buffer.data, buffer.size, AS_BYTES_LIST, true);
	
	as_status rc = aerospike_key_put(as, &err, NULL, &key, &rec);
    assert_int_eq(rc, AEROSPIKE_OK);
	
	as_key_destroy(&key);
	as_record_destroy(&rec);
}

TEST( key_basics_read_list , "read list" ) {
	as_error err;
	as_error_reset(&err);
		
	// Read list
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");

	as_record* rec = 0;
	as_status rc = aerospike_key_get(as, &err, NULL, &key, &rec);
    assert_int_eq(rc, AEROSPIKE_OK);
	
	// Compare list
	as_arraylist* list = (as_arraylist*) as_record_get_list(rec, "a");
    assert_int_eq(as_arraylist_get_int64(list, 0), 7);
    assert_int_eq(as_arraylist_get_int64(list, 1), 3);
    assert_int_eq(as_arraylist_get_int64(list, 2), -86);
	
	as_record_destroy(rec);
}

TEST( key_basics_read_raw_list , "read raw list" ) {
	as_error err;
	as_error_reset(&err);
	
	// Read raw list
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo");
	
	as_policy_read policy;
	as_policy_read_init(&policy);
	policy.deserialize = false;
	
	as_record* rec = 0;
	as_status rc = aerospike_key_get(as, &err, &policy, &key, &rec);
    assert_int_eq(rc, AEROSPIKE_OK);
	
	as_bytes* bytes = as_record_get_bytes(rec, "a");
	assert_int_eq(bytes->type, AS_BYTES_LIST);
	
	// Deserialize list
	as_val* value = 0;
	as_buffer buffer;
	buffer.data = bytes->value;
	buffer.size = bytes->size;
	
	as_serializer ser;
	as_msgpack_init(&ser);
	as_serializer_deserialize(&ser, &buffer, &value);
	as_serializer_destroy(&ser);
	
	// Compare list
	as_arraylist* list = (as_arraylist*)value;
    assert_int_eq(as_arraylist_get_int64(list, 0), 7);
    assert_int_eq(as_arraylist_get_int64(list, 1), 3);
    assert_int_eq(as_arraylist_get_int64(list, 2), -86);
	
    as_val_destroy(value);
	as_record_destroy(rec);
}


TEST( key_basics_list_map_double , "put/get double: (test,test,foo_double) = {b1: ['a',2, 3.56], b2: {w: 6.78, x: 7, y: 'b'}}" ) {

	if (server_has_double) {
		as_error err;
		as_error_reset(&err);

		as_key key;
		as_key_init(&key, NAMESPACE, SET, "foo_double");

		// remove record to have clean test
		as_status rc = aerospike_key_remove(as, &err, NULL, &key);

		// insert record with list & map with double
		as_arraylist mylist;
		as_arraylist_init(&mylist,3,0);
		as_arraylist_append_str(&mylist, "a");
		as_arraylist_append_int64(&mylist, 2);
		as_arraylist_append_double(&mylist,3.56);

		as_hashmap mymap;
		as_hashmap_init(&mymap, 32);
		as_stringmap_set_double((as_map *) &mymap, "w", 6.78);
		as_stringmap_set_int64((as_map *) &mymap, "x", 7);
		as_stringmap_set_str((as_map *) &mymap, "y", "b");

		as_record r, * rec = &r;
		as_record_init(rec, 2);
		as_record_set_list(rec, "b1", (as_list *) &mylist);
		as_record_set_map(rec, "b2", (as_map *) &mymap);

		rc = aerospike_key_put(as, &err, NULL, &key, rec);
	    assert_int_eq(rc, AEROSPIKE_OK);

	    as_list_destroy((as_list *)&mylist);
		as_map_destroy((as_map *)&mymap);
		as_record_destroy(rec);

		rec = NULL;
		rc = aerospike_key_get(as, &err, NULL, &key, &rec);
	    assert_int_eq(rc, AEROSPIKE_OK);

	    // compare list
	    as_arraylist* rlist = (as_arraylist*)as_record_get_list(rec, "b1");
		assert_not_null(rlist);
		assert_string_eq(as_arraylist_get_str(rlist, 0), "a");
	    assert_int_eq(as_arraylist_get_int64(rlist, 1), 2);
	    assert_double_eq(as_arraylist_get_double(rlist, 2), 3.56);

	    // compare map
	    as_hashmap* rmap = (as_hashmap*)as_record_get_map(rec, "b2");
		assert_not_null(rmap);

		as_hashmap_iterator it;
		as_hashmap_iterator_init(&it, (const as_hashmap*)rmap);

		// See if the elements match what we expect.
		while (as_hashmap_iterator_has_next(&it)) {
			as_pair* p_pair = (as_pair *)as_hashmap_iterator_next(&it);
			as_val* p_val = as_pair_2(p_pair);
			if (as_val_type(p_val) == AS_STRING ) {
				assert_string_eq( as_string_get(as_string_fromval(p_val)), "b");
			} else if (as_val_type(p_val) == AS_INTEGER ) {
				assert_int_eq( as_integer_get(as_integer_fromval(p_val)), 7);
			} else if (as_val_type(p_val) == AS_DOUBLE ) {
				assert_double_eq( as_double_get(as_double_fromval(p_val)), 6.78);
			} else {
				warn ("unexpected type %d", as_val_type(p_val));
				assert(FALSE);
			}
		}

		as_record_destroy(rec);
	    as_key_destroy(&key);
	}
}

TEST( key_basics_compression , "put with compression write policy: (test,test,foo_comp) = {a: <bytes>, b: 'abc', c: 456}" ) {

	as_error err;
	as_error_reset(&err);

	int count = 2000;
	uint8_t *mybytes = alloca (count);
	memset(mybytes, count, count);

	as_record r, * rec = &r;
	as_record_init(rec, 3);
	as_record_set_rawp(rec, "a", mybytes, count, false);
	as_record_set_str(rec, "b", "abc");
	as_record_set_integer(rec, "c", as_integer_new(456));

	// Set up a as_policy_write object to compress record beyond 1000 bytes
	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.compression_threshold = 1000;
	wpol.key = AS_POLICY_KEY_SEND;

	as_key key;
	as_key_init(&key, NAMESPACE, SET, "foo_comp");

	as_status rc = aerospike_key_put(as, &err, &wpol, &key, rec);
	assert_int_eq( rc, AEROSPIKE_OK );
	as_record_destroy(rec);

	as_error_reset(&err);
	as_record * rrec=NULL;
	rc = aerospike_key_get(as, &err, NULL, &key, &rrec);
	assert_int_eq( rc, AEROSPIKE_OK );
	assert_string_eq( as_record_get_str(rrec, "b"), "abc" );
	assert_int_eq( as_record_get_int64(rrec, "c", 0), 456 );

	as_key_destroy(&key);
	as_record_destroy(rrec);
}

static bool scan_cb(const as_val * val, void * udata)
{
	uint64_t *result = (uint64_t *) udata;

	// NULL is END OF SCAN
	if ( !val ) {
		return false;
	}

	as_record * rec = as_record_fromval(val);
	if ( !rec ) {
		error("Expected a record, but got type %d", as_val_type(val));
		*result = 1; // fail 1
		return false;
	}

	// check key
	if (!rec->key.valuep) {
		error("Expected record to have key returned, but no key");
		*result = 3; // fail 3
		return false;
	}

	const char* key_val_as_str = as_val_tostring(rec->key.valuep);

	if (! strcmp(key_val_as_str, "\"store_key\"")==0) {
		error("Expected record to have key [\"store_key\"], but got [%s]",key_val_as_str);
		*result = 4; // fail 4
		free((void *)key_val_as_str);
		return false;
	}

	// check set name
	const char * set = rec->key.set[0] == '\0' ? NULL : rec->key.set;
	if (! strcmp(set, "store_key_set")==0) {
		error("Expected record in set [store_key_set], but got set in [%s]", set);
		*result = 2; // fail 2
		return false;
	}


	*result = 0;

	free((void *)key_val_as_str);
	return true;
}

TEST( key_basics_storekey , "store key" ) {

	as_error err;
	as_error_reset(&err);

	as_key key;
	as_key_init(&key, NAMESPACE, "store_key_set", "store_key");

	as_record rec;
	as_record_init(&rec, 1);
	as_record_set_int64(&rec, "a", 123);

	as_policy_write sendKeyPolicy;
	as_policy_write_init(&sendKeyPolicy);
	sendKeyPolicy.key = AS_POLICY_KEY_SEND;

	as_status rc = aerospike_key_put(as, &err, &sendKeyPolicy, &key, &rec);
    assert_int_eq(rc, AEROSPIKE_OK);

	as_key_destroy(&key);
	as_record_destroy(&rec);

	// scan the 1 record set back, to get the key
	as_scan scan;
	as_scan_init(&scan, NAMESPACE, "store_key_set");

	uint64_t myresult;
	rc = aerospike_scan_foreach(as, &err, NULL, &scan, scan_cb, &myresult);

	assert_int_eq( rc, AEROSPIKE_OK );
	assert_int_eq( myresult, 0 );

	as_scan_destroy(&scan);

}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( key_basics, "aerospike_key basic tests" ) {
	server_has_double = aerospike_has_double(as);

	// Remove at beginning to clear out record.
    suite_add( key_basics_put_key );
    suite_add( key_basics_remove );
	suite_add( key_basics_put );
	suite_add( key_basics_exists );
	suite_add( key_basics_notexists );
	suite_add( key_basics_remove_generation );
	suite_add( key_basics_put_generation );
	suite_add( key_basics_put );
	suite_add( key_basics_get );
	suite_add( key_basics_select );
	suite_add( key_basics_operate );
	suite_add( key_basics_get2 );
	suite_add( key_basics_remove );
	suite_add( key_basics_remove_notexists );
	suite_add( key_basics_notexists );
	suite_add( key_basics_write_preserialized_list );
	suite_add( key_basics_read_list );
	suite_add( key_basics_read_raw_list );
	suite_add( key_basics_list_map_double );
	suite_add( key_basics_compression );
	suite_add( key_basics_storekey );
}
