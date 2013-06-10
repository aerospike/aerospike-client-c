
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_digest.h>

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
 * TEST CASES
 *****************************************************************************/

TEST( digest_basics_put , "put: (test,test,foo) = {a: 123, b: 'abc', c: 456, d: 'def', e: [1,2,3], f: {x: 7, y: 8, z: 9}}" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", "foo");

	as_list list;
	as_arraylist_init(&list, 3, 0);
	as_list_add_integer(&list, 1);
	as_list_add_integer(&list, 2);
	as_list_add_integer(&list, 3);

	as_map map;
	as_hashmap_init(&map, 32);
	as_map_set(&map, (as_val *) as_string_new("x",true), (as_val *) as_integer_new(7));
	as_map_set(&map, (as_val *) as_string_new("y",true), (as_val *) as_integer_new(8));
	as_map_set(&map, (as_val *) as_string_new("z",true), (as_val *) as_integer_new(9));

	as_record r;
	as_record_init(&r, 10);
	as_record_set_int64(&r, "a", 123);
	as_record_set_str(&r, "b", "abc");
	as_record_set_integer(&r, "c", as_integer_new(456));
	as_record_set_string(&r, "d", as_string_new("def",true));
	as_record_set_list(&r, "e", &list);
	as_record_set_map(&r, "f", &map);

	as_status rc = aerospike_digest_put(as, &err, NULL, "test", &digest, &r);

	assert_int_eq( rc, AEROSPIKE_OK );
}

TEST( digest_basics_get , "get: (test,test,foo) = {a: 123, b: 'abc', c: 456, d: 'def', e: [1,2,3], f: {x: 7, y: 8, z: 9}}" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", "foo");

	as_record r, *rec = &r;
	as_record_init(&r, 0);

	as_status rc = aerospike_digest_get(as, &err, NULL, "test", &digest, &rec);
    
    assert_int_eq( rc, AEROSPIKE_OK );
    assert_int_eq( as_record_numbins(rec), 6 );
    
    assert_int_eq( as_record_get_int64(rec, "a"), 123 );
    assert_not_null( as_record_get_integer(rec, "a") );
	assert_int_eq( as_integer_toint(as_record_get_integer(rec, "a")), 123 );

    assert_string_eq( as_record_get_str(rec, "b"), "abc" );
	assert_not_null( as_record_get_string(rec, "b") );
	assert_string_eq( as_string_tostring(as_record_get_string(rec, "b")), "abc" );
    
    assert_int_eq( as_record_get_int64(rec, "c"), 456 );
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

    as_record_destroy(rec);
}

TEST( digest_basics_select , "select: (test,test,foo) = {a: 123, b: 'abc'}" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", "foo");

	as_record r, *rec = &r;
	as_record_init(&r, 0);

	const char * bins[3] = { "a", "b", NULL };

	as_status rc = aerospike_digest_select(as, &err, NULL, "test", &digest, bins, &rec);
    
    assert_int_eq( rc, AEROSPIKE_OK );
    assert_int_eq( as_record_numbins(rec), 2 );
    
    assert_int_eq( as_record_get_int64(rec, "a"), 123 );
    assert_not_null( as_record_get_integer(rec, "a") );
	assert_int_eq( as_integer_toint(as_record_get_integer(rec, "a")), 123 );

    assert_string_eq( as_record_get_str(rec, "b"), "abc" );
	assert_not_null( as_record_get_string(rec, "b") );
	assert_string_eq( as_string_tostring(as_record_get_string(rec, "b")), "abc" );
    
    assert_int_eq( as_record_get_int64(rec, "c"), 0 );
    assert_null( as_record_get_integer(rec, "c") );
	assert_null( as_record_get_str(rec, "d") );
	assert_null( as_record_get_string(rec, "d") );
    assert_null( as_record_get_list(rec, "e") );
    assert_null( as_record_get_map(rec, "f") );

    as_record_destroy(rec);
}

TEST( digest_basics_exists , "exists: (test,test,foo)" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", "foo");

	bool exists = false;

	as_status rc = aerospike_digest_exists(as, &err, NULL, "test", &digest, &exists);

    assert_int_eq( rc, AEROSPIKE_OK );
	assert_true( exists );
}

TEST( digest_basics_notexists , "not exists: (test,test,foo)" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", "foo");

	bool exists = false;

	as_status rc = aerospike_digest_exists(as, &err, NULL, "test", &digest, &exists);

    assert_int_eq( rc, AEROSPIKE_OK );
	assert_false( exists );
}

TEST( digest_basics_remove , "remove: (test,test,foo)" ) {

	as_error err;
	as_error_reset(&err);

	as_digest digest;
	as_digest_init(&digest, "test", "foo");

	as_status rc = aerospike_digest_remove(as, &err, NULL, "test", &digest);

    assert_int_eq( rc, AEROSPIKE_OK );
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( digest_basics, "aerospike_key basic tests" ) {
    suite_add( digest_basics_put );
    suite_add( digest_basics_exists );
    suite_add( digest_basics_get );
    suite_add( digest_basics_select );
    suite_add( digest_basics_remove );
    suite_add( digest_basics_notexists );
}
