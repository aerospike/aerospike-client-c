
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_index.h>

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>

#include <aerospike/mod_lua.h>

#include "../test.h"
#include "../util/udf.h"
#include "../util/consumer_stream.h"
#include "../util/test_logger.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike * as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/client_stream_simple.lua"
#define UDF_FILE "client_stream_simple"

#define NAMESPACE "test"
#define SET "test"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool before(atf_suite * suite) {

    if ( mod_lua.logger == NULL ) {
        mod_lua.logger = test_logger_new();
    }

    if ( ! udf_put(LUA_FILE) ) {
        error("failure while uploading: %s", LUA_FILE);
        return false;
    }

    if ( ! udf_exists(LUA_FILE) ) {
        error("lua file does not exist: %s", LUA_FILE);
        return false;
    }


    return true;
}

static bool after(atf_suite * suite) {
    
    if ( mod_lua.logger ) {
        free(mod_lua.logger);
        mod_lua.logger = NULL;
    }

    if ( ! udf_remove(LUA_FILE) ) {
        error("failure while removing: %s", LUA_FILE);
        return false;
    }

    return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( query_stream_exists, UDF_FILE" exists" ) {
    assert_true( udf_exists(LUA_FILE) );
}

/**
 * Creates 100 records and 4 indices.
 *
 * Records are structured as:
 *      {a: String, b: Integer, c: Integer, d: Integer, e: Integer}
 *
 * The key is "a-b-c-d-e"
 *
 * The values are:
 *      a = "abc"
 *      b = 100
 *      c = <current index>
 *      d = c % 10
 *      e = b + (c + 1) * (d + 1) / 2
 */
TEST( query_stream_create, "create 100 records and 4 indices" ) {

	as_error err;
	as_error_reset(&err);

    int n_recs = 100;

    // create index on "a"

    aerospike_index_sparse_string_create(as, &err, NULL, NAMESPACE, SET, "a", "idx_test_a");
    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND ) {
        info("error(%d): %s", err.code, err.message);
    }

    // create index on "b"

    aerospike_index_sparse_integer_create(as, &err, NULL, NAMESPACE, SET, "b", "idx_test_b");
    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND ) {
        info("error(%d): %s", err.code, err.message);
    }

    // create index on "c"

    aerospike_index_sparse_integer_create(as, &err, NULL, NAMESPACE, SET, "c", "idx_test_c");
    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND ) {
        info("error(%d): %s", err.code, err.message);
    }

    // create index on "d"

    aerospike_index_sparse_integer_create(as, &err, NULL, NAMESPACE, SET, "d", "idx_test_d");
    if ( err.code != AEROSPIKE_OK && err.code != AEROSPIKE_ERR_INDEX_FOUND ) {
        info("error(%d): %s", err.code, err.message);
    }

    // insert records
    for ( int i = 0; i < n_recs; i++ ) {

        char * 	a = "abc";
        int 	b = n_recs;
        int 	c = i;
        int 	d = i % 10;
        int 	e = b + (c + 1) * (d + 1) / 2;

		char keystr[64] = { '\0' };
		snprintf(keystr, 64, "%s-%d-%d-%d-%d", a, b, c, d, e);

		as_record r;
		as_record_init(&r, 5);
		as_record_set_str(&r, 	"a", a);
		as_record_set_int64(&r, "b", b);
		as_record_set_int64(&r, "c", c);
		as_record_set_int64(&r, "d", d);
		as_record_set_int64(&r, "e", e);

		as_key key;
		as_key_init(&key, NAMESPACE, SET, keystr);

        aerospike_key_put(as, &err, NULL, &key, &r);

        assert_int_eq( err.code, AEROSPIKE_OK);

        as_record_destroy(&r);

        bool exists = false;

        aerospike_key_exists(as, &err, NULL, &key, &exists);
        
		as_key_destroy(&key);

        assert_int_eq( err.code, AEROSPIKE_OK );
        assert_true( exists );
    }
}

TEST( query_stream_1, "count(*) where a == 'abc' (non-aggregating)" ) {

	as_error err;
	as_error_reset(&err);

    int count = 0;

    as_stream_status consume(as_val * v) {
        if ( v == AS_STREAM_END ) {
            info("count: %d",count);
        }
        else {
            count++;
            as_val_destroy(v);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query q;
    as_query_init(&q, NAMESPACE, SET);
    as_query_select(&q, "c");
    as_query_where(&q, "a", string_equals("abc"));
    
    aerospike_query_stream(as, &err, NULL, &q, consumer);

    assert_int_eq( err.code, 0 );
    assert_int_eq( count, 100 );

    as_query_destroy(&q);
    as_stream_destroy(consumer);
}

TEST( query_stream_2, "count(*) where a == 'abc' (aggregating)" ) {

	as_error err;
	as_error_reset(&err);

    int64_t count = 0;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            as_integer * i = as_integer_fromval(v);
            count = i ? as_integer_toint(i) : 0;
            as_val_destroy(v);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query q;
    as_query_init(&q, NAMESPACE, SET);
    as_query_where(&q, "a", string_equals("abc"));
    as_query_apply(&q, UDF_FILE, "count", NULL);
    
    aerospike_query_stream(as, &err, NULL, &q, consumer);

	info("count: %d",count);
    
    assert_int_eq( err.code, 0 );
    assert_int_eq( count, 100 );


    as_query_destroy(&q);
    as_stream_destroy(consumer);
}

TEST( query_stream_3, "sum(e) where a == 'abc'" ) {
    
	as_error err;
	as_error_reset(&err);

    int64_t value = 0;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            as_integer * result = as_integer_fromval(v);
            if ( result != NULL ) {
            	value = as_integer_toint(result);
            }
            as_val_destroy(result);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);
    
    as_query q;
    as_query_init(&q, NAMESPACE, SET);
    as_query_where(&q, "a", string_equals("abc"));
    as_query_apply(&q, UDF_FILE, "sum", NULL);

    aerospike_query_stream(as, &err, NULL, &q, consumer);

    info("value: %d", value);

    assert_int_eq( err.code, 0 );
    assert_int_eq( value, 24275 );

    as_query_destroy(&q);
    as_stream_destroy(consumer);
}


TEST( query_stream_4, "sum(d) where b == 100 and d == 1" ) {
    
	as_error err;
	as_error_reset(&err);

    int64_t value = 0;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            as_integer * result = as_integer_fromval(v);
            if ( result != NULL ) {
            	value = as_integer_toint(result);
            }
            as_val_destroy(result);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_arraylist args;
    as_arraylist_init(&args, 2,0);
    as_arraylist_append_str(&args, "d");
    as_arraylist_append_int64(&args, 1);

    as_query q;
    as_query_init(&q, NAMESPACE, SET);
    as_query_where(&q, "b", integer_equals(100));
    as_query_apply(&q, UDF_FILE, "sum_on_match", &args);

    aerospike_query_stream(as, &err, NULL, &q, consumer);

    info("value: %d", value);

    assert_int_eq( err.code, 0 );
    assert_int_eq( value, 10 );

    as_query_destroy(&q);
    as_stream_destroy(consumer);
}

TEST( query_stream_5, "c where b == 100 group by d" ) {
    
	as_error err;
	as_error_reset(&err);

    as_val * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            result = v;
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query q;
    as_query_init(&q, "test", "test");
    as_query_where(&q, "b", integer_equals(100));
    as_query_apply(&q, UDF_FILE, "grouping", NULL);
    
    aerospike_query_stream(as, &err, NULL, &q, consumer);

	if (result) {
        char * s = as_val_tostring(result);
	    info("value: %s", s );
    	free(s);
	}

    assert_int_eq( err.code, 0 );
	assert_int_eq( as_val_type(result), AS_MAP );

	as_val_destroy(result);
    as_query_destroy(&q);
    as_stream_destroy(consumer);
}

TEST( query_stream_6, "c where d in range(4,6) groupby d") {

        as_error err;
        as_error_reset(&err);

    as_val * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            result = v;
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query q;
    as_query_init(&q, "test", "test");
    as_query_where(&q, "d", integer_range(4, 6));
    as_query_apply(&q, UDF_FILE, "grouping", NULL);

    aerospike_query_stream(as, &err, NULL, &q, consumer);

        if (result) {
        char * s = as_val_tostring(result);
            info("value: %s", s );
        free(s);
        }

    assert_int_eq( err.code, 0 );
        assert_int_eq( as_val_type(result), AS_MAP );

        as_val_destroy(result);
    as_query_destroy(&q);
    as_stream_destroy(consumer);
}



/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( query_stream, "aerospike_query_stream tests" ) {

    suite_before( before );
    suite_after( after   );
    
    suite_add( query_stream_create );
    suite_add( query_stream_1 );
    suite_add( query_stream_2 );
    // suite_add( query_stream_3 );
    // suite_add( query_stream_4 );
    // suite_add( query_stream_5 );
    // suite_add( query_stream_6 );
}
