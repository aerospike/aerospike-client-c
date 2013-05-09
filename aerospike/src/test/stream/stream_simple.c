
#include "../test.h"
#include "../util/udf.h"
#include "../util/consumer_stream.h"
#include "../util/test_logger.h"

#include <aerospike/as_stream.h>
#include <aerospike/as_types.h>
#include <aerospike/as_module.h>

#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include <citrusleaf/cl_query.h>
#include <citrusleaf/citrusleaf.h>

#include <limits.h>
#include <stdlib.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define LUA_FILE "src/test/lua/client_stream_simple.lua"
#define UDF_FILE "client_stream_simple"

/******************************************************************************
 * VARAIBLES
 *****************************************************************************/
     
extern cl_cluster * cluster;

/******************************************************************************
 * TEST CASES
 *****************************************************************************/
 
TEST( stream_simple_exists, UDF_FILE" exists" ) {
    int rc = udf_exists(LUA_FILE);
    assert_int_eq( rc, 0 );
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
TEST( stream_simple_create, "create 100 records and 4 indices" ) {

    int rc = 0;

    const char * ns = "test";
    const char * set = "test";

    int n_recs = 100;

    char * sindex_resp = NULL;

    // create index on "a"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_a", "a", "STRING", &sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, sindex_resp);
    }

    if ( sindex_resp ) {
        free(sindex_resp);
        sindex_resp = NULL;
    }

    // create index on "b"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_b", "b", "NUMERIC", &sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, sindex_resp);
    }
    
    if ( sindex_resp ) {
        free(sindex_resp);
        sindex_resp = NULL;
    }

    // create index on "c"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_c", "c", "NUMERIC", &sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, sindex_resp);
    }
    
    if ( sindex_resp ) {
        free(sindex_resp);
        sindex_resp = NULL;
    }

    // create index on "d"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_d", "d", "NUMERIC", &sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, sindex_resp);
    }
    
    if ( sindex_resp ) {
        free(sindex_resp);
        sindex_resp = NULL;
    }

    // insert records

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    cl_object okey;
    cl_bin bins[5];
    strcpy(bins[0].bin_name, "a");
    strcpy(bins[1].bin_name, "b");
    strcpy(bins[2].bin_name, "c");
    strcpy(bins[3].bin_name, "d");
    strcpy(bins[4].bin_name, "e");

    for ( int i = 0; i < n_recs; i++ ) {

        int nbins = 5;

        char        key[64] = { '\0' };
        
        const char * a = "abc";
        int b = n_recs;
        int c = i;
        int d = i % 10;
        int e = b + (c + 1) * (d + 1) / 2;

        snprintf(key, 64, "%s-%d-%d-%d-%d", a, b, c, d, e);

        citrusleaf_object_init_str(&okey, key);
        citrusleaf_object_init_str(&bins[0].object, a);
        citrusleaf_object_init_int(&bins[1].object, b);
        citrusleaf_object_init_int(&bins[2].object, c);
        citrusleaf_object_init_int(&bins[3].object, d);
        citrusleaf_object_init_int(&bins[4].object, e);

        rc = citrusleaf_put(cluster, ns, set, &okey, bins, nbins, &wp);

        assert_int_eq(rc, 0);

        cl_bin *    rbins = NULL;
        int         nrbins = 0;
        uint32_t    rgen = 0;

        rc = citrusleaf_get_all(cluster, "test", "test", &okey, &rbins, &nrbins, 1000, &rgen);

        if (rbins) {
            citrusleaf_bins_free(rbins, nrbins);
            free(rbins);
        }

        assert_int_eq(rc, 0);
    }
}

TEST( stream_simple_1, "count(*) where a == 'abc' (non-aggregating)" ) {

    int rc = 0;
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

    as_query * q = as_query_new("test", "test");
    as_query_select(q, "c");
    as_query_where(q, "a", string_equals("abc"));
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    assert_int_eq( rc, 0 );
    assert_int_eq( count, 100 );

    as_query_destroy(q);
    as_stream_destroy(consumer);
}

TEST( stream_simple_2, "count(*) where a == 'abc' (aggregating)" ) {

    int rc = 0;

    as_integer * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            if ( result != NULL ) as_val_destroy(result);
            result = as_integer_fromval(v);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query * q = as_query_new("test", "test");
    as_query_where(q, "a", string_equals("abc"));
    as_query_aggregate(q, UDF_FILE, "count", NULL);
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    if ( rc ) {
        error("error: %d", rc);
    }
    else {
        info("result: %d", as_integer_toint(result) );
    }

    assert_int_eq( rc, 0 );
    assert_not_null( result );
    assert_int_eq( as_integer_toint(result), 100 );

    as_integer_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}

TEST( stream_simple_3, "sum(e) where a == 'abc'" ) {
    
    int rc = 0;

    as_integer * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            if ( result != NULL ) as_val_destroy(result);
            result = as_integer_fromval(v);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_query * q = as_query_new("test", "test");
    as_query_where(q, "a", string_equals("abc"));
    as_query_aggregate(q, UDF_FILE, "sum", NULL);
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    if ( rc ) {
        error("error: %d", rc);
    }
    else {
        info("result: %d", as_integer_toint(result) );
    }

    assert_int_eq( rc, 0 );
    assert_not_null( result );
    assert_int_eq( as_integer_toint(result), 24275 );

    as_integer_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}


TEST( stream_simple_4, "sum(d) where b == 100 and d == 1" ) {
    
    int rc = 0;

    as_integer * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            result = as_integer_fromval(v);
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);

    as_list * args = as_arraylist_new(2,0);
    as_list_add_string(args, "d");
    as_list_add_integer(args, 1);

    as_query * q = as_query_new("test", "test");
    as_query_where(q, "b", integer_equals(100));
    as_query_aggregate(q, UDF_FILE, "sum_on_match", args);
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    if ( rc ) {
        error("error: %d", rc);
    }
    else {
        info("result: %d", as_integer_toint(result) );
    }

    assert_int_eq( rc, 0 );
    assert_not_null( result );
    assert_int_eq( as_integer_toint(result), 10 );

    as_integer_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}

TEST( stream_simple_5, "c where b == 100 group by d" ) {
    
    int rc = 0;

    as_val * result = NULL;

    as_stream_status consume(as_val * v) {
        if ( v != AS_STREAM_END ) {
            result = v;
        }
        return AS_STREAM_OK;
    }

    as_stream * consumer = consumer_stream_new(consume);


    as_query * q = as_query_new("test", "test");
    as_query_where(q, "b", integer_equals(100));
    as_query_aggregate(q, UDF_FILE, "grouping", NULL);
    
    rc = citrusleaf_query_stream(cluster, q, consumer);

    if ( rc ) {
        error("error: %d", rc);
    }
    else {
        char * s = as_val_tostring(result);
        info("result: %s", s );
        free(s);
    }

    assert_int_eq( rc, 0 );
    assert_not_null( result );
    assert_int_eq( as_val_type(result), AS_MAP );

    as_val_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {

    citrusleaf_query_init();
 
    int rc = 0;

    mod_lua_config config = {
        .server_mode    = false,
        .cache_enabled  = false,
        .system_path    = "../aerospike-mod-lua/src/lua",
        .user_path      = "src/test/lua"
    };

    if ( mod_lua.logger == NULL ) {
        mod_lua.logger = test_logger_new();
    }
        
    rc = as_module_configure(&mod_lua, &config);

    if ( rc != 0 ) {
        error("as_module_configure failed: %d", rc);
        return false;
    }



    rc = udf_put(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while uploading: %s (%d)", LUA_FILE, rc);
        return false;
    }

    rc = udf_exists(LUA_FILE);
    if ( rc != 0 ) {
        error("lua file does not exist: %s (%d)", LUA_FILE, rc);
        return false;
    }


    return true;
}

static bool after(atf_suite * suite) {
    
    if ( mod_lua.logger ) {
        free(mod_lua.logger);
        mod_lua.logger = NULL;
    }

    citrusleaf_query_shutdown();

    int rc = udf_remove(LUA_FILE);
    if ( rc != 0 ) {
        error("failure while removing: %s (%d)", LUA_FILE, rc);
        return false;
    }

    return true;
}

SUITE( stream_simple, "simple stream" ) {
    suite_before( before );
    suite_after( after   );
    
    suite_add( stream_simple_create );
    suite_add( stream_simple_1 );
    suite_add( stream_simple_2 );
    suite_add( stream_simple_3 );
    suite_add( stream_simple_4 );
    suite_add( stream_simple_5 );
}
