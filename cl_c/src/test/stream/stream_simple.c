
#include "../test.h"
#include "../util/udf.h"
#include "../util/consumer_stream.h"
#include <citrusleaf/as_stream.h>
#include <citrusleaf/as_types.h>
#include <citrusleaf/as_module.h>
#include <citrusleaf/mod_lua.h>
#include <citrusleaf/mod_lua_config.h>
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

TEST( stream_simple_create, "create records" ) {

    int rc = 0;

    const char * ns = "test";
    const char * set = "test";

    int n_recs = 100;

    char * sindex_resp[1] = { NULL };

    // create index on "a"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_a", "a", "STRING", sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, *sindex_resp);
    }

    // create index on "b"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_b", "b", "NUMERIC", sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, *sindex_resp);
    }

    // create index on "c"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_c", "c", "NUMERIC", sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, *sindex_resp);
    }

    // create index on "d"

    rc = citrusleaf_secondary_index_create(cluster, "test", "test", "test_d", "d", "NUMERIC", sindex_resp);
    if ( rc != CITRUSLEAF_OK && rc != CITRUSLEAF_FAIL_INDEX_EXISTS ) {
        info("error(%d): %s", rc, *sindex_resp);
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
        int e = ((i + 1) * b) / 2;

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

        assert_int_eq(rc, 0);
    }

    info("created %d records", n_recs);

}

TEST( stream_simple_1, "get numeric bin without aggregation" ) {

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

TEST( stream_simple_2, "sum of numeric bins" ) {
    
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
    assert_int_eq( as_integer_toint(result), 252500 );

    as_integer_destroy(result);
    as_query_destroy(q);
    as_stream_destroy(consumer);
}


TEST( stream_simple_3, "raj" ) {
    
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

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {

    citrusleaf_query_init();

    // as_aerospike_init(&as, NULL, &test_aerospike_hooks);

    // chris: disabling Lua cache, because it takes too long to prime.
    mod_lua_config_op conf_op = {
        .optype     = MOD_LUA_CONFIG_OP_INIT,
        .arg        = NULL,
        .config     = mod_lua_config_client(false, "modules/mod-lua/src/lua", "modules/mod-lua/src/test/lua")
    }; 

	// Chris (Todo) it is leaking here, Who cleans it
	// up ??
    // chris:   we don't have a destroy for modules (yet)
    //          the reason is we didn't need it in asd.
    as_module_init(&mod_lua);
    as_module_configure(&mod_lua, &conf_op);
 

    int rc = 0;

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
}
