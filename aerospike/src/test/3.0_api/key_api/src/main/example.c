#include <stdio.h>

#ifndef LUA_MODULE_PATH
#define LUA_MODULE_PATH "../../../../../examples/udf_examples/rec_udf/src/lua"
#endif

//********************************************************
// Citrusleaf Core Foundation Includes
//********************************************************
// #include <citrusleaf/cf_random.h>
// #include <citrusleaf/cf_atomic.h>
// #include <citrusleaf/cf_hist.h>

//********************************************************
// Citrusleaf Mod-Lua Includes
//********************************************************

// // as_val types (persistable)
// #include <aerospike/as_val.h>
// #include <aerospike/as_nil.h>
// #include <aerospike/as_boolean.h>
// #include <aerospike/as_integer.h>
// #include <aerospike/as_bytes.h>
// #include <aerospike/as_string.h>
// #include <aerospike/as_list.h>
// #include <aerospike/as_map.h>

// // as_val types (non-persisted)
// #include <aerospike/as_pair.h>
// #include <aerospike/as_rec.h>

// // implementations
// #include <aerospike/as_arraylist.h>
// #include <aerospike/as_linkedlist.h>
// #include <aerospike/as_hashmap.h>

// // others
// #include <aerospike/as_result.h>
// #include <aerospike/as_stream.h>
// #include <aerospike/as_aerospike.h>
// #include <aerospike/as_buffer.h>
// #include <aerospike/as_module.h>
// #include <aerospike/as_logger.h>
// #include <aerospike/as_serializer.h>

// // lua module
// #include <aerospike/mod_lua.h>
// #include <aerospike/mod_lua_config.h>

//********************************************************
// Citrusleaf Client Includes
//********************************************************

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include <citrusleaf/cf_log_internal.h>

void test_get(aerospike * as, as_error * err) {
    cf_debug("################################################################################");

    as_record * rec = NULL;

    if ( aerospike_key_get(as, err, NULL, "test", "demo", "foo", &rec) != AEROSPIKE_OK ) {
        cf_info("error(%d) - %s [%s:%d]", err->code, err->message, err->file, err->line);
    }
    else {
        cf_info("get succeeded: %d", rec->bins.size);
        
        cf_debug("a = %d", as_record_get_integer(rec, "a"));
        cf_debug("b = %d", as_record_get_integer(rec, "b"));
        cf_debug("c = %s", as_record_get_string(rec, "c"));
        cf_debug("m = %d", as_record_get_integer(rec, "m"));
        cf_debug("n = %s", as_record_get_string(rec, "n"));

        as_record_destroy(rec);
    }
}

void test_select(aerospike * as, as_error * err) {
    cf_debug("################################################################################");
    
    as_record * rec = NULL;

    const char * select[] = {"a","b","c","m","n",NULL};
    
    if ( aerospike_key_select(as, err, NULL, "test", "demo", "foo", select, &rec) != AEROSPIKE_OK ) {
        cf_info("error(%d) - %s [%s:%d]", err->code, err->message, err->file, err->line);
    }
    else {
        cf_info("select succeeded: %d", rec->bins.size);

        cf_debug("a = %d", as_record_get_integer(rec, "a"));
        cf_debug("b = %d", as_record_get_integer(rec, "b"));
        cf_debug("c = %s", as_record_get_string(rec, "c"));
        cf_debug("m = %d", as_record_get_integer(rec, "m"));
        cf_debug("n = %s", as_record_get_string(rec, "n"));

        as_record_destroy(rec);
    }

}

void test_put(aerospike * as, as_error * err) {
    cf_debug("################################################################################");
    
    as_record * rec = as_record_new(2);
    as_record_set_integer(rec, "m", 321);
    as_record_set_string(rec, "n", "cba");

    if ( aerospike_key_put(as, err, NULL, "test", "demo", "foo", rec) != AEROSPIKE_OK ) {
        cf_info("error(%d): %s [%s:%d]", err->code, err->message, err->file, err->line);
    }
    else {
        cf_info("put succeeded");
    }

    as_record_destroy(rec);
}

void test_remove(aerospike * as, as_error * err) {
    cf_debug("################################################################################");
    
    if ( aerospike_key_remove(as, err, NULL, "test", "demo", "foo") != AEROSPIKE_OK ) {
        cf_info("error(%d): %s [%s:%d]", err->code, err->message, err->file, err->line);
    }
    else {
        cf_info("record removed");
    }
}

void test_exists(aerospike * as, as_error * err) {
    cf_debug("################################################################################");
    
    bool exists = false;
    if ( aerospike_key_exists(as, err, NULL, "test", "demo", "foo", &exists) != AEROSPIKE_OK ) {
        cf_info("error(%d): %s [%s:%d]", err->code, err->message, err->file, err->line);
    }
    else {
        cf_info("record exists: %s", exists ? "true" : "false");
    }
}



int main(int argc, char **argv) {

    cf_set_log_level(CF_DEBUG);

    as_config config = {
        .nonblocking = false,
        .hosts[0] = { .addr = "127.0.0.1", .port = 3000 }
    };

    as_error err;
    as_error_reset(&err);

    aerospike as;
    aerospike_init(&as, &config);
    aerospike_connect(&as);
    
    test_exists(&as, &err);
    test_put(&as, &err);
    test_exists(&as, &err);
    test_get(&as, &err);
    test_exists(&as, &err);
    test_select(&as, &err);
    // test_exists(&as, &err);
    test_remove(&as, &err);
    test_exists(&as, &err);
    
    aerospike_close(&as);
    aerospike_destroy(&as);

    return 0;
}
