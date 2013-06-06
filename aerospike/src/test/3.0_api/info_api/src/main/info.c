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
#include <aerospike/aerospike_info.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include <citrusleaf/cf_log_internal.h>

static bool aerospike_foreach_callback_fn(const as_error * err, const char * node, char * res, void * udata)
{
  // TODO : Write a call-back handler function to pass
	// Potentially with a udf callback
	return TRUE;
}


void get_node_info_for_each(aerospike * as, as_error * err) {
    cf_debug("################################################################################");
    void *udata;
    as_status rc = aerospike_info_foreach(as, err, NULL, NULL,
        udata, aerospike_foreach_callback_fn);

    if(rc != AEROSPIKE_OK) {
       	cf_info("error(%d): %s [%s:%d]", err->code, err->message, err->file, err->line);
    }else{
       	 cf_info("aerospike_info_foreach : success error(%d): %s [%s:%d] \n", err->code, err->message, err->file, err->line );
    }

}

void get_node_info(aerospike * as, as_error * err) {
    cf_debug("################################################################################");
    
    bool exists = false;
    char    query[512]  = {0};
    char *  result      = NULL;
    as_status rc = aerospike_info_node(as, err, NULL, query, NULL, &result);

    if(rc != AEROSPIKE_OK) {
    	cf_info("error(%d): %s [%s:%d]", err->code, err->message, err->file, err->line);
    }else{
    	 cf_info("aerospike_info_node : success error(%d): %s [%s:%d] \n", err->code, err->message, err->file, err->line );
    	 cf_info(" Result obtained : \n", result);
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
    
    get_node_info(&as, &err);
    get_node_info_for_each(&as, &err);

    aerospike_close(&as);
    aerospike_destroy(&as);

    return 0;
}
