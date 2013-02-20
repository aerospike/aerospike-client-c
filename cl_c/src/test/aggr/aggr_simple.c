
#include "../test.h"
#include <citrusleaf/as_stream.h>
#include <citrusleaf/as_types.h>
#include <citrusleaf/as_module.h>
#include <citrusleaf/mod_lua.h>
#include <citrusleaf/mod_lua_config.h>
#include <citrusleaf/cl_query.h>
#include <limits.h>
#include <stdlib.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_aerospike as;



as_stream_status list_stream_write(const as_stream * s, const as_val * v) {
    as_list * l = (as_list *) as_stream_source(s);
    if ( v != NULL ) {
        as_list_append(l, (as_val *) v);
    }
    return AS_STREAM_OK;
}

const as_stream_hooks list_stream_hooks = {
    .read   = NULL,
    .write  = list_stream_write
};

as_stream * list_stream_new(as_list * l) {
    return as_stream_new(l, &list_stream_hooks);
}



// extern as_rec * map_rec_new();
// extern as_stream * rec_stream_new();
// extern as_stream * integer_stream_new(uint32_t start, uint32_t end);
// extern as_stream * list_stream_new(as_list * l);
// extern uint32_t stream_pipe(as_stream * istream, as_stream * ostream);

static int test_log(const as_aerospike * as, const char * file, const int line, const int level, const char * msg) {
    char l[10] = {'\0'};
    switch(level) {
        case 1:
            strncpy(l,"WARN",10);
            break;
        case 2:
            strncpy(l,"INFO",10);
            break;
        case 3:
            strncpy(l,"DEBUG",10);
            break;
        default:
            strncpy(l,"TRACE",10);
            break;
    }
    atf_log_line(stderr, l, ATF_LOG_PREFIX, file, line, msg);
    return 0;
}

static const as_aerospike_hooks test_aerospike_hooks = {
    .destroy = NULL,
    .rec_create = NULL,
    .rec_update = NULL,
    .rec_remove = NULL,
    .rec_exists = NULL,
    .log = test_log,
};

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( aggr_simple_1, "get numeric bin without aggregation" ) {
 

    extern cl_cluster * cluster;

    as_list * results = as_arraylist_new(320,32);
    as_stream * rstream = list_stream_new(results);

    as_query * q = as_query_new("test","test");
    as_query_select(q, "b");
    as_query_where(q, "a", string_equals("abc"));
    
    citrusleaf_query_execute(cluster, q, rstream);

    as_iterator i;
    as_list_iterator_init(&i, results);
    while ( as_iterator_has_next(&i) ) {
        as_val * val = as_iterator_next(&i);
        info("result: %s", as_val_tostring(val));
		// Chris(todo) val is leaking here
    }

	// Chris(todo) results and rstream leaking memory here
    as_iterator_destroy(&i);
	as_query_destroy(q);


}

TEST( aggr_simple_2, "sum of numeric bins" ) {
 

    extern cl_cluster * cluster;

    as_list * results = as_arraylist_new(320,32);
    as_stream * rstream = list_stream_new(results);

    as_query * q = as_query_new("test","test");
	// Chris (Todo) This is not needed selectivity has
	// not meaning for aggregation
    // as_query_select(q, "b");
    as_query_where(q, "a", string_equals("abc"));
    as_query_aggregate(q, "aggr", "sum", NULL);
    
    citrusleaf_query_execute(cluster, q, rstream);

    as_iterator i;
    as_list_iterator_init(&i, results);
    while ( as_iterator_has_next(&i) ) {
        as_val * val = as_iterator_next(&i);
        info("result: %s", as_val_tostring(val));
		// Chris(todo) val is leaking here
    }

	// Chris(todo) results and rstream leaking memory here
    as_iterator_destroy(&i);
	as_query_destroy(q);


}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

static bool before(atf_suite * suite) {

    citrusleaf_query_init();

    as_aerospike_init(&as, NULL, &test_aerospike_hooks);

    mod_lua_config_op conf_op = {
        .optype     = MOD_LUA_CONFIG_OP_INIT,
        .arg        = NULL,
        .config     = mod_lua_config_client(true, "modules/mod-lua/src/lua", "modules/mod-lua/src/test/lua")
    }; 

	// Chris (Todo) it is leaking here, Who cleans it
	// up ??
    as_module_init(&mod_lua);
    as_module_configure(&mod_lua, &conf_op);
 
    return true;
}

static bool after(atf_suite * suite) {


    citrusleaf_query_shutdown();
    return true;
}

SUITE( aggr_simple, "aggregate simple" ) {
    suite_before( before );
    suite_after( after );
    
    suite_add( aggr_simple_1 );
    suite_add( aggr_simple_2 );
}
