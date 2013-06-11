#include <errno.h>

#include <aerospike/aerospike.h>
#include <citrusleaf/cf_log.h>

#include "../main/aerospike/log.h"
#include "test.h"
#include "util/info_util.h"

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 1000

#define SCRIPT_LEN_MAX 1048576

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

aerospike * as = NULL;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static void citrusleaf_log_callback(cf_log_level level, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
	switch(level) {
		case CF_ERROR: 
			atf_logv(stderr, "ERROR", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		case CF_WARN: 
			atf_logv(stderr, "WARN", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		case CF_INFO: 
			atf_logv(stderr, "INFO", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		case CF_DEBUG: 
			atf_logv(stderr, "DEBUG", ATF_LOG_PREFIX, NULL, 0, fmt, ap);
			break;
		default:
			break;
	}
    va_end(ap);
}

static bool before(atf_plan * plan) {

	// cf_set_log_level(CF_DEBUG);
	cf_set_log_callback(citrusleaf_log_callback);

    if ( as ) {
        error("aerospike was already initialized");
        return false;
    }

    as_config config = {
        .nonblocking = false,
        .hosts = { 
        	{ .addr = HOST, .port = PORT },
        	{ 0 }
        }
    };

    as_policies_init(&config.policies);

	as_error err;
	as_error_reset(&err);

	as = aerospike_new(&config);
	
	if ( aerospike_connect(as, &err) == AEROSPIKE_OK ) {
		info("connected to %s:%d", HOST, PORT);
    	return true;
	}
	else {
		error("%s @ %s[%s:%d]", err.message, err.func, err.file, err.line);
		return false;
	}
}

static bool after(atf_plan * plan) {

    if ( ! as ) {
        error("aerospike was not initialized");
        return false;
    }

	as_error err;
	as_error_reset(&err);
	
	if ( aerospike_close(as, &err) == AEROSPIKE_OK ) {
		info("disconnected from %s:%d", HOST, PORT);
    	return true;
	}
	else {
		error("%s @ %s[%s:%d]", HOST, PORT, err.message, err.func, err.file, err.line);
		return false;
	}
	
	aerospike_destroy(as);
	
	as = NULL;

    return true;
}

/******************************************************************************
 * TEST PLAN
 *****************************************************************************/

PLAN( aerospike_test ) {

    plan_before( before );
    plan_after( after );
    
    // aerospike_key module
    plan_add( key_basics );
    plan_add( key_apply );

    // aerospike_digest module
    plan_add( digest_basics );
    plan_add( digest_apply );

    // aerospike_info module
    plan_add( info_basics );

    // aerospike_info module
    plan_add( udf_basics );

    // aerospike_query module
    plan_add( query_stream );
    // plan_add( query_foreach );

}

