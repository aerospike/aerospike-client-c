/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_module.h>
#include <aerospike/mod_lua.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/cf_log_internal.h>

#include "log.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize the aerospike object on the stack
 * @returns the initialized aerospike object
 */
aerospike * aerospike_init(aerospike * as, as_config * config) 
{
	as->_free = false;
	as->cluster = NULL;
	if ( config != NULL ) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	// else {
	//     as_config_init(&as->config);
	// }
	return as;
}

/**
 * Creates a new aerospike object on the heap
 * @returns a new aerospike object
 */
aerospike * aerospike_new(as_config * config) 
{
	aerospike * as = (aerospike *) malloc(sizeof(aerospike));
	as->_free = true;
	as->cluster = NULL;
	if ( config != NULL ) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	// else {
	//     as_config_init(&as->config);
	// }
	return as;
}

/**
 * Destroy the aerospike obect
 */
void aerospike_destroy(aerospike * as) {
	as_error err;
	aerospike_close(as, &err);
	if ( as->_free ) {
		free(as);
		as->_free = false;
	}
}

/**
 * Connect to the cluster
 */
as_status aerospike_connect(aerospike * as, as_error * err) 
{
	extern cf_atomic32 g_initialized;
	extern int g_init_pid;

	if ( ! g_initialized ) {
		_log_debug("connecting...");
		
		// remember the process id which is spawning the background threads.
		// only this process can call a pthread_join() on the threads that it spawned.
		g_init_pid = getpid();

	    mod_lua_config config = {
	        .server_mode    = false,
	        .cache_enabled  = as->config.mod_lua.cache_enabled,
	        .system_path    = {0},
	        .user_path      = {0}
	    };
	    memcpy(config.system_path, as->config.mod_lua.system_path, sizeof(config.system_path));
	    memcpy(config.user_path, as->config.mod_lua.user_path, sizeof(config.user_path));

	    if ( mod_lua.logger == NULL ) {
	        mod_lua.logger = test_logger_new();
	    }
	    
	    as_module_configure(&mod_lua, &config);
		_log_debug("as_module_configure(mod_lua) OK");


		// initialize the cluster
		citrusleaf_cluster_init();
		_log_debug("citrusleaf_cluster_init() OK");

		// create the cluster object
    	as->cluster = citrusleaf_cluster_create();
		_log_debug("citrusleaf_cluster_create() OK");

#ifdef DEBUG_HISTOGRAM  
		if ( NULL == (cf_hist = cf_histogram_create("transaction times"))) {
			cf_error("couldn't create histogram for client");	
		}
#endif  

		g_initialized = true;
		
		if ( as->cluster == NULL ) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT, "Can't create client");
			return err->code;
		}

		as->cluster->nbconnect = as->config.nonblocking;

		uint32_t nhosts = sizeof(as->config.hosts) / sizeof(as_config_host);
		
		for ( int i = 0; as->config.hosts[i].addr != NULL && i < nhosts; i ++ ) {
			_log_debug("connecting to %s:%d", as->config.hosts[i].addr, as->config.hosts[i].port);
			int rc = citrusleaf_cluster_add_host(as->cluster, as->config.hosts[i].addr, as->config.hosts[i].port, as->config.policies.timeout);
			if ( rc != 0 ) {
				as_error_update(err, AEROSPIKE_ERR_CLIENT, "Can't connect to %s:%d", as->config.hosts[i].addr, as->config.hosts[i].port);
			}
			else {
				_log_debug("citrusleaf_cluster_add_host() OK");
			}
		}

		if ( err->code == AEROSPIKE_OK ) {
			_log_debug("connected.");
		}
	}
	
	return err->code;
}

/**
 * Close connections to the cluster
 */
as_status aerospike_close(aerospike * as, as_error * err) 
{
	as_error_reset(err);
	
	as_status rc = AEROSPIKE_OK;
	
	// extern as_status aerospike_async_destroy(aerospike * as, as_error * err);
	// rc = rc || aerospike_async_destroy(as, err);

	// extern as_status aerospike_batch_destroy(aerospike * as, as_error * err);
	// rc = rc || aerospike_batch_destroy(as, err);

	extern as_status aerospike_query_destroy(aerospike * as, as_error * err);
	rc = rc || aerospike_query_destroy(as, err);

	extern as_status aerospike_scan_destroy(aerospike * as, as_error * err);
	rc = rc || aerospike_scan_destroy(as, err);

	return rc;
}
