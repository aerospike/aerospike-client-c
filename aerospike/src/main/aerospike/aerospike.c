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
#include <aerospike/as_log.h>
#include <aerospike/as_logger.h>
#include <aerospike/as_module.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/cf_log_internal.h>

#include "_logger.h"
#include "_log.h"


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static aerospike * aerospike_defaults(aerospike * as, bool free, as_config * config)
{
	as->_free = free;
	as->cluster = NULL;
	if ( config != NULL ) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	else {
		as_config_init(&as->config);
	}
	as_log_init(&as->log);
	return as;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize the aerospike object on the stack
 * @returns the initialized aerospike object
 */
aerospike * aerospike_init(aerospike * as, as_config * config) 
{
	if ( !as ) return as;
	return aerospike_defaults(as, false, config);
}

/**
 * Creates a new aerospike object on the heap
 * @returns a new aerospike object
 */
aerospike * aerospike_new(as_config * config) 
{
	aerospike * as = (aerospike *) malloc(sizeof(aerospike));
	if ( !as ) return as;
	return aerospike_defaults(as, true, config);
}

/**
 * Destroy the aerospike instance
 */
void aerospike_destroy(aerospike * as) {
	if ( as->_free ) {
		free(as);
	}
}

/**
 * Connect to the cluster
 */
as_status aerospike_connect(aerospike * as, as_error * err) 
{
	as_error_reset(err);

	extern cf_atomic32 g_initialized;
	extern int g_init_pid;

	if ( g_initialized ) {
		as_debug(LOGGER, "already connected.");
		return AEROSPIKE_OK;
	}

	as_debug(LOGGER, "connecting...");

	// configuration checks
	if ( as->config.hosts[0].addr == NULL ) {
		as_err(LOGGER, "no hosts provided");
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "no hosts provided");
	}

	// remember the process id which is spawning the background threads.
	// only this process can call a pthread_join() on the threads that it spawned.
	g_init_pid = getpid();

    mod_lua_config config = {
        .server_mode    = false,
        .cache_enabled  = as->config.lua.cache_enabled,
        .system_path    = {0},
        .user_path      = {0}
    };
    memcpy(config.system_path, as->config.lua.system_path, sizeof(config.system_path));
    memcpy(config.user_path, as->config.lua.user_path, sizeof(config.user_path));
    
	as_trace(LOGGER, "as_module_configure: ...");
    as_module_configure(&mod_lua, &config);
    mod_lua.logger = aerospike_logger(as);
	as_debug(LOGGER, "as_module_configure: OK");

	// start the cluster tend thread (for all clusters)
	as_trace(LOGGER, "citrusleaf_cluster_init: ...");
	citrusleaf_cluster_init();
	as_debug(LOGGER, "citrusleaf_cluster_init: OK");

	// Hack to stop tender thread interfering with add_host loop ... cluster's
	// tend interval set after loop will govern tending anyway.
	citrusleaf_change_tend_speed(100);

	// create the cluster object
	as_trace(LOGGER, "citrusleaf_cluster_create: ...");
	as->cluster = citrusleaf_cluster_create();
	as_debug(LOGGER, "citrusleaf_cluster_create: OK");

#ifdef DEBUG_HISTOGRAM  
	if ( NULL == (cf_hist = cf_histogram_create("transaction times"))) {
		as_err(LOGGER, "couldn't create histogram for client");
	}
#endif  
	
	if ( as->cluster == NULL ) {
		as_err(LOGGER, "Can't create client");
		citrusleaf_cluster_destroy(as->cluster);
		as->cluster = NULL;
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Can't create client");
	}

	as->cluster->nbconnect = as->config.non_blocking;

	uint32_t nhosts = sizeof(as->config.hosts) / sizeof(as_config_host);

	as_trace(LOGGER, "citrusleaf_cluster_add_host: ...");

	for ( int i = 0; as->config.hosts[i].addr != NULL && i < nhosts; i ++ ) {
		as_trace(LOGGER, "connecting to %s:%d", as->config.hosts[i].addr, as->config.hosts[i].port);

		int rc = citrusleaf_cluster_add_host(as->cluster, as->config.hosts[i].addr, as->config.hosts[i].port, 1000);

		// as long as we succeed with one host, we've found the cluster
		if ( rc == 0 ) {
			as_debug(LOGGER, "citrusleaf_cluster_add_host: OK");
			as_error_reset(err);
			break;
		}

		as_warn(LOGGER, "can't connect to %s:%d", as->config.hosts[i].addr, as->config.hosts[i].port);
		as_error_update(err, AEROSPIKE_ERR_CLIENT, NULL);
	}

	if ( err->code != AEROSPIKE_OK ) {
		as_err(LOGGER, "can't connect to any host");
		citrusleaf_cluster_destroy(as->cluster);
		as->cluster = NULL;
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "can't connect to any host");
	}

	// Set cluster tend interval, now that tend thread can't mess up add_host.
	citrusleaf_cluster_change_tend_speed(as->cluster,
			as->config.tender_interval == 0 ? 1 : (as->config.tender_interval + 999) / 1000);

	as_debug(LOGGER, "connected.");
	g_initialized = true;

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
	rc = rc ? rc : aerospike_query_destroy(as, err);

	extern as_status aerospike_scan_destroy(aerospike * as, as_error * err);
	rc = rc ? rc : aerospike_scan_destroy(as, err);

	if ( as->cluster ) {
		citrusleaf_cluster_destroy(as->cluster);
		as->cluster = NULL;
	}

	if ( mod_lua.logger != NULL ) {
		as_logger_destroy(mod_lua.logger);
		mod_lua.logger = NULL;
	}

	return err->code;
}
