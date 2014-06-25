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
#include <aerospike/as_cluster.h>
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

	// This is not 100% bulletproof against, say, simultaneously calling
	// aerospike_connect() from two different threads with the same as object...
	if ( as->cluster ) {
		as_debug(LOGGER, "already connected.");
		return AEROSPIKE_OK;
	}

	as_debug(LOGGER, "connecting...");

	// configuration checks
	if ( as->config.hosts[0].addr == NULL ) {
		as_err(LOGGER, "no hosts provided");
		return as_error_update(err, AEROSPIKE_ERR_CLUSTER, "no hosts provided");
	}

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
	
	// Create the cluster object.
	as->cluster = as_cluster_create(&as->config);
	
	if (! as->cluster) {
		as_err(LOGGER, "failed to initialize cluster");
		return as_error_update(err, AEROSPIKE_ERR_CLUSTER, "failed to initialize cluster");
	}
	
	return err->code;
}

/**
 * Close connections to the cluster
 */
as_status aerospike_close(aerospike * as, as_error * err) 
{
	as_error_reset(err);

	// This is not 100% bulletproof against simultaneous aerospike_close() calls
	// from different threads.
	if ( as->cluster ) {
		as_cluster_destroy(as->cluster);
		as->cluster = NULL;
	}

	if ( mod_lua.logger != NULL ) {
		as_logger_destroy(mod_lua.logger);
		mod_lua.logger = NULL;
	}

	return err->code;
}
