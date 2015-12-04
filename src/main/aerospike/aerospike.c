/*
 * Copyright 2008-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_module.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static aerospike*
aerospike_defaults(aerospike* as, bool free, as_config* config)
{
	as->_free = free;
	as->cluster = NULL;
	if ( config != NULL ) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	else {
		as_config_init(&as->config);
	}
	as_policies_resolve(&as->config.policies);
	return as;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize the aerospike object on the stack
 * @returns the initialized aerospike object
 */
aerospike*
aerospike_init(aerospike* as, as_config* config)
{
	if ( !as ) return as;
	return aerospike_defaults(as, false, config);
}

/**
 * Creates a new aerospike object on the heap
 * @returns a new aerospike object
 */
aerospike*
aerospike_new(as_config* config)
{
	aerospike * as = (aerospike *) malloc(sizeof(aerospike));
	if ( !as ) return as;
	return aerospike_defaults(as, true, config);
}

/**
 * Destroy the aerospike instance
 */
void aerospike_destroy(aerospike* as)
{
	if ( as->_free ) {
		free(as);
	}
}

/**
 * Connect to the cluster
 */
as_status
aerospike_connect(aerospike* as, as_error* err)
{
	as_error_reset(err);

	// This is not 100% bulletproof against, say, simultaneously calling
	// aerospike_connect() from two different threads with the same as object...
	if ( as->cluster ) {
		return AEROSPIKE_OK;
	}

	// configuration checks
	if ( as->config.hosts[0].addr == NULL ) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No hosts provided");
	}

#if !defined USE_XDR
    mod_lua_config config = {
        .server_mode    = false,
        .cache_enabled  = as->config.lua.cache_enabled,
        .system_path    = {0},
        .user_path      = {0}
    };
    memcpy(config.system_path, as->config.lua.system_path, sizeof(config.system_path));
    memcpy(config.user_path, as->config.lua.user_path, sizeof(config.user_path));
    
    as_module_configure(&mod_lua, &config);
#endif
	
	// Create the cluster object.
	return as_cluster_create(&as->config, err, &as->cluster);
}

/**
 * Close connections to the cluster
 */
as_status
aerospike_close(aerospike* as, as_error* err)
{
	as_error_reset(err);

	// This is not 100% bulletproof against simultaneous aerospike_close() calls
	// from different threads.
	if ( as->cluster ) {
		as_cluster_destroy(as->cluster);
		as->cluster = NULL;
	}

	return err->code;
}

bool
aerospike_cluster_is_connected(aerospike* as)
{
	return as_cluster_is_connected(as->cluster);
}
