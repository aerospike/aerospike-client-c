/*
 * Copyright 2008-2018 Aerospike, Inc.
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
#include <aerospike/as_info.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_module.h>
#include <aerospike/as_string_builder.h>
#include <aerospike/as_tls.h>
#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <pthread.h>

#if defined(_MSC_VER)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
static bool library_initialized = false;
#endif

pthread_mutex_t init_lock = PTHREAD_MUTEX_INITIALIZER;

extern uint32_t as_event_loop_capacity;
static bool lua_initialized = false;

void
as_config_destroy(as_config* config);

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static aerospike*
aerospike_defaults(aerospike* as, bool free, as_config* config)
{
	as->_free = free;
	as->cluster = NULL;

	if (config) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	else {
		as_config_init(&as->config);
	}
	return as;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
aerospike_library_init(as_error* err)
{
#if defined(_MSC_VER)
	// Aerospike library initialization on windows.
	pthread_mutex_lock(&init_lock);

	if (!library_initialized) {
		if (!cf_clock_init()) {
			pthread_mutex_unlock(&init_lock);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "cf_clock_init() failed");
		}

#if defined(AS_USE_LIBEVENT)
		int evthread_use_windows_threads();
		if (evthread_use_windows_threads() == -1) {
			pthread_mutex_unlock(&init_lock);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "evthread_use_windows_threads() failed");
		}
#endif
		library_initialized = true;
	}
	pthread_mutex_unlock(&init_lock);
#endif
	return AEROSPIKE_OK;
}

/**
 * Initialize the aerospike object on the stack
 * @returns the initialized aerospike object
 */
aerospike*
aerospike_init(aerospike* as, as_config* config)
{
	return aerospike_defaults(as, false, config);
}

/**
 * Creates a new aerospike object on the heap
 * @returns a new aerospike object
 */
aerospike*
aerospike_new(as_config* config)
{
	aerospike* as = cf_malloc(sizeof(aerospike));

	if (!as) {
		as_config_destroy(config);
		return as;
	}
	return aerospike_defaults(as, true, config);
}

/**
 * Initialize global lua configuration.
 */
void
aerospike_init_lua(as_config_lua* config)
{
    mod_lua_config lua = {
        .server_mode    = false,
        .cache_enabled  = config->cache_enabled,
        .system_path    = {0},
        .user_path      = {0}
    };
    as_strncpy(lua.system_path, config->system_path, sizeof(lua.system_path));
    as_strncpy(lua.user_path, config->user_path, sizeof(lua.user_path));
    
    as_module_configure(&mod_lua, &lua);
	lua_initialized = true;
}

/**
 * Destroy the aerospike instance
 */
void aerospike_destroy(aerospike* as)
{
	as_config_destroy(&as->config);

	if (as->_free) {
		cf_free(as);
	}
}

/**
 * Connect to the cluster
 */
as_status
aerospike_connect(aerospike* as, as_error* err)
{
	as_error_reset(err);

	as_status status = aerospike_library_init(err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	// This is not 100% bulletproof against, say, simultaneously calling
	// aerospike_connect() from two different threads with the same as object...
	if (as->cluster) {
		return AEROSPIKE_OK;
	}

	as_config* config = &as->config;
	as_vector* hosts = config->hosts;
	
	// Verify seed hosts are specified.
	if (hosts == NULL || hosts->size == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No hosts provided");
	}
	
	// Set TLS names to default when enabled.
	if (config->tls.enable) {
		for (uint32_t i = 0; i < hosts->size; i++) {
			as_host* host = as_vector_get(hosts, i);
			
			if (! host->name) {
				return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Seed host is null");
			}
			
			if (! host->tls_name) {
				if (config->cluster_name) {
					host->tls_name = cf_strdup(config->cluster_name);
				}
				else {
					host->tls_name = cf_strdup(host->name);
				}
			}
		}
	}
	
#if !defined USE_XDR
	// Only change global lua configuration once.
	if (!lua_initialized) {
		aerospike_init_lua(&as->config.lua);
	}
#endif

	// Create the cluster object.
	return as_cluster_create(&as->config, err, &as->cluster);
}

void as_event_close_cluster(as_cluster* cluster);

/**
 * Close connections to the cluster
 */
as_status
aerospike_close(aerospike* as, as_error* err)
{
	// This is not 100% bulletproof against simultaneous aerospike_close() calls
	// from different threads.
	as_error_reset(err);
	as_cluster* cluster = as->cluster;
	
	if (cluster) {
		if (as_event_loop_capacity > 0) {
			// Async configurations will attempt to wait till pending async commands have completed.
			as_event_close_cluster(cluster);
		}
		else {
			// Close sync only configurations immediately.
			as_cluster_destroy(cluster);
		}
		as->cluster = NULL;
	}
	return err->code;
}

bool
aerospike_cluster_is_connected(aerospike* as)
{
	return as_cluster_is_connected(as->cluster);
}

bool
aerospike_has_pipelining(aerospike* as)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	
	if (nodes->size == 0) {
		as_nodes_release(nodes);
		return false;
	}
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		if (! (nodes->array[i]->features & AS_FEATURES_PIPELINING)) {
			as_nodes_release(nodes);
			return false;
		}
	}
	as_nodes_release(nodes);
	return true;
}

extern bool as_socket_stop_on_interrupt;

void
aerospike_stop_on_interrupt(bool stop)
{
	as_socket_stop_on_interrupt = stop;
}

as_status
aerospike_truncate(aerospike* as, as_error* err, as_policy_info* policy, const char* ns, const char* set, uint64_t before_nanos)
{
	as_string_builder sb;
	as_string_builder_inita(&sb, 500, false);
	as_string_builder_append(&sb, "truncate:namespace=");
	as_string_builder_append(&sb, ns);

	if (set) {
		as_string_builder_append(&sb, ";set=");
		as_string_builder_append(&sb, set);
	}

	if (before_nanos) {
		as_string_builder_append(&sb, ";lut=");

		char buff[100];
		snprintf(buff, sizeof(buff), "%" PRIu64, before_nanos);
		as_string_builder_append(&sb, buff);
	}

	// Send truncate command to one node. That node will distribute the command to other nodes.
	return as_info_command_random_node(as, err, policy, sb.data);
}

as_status
aerospike_reload_tls_config(aerospike* as, as_error* err)
{
	as_error_reset(err);
	return as_tls_config_reload(&as->config.tls, &as->cluster->tls_ctx, err);
}
