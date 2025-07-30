/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_config_file.h>
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
#endif

pthread_mutex_t init_lock = PTHREAD_MUTEX_INITIALIZER;

extern uint32_t as_event_loop_capacity;
extern bool as_event_single_thread;
static bool lua_initialized = false;

#if defined(_MSC_VER) || defined(AS_USE_LIBEVENT)
static bool library_initialized = false;
#endif

//---------------------------------
// Static Functions
//---------------------------------

static aerospike*
aerospike_defaults(aerospike* as, bool free, as_config* config)
{
	as->_free = free;
	as->cluster = NULL;
	as->config_orig = NULL;
	as->config_bitmap = NULL;

	if (config) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	else {
		as_config_init(&as->config);
	}

    char* url = getenv("AEROSPIKE_CLIENT_CONFIG_URL");

    if (url) {
		// Environment variable takes precedence over original
		// config path.
		char* path;

		if (strncmp(url, "file://", 7) == 0) {
			path = url + 7;
		}
		else {
			path = url;
		}

		// Set config path from environment variable.
		as_config_provider_set_path(&as->config, path);
	}

	if (as->config.config_provider.path) {
		as_error err;
		as_status status = as_config_file_init(as, &err);

		if (status != AEROSPIKE_OK) {
			as_log_error("%s", err.message);
		}
	}
	return as;
}

//---------------------------------
// Functions
//---------------------------------

as_status
aerospike_library_init(as_error* err)
{
#if defined(_MSC_VER) || defined(AS_USE_LIBEVENT)
	pthread_mutex_lock(&init_lock);

	if (!library_initialized) {
#if defined(_MSC_VER)
		if (!cf_clock_init()) {
			pthread_mutex_unlock(&init_lock);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "cf_clock_init() failed");
		}
#endif

#if defined(AS_USE_LIBEVENT)
		if (! as_event_single_thread) {
#if defined(_MSC_VER)
			int evthread_use_windows_threads();
			if (evthread_use_windows_threads() == -1) {
				pthread_mutex_unlock(&init_lock);
				return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "evthread_use_windows_threads() failed");
			}
#else
			int evthread_use_pthreads();
			if (evthread_use_pthreads() == -1) {
				pthread_mutex_unlock(&init_lock);
				return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "evthread_use_pthreads() failed");
			}
#endif
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
        .user_path      = {0}
    };
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

	if (as->config_orig) {
		// Do not call as_config_destroy() since config_orig is
		// a shallow copy of as->config and as->config owns
		// the fields.
		cf_free(as->config_orig);
	}

	if (as->config_bitmap) {
		cf_free(as->config_bitmap);
	}

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
	// Disable log subscribe requirement to avoid a breaking change in a minor release.
	// TODO: Reintroduce requirement in the next major client release.
	/*
	if (! g_as_log.callback_set) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"as_log_set_callback() must be called. "
			"See https://developer.aerospike.com/client/c/usage/logging for details."
			);
	}
	*/

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

	// Verify max_socket_idle.
	if (config->max_socket_idle > 86400) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "max_socket_idle must be <= 86400");
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
	status = as_cluster_create(as, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	// Dynamic configuration allows metrics to be enabled from a file.
	if (as->config.policies.metrics.enable) {
		as_log_info("Enable metrics");
		// Call as_cluster_enable_metrics() instead of aerospike_enable_metrics() to avoid
		// the uneccessary policy merge with the default metrics policy.
		as_cluster* cluster = as->cluster;

		pthread_mutex_lock(&cluster->metrics_lock);
		status = as_cluster_enable_metrics(err, cluster, &as->config.policies.metrics);
		pthread_mutex_unlock(&cluster->metrics_lock);
	}

	return status;
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
		if (cluster->metrics_enabled) {
			// Call as_cluster_disable_metrics() instead of aerospike_disable_metrics() to avoid
			// the unwanted dynamic config enabled denial. On aerospike_close(), the metrics
			// must be closed.
			pthread_mutex_lock(&cluster->metrics_lock);
			as_status status = as_cluster_disable_metrics(err, cluster);
			pthread_mutex_unlock(&cluster->metrics_lock);

			if (status != AEROSPIKE_OK) {
				as_log_warn("Metrics error: %s %s", as_error_string(status), err->message);
				as_error_reset(err);
			}
		}
		
		if (as_event_loop_size > 0 && !as_event_single_thread) {
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

extern bool as_socket_stop_on_interrupt;

void
aerospike_stop_on_interrupt(bool stop)
{
	as_socket_stop_on_interrupt = stop;
}

as_status
aerospike_truncate(
	aerospike* as, as_error* err, as_policy_info* policy, const char* ns, const char* set,
	uint64_t before_nanos
	)
{
	as_error_reset(err);

	if (! policy) {
		as_config* config = aerospike_load_config(as);
		policy = &config->policies.info;
	}

	// Send truncate command to one node. That node will distribute the command to other nodes.
	as_node* node = as_node_get_random(as->cluster);

	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node.");
	}

	as_string_builder sb;
	as_string_builder_inita(&sb, 300, false);

	if (set) {
		as_string_builder_append(&sb, "truncate:namespace=");
		as_string_builder_append(&sb, ns);
		as_string_builder_append(&sb, ";set=");
		as_string_builder_append(&sb, set);
	}
	else {
		as_string_builder_append(&sb, "truncate-namespace:namespace=");
		as_string_builder_append(&sb, ns);
	}

	if (before_nanos) {
		as_string_builder_append(&sb, ";lut=");

		char buff[100];
		snprintf(buff, sizeof(buff), "%" PRIu64, before_nanos);
		as_string_builder_append(&sb, buff);
	}
	as_string_builder_append_char(&sb, '\n');

	uint64_t deadline = as_socket_deadline(policy->timeout);
	char* response;

	as_status status = as_info_command_node(err, node, sb.data, true, deadline, &response);

	if (status == AEROSPIKE_OK) {
		cf_free(response);
	}

	as_node_release(node);
	return status;
}

as_status
aerospike_reload_tls_config(aerospike* as, as_error* err)
{
	as_error_reset(err);
	as_config* config = aerospike_load_config(as);
	return as_tls_config_reload(&config->tls, as->cluster->tls_ctx, err);
}

as_status
aerospike_set_xdr_filter(
	aerospike* as, as_error* err, as_policy_info* policy, const char* dc, const char* ns,
	const char* filter_b64)
{
	as_error_reset(err);

	if (! policy) {
		as_config* config = aerospike_load_config(as);
		policy = &config->policies.info;
	}

	// Send truncate command to one node. That node will distribute the command to other nodes.
	as_node* node = as_node_get_random(as->cluster);

	if (! node) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find server node.");
	}

	as_string_builder sb;
	as_string_builder_inita(&sb, 512, true);
	as_string_builder_append(&sb, "xdr-set-filter:dc=");
	as_string_builder_append(&sb, dc);
	as_string_builder_append(&sb, ";namespace=");
	as_string_builder_append(&sb, ns);
	as_string_builder_append(&sb, ";exp=");
	as_string_builder_append(&sb, filter_b64);
	as_string_builder_append_char(&sb, '\n');

	uint64_t deadline = as_socket_deadline(policy->timeout);
	char* response;

	as_status status = as_info_command_node(err, node, sb.data, true, deadline, &response);

	if (status == AEROSPIKE_OK) {
		cf_free(response);
	}

	as_string_builder_destroy(&sb);
	as_node_release(node);
	return status;
}
