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
#include <aerospike/as_config.h>
#include <aerospike/as_password.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_string.h>
#include <aerospike/mod_lua_config.h>
#include <ctype.h>
#include <stdlib.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_config*
as_config_init(as_config* c)
{
	c->hosts = NULL;
	memset(c->user, 0, sizeof(c->user));
	memset(c->password, 0, sizeof(c->password));
	c->cluster_name = NULL;
	c->event_callback = NULL;
	c->event_callback_udata = NULL;
	c->ip_map = NULL;
	c->ip_map_size = 0;
	c->min_conns_per_node = 0;
	c->max_conns_per_node = 100;
	c->async_min_conns_per_node = 0;
	c->async_max_conns_per_node = 100;
	c->pipe_max_conns_per_node = 64;
	c->conn_pools_per_node = 1;
	c->conn_timeout_ms = 1000;
	c->login_timeout_ms = 5000;
	c->max_socket_idle = 0;
	c->max_error_rate = 100;
	c->error_rate_window = 1;
	c->tender_interval = 1000;
	c->thread_pool_size = 16;
	c->tend_thread_cpu = -1;
	as_policies_init(&c->policies);
	as_config_lua_init(&c->lua);
	memset(&c->tls, 0, sizeof(as_config_tls));
	c->auth_mode = AS_AUTH_INTERNAL;
	c->fail_if_not_connected = true;
	c->use_services_alternate = false;
	c->force_single_node = false;
	c->rack_aware = false;
	c->rack_id = 0;
	c->rack_ids = NULL;
	c->use_shm = false;
	c->shm_key = 0xA9000000;
	c->shm_max_nodes = 16;
	c->shm_max_namespaces = 8;
	c->shm_takeover_threshold_sec = 30;
	return c;
}

void
as_config_destroy(as_config* config) {
	// Private: For internal use only.
	// Never call from applications because ownership of config fields is transferred
	// to aerospike in aerospike_init() or aerospike_new().
	// Only exception is ip_map which is left alone for legacy reasons.
	as_vector* hosts = config->hosts;
	
	if (hosts) {
		for (uint32_t i = 0; i < hosts->size; i++) {
			as_host* host = as_vector_get(hosts, i);
			as_host_destroy(host);
		}
		as_vector_destroy(hosts);
	}

	if (config->rack_ids) {
		as_vector_destroy(config->rack_ids);
	}

	if (config->cluster_name) {
		cf_free(config->cluster_name);
	}

	as_policies_destroy(&config->policies);

	as_config_tls* tls = &config->tls;

	if (tls->cafile) {
		cf_free(tls->cafile);
	}

	if (tls->castring) {
		cf_free(tls->castring);
	}

	if (tls->capath) {
		cf_free(tls->capath);
	}

	if (tls->protocols) {
		cf_free(tls->protocols);
	}

	if (tls->cipher_suite) {
		cf_free(tls->cipher_suite);
	}

	if (tls->cert_blacklist) {
		cf_free(tls->cert_blacklist);
	}

	if (tls->keyfile) {
		cf_free(tls->keyfile);
	}

	if (tls->keyfile_pw) {
		cf_free(tls->keyfile_pw);
	}

	if (tls->keystring) {
		cf_free(tls->keystring);
	}

	if (tls->certfile) {
		cf_free(tls->certfile);
	}

	if (tls->certstring) {
		cf_free(tls->certstring);
	}
}

bool
as_config_add_hosts(as_config* config, const char* string, uint16_t default_port)
{
	// String format: hostname1[:tlsname1][:port1],...
	// Hostname may also be an IP address in the following formats.
	// IPv4: xxx.xxx.xxx.xxx
	// IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]
	// IPv6: [xxxx::xxxx]
	const char* p = string;
	
	if (! config->hosts) {
		// Initialize list.  Determine list capacity in first pass.
		uint32_t capacity = 1;
		
		while (*p) {
			if (*p == ',') {
				capacity++;
			}
			p++;
		}
		config->hosts = as_vector_create(sizeof(as_host), capacity);
		p = string;
	}

	// Parse hosts
	as_host host;
	const char* begin;
	uint32_t length;
	bool more;
	
	while (*p) {
		host.name = 0;
		host.tls_name = 0;
		host.port = default_port;
		length = 0;
		more = false;
		
		// Parse hostname or IP address.
		if (*p == '[') {
			// IPv6 addresses are enclosed by brackets.
			p++;
			begin = p;
			
			while (*p) {
				if (*p == ']') {
					p++;
					
					if (*p == ':') {
						p++;
						more = true;
					}
					else if (*p == ',') {
						p++;
					}
					break;
				}
				length++;
				p++;
			}
		}
		else {
			begin = p;
			
			while (*p) {
				if (*p == ',') {
					p++;
					break;
				}
				else if (*p == ':') {
					p++;
					more = true;
					break;
				}
				length++;
				p++;
			}
		}
		host.name = cf_malloc(length + 1);
		memcpy(host.name, begin, length);
		host.name[length] = 0;
		
		if (more) {
			// Parse tls name or port.
			begin = p;
			length = 0;
			more = false;
			
			while (*p) {
				if (*p == ',') {
					p++;
					break;
				}
				else if (*p == ':') {
					// Must be tls name.
					host.tls_name = cf_malloc(length + 1);
					memcpy(host.tls_name, begin, length);
					host.tls_name[length] = 0;
					p++;
					more = true;
					break;
				}
				length++;
				p++;
			}
			
			if (more) {
				if (! isdigit(*p)) {
					return false;
				}
				host.port = (uint16_t)strtol(p, (char**)&p, 10);
				
				if (*p) {
					if (*p == ',') {
						p++;
					}
					else {
						return false;
					}
				}
			}
			else {
				if (isdigit(*begin)) {
					char* q = NULL;
					host.port = (uint16_t)strtol(begin, &q, 10);
					
					if (*q && *q != ',') {
						return false;
					}
				}
				else {
					host.tls_name = cf_malloc(length + 1);
					memcpy(host.tls_name, begin, length);
					host.tls_name[length] = 0;
				}
			}
		}
		as_vector_append(config->hosts, &host);
	}
	return true;
}

static void
add_host(as_config* config, char* addr, char* tls_name, uint16_t port)
{
	if (! config->hosts) {
		config->hosts = as_vector_create(sizeof(as_host), 16);
	}

	as_host* host = as_vector_reserve(config->hosts);
	host->name = addr;
	host->tls_name = tls_name;
	host->port = port;
}

void
as_config_add_host(as_config* config, const char* addr, uint16_t port)
{
	add_host(config, cf_strdup(addr), NULL, port);
}

void
as_config_tls_add_host(as_config* config, const char* addr, const char* tls_name, uint16_t port)
{
	add_host(config, cf_strdup(addr), cf_strdup(tls_name), port);
}

void
as_config_clear_hosts(as_config* config)
{
	as_vector* hosts = config->hosts;

	if (hosts) {
		for (uint32_t i = 0; i < hosts->size; i++) {
			as_host* host = as_vector_get(hosts, i);
			as_host_destroy(host);
		}
		as_vector_clear(hosts);
	}
}

bool
as_config_set_user(as_config* config, const char* user, const char* password)
{
	if (user && *user) {
		if (as_strncpy(config->user, user, sizeof(config->user))) {
			return false;
		}

		if (as_strncpy(config->password, password, sizeof(config->password))) {
			return false;
		}
		return true;
	}
	else {
		return false;
	}
}

void
as_config_add_rack_id(as_config* config, int rack_id)
{
	if (! config->rack_ids) {
		config->rack_ids = as_vector_create(sizeof(int), 8);
	}
	as_vector_append(config->rack_ids, &rack_id);
}

void
as_config_set_string(char** str, const char* value)
{
	if (*str) {
		cf_free(*str);
	}

	*str = value ? cf_strdup(value) : NULL;
}

bool
as_auth_mode_from_string(as_auth_mode* auth, const char* str)
{
	if (strcasecmp(str, "INTERNAL") == 0) {
		*auth = AS_AUTH_INTERNAL;
		return true;
	}

	if (strcasecmp(str, "EXTERNAL") == 0) {
		*auth = AS_AUTH_EXTERNAL;
		return true;
	}

	if (strcasecmp(str, "EXTERNAL_INSECURE") == 0) {
		*auth = AS_AUTH_EXTERNAL_INSECURE;
		return true;
	}

	if (strcasecmp(str, "PKI") == 0) {
		*auth = AS_AUTH_PKI;
		return true;
	}

	return false;
}
