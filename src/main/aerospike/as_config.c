/*
 * Copyright 2008-2016 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_config * as_config_init(as_config * c) 
{
	c->ip_map = 0;
	c->ip_map_size = 0;
	c->max_conns_per_node = 300;
	c->async_max_conns_per_node = 300;
	c->pipe_max_conns_per_node = 64;
	c->conn_timeout_ms = 1000;
	c->tender_interval = 1000;
	c->use_services_alternate = false;
	c->thread_pool_size = 16;
	c->hosts_size = 0;
	memset(c->user, 0, sizeof(c->user));
	memset(c->password, 0, sizeof(c->password));
	memset(c->hosts, 0, sizeof(c->hosts));
	as_policies_init(&c->policies);
	as_config_lua_init(&c->lua);
	c->fail_if_not_connected = true;
	
	c->use_shm = false;
	c->shm_key = 0xA5000000;
	c->shm_max_nodes = 16;
	c->shm_max_namespaces = 8;
	c->shm_takeover_threshold_sec = 30;
	return c;
}

bool
as_config_set_user(as_config* config, const char* user, const char* password)
{
	if (user && *user) {
		if (as_strncpy(config->user, user, sizeof(config->user))) {
			return false;
		}
		
		return as_password_get_constant_hash(password, config->password);
	}
	else {
		return false;
	}
}
