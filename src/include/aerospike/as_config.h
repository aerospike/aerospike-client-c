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
#pragma once 

#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_password.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 *	MACROS
 *****************************************************************************/

#ifdef __linux__
/**
 * Default path to the system UDF files.
 */
#define AS_CONFIG_LUA_SYSTEM_PATH "/opt/aerospike/client/sys/udf/lua"

/**
 * Default path to the user UDF files.
 */
#define AS_CONFIG_LUA_USER_PATH "/opt/aerospike/client/usr/udf/lua"
#endif

#ifdef __APPLE__
/**
 * Default path to the system UDF files.
 */
#define AS_CONFIG_LUA_SYSTEM_PATH "/usr/local/aerospike/client/sys/udf/lua"

/**
 * Default path to the user UDF files.
 */
#define AS_CONFIG_LUA_USER_PATH "/usr/local/aerospike/client/usr/udf/lua"
#endif

/**
 * The size of path strings
 */
#define AS_CONFIG_PATH_MAX_SIZE 256

/**
 * The maximum string length of path strings
 */
#define AS_CONFIG_PATH_MAX_LEN 	(AS_CONFIG_PATH_MAX_SIZE - 1)

/**
 * The size of as_config.hosts
 */
#define AS_CONFIG_HOSTS_SIZE 256

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Host Information
 *
 *	@ingroup as_config_object
 */
typedef struct as_config_host_s {
	
	/**
	 *	Host address
	 */
	const char * addr;
	
	/**
	 *	Host port
	 */
	uint16_t port;

} as_config_host;

/**
 *	IP translation table.
 *
 *	@ingroup as_config_object
 */
typedef struct as_addr_map_s {
	
	/**
	 *	Original hostname or IP address in string format.
	 */
    char * orig;
	
	/**
	 *	Use this IP address instead.
	 */
    char * alt;
	
} as_addr_map;

/**
 *	lua module config
 *
 *	@ingroup as_config_object
 */
typedef struct as_config_lua_s {

	/**
	 *	Enable caching of UDF files in the client
	 *	application.
	 */
	bool cache_enabled;

	/**
	 *	The path to the system UDF files. These UDF files 
	 *	are installed with the aerospike client library.
	 *	Default location defined in: AS_CONFIG_LUA_SYSTEM_PATH
	 */
	char system_path[AS_CONFIG_PATH_MAX_SIZE];

	/**
	 *	The path to user's UDF files.
	 *	Default location defined in: AS_CONFIG_LUA_USER_PATH
	 */
	char user_path[AS_CONFIG_PATH_MAX_SIZE];

} as_config_lua;

/**
 *	The `as_config` contains the settings for the `aerospike` client. Including
 *	default policies, seed hosts in the cluster and other settings.
 *
 *	## Initialization
 *
 *	Before using as_config, you must first initialize it. This will setup the 
 *	default values.
 *
 *	~~~~~~~~~~{.c}
 *	as_config config;
 *	as_config_init(&config);
 *	~~~~~~~~~~
 *
 *	Once initialized, you can populate the values.
 *
 *	## Seed Hosts
 *	
 *	The client will require at least one seed host defined in the 
 *	configuration. The seed host is defined in `as_config.hosts`. 
 *
 *	~~~~~~~~~~{.c}
 *	as_config_add_host(&config, "127.0.0.1", 3000);
 *	~~~~~~~~~~
 *
 *	You can define up to 256 hosts for the seed. The client will iterate over
 *	the list until it connects with one of the hosts. 
 *
 *	## Policies
 *
 *	The configuration also defines default policies for the application. The 
 *	`as_config_init()` function already presets default values for the policies.
 *	
 *	Policies define the behavior of the client, which can be global across
 *	operations, global to a single operation, or local to a single use of an
 *	operation.
 *	
 *	Each database operation accepts a policy for that operation as an a argument.
 *	This is considered a local policy, and is a single use policy. This policy
 *	supersedes any global policy defined.
 *	
 *	If a value of the policy is not defined, then the rule is to fallback to the
 *	global policy for that operation. If the global policy for that operation is
 *	undefined, then the global default value will be used.
 *
 *	If you find that you have behavior that you want every use of an operation
 *	to utilize, then you can specify the default policy in as_config.policies.
 *
 *	For example, the `aerospike_key_put()` operation takes an `as_policy_write`
 *	policy. If you find yourself setting the `key` policy value for every call 
 *	to `aerospike_key_put()`, then you may find it beneficial to set the global
 *	`as_policy_write` in `as_policies.write`, which all write operations will use.
 *
 *	~~~~~~~~~~{.c}
 *	config.policies.write.key = AS_POLICY_KEY_SEND;
 *	~~~~~~~~~~
 *
 *	If you find that you want to use a policy value across all operations, then 
 *	you may find it beneficial to set the default policy value for that policy 
 *	value.
 *
 *	For example, if you keep setting the key policy value to 
 *	`AS_POLICY_KEY_SEND`, then you may want to just set `as_policies.key`. This
 *	will set the global default value for the policy value. So, if an global
 *  operation policy or a local operation policy does not define a value, then
 *	this value will be used.
 *
 *	~~~~~~~~~~{.c}
 *	config.policies.key = AS_POLICY_KEY_SEND;
 *	~~~~~~~~~~
 *
 *	Global default policy values:
 *	-	as_policies.timeout
 *	-	as_policies.retry
 *	-	as_policies.key
 *	-	as_policies.gen
 *	-	as_policies.exists
 *
 *	Global operation policies:
 *	-	as_policies.read
 *	-	as_policies.write
 *	-	as_policies.operate
 *	-	as_policies.remove
 *	-	as_policies.query
 *	-	as_policies.scan
 *	-	as_policies.info
 *
 *
 *	## User-Defined Function Settings
 *	
 *	If you are using using user-defined functions (UDF) for processing query 
 *	results (i.e aggregations), then you will find it useful to set the 
 *	`mod_lua` settings. Of particular importance is the `mod_lua.user_path`, 
 *	which allows you to define a path to where the client library will look for
 *	Lua files for processing.
 *	
 *	~~~~~~~~~~{.c}
 *	strcpy(config.mod_lua.user_path, "/home/me/lua");
 *	~~~~~~~~~~
 *
 *	@ingroup client_objects
 */
typedef struct as_config_s {

	/**
	 *	User authentication to cluster.  Leave empty for clusters running without restricted access.
	 */
	char user[AS_USER_SIZE];
	
	/**
	 *	Password authentication to cluster.  The hashed value of password will be stored by the client 
	 *	and sent to server in same format.  Leave empty for clusters running without restricted access.
	 */
	char password[AS_PASSWORD_HASH_SIZE];
	
	/**
	 *	A IP translation table is used in cases where different clients use different server
	 *	IP addresses.  This may be necessary when using clients from both inside and outside
	 *	a local area network.  Default is no translation.
	 *
	 *	The key is the IP address returned from friend info requests to other servers.  The
	 *	value is the real IP address used to connect to the server.
	 *
	 *	A deep copy of ip_map is performed in aerospike_connect().  The caller is
	 *  responsible for memory deallocation of the original data structure.
	 */
	as_addr_map * ip_map;
	
	/**
	 *	Length of ip_map array.
	 *  Default: 0
	 */
	uint32_t ip_map_size;
	
	/**
	 * Maximum number of synchronous connections allowed per server node.  Synchronous transactions
	 * will go through retry logic and potentially fail with error code "AEROSPIKE_ERR_NO_MORE_CONNECTIONS"
	 * if the maximum number of connections would be exceeded.
	 * 
	 * The number of connections used per node depends on how many concurrent threads issue
	 * database commands plus sub-threads used for parallel multi-node commands (batch, scan,
	 * and query). One connection will be used for each thread.
	 *
	 * Default: 300
	 */
	uint32_t max_conns_per_node;
	
	/**
	 *	Maximum number of asynchronous (non-pipeline) connections allowed for each node.
	 *	This limit will be enforced at the node/event loop level.  If the value is 100 and 2 event
	 *	loops are created, then each node/event loop asynchronous (non-pipeline) connection pool 
	 *	will have a limit of 50. Async transactions will be rejected if the limit would be exceeded.
	 *	This variable is ignored if asynchronous event loops are not created.
	 *	Default: 300
	 */
	uint32_t async_max_conns_per_node;

	/**
	 *	Maximum number of pipeline connections allowed for each node.
	 *	This limit will be enforced at the node/event loop level.  If the value is 100 and 2 event
	 *	loops are created, then each node/event loop pipeline connection pool will have a limit of 50. 
	 *	Async transactions will be rejected if the limit would be exceeded.
	 *	This variable is ignored if asynchronous event loops are not created.
	 *	Default: 64
	 */
	uint32_t pipe_max_conns_per_node;
	
	/**
	 *	Initial host connection timeout in milliseconds.  The timeout when opening a connection
	 *	to the server host for the first time.
	 *	Default: 1000
	 */
	uint32_t conn_timeout_ms;

	/**
	 *	Polling interval in milliseconds for cluster tender
	 *	Default: 1000
	 */
	uint32_t tender_interval;

	/**
	 *	Number of threads stored in underlying thread pool that is used in batch/scan/query commands.
	 *	These commands are often sent to multiple server nodes in parallel threads.  A thread pool 
	 *	improves performance because threads do not have to be created/destroyed for each command.
	 *	Calculate your value using the following formula:
	 *
	 *	thread_pool_size = (concurrent batch/scan/query commands) * (server nodes)
	 *
	 *	Default: 16
	 */
	uint32_t thread_pool_size;

	/**
	 *	Count of entries in hosts array.
	 */
	uint32_t hosts_size;
	
	/**
	 *	(seed) hosts
	 *	Populate with one or more hosts in the cluster
	 *	that you intend to connect with.
	 */
	as_config_host hosts[AS_CONFIG_HOSTS_SIZE];

	/**
	 *	Client policies
	 */
	as_policies policies;

	/**
	 *	lua config.  This is a global config even though it's located here in cluster config.
	 *	This config has been left here to avoid breaking the API.
	 *
	 *	The global lua config will only be changed once on first cluster initialization.
	 *	A better method for initializing lua configuration is to leave this field alone and
	 *	instead call aerospike_init_lua():
	 *
	 *	~~~~~~~~~~{.c}
	 *	// Get default global lua configuration.
	 *	as_config_lua lua;
	 *	as_config_lua_init(&lua);
	 *
	 *	// Optionally modify lua defaults.
	 *	lua.cache_enabled = <enable lua cache>;
	 *	strcpy(lua.system_path, <lua system directory>);
	 *	strcpy(lua.user_path, <lua user directory>);
	 *
	 *	// Initialize global lua configuration.
	 *	aerospike_init_lua(&lua);
	 *	~~~~~~~~~~
	 */
	as_config_lua lua;

	/**
	 *	Action to perform if client fails to connect to seed hosts.
	 *
	 *	If fail_if_not_connected is true (default), the cluster creation will fail
	 *	when all seed hosts are not reachable.
	 *
	 *	If fail_if_not_connected is false, an empty cluster will be created and the 
	 *	client will automatically connect when Aerospike server becomes available.
	 */
	bool fail_if_not_connected;
	
	/**
	 *	Flag to signify if "services-alternate" should be used instead of "services"
	 *	Default : false
	 */
	bool use_services_alternate;

	/**
	 *	Indicates if shared memory should be used for cluster tending.  Shared memory
	 *	is useful when operating in single threaded mode with multiple client processes.
	 *	This model is used by wrapper languages such as PHP and Python.  When enabled, 
	 *	the data partition maps are maintained by only one process and all other processes 
	 *	use these shared memory maps.
	 *
	 *	Shared memory should not be enabled for multi-threaded programs.
	 *	Default: false
	 */
	bool use_shm;

	/**
	 *	Shared memory identifier.  This identifier should be the same for all applications
	 *	that use the Aerospike C client. 
	 *	Default: 0xA5000000
	 */
	int shm_key;
	
	/**
	 *	Shared memory maximum number of server nodes allowed.  This value is used to size
	 *	the fixed shared memory segment.  Leave a cushion between actual server node
	 *	count and shm_max_nodes so new nodes can be added without having to reboot the client.
	 *	Default: 16
	 */
	uint32_t shm_max_nodes;
	
	/**
	 *	Shared memory maximum number of namespaces allowed.  This value is used to size
	 *	the fixed shared memory segment.  Leave a cushion between actual namespaces
	 *	and shm_max_namespaces so new namespaces can be added without having to reboot the client.
	 *	Default: 8
	 */
	uint32_t shm_max_namespaces;
	
	/**
	 *	Take over shared memory cluster tending if the cluster hasn't been tended by this
	 *	threshold in seconds.
	 *	Default: 30
	 */
	uint32_t shm_takeover_threshold_sec;
} as_config;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize the configuration to default values.
 *
 *	You should do this to ensure the configuration has valid values, before 
 *	populating it with custom options.
 *
 *	~~~~~~~~~~{.c}
 *		as_config config;
 *		as_config_init(&config);
 *		as_config_add_host(&config, "127.0.0.1", 3000);
 *	~~~~~~~~~~
 *	
 *	@param c The configuration to initialize.
 *	
 *	@return The initialized configuration on success. Otherwise NULL.
 *
 *	@relates as_config
 */
as_config * as_config_init(as_config * c);

/**
 *	Add host to seed the cluster.
 *
 *	~~~~~~~~~~{.c}
 *		as_config config;
 *		as_config_init(&config);
 *		as_config_add_host(&config, "127.0.0.1", 3000);
 *	~~~~~~~~~~
 *
 *	@relates as_config
 */
static inline void
as_config_add_host(as_config* config, const char* addr, uint16_t port)
{
	as_config_host* host = &config->hosts[config->hosts_size++];
	host->addr = addr;
	host->port = port;
}

/**
 *	User authentication for servers with restricted access.  The password will be stored by the
 *	client and sent to server in hashed format.
 *
 *	~~~~~~~~~~{.c}
 *		as_config config;
 *		as_config_init(&config);
 *		as_config_set_user(&config, "charlie", "mypassword");
 *	~~~~~~~~~~
 *
 *	@relates as_config
 */
bool
as_config_set_user(as_config* config, const char* user, const char* password);
	
/**
 *	Initialize global lua configuration to defaults.
 */
static inline void
as_config_lua_init(as_config_lua* lua)
{
	lua->cache_enabled = false;
	strcpy(lua->system_path, AS_CONFIG_LUA_SYSTEM_PATH);
	strcpy(lua->user_path, AS_CONFIG_LUA_USER_PATH);
}

#ifdef __cplusplus
} // end extern "C"
#endif
