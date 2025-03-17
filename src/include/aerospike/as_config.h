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
#pragma once 

#include <aerospike/as_error.h>
#include <aerospike/as_host.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_password.h>
#include <aerospike/as_vector.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Macros
//---------------------------------

#ifdef __linux__
/**
 * Default path to the user UDF files.
 */
#define AS_CONFIG_LUA_USER_PATH "/opt/aerospike/client/usr/udf/lua"
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
#define AS_CONFIG_LUA_USER_PATH "/usr/local/aerospike/client/usr/udf/lua"
#endif

#if defined(_MSC_VER)
#define AS_CONFIG_LUA_USER_PATH "C:/aerospike/client/usr/udf/lua"
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
 * Max clear text password size.
 */
#define AS_PASSWORD_SIZE 64

//---------------------------------
// Types
//---------------------------------

/**
 * IP translation table.
 *
 * @relates as_config
 */
typedef struct as_addr_map_s {
	
	/**
	 * Original hostname or IP address in string format.
	 */
    char* orig;
	
	/**
	 * Use this IP address instead.
	 */
    char* alt;
	
} as_addr_map;

/**
 * Authentication mode.
 *
 * @relates as_config
 */
typedef enum as_auth_mode_e {
	/**
	 * Use internal authentication when user/password defined. Hashed password is stored
	 * on the server. Do not send clear password. This is the default.
	 */
	AS_AUTH_INTERNAL,

	/**
	 * Use external authentication (like LDAP) when user/password defined. Specific external
	 * authentication is configured on server. If TLS defined, send clear password on node
	 * login via TLS. Return error if TLS is not defined.
	 */
	AS_AUTH_EXTERNAL,

	/**
	 * Use external authentication (like LDAP) when user/password defined. Specific external
	 * authentication is configured on server.  Send clear password on node login whether or
	 * not TLS is defined. This mode should only be used for testing purposes because it is
	 * not secure authentication.
	 */
	AS_AUTH_EXTERNAL_INSECURE,

	/**
	 * Authentication and authorization based on a certificate.  No user name or
	 * password needs to be configured.  Requires TLS and a client certificate.
	 */
	AS_AUTH_PKI
} as_auth_mode;

/**
 * Cluster event notification type.
 *
 * @relates as_config
 */
typedef enum as_cluster_event_type_e {
	/**
	 * Node was added to cluster.
	 */
	AS_CLUSTER_ADD_NODE = 0,

	/**
	 * Node was removed fron cluster.
	 */
	AS_CLUSTER_REMOVE_NODE = 1,

	/**
	 * There are no active nodes in the cluster.
	 */
	AS_CLUSTER_DISCONNECTED = 2
} as_cluster_event_type;

/**
 * Cluster event notification data.
 *
 * @relates as_config
 */
typedef struct as_cluster_event_s {
	/**
	 * Node name.
	 */
	const char* node_name;

	/**
	 * Node IP address in string format.
	 */
	const char* node_address;

	/**
	 * User defined data.
	 */
	void* udata;

	/**
	 * Cluster event notification type.
	 */
	as_cluster_event_type type;
} as_cluster_event;

/**
 * Cluster event notification callback function.
 * as_cluster_event is placed on the stack before calling.
 * Do not free node_name or node_address.
 *
 * @relates as_config
 */
typedef void (*as_cluster_event_callback) (as_cluster_event* event);

/**
 * lua module config
 *
 * @relates as_config
 */
typedef struct as_config_lua_s {

	/**
	 * Enable caching of UDF files in the client
	 * application.
	 */
	bool cache_enabled;

	/**
	 * The path to user's UDF files.
	 * Default location defined in: AS_CONFIG_LUA_USER_PATH
	 */
	char user_path[AS_CONFIG_PATH_MAX_SIZE];

} as_config_lua;

/**
 * TLS module config
 *
 * @relates as_config
 */
typedef struct as_config_tls_s {

	/**
	 * Enable TLS on connections.
     * By default TLS is disabled.
	 */
	bool enable;

	/**
	 * Path to a trusted CA certificate file.
	 * By default TLS will use system standard trusted CA certificates.
	 * Use as_config_tls_set_cafile() to set this field.
	 * If cafile is populated, castring is ignored.
	 */
	char* cafile;

	/**
	 * String containing trusted CA certificate(s).
	 * Use as_config_tls_set_castring() to set this field.
	 * If cafile is populated, castring is ignored.
	 */
	char* castring;

	/**
	 * Path to a directory of trusted certificates.
	 * See the OpenSSL SSL_CTX_load_verify_locations manual page for
	 * more information about the format of the directory.
	 * Use as_config_tls_set_capath() to set this field.
	 */
	char* capath;

	/**
	 * Specifies enabled protocols.
	 *
	 * This format is the same as Apache's SSLProtocol documented
	 * at https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol
	 *
	 * If not specified (NULL) the client will use "-all +TLSv1.2".
	 *
	 * If you are not sure what protocols to select this option is
	 * best left unspecified (NULL).
	 *
	 * Use as_config_tls_set_protocols() to set this field.
	 */
	char* protocols;
	
	/**
	 * Specifies enabled cipher suites.
	 *
	 * The format is the same as OpenSSL's Cipher List Format documented
	 * at https://www.openssl.org/docs/manmaster/apps/ciphers.html
	 *
	 * If not specified the OpenSSL default cipher suite described in
	 * the ciphers documentation will be used.
	 *
	 * If you are not sure what cipher suite to select this option
	 * is best left unspecified (NULL).
	 *
	 * Use as_config_tls_set_cipher_suite() to set this field.
	 */
	char* cipher_suite;
	
	/**
	 * Path to a certificate blacklist file.
	 * The file should contain one line for each blacklisted certificate.
	 * Each line starts with the certificate serial number expressed in hex.
	 * Each entry may optionally specify the issuer name of the
	 * certificate (serial numbers are only required to be unique per
	 * issuer).  Example records:
	 * 867EC87482B2 /C=US/ST=CA/O=Acme/OU=Engineering/CN=Test Chain CA
	 * E2D4B0E570F9EF8E885C065899886461
	 *
	 * Use as_config_tls_set_cert_blacklist() to set this field.
	 */
	char* cert_blacklist;

	/**
	 * Path to the client's key for mutual authentication.
	 * By default mutual authentication is disabled.
	 * Use as_config_tls_set_keyfile() to set this field.
	 * If keyfile is populated, keystring is ignored.
	 */
	char* keyfile;

	/**
	 * Decryption password for the client's key for mutual authentication.
	 * By default the key is assumed not to be encrypted.
	 *
	 * Use as_config_tls_set_keyfile_pw() to set this field.
	 */
	char* keyfile_pw;

	/**
	 * Client's key string for mutual authentication.
	 * By default mutual authentication is disabled.
	 * Use as_config_tls_set_keystring() to set this field.
	 * If keyfile is populated, keystring is ignored.
	 */
	char* keystring;

	/**
	 * Path to the client's certificate chain file for mutual authentication.
	 * By default mutual authentication is disabled.
	 * Use as_config_tls_set_certfile() to set this field.
	 * If certfile is populated, certstring is ignored.
	 */
	char* certfile;

	/**
	 * Client's certificate chain file string for mutual authentication.
	 * By default mutual authentication is disabled.
	 * Use as_config_tls_set_certstring() to set this field.
	 * If certfile is populated, certstring is ignored.
	 */
	char* certstring;

	/**
	 * Enable CRL checking for the certificate chain leaf certificate.
	 * An error occurs if a suitable CRL cannot be found.
	 * By default CRL checking is disabled.
	 */
	bool crl_check;

	/**
	 * Enable CRL checking for the entire certificate chain.
	 * An error occurs if a suitable CRL cannot be found.
	 * By default CRL checking is disabled.
	 */
	bool crl_check_all;

	/**
	 * Log session information for each connection.
	 */
	bool log_session_info;

	/**
	 * Use TLS connections only for login authentication.
	 * All other communication with the server will be done with non-TLS connections.
	 * Default: false (Use TLS connections for all communication with server.)
	 */
	bool for_login_only;

} as_config_tls;

/**
 * The `as_config` contains the settings for the `aerospike` client. Including
 * default policies, seed hosts in the cluster and other settings.
 *
 * ## Initialization
 *
 * Before using as_config, you must first initialize it. This will setup the 
 * default values.
 *
 * ~~~~~~~~~~{.c}
 * as_config config;
 * as_config_init(&config);
 * ~~~~~~~~~~
 *
 * Once initialized, you can populate the values.
 *
 * ## Seed Hosts
 * 
 * The client will require at least one seed host defined in the 
 * configuration. The seed host is defined in `as_config.hosts`. 
 *
 * ~~~~~~~~~~{.c}
 * as_config_add_host(&config, "127.0.0.1", 3000);
 * ~~~~~~~~~~
 *
 * The client will iterate over the list until it connects with one of the hosts.
 *
 * ## Policies
 *
 * The configuration also defines default policies for the application. The 
 * `as_config_init()` function already presets default values for the policies.
 * 
 * Policies define the behavior of the client, which can be global across
 * operations, global to a single operation, or local to a single use of an
 * operation.
 * 
 * Each database operation accepts a policy for that operation as an a argument.
 * This is considered a local policy, and is a single use policy. This policy
 * supersedes any global policy defined.
 * 
 * If a value of the policy is not defined, then the rule is to fallback to the
 * global policy for that operation. If the global policy for that operation is
 * undefined, then the global default value will be used.
 *
 * If you find that you have behavior that you want every use of an operation
 * to utilize, then you can specify the default policy in as_config.policies.
 *
 * For example, the `aerospike_key_put()` operation takes an `as_policy_write`
 * policy. If you find yourself setting the `key` policy value for every call 
 * to `aerospike_key_put()`, then you may find it beneficial to set the global
 * `as_policy_write` in `as_policies.write`, which all write operations will use.
 *
 * ~~~~~~~~~~{.c}
 * config.policies.write.key = AS_POLICY_KEY_SEND;
 * ~~~~~~~~~~
 *
 * Global operation policies:
 * - as_policies.read
 * - as_policies.write
 * - as_policies.operate
 * - as_policies.remove
 * - as_policies.query
 * - as_policies.scan
 * - as_policies.info
 *
 * ## User-Defined Function Settings
 * 
 * If you are using user-defined functions (UDF) for processing query 
 * results (i.e aggregations), then you will find it useful to set the 
 * `mod_lua` settings. Of particular importance is the `mod_lua.user_path`, 
 * which allows you to define a path to where the client library will look for
 * Lua files for processing.
 * 
 * ~~~~~~~~~~{.c}
 * strcpy(config.mod_lua.user_path, "/home/me/lua");
 * ~~~~~~~~~~
 *
 * Never call as_config_destroy() directly because ownership of config fields
 * is transferred to aerospike in aerospike_init() or aerospike_new().
 *
 * @ingroup client_objects
 */
typedef struct as_config_s {
	/**
	 * Seed hosts. Populate with one or more hosts in the cluster that you intend to connect with.
	 * Do not set directly.  Use as_config_add_hosts() or as_config_add_host() to add seed hosts.
	 */
	as_vector* hosts;
	
	/**
	 * User authentication to cluster.  Leave empty for clusters running without restricted access.
	 */
	char user[AS_USER_SIZE];
	
	/**
	 * Password authentication to cluster.  Leave empty for clusters running without restricted access.
	 */
	char password[AS_PASSWORD_SIZE];
	
	/**
	 * Expected cluster name.  If populated, the cluster_name must match the cluster-name field
	 * in the service section in each server configuration.  This ensures that the specified
	 * seed nodes belong to the expected cluster on startup.  If not, the client will refuse
	 * to add the node to the client's view of the cluster.
	 *
	 * Default: NULL
	 */
	char* cluster_name;
	
	/**
	 * Cluster event function that will be called when nodes are added/removed from the cluster.
	 *
	 * Default: NULL (no callback will be made)
	 */
	as_cluster_event_callback event_callback;

	/**
	 * Cluster event user data that will be passed back to event_callback.
	 *
	 * Default: NULL
	 */
	void* event_callback_udata;

	/**
	 * A IP translation table is used in cases where different clients use different server
	 * IP addresses.  This may be necessary when using clients from both inside and outside
	 * a local area network.  Default is no translation.
	 *
	 * The key is the IP address returned from friend info requests to other servers.  The
	 * value is the real IP address used to connect to the server.
	 *
	 * A deep copy of ip_map is performed in aerospike_connect().  The caller is
	 * responsible for memory deallocation of the original data structure.
	 */
	as_addr_map* ip_map;
	
	/**
	 * Length of ip_map array.
	 * Default: 0
	 */
	uint32_t ip_map_size;
	
	/**
	 * Minimum number of synchronous connections allowed per server node.  Preallocate min
	 * connections on client node creation.  The client will periodically allocate new connections
	 * if count falls below min connections.
	 *
	 * Server proto-fd-idle-ms and client max_socket_idle should be set to zero (no reap) if
	 * min_conns_per_node is greater than zero.  Reaping connections can defeat the purpose
	 * of keeping connections in reserve for a future burst of activity.
	 *
	 * Default: 0
	 */
	uint32_t min_conns_per_node;

	/**
	 * Maximum number of synchronous connections allowed per server node.  Synchronous commands
	 * will go through retry logic and potentially fail with error code
	 * "AEROSPIKE_ERR_NO_MORE_CONNECTIONS" if the maximum number of connections would be exceeded.
	 * 
	 * The number of connections used per node depends on how many concurrent threads issue
	 * database commands plus sub-threads used for parallel multi-node commands (batch, scan,
	 * and query). One connection will be used for each thread.
	 *
	 * Default: 100
	 */
	uint32_t max_conns_per_node;
	
	/**
	 * Minimum number of asynchronous connections allowed per server node.  Preallocate min
	 * connections on client node creation.  The client will periodically allocate new connections
	 * if count falls below min connections.
	 *
	 * Server proto-fd-idle-ms and client max_socket_idle should be set to zero (no reap) if
	 * async_min_conns_per_node is greater than zero.  Reaping connections can defeat the purpose
	 * of keeping connections in reserve for a future burst of activity.
	 *
	 * Default: 0
	 */
	uint32_t async_min_conns_per_node;

	/**
	 * Maximum number of asynchronous (non-pipeline) connections allowed for each node.
	 * This limit will be enforced at the node/event loop level.  If the value is 100 and 2 event
	 * loops are created, then each node/event loop asynchronous (non-pipeline) connection pool 
	 * will have a limit of 50. Async commands will be rejected if the limit would be exceeded.
	 * This variable is ignored if asynchronous event loops are not created.
	 *
	 * Default: 100
	 */
	uint32_t async_max_conns_per_node;

	/**
	 * Maximum number of pipeline connections allowed for each node.
	 * This limit will be enforced at the node/event loop level.  If the value is 100 and 2 event
	 * loops are created, then each node/event loop pipeline connection pool will have a limit of 50. 
	 * Async commands will be rejected if the limit would be exceeded.
	 * This variable is ignored if asynchronous event loops are not created.
	 *
	 * Default: 64
	 */
	uint32_t pipe_max_conns_per_node;
	
	/**
	 * Number of synchronous connection pools used for each node.  Machines with 8 cpu cores or
	 * less usually need just one connection pool per node.  Machines with a large number of cpu
	 * cores may have their synchronous performance limited by contention for pooled connections.
	 * Contention for pooled connections can be reduced by creating multiple mini connection pools
	 * per node.
	 *
	 * Default: 1
	 */
	uint32_t conn_pools_per_node;

	/**
	 * Initial host connection timeout in milliseconds.  The timeout when opening a connection
	 * to the server host for the first time.
	 * Default: 1000
	 */
	uint32_t conn_timeout_ms;

	/**
	 * Node login timeout in milliseconds.
	 * Default: 5000
	 */
	uint32_t login_timeout_ms;

	/**
	 * Maximum socket idle in seconds.  Connection pools will discard sockets that have been 
	 * idle longer than the maximum.
	 *
	 * Connection pools are now implemented by a LIFO stack.  Connections at the tail of the
	 * stack will always be the least used.  These connections are checked for max_socket_idle
	 * once every 30 tend iterations (usually 30 seconds).
	 *
	 * If server's proto-fd-idle-ms is greater than zero, then max_socket_idle should be
	 * at least a few seconds less than the server's proto-fd-idle-ms, so the client does not
	 * attempt to use a socket that has already been reaped by the server.
	 *
	 * If server's proto-fd-idle-ms is zero (no reap), then max_socket_idle should also be zero.
	 * Connections retrieved from a pool in commands will not be checked for max_socket_idle
	 * when max_socket_idle is zero.  Idle connections will still be trimmed down from peak
	 * connections to min connections (min_conns_per_node and async_min_conns_per_node) using a
	 * hard-coded 55 second limit in the cluster tend thread.
	 *
	 * Default: 0 seconds
	 */
	uint32_t max_socket_idle;

	/**
	 * Maximum number of errors allowed per node per error_rate_window before backoff
	 * algorithm returns AEROSPIKE_MAX_ERROR_RATE for database commands to that node.
	 * If max_error_rate is zero, there is no error limit.
	 *
	 * The counted error types are any error that causes the connection to close (socket errors
	 * and client timeouts), server device overload and server timeouts.
	 *
	 * The application should backoff or reduce the command load until AEROSPIKE_MAX_ERROR_RATE
	 * stops being returned.
	 *
	 * Default: 100
	 */
	uint32_t max_error_rate;

	/**
	 * The number of cluster tend iterations that defines the window for max_error_rate.
	 * One tend iteration is defined as tender_interval plus the time to tend all nodes.
	 * At the end of the window, the error count is reset to zero and backoff state is removed
	 * on all nodes.
	 *
	 * Default: 1
	 */
	uint32_t error_rate_window;

	/**
	 * Polling interval in milliseconds for cluster tender
	 * Default: 1000
	 */
	uint32_t tender_interval;

	/**
	 * Number of threads stored in underlying thread pool used by synchronous batch/scan/query commands.
	 * These commands are often sent to multiple server nodes in parallel threads.  A thread pool 
	 * improves performance because threads do not have to be created/destroyed for each command.
	 * Calculate your value using the following formula:
	 *
	 * thread_pool_size = (concurrent synchronous batch/scan/query commands) * (server nodes)
	 *
	 * If your application only uses async commands, this field can be set to zero.
	 * Default: 16
	 */
	uint32_t thread_pool_size;

	/**
	 * Assign tend thread to this specific CPU ID.
	 * Default: -1 (Any CPU).
	 */
	int tend_thread_cpu;

	/**
	 * Client policies
	 */
	as_policies policies;

	/**
	 * lua config.  This is a global config even though it's located here in cluster config.
	 * This config has been left here to avoid breaking the API.
	 *
	 * The global lua config will only be changed once on first cluster initialization.
	 * A better method for initializing lua configuration is to leave this field alone and
	 * instead call aerospike_init_lua():
	 *
	 * ~~~~~~~~~~{.c}
	 * // Get default global lua configuration.
	 * as_config_lua lua;
	 * as_config_lua_init(&lua);
	 *
	 * // Optionally modify lua defaults.
	 * lua.cache_enabled = <enable lua cache>;
	 * strcpy(lua.user_path, <lua user directory>);
	 *
	 * // Initialize global lua configuration.
	 * aerospike_init_lua(&lua);
	 * ~~~~~~~~~~
	 */
	as_config_lua lua;

	/**
	 * TLS configuration parameters.
	 */
	as_config_tls tls;

	/**
	 * Authentication mode.
	 * Default: AS_AUTH_INTERNAL
	 */
	as_auth_mode auth_mode;

	/**
	 * Should cluster instantiation fail if the client fails to connect to a seed or
	 * all the seed's peers.
	 *
	 * If true, return an error if all seed connections fail or a seed is valid,
	 * but all peers from that seed are not reachable.
	 *
	 * If false, a partial cluster will be created and the client will automatically connect
	 * to the remaining nodes when they become available.
	 */
	bool fail_if_not_connected;
	
	/**
	 * Flag to signify if "services-alternate" should be used instead of "services"
	 * Default: false
	 */
	bool use_services_alternate;

	/**
	 * For testing purposes only.  Do not modify.
	 *
	 * Should the aerospike instance communicate with the first seed node only
	 * instead of using the data partition map to determine which node to send the
	 * database command.
	 *
	 * Default: false
	 */
	bool force_single_node;

	/**
	 * Track server rack data.  This field is useful when directing read commands to 
	 * the server node that contains the key and exists on the same rack as the client.
	 * This serves to lower cloud provider costs when nodes are distributed across different
	 * racks/data centers.
	 *
	 * rack_id or rack_ids, AS_POLICY_REPLICA_PREFER_RACK and server rack configuration must
	 * also be set to enable this functionality.
	 *
	 * Default: false
	 */
	bool rack_aware;

	/**
	 * Rack where this client instance resides. If rack_ids is set, rack_id is ignored.
	 *
	 * rack_aware, AS_POLICY_REPLICA_PREFER_RACK and server rack configuration must also be
	 * set to enable this functionality.
	 *
	 * Default: 0
	 */
	int rack_id;

	/**
	 * List of preferred racks in order of preference. If rack_ids is set, rack_id is ignored.
	 * Do not set directly. Use multiple as_config_add_rack_id() calls to add rack ids.
	 *
	 * rack_aware, AS_POLICY_REPLICA_PREFER_RACK and server rack configuration must also be
	 * set to enable this functionality.
	 *
	 * Default: NULL
	 */
	 as_vector* rack_ids;

	/**
	 * Indicates if shared memory should be used for cluster tending.  Shared memory
	 * is useful when operating in single threaded mode with multiple client processes.
	 * This model is used by wrapper languages such as PHP and Python.  When enabled, 
	 * the data partition maps are maintained by only one process and all other processes 
	 * use these shared memory maps.
	 *
	 * Shared memory should not be enabled for multi-threaded programs.
	 * Default: false
	 */
	bool use_shm;

	/**
	 * Identifier for the shared memory segment associated with the target Aerospike cluster.
	 * Each shared memory segment contains state for one Aerospike cluster.  If there are
	 * multiple Aerospike clusters, a different shm_key must be defined for each cluster.
	 * 
	 * Default: 0xA9000000
	 */
	int shm_key;
	
	/**
	 * Shared memory maximum number of server nodes allowed.  This value is used to size
	 * the fixed shared memory segment.  Leave a cushion between actual server node
	 * count and shm_max_nodes so new nodes can be added without having to reboot the client.
	 * Default: 16
	 */
	uint32_t shm_max_nodes;
	
	/**
	 * Shared memory maximum number of namespaces allowed.  This value is used to size
	 * the fixed shared memory segment.  Leave a cushion between actual namespaces
	 * and shm_max_namespaces so new namespaces can be added without having to reboot the client.
	 * Default: 8
	 */
	uint32_t shm_max_namespaces;
	
	/**
	 * Take over shared memory cluster tending if the cluster hasn't been tended by this
	 * threshold in seconds.
	 * Default: 30
	 */
	uint32_t shm_takeover_threshold_sec;
} as_config;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize the configuration to default values.
 *
 * You should do this to ensure the configuration has valid values, before 
 * populating it with custom options.
 *
 * ~~~~~~~~~~{.c}
 * as_config config;
 * as_config_init(&config);
 * as_config_add_host(&config, "127.0.0.1", 3000);
 * ~~~~~~~~~~
 * 
 * @relates as_config
 */
AS_EXTERN as_config*
as_config_init(as_config* config);

/**
 * Add seed host(s) from a string with format: hostname1[:tlsname1][:port1],...
 * Hostname may also be an IP address in the following formats.
 *
 * ~~~~~~~~~~{.c}
 * IPv4: xxx.xxx.xxx.xxx
 * IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]
 * IPv6: [xxxx::xxxx]
 * ~~~~~~~~~~
 *
 * The host addresses will be copied.
 * The caller is responsible for the original string.
 *
 * ~~~~~~~~~~{.c}
 * as_config config;
 * as_config_init(&config);
 * as_config_add_hosts(&config, "host1,host2:3010,192.168.20.1:3020,[2001::1000]:3030", 3000);
 * ~~~~~~~~~~
 *
 * @relates as_config
 */
AS_EXTERN bool
as_config_add_hosts(as_config* config, const char* string, uint16_t default_port);
	
/**
 * Add host to seed the cluster.
 * The host address will be copied.
 * The caller is responsible for the original address string.
 *
 * ~~~~~~~~~~{.c}
 * as_config config;
 * as_config_init(&config);
 * as_config_add_host(&config, "127.0.0.1", 3000);
 * ~~~~~~~~~~
 *
 * @relates as_config
 */
AS_EXTERN void
as_config_add_host(as_config* config, const char* address, uint16_t port);

/**
 * Remove all hosts.
 *
 * @relates as_config
 */
AS_EXTERN void
as_config_clear_hosts(as_config* config);

/**
 * User authentication for servers with restricted access.  The password will be stored by the
 * client and sent to server in hashed format.
 *
 * ~~~~~~~~~~{.c}
 * 	as_config config;
 * 	as_config_init(&config);
 * 	as_config_set_user(&config, "charlie", "mypassword");
 * ~~~~~~~~~~
 *
 * @relates as_config
 */
AS_EXTERN bool
as_config_set_user(as_config* config, const char* user, const char* password);

/**
 * Free existing string if not null and copy value to string.
 */
AS_EXTERN void
as_config_set_string(char** str, const char* value);

/**
 * Set expected cluster name.
 *
 * @relates as_config
 */
static inline void
as_config_set_cluster_name(as_config* config, const char* cluster_name)
{
	as_config_set_string(&config->cluster_name, cluster_name);
}

/**
 * Set cluster event callback and user data.
 *
 * @relates as_config
 */
static inline void
as_config_set_cluster_event_callback(as_config* config, as_cluster_event_callback callback, void* udata)
{
	config->event_callback = callback;
	config->event_callback_udata = udata;
}

/**
 * Initialize global lua configuration to defaults.
 *
 * @relates as_config
 */
static inline void
as_config_lua_init(as_config_lua* lua)
{
	lua->cache_enabled = false;
	strcpy(lua->user_path, AS_CONFIG_LUA_USER_PATH);
}

/**
 * Set TLS path to a trusted CA certificate file.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_cafile(as_config* config, const char* cafile)
{
	as_config_set_string(&config->tls.cafile, cafile);
}

/**
 * Set string containing trusted CA certificate(s).
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_castring(as_config* config, const char* castring)
{
	as_config_set_string(&config->tls.castring, castring);
}

/**
 * Set TLS path to a directory of trusted certificates.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_capath(as_config* config, const char* capath)
{
	as_config_set_string(&config->tls.capath, capath);
}

/**
 * Set TLS enabled protocols.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_protocols(as_config* config, const char* protocols)
{
	as_config_set_string(&config->tls.protocols, protocols);
}

/**
 * Set TLS enabled cipher suites.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_cipher_suite(as_config* config, const char* cipher_suite)
{
	as_config_set_string(&config->tls.cipher_suite, cipher_suite);
}

/**
 * Set TLS path to a certificate blacklist file.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_cert_blacklist(as_config* config, const char* cert_blacklist)
{
	as_config_set_string(&config->tls.cert_blacklist, cert_blacklist);
}

/**
 * Set TLS path to the client's key for mutual authentication.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_keyfile(as_config* config, const char* keyfile)
{
	as_config_set_string(&config->tls.keyfile, keyfile);
}

/**
 * Set decryption password for the client's key.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_keyfile_pw(as_config* config, const char* pw)
{
	as_config_set_string(&config->tls.keyfile_pw, pw);
}

/**
 * Set client's key string for mutual authentication.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_keystring(as_config* config, const char* keystring)
{
	as_config_set_string(&config->tls.keystring, keystring);
}

/**
 * Set TLS path to the client's certificate chain file for mutual authentication.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_certfile(as_config* config, const char* certfile)
{
	as_config_set_string(&config->tls.certfile, certfile);
}

/**
 * Set client's certificate chain file string for mutual authentication.
 *
 * @relates as_config
 */
static inline void
as_config_tls_set_certstring(as_config* config, const char* certstring)
{
	as_config_set_string(&config->tls.certstring, certstring);
}

/**
 * Add TLS host to seed the cluster.
 * The host address and TLS name will be copied.
 * The caller is responsible for the original address string.
 *
 * ~~~~~~~~~~{.c}
 * as_config config;
 * as_config_init(&config);
 * as_config_tls_add_host(&config, "127.0.0.1", "node1.test.org", 3000);
 * ~~~~~~~~~~
 *
 * @relates as_config
 */
AS_EXTERN void
as_config_tls_add_host(as_config* config, const char* address, const char* tls_name, uint16_t port);

/**
 * Add rack id to list of server racks in order of preference. Only add racks that
 * are close to the client rack. Do not add racks that are far away from the client.
 * The client will still direct commands to far away racks if nodes on closer racks are not
 * available.
 *
 * rack_aware, AS_POLICY_REPLICA_PREFER_RACK and server rack configuration must also be
 * set to enable this functionality.
 *
 * ~~~~~~~~~~{.c}
 * as_config config;
 * as_config_init(&config);
 * // Rack 4 is where the client machine is located.
 * as_config_add_rack_id(&config, 4);
 * // Rack 2 is located in same datacenter complex (maybe in a close by building).
 * as_config_add_rack_id(&config, 2);
 * // All other racks are far enough away that they are equally not preferred, so do not include
 * // them here.
 * ~~~~~~~~~~
 *
 * @relates as_config
 */
AS_EXTERN void
as_config_add_rack_id(as_config* config, int rack_id);

/**
 * Convert string into as_auth_mode enum.
 */
AS_EXTERN bool
as_auth_mode_from_string(as_auth_mode* auth, const char* str);

#ifdef __cplusplus
} // end extern "C"
#endif
