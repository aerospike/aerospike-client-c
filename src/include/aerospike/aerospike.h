/*
 * Copyright 2008-2024 Aerospike, Inc.
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

/** 
 * @mainpage Introduction
 *
 * @section intro_sec About the Aerospike C Client
 *
 * You can use the Aerospike C client to build C/C++ applications that store and retrieve data from an
 * Aerospike cluster. The C client is a smart client that periodically pings nodes for cluster
 * status and manages interactions with the cluster. The following functionality is supported:
 *
 * - Database commands
 *   - Key/Value
 *   - Map/List collections
 *   - Batch read
 *   - Scan
 *   - Secondary index query
 *   - User defined Lua functions
 *   - Expression filters
 * - Both synchronous and asynchronous command models
 * - Asynchronous model supports the following event frameworks.
 *   - libev
 *   - libevent
 *   - libuv
 * - Thread safe API
 * - Shared memory cluster tend state for multi-process applications
 * - TLS secure sockets
 *
 * See <a href="files.html">Files</a> for Aerospike header files.<br/>
 * See <a href="classes.html">Data Structures </a> for Aerospike types.<br/>
 * See <a href="https://aerospike.com/developer/client/install?client=c">Developer Guide</a> for installation
 * instructions and example code.<br/>
 */
 
/**
 * @defgroup client_objects Client Objects
 */

/**
 * @defgroup client_operations Client Operations
 *
 * Client operations require an initialized aerospike client.
 */

/**
 * @defgroup aerospike_t Client Types
 */

#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_log.h>
#include <aerospike/as_status.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * @private
 * Forward declaration of a cluster object.
 */
struct as_cluster_s;

/**
 * The aerospike struct is used to connect and execute operations against an 
 * Aerospike database cluster.
 *
 * ## Configuration
 *
 * A client configuration is required to initialize an aerospike client.
 * See as_config for details on configuration options.
 * 
 * At least one seed host must be defined.
 *
 * ~~~~~~~~~~{.c}
 * as_config config;
 * as_config_init(&config);
 * as_config_add_host(&config, "127.0.0.1", 3000);
 * ~~~~~~~~~~
 *
 * Once connected to a host in the cluster, then client will gather information
 * about the cluster, including all other nodes in the cluster. So, all that
 * is needed is a single valid host.  Multiple hosts can still be provided in
 * case the first host is not currently active.
 * 
 * ## Initialization
 *
 * Initialization requires a configuration to bind to the client instance.
 *
 * The aerospike instance can be initialized via either:
 *
 * - aerospike_init() — Initialize a stack allocated aerospike instance.
 * - aerospike_new() — Create and initialize a heap allocated aerospike instance.
 *
 * Once initialized, the ownership of the as_config instance fields are transferred 
 * to the aerospike instance.  The user should never call as_config_destroy() directly.
 *
 * The following uses a stack allocated aerospike instance and initializes it
 * with aerospike_init():
 *
 * ~~~~~~~~~~{.c}
 * aerospike as;
 * aerospike_init(&as, &config);
 * ~~~~~~~~~~
 * 
 * ## Connecting
 *
 * The client will be connected if `aerospike_connect()` completes successfully:
 * 
 * ~~~~~~~~~~{.c}
 * if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
 *   fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 * }
 * ~~~~~~~~~~
 *
 * The `err` parameter will be populated if an error occurs. See as_error for more information
 * on error handling.
 * 
 * An aerospike object internally keeps cluster state and maintains connection pools to the cluster. 
 * The same aerospike object should be reused by the application for database operations 
 * to a given cluster. 
 *
 * If the application requires connecting to multiple Aerospike clusters, the application must
 * create multiple aerospike objects, each connecting to a different cluster.
 * 
 * ## Disconnecting
 *
 * When the connection to the database is not longer required, then the 
 * connection to the cluster can be closed via `aerospike_close()`:
 *
 * ~~~~~~~~~~{.c}
 * aerospike_close(&as, &err);
 * ~~~~~~~~~~
 *
 * ## Destruction
 *
 * When the client is not longer required, the client and its resources should 
 * be releases via `aerospike_destroy()`:
 *
 * ~~~~~~~~~~{.c}
 * aerospike_destroy(&as);
 * ~~~~~~~~~~
 *
 * @ingroup client_objects
 */
typedef struct aerospike_s {

	/**
	 * @private
	 * Cluster state.
	 */
	struct as_cluster_s* cluster;

	/**
	 * Client configuration.
	 */
	as_config config;

	/**
	* @private
	* If true, then aerospike_destroy() will free this instance.
	*/
	bool _free;

} aerospike;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize a stack allocated aerospike instance. 
 *
 * The config parameter can be an instance of `as_config` or `NULL`. If `NULL`,
 * then the default configuration will be used.
 *
 * Ownership of the as_config instance fields are transferred to the aerospike instance.
 * The user should never call as_config_destroy() directly.
 *
 * ~~~~~~~~~~{.c}
 * aerospike as;
 * aerospike_init(&as, &config);
 * ~~~~~~~~~~
 *
 * Once you are finished using the instance, then you should destroy it via the 
 * `aerospike_destroy()` function.
 *
 * @param as 		The aerospike instance to initialize.
 * @param config 	The configuration to use for the instance.
 *
 * @returns the initialized aerospike instance
 *
 * @see config for information on configuring the client.
 *
 * @relates aerospike
 */
AS_EXTERN aerospike*
aerospike_init(aerospike* as, as_config* config);

/**
 * Creates a new heap allocated aerospike instance.
 *
 * Ownership of the as_config instance fields are transferred to the aerospike instance.
 * The user should never call as_config_destroy() directly.
 *
 * ~~~~~~~~~~{.c}
 * aerospike* as = aerospike_new(&config);
 * ~~~~~~~~~~
 *
 * Once you are finished using the instance, then you should destroy it via the 
 * `aerospike_destroy()` function.
 *
 * @param config	The configuration to use for the instance.
 *
 * @returns a new aerospike instance
 *
 * @see config for information on configuring the client.
 *
 * @relates aerospike
 */
AS_EXTERN aerospike*
aerospike_new(as_config* config);

/**
 * Initialize global lua configuration.
 *
 * @param config 	The lua configuration to use for all cluster instances.
 *
 * @relates aerospike
 */
AS_EXTERN void
aerospike_init_lua(as_config_lua* config);	

/**
 * Destroy the aerospike instance and associated resources.
 *
 * ~~~~~~~~~~{.c}
 * aerospike_destroy(&as);
 * ~~~~~~~~~~
 *
 * @param as 		The aerospike instance to destroy
 *
 * @relates aerospike
 */
AS_EXTERN void
aerospike_destroy(aerospike* as);

/**
 * Connect an aerospike instance to the cluster.
 *
 * ~~~~~~~~~~{.c}
 * aerospike_connect(&as, &err);
 * ~~~~~~~~~~
 *
 * Once you are finished using the connection, then you must close it via
 * the `aerospike_close()` function.
 *
 * If connect fails, then you do not need to call `aerospike_close()`.
 *
 * @param as 		The aerospike instance to connect to a cluster.
 * @param err 		If an error occurs, the err will be populated.
 *
 * @returns AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_connect(aerospike* as, as_error* err);

/**
 * Close connections to the cluster.
 *
 * ~~~~~~~~~~{.c}
 * aerospike_close(&as, &err);
 * ~~~~~~~~~~
 *
 * @param as 		The aerospike instance to disconnect from a cluster.
 * @param err 		If an error occurs, the err will be populated.
 *
 * @returns AEROSPIKE_OK on success. Otherwise an error occurred. 
 *
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_close(aerospike* as, as_error* err);

/**
 * Is cluster connected to any server nodes.
 *
 * ~~~~~~~~~~{.c}
 * bool connected = aerospike_cluster_is_connected(&as);
 * ~~~~~~~~~~
 *
 * @param as 		The aerospike instance to check.
 *
 * @returns true when cluster is connected.
 *
 * @relates aerospike
 */
AS_EXTERN bool
aerospike_cluster_is_connected(aerospike* as);

/**
 * Should stop socket operation if interrupted by a signal.  Default is false which means
 * the socket operation will be retried until timeout.
 *
 * @relates aerospike
 */
AS_EXTERN void
aerospike_stop_on_interrupt(bool stop);

/**
 * Remove records in specified namespace/set efficiently.  This method is many orders of magnitude
 * faster than deleting records one at a time.
 *
 * See <a href="https://www.aerospike.com/docs/reference/info#truncate">https://www.aerospike.com/docs/reference/info#truncate</a>
 *
 * This asynchronous server call may return before the truncation is complete.  The user can still
 * write new records after the server returns because new records will have last update times
 * greater than the truncate cutoff (set at the time of truncate call).
 *
 * @param as			Aerospike instance.
 * @param err			If an error occurs, the err will be populated.
 * @param policy		Info policy. If NULL, then the default policy will be used.
 * @param ns			Required namespace.
 * @param set			Optional set name.  Pass in NULL to delete all sets in namespace.
 * @param before_nanos	Optionally delete records before record last update time.
 * 					Units are in nanoseconds since unix epoch (1970-01-01).
 * 					If specified, value must be before the current time.
 * 					Pass in 0 to delete all records in namespace/set regardless of last update time.
 * @returns AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_truncate(
	aerospike* as, as_error* err, as_policy_info* policy, const char* ns, const char* set,
	uint64_t before_nanos
	);

/**
 * Refresh the current TLS configuration by reloading its certificate, key, and blacklist files.
 *
 * @param as 		Aerospike instance whose TLS configuration to refresh.
 * @param err		If an error occurs, this will be populated.
 *
 * @returns AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_reload_tls_config(aerospike* as, as_error* err);

/**
 * Set XDR filter for given datacenter name and namespace. The expression filter indicates
 * which records XDR should ship to the datacenter.
 *
 * @param as			Aerospike instance.
 * @param err			If an error occurs, this will be populated.
 * @param policy		Info policy. If NULL, then the default policy will be used.
 * @param dc			Datacenter name.
 * @param ns			Namespace.
 * @param filter_b64	expression filter in base64 encoding. Use as_exp_build_b64() to create.
 *
 * @returns AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 * @relates aerospike
 */
AS_EXTERN as_status
aerospike_set_xdr_filter(
	aerospike* as, as_error* err, as_policy_info* policy, const char* dc, const char* ns,
	const char* filter_b64
	);

#ifdef __cplusplus
} // end extern "C"
#endif
