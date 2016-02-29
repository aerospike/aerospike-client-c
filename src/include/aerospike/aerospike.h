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

/** 
 * @mainpage Aerospike C Client
 *
 * @section intro_sec Introduction
 *
 * This package describes the Aerospike C Client API in detail.
 * Click on "Modules" to see the API.
 *
 * For Overview and Developer Guide, please go to http://www.aerospike.com.
 * 
 * 
 */
 
/**
 *	@defgroup client_operations Client Operations
 *
 *	Each of the client operations require an initialized @ref aerospike client. 
 */

/**
 *	@defgroup client_objects Client Objects
 */

/**
 *	@defgroup aerospike_t Aerospike Types
 */

/**
 *	@defgroup client_utilities Client Utilities
 *  @{
 *    @defgroup stringmap_t StringMap
 *  @}
 */

#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_log.h>
#include <aerospike/as_status.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	@private
 *	Forward declaration of a cluster object.
 */
struct as_cluster_s;

/**
 * 	An instance of @ref aerospike is required to connect to and execute 
 *	operations against an Aerospike Database cluster.
 *
 *	## Configuration
 *
 *	An initialized client configuration is required to initialize a 
 *	@ref aerospike client. See as_config for details on configuration options.
 *	
 *	At a minimum, a configuration needs to be initialized and have at least
 *	one host defined:
 *	
 *	~~~~~~~~~~{.c}
 *	as_config config;
 *	as_config_init(&config);
 *	config.hosts[0] = { "127.0.0.1", 3000 };
 *	~~~~~~~~~~
 *
 *	A single host is used to specify a host in the database cluster to connect to. 
 *	Once connected to a host in the cluster, then client will gather information
 *	about the cluster, including all the other nodes in the cluster. So, all that
 *	is needed is a single valid host, because once a single host is connected, the 
 *	then no other hosts in the configuration will be processed.
 *	
 *	## Initialization
 *
 *	An initialized @ref aerospike object is required to connect to the 
 *	database. Initialization requires a configuration, to bind to the client
 *	instance. 
 *
 *	The @ref aerospike object can be initialized via either:
 *
 * 	- aerospike_init() — Initialize a stack allocated @ref aerospike.
 *	- aerospike_new() — Create and initialize a heap allocated @ref aerospike.
 *
 *	Both initialization functions require a configuration.
 *
 *	The following uses a stack allocated @ref aerospike and initializes it
 *	with aerospike_init():
 *
 *	~~~~~~~~~~{.c}
 *	aerospike as;
 *	aerospike_init(&as, &config);
 *	~~~~~~~~~~
 *	
 *	## Connecting
 *
 *	An application can connect to the database with an initialized
 *	@ref aerospike. At this point, the client has not connected. The 
 *	client will be connected if `aerospike_connect()` completes 
 *	successfully:
 *	
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_connect(&as, &err) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	The `err` parameter will be populated if an error while attempting to
 *	connect to the database. See as_error, for more information on error 
 *	handling.
 * 
 *	An aerospike object internally keeps cluster state and maintains connection pools to the cluster. 
 *	The same aerospike object should be reused by the application for database operations 
 *	to a given cluster. 
 *
 *	If the application requires connecting to multiple Aerospike clusters, the application must
 *	create multiple aerospike objects, each connecting to a different cluster.
 * 
 *	## Disconnecting
 *
 *	When the connection to the database is not longer required, then the 
 *	connection to the cluster can be closed via `aerospike_close()`:
 *
 *	~~~~~~~~~~{.c}
 *	aerospike_close(&as, &err);
 *	~~~~~~~~~~
 *
 *	## Destruction
 *
 *	When the client is not longer required, the client and its resources should 
 *	be releases via `aerospike_destroy()`:
 *
 *	~~~~~~~~~~{.c}
 *	aerospike_destroy(&as);
 *	~~~~~~~~~~
 *
 *	@ingroup client_objects
 */
typedef struct aerospike_s {

	/**
	 *	@private
	 *	If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	@private
	 *	Cluster state.
	 */
	struct as_cluster_s * cluster;

	/**
	 *	client configuration
	 */
	as_config config;

} aerospike;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize a stack allocated aerospike instance. 
 *
 *	The config parameter can be an instance of `as_config` or `NULL`. If `NULL`,
 *	then the default configuration will be used.
 *
 *	~~~~~~~~~~{.c}
 *	aerospike as;
 *	aerospike_init(&as, &config);
 *	~~~~~~~~~~
 *
 *	Once you are finished using the instance, then you should destroy it via the 
 *	`aerospike_destroy()` function.
 *
 *	@param as 		The aerospike instance to initialize.
 *	@param config 	The configuration to use for the instance.
 *
 *	@returns the initialized aerospike instance
 *
 *	@see config for information on configuring the client.
 *
 *	@relates aerospike
 */
aerospike*
aerospike_init(aerospike* as, as_config* config);

/**
 *	Creates a new heap allocated aerospike instance.
 *
 *	~~~~~~~~~~{.c}
 *	aerospike * as = aerospike_new(&config);
 *	~~~~~~~~~~
 *
 *	Once you are finished using the instance, then you should destroy it via the 
 *	`aerospike_destroy()` function.
 *
 *	@param config	The configuration to use for the instance.
 *
 *	@returns a new aerospike instance
 *
 *	@see config for information on configuring the client.
 *
 *	@relates aerospike
 */
aerospike*
aerospike_new(as_config* config);

/**
 * Initialize global lua configuration.
 *
 *	@param config 	The lua configuration to use for all cluster instances.
 */
void
aerospike_init_lua(as_config_lua* config);	

/**
 *	Destroy the aerospike instance and associated resources.
 *
 *	~~~~~~~~~~{.c}
 *	aerospike_destroy(&as);
 *	~~~~~~~~~~
 *
 *	@param as 		The aerospike instance to destroy
 *
 *	@relates aerospike
 */
void
aerospike_destroy(aerospike* as);

/**
 *	Connect an aerospike instance to the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	aerospike_connect(&as, &err);
 *	~~~~~~~~~~
 *
 *	Once you are finished using the connection, then you must close it via
 *	the `aerospike_close()` function.
 *
 *	If connect fails, then you do not need to call `aerospike_close()`.
 *
 *	@param as 		The aerospike instance to connect to a cluster.
 *	@param err 		If an error occurs, the err will be populated.
 *
 *	@returns AEROSPIKE_OK on success. Otherwise an error occurred.
 *
 *	@relates aerospike
 */
as_status
aerospike_connect(aerospike* as, as_error* err);

/**
 *	Close connections to the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	aerospike_close(&as, &err);
 *	~~~~~~~~~~
 *
 *	@param as 		The aerospike instance to disconnect from a cluster.
 *	@param err 		If an error occurs, the err will be populated.
 *
 *	@returns AEROSPIKE_OK on success. Otherwise an error occurred. 
 *
 *	@relates aerospike
 */
as_status
aerospike_close(aerospike* as, as_error* err);

/**
 *	Is cluster connected to any server nodes.
 *
 *	~~~~~~~~~~{.c}
 *	bool connected = aerospike_cluster_is_connected(&as);
 *	~~~~~~~~~~
 *
 *	@param as 		The aerospike instance to check.
 *
 *	@returns true when cluster is connected.
 *
 *	@relates aerospike
 */
bool
aerospike_cluster_is_connected(aerospike* as);

/**
 *	Is cluster connected to any server nodes.
 *
 *	~~~~~~~~~~{.c}
 *	bool connected = aerospike_cluster_is_connected(&as);
 *	~~~~~~~~~~
 *
 *	@param as 		The aerospike instance to check.
 *
 *	@returns true when cluster is connected.
 *
 *	@relates aerospike
 */
bool
aerospike_cluster_is_connected(aerospike* as);

/**
 *	Should stop socket operation if interrupted by a signal.  Default is false which means
 *	the socket operation will be retried until timeout.
 *
 *	@relates aerospike
 */
void
aerospike_stop_on_interrupt(bool stop);

#ifdef __cplusplus
} // end extern "C"
#endif
