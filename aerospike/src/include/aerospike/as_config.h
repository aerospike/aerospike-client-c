/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

/** 
 *	@defgroup config Configuration
 *
 *	The `as_config` object defines the settings for the `aerospike` client.
 *
 *	Before populating the object, you will want to initialize it with 
 *	default values.
 *
 *	~~~~~~~~~~{.c}
 *	as_config config;
 *	as_config_init(&config);
 *	~~~~~~~~~~
 *
 *	The client will require at least one seed host to connect to:
 *
 *	~~~~~~~~~~{.c}
 *	    config.hosts[0] = { .addr = "127.0.0.1", .port = 3000 };
 *	~~~~~~~~~~
 *
 *	You can define up to 16 hosts for the seed. The client will iterate over 
 *	the list until it connects with one of the hosts. 
 *
 *	The configuration also defines default policies for the application. The 
 *	`as_config_init()` function already presets default values for the policies.
 *
 *	Depending on your application, you may want to set your own default values
 *	for the policies to use for the client. 
 *
 *	However, you should note that each client call accepts a policy, allowing 
 *	you to override the default policy.
 *
 *	If you are using using user-defined functions (UDF) for processing query 
 *	results (i.e aggregations), then you will find it useful to set the 
 *	`mod_lua` settings. Of particular importance is the `mod_lua.user_path`, 
 *	which allows you to define a path to where the client library will look for
 *	Lua files for processing.
 *
 *	~~~~~~~~~~{.c}
 *	    strcpy(config.mod_lua.user_path, "/home/me/lua");
 *	~~~~~~~~~~
 *
 *
 *	@{
 */

#pragma once 

#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 * The size of path strings
 */
#define AS_CONFIG_PATH_SIZE 256

/**
 * The maximum string length of path strings
 */
#define AS_CONFIG_PATH_LEN 	AS_CONFIG_PATH_SIZE - 1

/**
 * The size of as_config.hosts
 */
#define AS_CONFIG_HOSTS_SIZE 256

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Host Information
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
 *	lua module config
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
	 *	Default location is: /opt/citrusleaf/sys/udf/lua
	 */
	char system_path[AS_CONFIG_PATH_SIZE];

	/**
	 *	The path to user's UDF files.
	 *	Default location is: /opt/citrusleaf/usr/udf/lua
	 */
	char user_path[AS_CONFIG_PATH_SIZE];

} as_config_lua;

/**
 *	Client Configuration 
 *
 *	@see config for information on using as_config.
 */
typedef struct as_config_s {

	/**
	 *	Use non-blocking sockets
	 */
	bool non_blocking;

	/**
	 *	Polling interval in milliseconds for cluster tender
	 */
	uint32_t tender_interval;

	/**
	 *	Client policies
	 */
	as_policies policies;

	/**
	 *	(seed) hosts
	 *	Populate with one or more hosts in the cluster
	 *	that you intend to connect with.
	 */
	as_config_host hosts[AS_CONFIG_HOSTS_SIZE];

	/**
	 *	lua config
	 */
	as_config_lua lua;

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
 *
 *		config.hosts[0] = {.addr = "127.0.0.1", .port = 3000};
 *	~~~~~~~~~~
 *	
 *	@param c The configuration to initialize.
 *	
 *	@return The initialized configuration on success. Otherwise NULL.
 */
as_config * as_config_init(as_config * c);


/**
 *	@}
 */
