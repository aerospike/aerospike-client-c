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
 *	@defgroup aerospike Aerospike Instance
 *	@{
 */

#pragma once 

#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_logger.h>
#include <aerospike/mod_lua_config.h>

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
#define AS_CONFIG_PATH_LEN 	AS_CONFIG_PATH_SIZE - 256

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
 *	Client Configuration 
 *
 *	The configuration is usually created on the stack. Once you have a config instance,
 *	you will want to initialize it to default values.
 *
 *	~~~~~~~~~~{.c}
 *	    as_config config;
 *	    as_config_init(&config);
 *	~~~~~~~~~~
 *
 *	You will want to then add hosts to seed the cluster discovery:
 *
 *	~~~~~~~~~~{.c}
 *	    config.hosts[0] = { .addr = "127.0.0.1", .port = 3000 };
 *	~~~~~~~~~~
 *
 *	You can add up to 16 hosts for the seed.
 *
 *	The configuration also defines default policies for the application. The `as_config_init()`
 *	function already presets default values for the policies. You can feel free to change the 
 *	default policies to match your needs. However, you should note that each call that utilizes
 *	a policy, will allow you to override the default policy.
 *
 *	If you are using using user-defined functions (UDF) for processing query results 
 *	(i.e aggregations), then you will find it useful to set the `mod_lua` settings. Of particular
 *	importance is the `mod_lua.user_path`, which allows you to define a path to where the client
 *	library will look for Lua files for processing.
 *
 *	~~~~~~~~~~{.c}
 *	    strcpy(config.mod_lua.user_path, "/home/me/lua");
 *	~~~~~~~~~~
 *
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
	 *	lua module config
	 */
	struct {

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

	} mod_lua;

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