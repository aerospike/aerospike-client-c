/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#pragma once 

#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_logger.h>
#include <aerospike/mod_lua_config.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Host Information
 */
typedef struct as_config_host_s {
	
	/**
	 * Host address
	 */
	const char * addr;
	
	/**
	 * Host port
	 */
	uint16_t port;

} as_config_host;

/**
 * Client Configuration 
 */
typedef struct as_config_s {

	/**
	 * Use non-blocking sockets
	 */
	bool non_blocking;

	/**
	 * Polling interval in milliseconds for cluster tender
	 */
	uint32_t tender_interval;

	/**
	 * Client policies
	 */
	as_policies policies;

	/**
	 * (seed) hosts
	 * Populate with one or more hosts in the cluster
	 * that you intend to connect with.
	 */
	as_config_host hosts[16];

	/**
	 * lua module config
	 */
	struct {
		bool	cache_enabled;
		char	system_path[256];
		char	user_path[256];
	} mod_lua;

} as_config;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_config * as_config_init(as_config * c);




