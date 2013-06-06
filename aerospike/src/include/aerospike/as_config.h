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
struct as_config_host_s {
	
	// host address
	const char * addr;
	
	// host port
	uint16_t port;
};

typedef struct as_config_host_s as_config_host;

/**
 * Client Configuration 
 */
struct as_config_s {

	// global timeout (ms). Used if policies do not specify a timeout.
	uint32_t timeout;

	// use non-blocking sockets
	bool nonblocking;

	// frequency (seconds) for updating cluster state information
	uint32_t tend_frequency;

	// client policies
	as_policies policies;

	// client logger
	as_logger * logger;

	// (seed) hosts
	as_config_host hosts[16];

	// lua module config
	mod_lua_config mod_lua;

};

typedef struct as_config_s as_config;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

// as_config * as_config_new();

// as_config * as_config_init(as_config * c);

// as_config * as_config_set_timeout(as_config * c, uint32_t timeout);

// as_config * as_config_set_nonblocking(as_config * c, bool enable);

// as_config * as_config_set_tend_frequency(as_config * c, uint32_t ms);

// as_config * as_config_set_policies(as_config * c, as_policies * p);

// as_config * as_config_set_policy_read(as_config * c, as_policy_read * p);

// as_config * as_config_set_policy_write(as_config * c, as_policy_write * p);

// as_config * as_config_set_policy_remove(as_config * c, as_policy_remove * p);

// as_config * as_config_set_policy_scan(as_config * c, as_policy_scan * p);

// as_config * as_config_set_policy_query(as_config * c, as_policy_query * p);

// as_config * as_config_set_policy_ldt(as_config * c, as_policy_ldt * read);

// as_config * as_config_add_host(as_config * c, const char * addr, uint32_t port);





