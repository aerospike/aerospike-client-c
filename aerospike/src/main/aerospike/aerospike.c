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

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/cf_log_internal.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize the aerospike object on the stack
 * @returns the initialized aerospike object
 */
aerospike * aerospike_init(aerospike * as, as_config * config) 
{
	as->_free = false;
	as->cluster = NULL;
	if ( config != NULL ) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	// else {
	//     as_config_init(&as->config);
	// }
	return as;
}

/**
 * Creates a new aerospike object on the heap
 * @returns a new aerospike object
 */
aerospike * aerospike_new(as_config * config) 
{
	aerospike * as = (aerospike *) malloc(sizeof(aerospike));
	as->_free = true;
	as->cluster = NULL;
	if ( config != NULL ) {
		memcpy(&as->config, config, sizeof(as_config));
	}
	// else {
	//     as_config_init(&as->config);
	// }
	return as;
}

/**
 * Destroy the aerospike obect
 */
void aerospike_destroy(aerospike * as) {
	aerospike_close(as);
	if ( as->_free ) {
		free(as);
		as->_free = false;
	}
}

/**
 * Connect to the cluster
 */
as_status aerospike_connect(aerospike * as) 
{
	extern cf_atomic32 g_initialized;
	extern int g_init_pid;
	if ( ! g_initialized ) {

		// remember the process id which is spawning the background threads.
		// only this process can call a pthread_join() on the threads that it spawned.
		g_init_pid = getpid();

		// initialize the cluster
		citrusleaf_cluster_init();

#ifdef DEBUG_HISTOGRAM  
		if (NULL == (cf_hist = cf_histogram_create("transaction times")))
			cf_error("couldn't create histogram for client");
#endif  

		g_initialized = true;
		
		cf_debug("aerospike_connect: as=%p as->cluster=%p",as,as->cluster);
		as->cluster = citrusleaf_cluster_create();
		cf_debug("aerospike_connect: as=%p as->cluster=%p",as,as->cluster);
		as->cluster->nbconnect = as->config.nonblocking;
		uint32_t nhosts = sizeof(as->config.hosts) / sizeof(as_config_host);
		for ( int i = 0; as->config.hosts[i].addr != NULL && i < nhosts; i ++ ) {
			citrusleaf_cluster_add_host(as->cluster, as->config.hosts[i].addr, as->config.hosts[i].port, as->config.policies.timeout);
		}
	}
	
	return AEROSPIKE_OK;
}

/**
 * Close connections to the cluster
 */
as_status aerospike_close(aerospike * as) 
{
	// TODO: shutdown: cluster, batch, async, query and scan
	return AEROSPIKE_OK;
}
