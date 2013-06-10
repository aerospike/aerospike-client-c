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
#include <aerospike/as_config.h>
#include <aerospike/as_status.h>
#include <stdbool.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

struct cl_cluster_s;

/**
 * Client handle used for all calls to a cluster.
 */

struct aerospike_s {

	/**
	 * Specifies whether the object can be free()'d
	 */
	bool _free;

	/**
	 * cluster state
	 */
	struct cl_cluster_s * cluster;

	/**
	 * client configuration
	 */
	as_config config;

};

typedef struct aerospike_s aerospike;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize the aerospike object on the stack
 * @returns the initialized aerospike object
 */
aerospike * aerospike_init(aerospike * as, as_config * config);

/**
 * Creates a new aerospike object on the heap
 * @returns a new aerospike object
 */
aerospike * aerospike_new(as_config * config);

/**
 * Destroy the aerospike obect
 */
void aerospike_destroy(aerospike * as);

/**
 * Connect to the cluster
 */
as_status aerospike_connect(aerospike * as, as_error * err);

/**
 * Close connections to the cluster
 */
as_status aerospike_close(aerospike * as, as_error * err);



