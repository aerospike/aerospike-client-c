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

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Status codes used as return values as as_error.code values.
 */
typedef enum as_status_e {

	/**
	 * Success
	 */
	AEROSPIKE_OK							= 0,

	/**
	 * Generic Error
	 */
	AEROSPIKE_ERR							= 100,

	/**
	 * Client usage error.
	 * Includes error is configuring or using
	 * the client, such as bad arguments.
	 */
	AEROSPIKE_ERR_CLIENT					= 200,

	/**
	 * Cluster state or connection error.
	 */
	AEROSPIKE_ERR_CLUSTER					= 300,

	/**
	 * Write operation error.
	 */
	AEROSPIKE_ERR_WRITE						= 400,

	/**
	 * Read operation error.
	 */
	AEROSPIKE_ERR_READ						= 400,

	/**
	 * Scan operation error.
	 */
	AEROSPIKE_ERR_SCAN						= 500,

	/**
	 * Query operation error.
	 */
	AEROSPIKE_ERR_QUERY						= 600,

		/**
		 * The query was aborted.
		 */
	    AEROSPIKE_ERR_QUERY_ABORTED 		= 601,

	    /**
	     * The result queue is full.
	     */
	    AEROSPIKE_ERR_QUERY_QUEUEFULL 		= 602,

    /**
     * Index operation error.
     */
	AEROSPIKE_ERR_INDEX     				= 700,
	    AEROSPIKE_ERR_INDEX_KEY_NOTFOUND 	= 701,
	    AEROSPIKE_ERR_INDEX_TYPE_MISMATCH 	= 703,
	    AEROSPIKE_ERR_INDEX_NOTFOUND 		= 704,
	    AEROSPIKE_ERR_INDEX_OOM 			= 705,
	    AEROSPIKE_ERR_INDEX_GENERIC 		= 706,
	    AEROSPIKE_ERR_INDEX_EXISTS 			= 707,
	    AEROSPIKE_ERR_INDEX_SINGLEBIN_NS 	= 708,
	    AEROSPIKE_ERR_INDEX_UNKNOWN_TYPE 	= 709,
	    AEROSPIKE_ERR_INDEX_FOUND 			= 710,
	    AEROSPIKE_ERR_INDEX_NOTREADABLE 	= 711,

    /**
     * UDF operation error.
     */
	AEROSPIKE_ERR_UDF       				= 800,

} as_status;


