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

#pragma once 

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Status codes used as return values as as_error.code values.
 */
typedef enum as_status_e {

	/**
	 *	Success
	 */
	AEROSPIKE_OK							= 0,

	/**
	 *	Generic Error
	 */
	AEROSPIKE_ERR							= 100,

	/**
	 *	Client usage error.
	 *	This includes errors in configuring or using 
	 *	the client.
	 */
	AEROSPIKE_ERR_CLIENT					= 200,

	/**
	 *	Cluster state or connection error.
	 */
	AEROSPIKE_ERR_CLUSTER					= 300,

	/**
	 *	Write operation error.
	 */
	AEROSPIKE_ERR_WRITE						= 400,
		AEROSPIKE_ERR_WRITE_GENERATION		= 401,

	/**
	 *	Read operation error.
	 */
	AEROSPIKE_ERR_READ						= 500,

	/**
	 *	Scan operation error.
	 */
	AEROSPIKE_ERR_SCAN						= 600,

	/**
	 *	Query operation error.
	 */
	AEROSPIKE_ERR_QUERY						= 700,

		/**
		 *	The query was aborted.
		 */
		AEROSPIKE_ERR_QUERY_ABORTED 		= 701,

		/**
		 *	The result queue is full.
		 */
		AEROSPIKE_ERR_QUERY_QUEUEFULL 		= 702,

	/**
	 *	Index operation error.
	 */
	AEROSPIKE_ERR_INDEX 					= 800,
		AEROSPIKE_ERR_INDEX_KEY_NOTFOUND 	= 801,
		AEROSPIKE_ERR_INDEX_TYPE_MISMATCH 	= 803,
		AEROSPIKE_ERR_INDEX_NOTFOUND 		= 804,
		AEROSPIKE_ERR_INDEX_OOM 			= 805,
		AEROSPIKE_ERR_INDEX_GENERIC 		= 806,
		AEROSPIKE_ERR_INDEX_EXISTS 			= 807,
		AEROSPIKE_ERR_INDEX_SINGLEBIN_NS 	= 808,
		AEROSPIKE_ERR_INDEX_UNKNOWN_TYPE 	= 809,
		AEROSPIKE_ERR_INDEX_FOUND 			= 810,
		AEROSPIKE_ERR_INDEX_NOTREADABLE 	= 811,

	/**
	 *	UDF operation error.
	 */
	AEROSPIKE_ERR_UDF						= 900,

} as_status;


