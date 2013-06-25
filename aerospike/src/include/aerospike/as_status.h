/*******************************************************************************
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
 ******************************************************************************/

#pragma once

/*******************************************************************************
 *	TYPES
 ******************************************************************************/

/**
 *	Status codes used as return values as as_error.code values.
 */
typedef enum as_status_e {

	/***************************************************************************
	 *	SUCCESS (all < 100)
	 **************************************************************************/

	/**
	 *	Generic success.
	 */
	AEROSPIKE_OK							= 0,

	/***************************************************************************
	 *	ERRORS (all >= 100)
	 **************************************************************************/

	/**
	 *	Generic error.
	 */
	AEROSPIKE_ERR							= 100,

	/***************************************************************************
	 *	CLIENT API USAGE
	 **************************************************************************/

	/**
	 *	Generic client API usage error.
	 */
	AEROSPIKE_ERR_CLIENT					= 200,

	/**
	 *	Invalid client API parameter.
	 */
	AEROSPIKE_ERR_PARAM						= 201,

	/***************************************************************************
	 *	CLUSTER DISCOVERY & CONNECTION
	 **************************************************************************/

	/**
	 *	Generic cluster discovery & connection error.
	 */
	AEROSPIKE_ERR_CLUSTER					= 300,

	/***************************************************************************
	 *	INCOMPLETE REQUESTS (i.e. NOT from server-returned error codes)
	 **************************************************************************/

	/**
	 *	Request timed out.
	 */
	AEROSPIKE_ERR_TIMEOUT					= 400,

	/**
	 *	Request randomly dropped by client for throttling.
	 *	@warning	Not yet supported.
	 */
	AEROSPIKE_ERR_THROTTLED					= 401,

	/***************************************************************************
	 *	COMPLETED REQUESTS (all >= 500, from server-returned error codes)
	 **************************************************************************/

	/**
	 *	Generic error returned by server.
	 */
	AEROSPIKE_ERR_SERVER					= 500,

	/**
	 *	Request protocol invalid, or invalid protocol field.
	 */
	AEROSPIKE_ERR_REQUEST_INVALID			= 501,

	/**
	 *	Namespace in request not found on server.
	 *	@warning	Not yet supported, shows as AEROSPIKE_ERR_REQUEST_INVALID.
	 */
	AEROSPIKE_ERR_NAMESPACE_NOT_FOUND		= 502,

	/**
	 *	The server node is running out of memory and/or storage device space
	 *	reserved for the specified namespace.
	 */
	AEROSPIKE_ERR_SERVER_FULL				= 503,

	/**
	 *	A cluster state change occurred during the request. This may also be
	 *	returned by scan operations with the fail_on_cluster_change flag set.
	 */
	AEROSPIKE_ERR_CLUSTER_CHANGE			= 504,

	/***************************************************************************
	 *	RECORD-SPECIFIC
	 **************************************************************************/

	/**
	 *	Generic record error.
	 */
	AEROSPIKE_ERR_RECORD					= 600,

	/**
	 *	Too may concurrent requests for one record - a "hot-key" situation.
	 */
	AEROSPIKE_ERR_RECORD_BUSY				= 601,

	/**
	 *	Record does not exist in database. May be returned by read, or write
	 *	with policy AS_POLICY_EXISTS_UPDATE.
	 *	@warning	AS_POLICY_EXISTS_UPDATE not yet supported.
	 */
	AEROSPIKE_ERR_RECORD_NOT_FOUND			= 602,

	/**
	 *	Record already exists. May be returned by write with policy
	 *	AS_POLICY_EXISTS_CREATE.
	 */
	AEROSPIKE_ERR_RECORD_EXISTS				= 603,

	/**
	 *	Generation of record in database does not satisfy write policy.
	 */
	AEROSPIKE_ERR_RECORD_GENERATION			= 604,

	/**
	 *	Record being (re-)written can't fit in a storage write block.
	 */
	AEROSPIKE_ERR_RECORD_TOO_BIG			= 605,

	/**
	 *	Bin modification operation can't be done on an existing bin due to its
	 *	value type.
	 */
	AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE		= 606,

	/***************************************************************************
	 * XDR-SPECIFIC
	 **************************************************************************/

	/**
	 *	XDR is not available for the cluster.
	 */
	AEROSPIKE_ERR_NO_XDR					= 900,

	/***************************************************************************
	 *	SCAN OPERATIONS
	 **************************************************************************/

	/**
	 *	Generic scan error.
	 */
	AEROSPIKE_ERR_SCAN						= 1000,

	/**
	 *	Scan aborted by user.
	 */
	AEROSPIKE_ERR_SCAN_ABORTED				= 1001,

	/***************************************************************************
	 *	QUERY OPERATIONS
	 **************************************************************************/

	/**
	 *	Generic query error.
	 */
	AEROSPIKE_ERR_QUERY						= 1100,

	/**
	 *	Query was aborted.
	 */
	AEROSPIKE_ERR_QUERY_ABORTED 			= 1101,

	/**
	 *	Query processing queue is full.
	 */
	AEROSPIKE_ERR_QUERY_QUEUE_FULL 			= 1102,

	/***************************************************************************
	 *	SECONDARY INDEX OPERATIONS
	 **************************************************************************/

	/**
	 *	Generic secondary index error.
	 */
	AEROSPIKE_ERR_INDEX 					= 1200,

	/**
	 *	Index is out of memory
	 */
	AEROSPIKE_ERR_INDEX_OOM 				= 1201,

	/**
	 *	Index not found
	 */
	AEROSPIKE_ERR_INDEX_NOT_FOUND 			= 1202,

	/**
	 *	Index found.
	 */
	AEROSPIKE_ERR_INDEX_FOUND 				= 1203,

	/**
	 *	Unable to read the index.
	 */
	AEROSPIKE_ERR_INDEX_NOT_READABLE 		= 1204,

	/***************************************************************************
	 *	UDF OPERATIONS
	 **************************************************************************/

	/**
	 *	Generic UDF error.
	 */
	AEROSPIKE_ERR_UDF						= 1300,

	/**
	 *	UDF does not exist.
	 */
	AEROSPIKE_ERR_UDF_NOT_FOUND				= 1301

} as_status;
