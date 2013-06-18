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

#include <aerospike/as_udf.h>

/** 
 *	@defgroup scan Scan API
 *	@{
 */

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Priority levels for a scan operation.
 */
typedef enum as_scan_priority_e { 

	/**
	 *	The cluster will auto adjust the scan priroty.
	 */
	AS_SCAN_PRIORITY_AUTO, 

	/**
	 *	Low priority scan.
	 */
	AS_SCAN_PRIORITY_LOW,

	/**
	 *	Medium priority scan.
	 */ 
	AS_SCAN_PRIORITY_MEDIUM,

	/**
	 *	High priority scan.
	 */ 
	AS_SCAN_PRIORITY_HIGH
	
} as_scan_priority;

/**
 *	Defines the scan operation.
 */
typedef struct as_scan_s {

	/**
	 *	@private
	 *	If true, then as_scan_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Priority of scan.
	 */
	as_scan_priority priority;

	/**
	 *	Percentage of the data to scan.
	 */
	uint8_t percent;

	/**
	 *	Set to true if the scan should return only the metadata of the record.
	 */
	bool no_bins;

	/**
	 *	The namespace to scan.
	 */
	char * namespace;

	/**
	 *	The set to scan
	 */
	char * set;
	
	/**
	 *	Apply the function for each record scanned.
	 */
	as_udf_call foreach;

} as_scan;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initializes a scan.
 *
 *	@param ns 		The namespace to scan.
 *	@param set 		The set to scan.
 *
 *	@returns the initialized scan on success. Otherwise NULL.
 */
as_scan * as_scan_init(as_scan * scan, const char * ns, const char * set);

/**
 *	Create and initializes a new scan on the heap.
 *
 *	@param ns 		The namespace to scan.
 *	@param set 		The set to scan.
 *
 *	@returns the initialized scan on success. Otherwise NULL.
 */
as_scan * as_scan_new(const char * ns, const char * set);

/**
 *	Releases all resources allocated to the scan.
 */
void as_scan_destroy(as_scan * scan);

/**
 *	Apply a UDF to each record scanned on the server.
 *
 *	@param scan 		The scan to apply the UDF to.
 *	@param module 	The module containing the function to execute.
 *	@param function 	The function to execute.
 *	@param arglist 	The arguments for the function.
 */
void as_scan_foreach(as_scan * scan, const char * module, const char * function, as_list * arglist);
