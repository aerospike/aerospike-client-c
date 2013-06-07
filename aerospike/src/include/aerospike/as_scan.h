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

#include <aerospike/as_udf.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Defines the scan operation.
 */
struct as_scan_s {

	/**
	 * Object can be free()'d
	 */
	bool _free;

	/**
	 * The namespace to scan.
	 */
	const char * namespace;

	/**
	 * The keyset to scan.
	 */
	const char * set;

	/**
	 * Priority of scan.
	 */
	as_scan_priority priority;

	/**
	 * Percentage of the data to scan.
	 */
	uint8_t percent;
	
	/**
	 * Apply the function for each record scanned.
	 */
	as_udf_call foreach;
};

typedef struct as_scan_s as_scan;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initializes a scan.
 */
as_scan * as_scan_init(as_scan * scan, const char * ns, const char * set);

/**
 * Create and initializes a new scan on the heap.
 */
as_scan * as_scan_new(const char * ns, const char * set);

/**
 * Releases all resources allocated to the scan.
 */
void as_scan_destroy(as_scan * scan);

/**
 * Apply a UDF to each record scanned on the server.
 */
void as_scan_foreach(as_scan * scan, const char * module, const char * function, as_list * arglist);
