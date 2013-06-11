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
typedef enum as_scan_type_e {
	AS_SCAN_TYPE_NORMAL = 0,
	AS_SCAN_TYPE_UDF_RECORD = 1,
	AS_SCAN_TYPE_UDF_BACKGROUND = 2
} as_scan_type;

/**
 * Defines the scan operation.
 */
struct as_scan_s {
	bool			_free;		// Object can be free()'d
	as_scan_type	type;		// Type of the scan
	as_scan_priority priority;	// Priority of scan.
	uint8_t			percent;	// Percentage of the data to scan.
	bool			nobindata;	// Set to true if the scan should return only the metadata of the record.
	char *			namespace;	// The namespace to scan.
	char *			set;		// The keyset to scan.
	uint64_t		job_id;		// Unique id of this scan
	as_udf_call		foreach;	// Apply the function for each record scanned.
};

typedef struct as_scan_s as_scan;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initializes a scan.
 */
as_scan * as_scan_init(as_scan * scan, const char * ns, const char * set, uint64_t *job_id);

/**
 * Create and initializes a new scan on the heap.
 */
as_scan * as_scan_new(const char * ns, const char * set, uint64_t *job_id);

/**
 * Releases all resources allocated to the scan.
 */
void as_scan_destroy(as_scan * scan);

/**
 * Apply a UDF to each record scanned on the server.
 */
void as_scan_foreach(as_scan * scan, as_scan_type type, const char * module, const char * function, as_list * arglist);
