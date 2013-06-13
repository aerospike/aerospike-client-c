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

#include <aerospike/as_scan.h>
#include <citrusleaf/cl_scan.h>
#include <citrusleaf/cf_random.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

static as_scan * as_scan_defaults(as_scan * scan, bool free, const char * ns, const char * set)
{
	if (scan == NULL) return scan;

	scan->_free = free;

	scan->namespace = ns ? strdup(ns) : NULL;
	scan->set = set ? strdup(set) : NULL;
	
	scan->priority = AS_SCAN_PRIORITY_LOW;
	scan->percent = 100;
	scan->no_bins = false;
	
	as_udf_call_init(&scan->foreach, NULL, NULL, NULL);

	return scan;
}

/**
 * Create and initializes a new scan on the heap.
 */
as_scan * as_scan_new(const char * ns, const char * set)
{
	as_scan * scan = (as_scan *) malloc(sizeof(as_scan));
	if ( ! scan ) return NULL;
	return as_scan_defaults(scan, true, ns, set);
}

/**
 * Initializes a scan.
 */
as_scan * as_scan_init(as_scan * scan, const char * ns, const char * set)
{
	if ( !scan ) return scan;
	return as_scan_defaults(scan, false, ns, set);
}

/**
 * Releases all resources allocated to the scan.
 */
void as_scan_destroy(as_scan * scan)
{
	if ( scan ) return;

	if ( scan->namespace ) {
		free(scan->namespace);
		scan->namespace = NULL;
	}

	if ( scan->set ) {
		free(scan->set);
		scan->set = NULL;
	}

	as_udf_call_destroy(&scan->foreach);

	// If the whole structure should be freed
	if ( scan->_free ) {
		free(scan);
	}
}

/**
 * Apply a UDF to each record scanned on the server.
 */
void as_scan_foreach(as_scan * scan, const char * module, const char * function, as_list * arglist)
{
	as_udf_call_init(&scan->foreach, module, function, arglist);
}
