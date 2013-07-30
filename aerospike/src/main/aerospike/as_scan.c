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
 * INSTANCE FUNCTIONS
 *****************************************************************************/

static as_scan * as_scan_defaults(as_scan * scan, bool free, const as_namespace ns, const as_set set)
{
	if (scan == NULL) return scan;

	scan->_free = free;

	if ( strlen(ns) < AS_NAMESPACE_MAX_SIZE ) {
		strcpy(scan->ns, ns);
	}
	else {
		scan->ns[0] = '\0';
	}
	
    //check set==NULL and set name length
	if ( set && strlen(set) < AS_SET_MAX_SIZE ) {
		strcpy(scan->set, set);
	}
	else {
		scan->set[0] = '\0';
	}
	
	scan->select._free = false;
	scan->select.capacity = 0;
	scan->select.size = 0;
	scan->select.entries = NULL;

	scan->priority = AS_SCAN_PRIORITY_DEFAULT;
	scan->percent = AS_SCAN_PERCENT_DEFAULT;
	scan->no_bins = AS_SCAN_NOBINS_DEFAULT;
	
	as_udf_call_init(&scan->apply_each, NULL, NULL, NULL);

	return scan;
}

/**
 * Create and initializes a new scan on the heap.
 */
as_scan * as_scan_new(const as_namespace ns, const as_set set)
{
	as_scan * scan = (as_scan *) malloc(sizeof(as_scan));
	if ( ! scan ) return NULL;
	return as_scan_defaults(scan, true, ns, set);
}

/**
 * Initializes a scan.
 */
as_scan * as_scan_init(as_scan * scan, const as_namespace ns, const as_set set)
{
	if ( !scan ) return scan;
	return as_scan_defaults(scan, false, ns, set);
}

/**
 * Releases all resources allocated to the scan.
 */
void as_scan_destroy(as_scan * scan)
{
	if ( !scan ) return;

	scan->ns[0] = '\0';
	scan->set[0] = '\0';

	as_udf_call_destroy(&scan->apply_each);

	// If the whole structure should be freed
	if ( scan->_free ) {
		free(scan);
	}
}

/******************************************************************************
 *	SELECT FUNCTIONS
 *****************************************************************************/

/** 
 *	Initializes `as_scan.select` with a capacity of `n` using `malloc()`.
 *	
 *	For stack allocation, use `as_scan_select_inita()`.
 *
 *	~~~~~~~~~~{.c}
 *	as_scan_select_init(&scan, 2);
 *	as_scan_select(&scan, "bin1");
 *	as_scan_select(&scan, "bin2");
 *	~~~~~~~~~~
 *
 *	@param scan		The scan to initialize.
 *	@param n		The number of bins to allocate.
 *
 *	@return On success, the initialized. Otherwise an error occurred.
 *
 *	@relates as_scan
 *	@ingroup as_scan_t
 */
bool as_scan_select_init(as_scan * scan, uint16_t n) 
{
	if ( !scan ) return false;
	if ( scan->select.entries ) return false;

	scan->select.entries = (as_bin_name *) calloc(n, sizeof(as_bin_name));
	if ( !scan->select.entries ) return false;

	scan->select._free = true;
	scan->select.capacity = n;
	scan->select.size = 0;

	return true;
}

/**
 *	Select bins to be projected from matching records.
 *
 *	You have to ensure as_scan.select has sufficient capacity, prior to 
 *	adding a bin. If capacity is insufficient then false is returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_scan_select_init(&scan, 2);
 *	as_scan_select(&scan, "bin1");
 *	as_scan_select(&scan, "bin2");
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to modify.
 *	@param bin 			The name of the bin to select.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_scan
 *	@ingroup as_scany_t
 */
bool as_scan_select(as_scan * scan, const char * bin)
{
	// test preconditions
	if ( !scan || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( scan->select.size >= scan->select.capacity ) return false;

	strcpy(scan->select.entries[scan->select.size], bin);
	scan->select.size++;

	return true;
}

/******************************************************************************
 * MODIFIER FUNCTIONS
 *****************************************************************************/

/**
 *	The percentage of data to scan.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_set_percent(&q, 100);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to set the priority on.
 *	@param percent		The percent to scan.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_set_percent(as_scan * scan, uint8_t percent)
{
	if ( !scan ) return false;
	scan->percent = percent;
	return true;
}

/**
 *	Set the priority for the scan.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_set_priority(&q, AS_SCAN_PRIORITY_LOW);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to set the priority on.
 *	@param priority		The priority for the scan.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_set_priority(as_scan * scan, as_scan_priority priority)
{
	if ( !scan ) return false;
	scan->priority = priority;
	return true;
}

/**
 *	Do not return bins. This will only return the metadata for the records.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_set_nobins(&q, true);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to set the priority on.
 *	@param nobins		If true, then do not return bins.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_set_nobins(as_scan * scan, bool nobins)
{
	if ( !scan ) return false;
	scan->no_bins = nobins;
	return true;
}

/**
 *	Apply a UDF to each record scanned on the server.
 *	
 *	~~~~~~~~~~{.c}
 *	as_arraylist arglist;
 *	as_arraylist_init(&arglist, 2, 0);
 *	as_arraylist_append_int64(&arglist, 1);
 *	as_arraylist_append_int64(&arglist, 2);
 *	
 *	as_scan_apply_each(&q, "module", "func", (as_list *) &arglist);
 *
 *	as_arraylist_destroy(&arglist);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to apply the UDF to.
 *	@param module 		The module containing the function to execute.
 *	@param function 	The function to execute.
 *	@param arglist 		The arguments for the function.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_apply_each(as_scan * scan, const char * module, const char * function, as_list * arglist)
{
	if ( !module || !function ) return false;
	as_udf_call_init(&scan->apply_each, module, function, arglist);
	return true;
}
