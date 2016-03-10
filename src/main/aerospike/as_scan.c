/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/as_scan.h>

#include <citrusleaf/alloc.h>

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
	scan->concurrent = AS_SCAN_CONCURRENT_DEFAULT;
	scan->include_ldt = AS_SCAN_INCLUDE_LDT_DEFAULT;
	scan->deserialize_list_map = AS_SCAN_DESERIALIZE_DEFAULT;
	
	as_udf_call_init(&scan->apply_each, NULL, NULL, NULL);

	return scan;
}

/**
 * Create and initializes a new scan on the heap.
 */
as_scan * as_scan_new(const as_namespace ns, const as_set set)
{
	as_scan * scan = (as_scan *) cf_malloc(sizeof(as_scan));
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

	if ( scan->select._free ) {
		cf_free(scan->select.entries);
	}

	as_udf_call_destroy(&scan->apply_each);

	// If the whole structure should be freed
	if ( scan->_free ) {
		cf_free(scan);
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

	scan->select.entries = (as_bin_name *) cf_calloc(n, sizeof(as_bin_name));
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
 *	Scan all the nodes in prallel
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_set_concurrent(&q, true);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to set the concurrency on.
 *	@param concurrent	If true, scan all the nodes in parallel
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_set_concurrent(as_scan * scan, bool concurrent)
{
	if ( !scan ) return false;
	scan->concurrent = concurrent;
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
