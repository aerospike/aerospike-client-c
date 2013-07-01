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

#include <aerospike/as_key.h>
#include <aerospike/as_udf.h>

/** 
 *	@addtogroup scan_t
 *	@{
 */

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Default value for as_scan.priority
 */
#define AS_SCAN_PRIORITY_DEFAULT AS_SCAN_PRIORITY_AUTO

/**
 *	Default value for as_scan.percent
 */
#define AS_SCAN_PERCENT_DEFAULT 100

/**
 *	Default value for as_scan.no_bins
 */
#define AS_SCAN_NOBINS_DEFAULT false

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
 *	Defines the scan to be executed against an Aerospike cluster.
 *
 *	A query must be initialized via either `as_scan_init()` or `as_scan_new()`.
 *	Both functions require a namespace and set to query.
 *
 *	`as_scan_init()` will initialize a stack allocated `as_scan`:
 *
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "namespace", "set");
 *	~~~~~~~~~~
 *
 *	`as_scan_new()` will create and initialize a new heap allocated `as_scan`:
 *
 *	~~~~~~~~~~{.c}
 *	as_scan * scan = as_scan_new("namespace", "set");
 *	~~~~~~~~~~
 *
 *	When you are finished with the scan, you can destroy it and associated
 *	resources:
 *
 *	~~~~~~~~~~{.c}
 *	as_scan_destroy(scan);
 *	~~~~~~~~~~
 */
typedef struct as_scan_s {

	/**
	 *	@private
	 *	If true, then as_scan_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Priority of scan.
	 *
	 *	Default value is AS_SCAN_PRIORITY_DEFAULT.
	 */
	as_scan_priority priority;

	/**
	 *	Percentage of the data to scan.
	 *
	 *	Default value is AS_SCAN_PERCENT_DEFAULT.
	 */
	uint8_t percent;

	/**
	 *	Set to true if the scan should return only the metadata of the record.
	 *
	 *	Default value is AS_SCAN_NOBINS_DEFAULT.
	 */
	bool no_bins;

	/**
	 * 	@memberof as_scan
	 *	Namespace to be scanned.
	 *
	 *	Should be initialized via either:
	 *	-	as_scan_init() -	To initialize a stack allocated scan.
	 *	-	as_scan_new() -		To heap allocate and initialize a scan.
	 *
	 */
	as_namespace ns;

	/**
	 *	Set to be scanned.
	 *
	 *	Should be initialized via either:
	 *	-	as_scan_init() -	To initialize a stack allocated scan.
	 *	-	as_scan_new() -		To heap allocate and initialize a scan.
	 *
	 */
	as_set set;
	
	/**
	 *	Apply the UDF for each record scanned on the server.
	 *
	 *	Should be set via `as_scan_foreach()`.
	 */
	as_udf_call foreach;

} as_scan;

/******************************************************************************
 *	INSTANCE FUNCTIONS
 *****************************************************************************/

/**
 *	Initializes a scan.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan scan;
 *	as_scan_init(&scan, "test", "demo");
 *	~~~~~~~~~~
 *
 *	When you no longer require the scan, you should release the scan and 
 *	related resources via `as_scan_destroy()`.
 *
 *	@param scan		The scan to initialize.
 *	@param ns 		The namespace to scan.
 *	@param set 		The set to scan.
 *
 *	@returns On succes, the initialized scan. Otherwise NULL.
 */
as_scan * as_scan_init(as_scan * scan, const as_namespace ns, const as_set set);

/**
 *	Create and initializes a new scan on the heap.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan * scan = as_scan_new("test","demo");
 *	~~~~~~~~~~
 *
 *	When you no longer require the scan, you should release the scan and 
 *	related resources via `as_scan_destroy()`.
 *
 *	@param ns 		The namespace to scan.
 *	@param set 		The set to scan.
 *
 *	@returns On success, a new scan. Otherwise NULL.
 */
as_scan * as_scan_new(const as_namespace ns, const as_set set);

/**
 *	Releases all resources allocated to the scan.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_destroy(scan);
 *	~~~~~~~~~~
 */
void as_scan_destroy(as_scan * scan);

/******************************************************************************
 *	MODIFIER FUNCTIONS
 *****************************************************************************/

/**
 *	The percentage of data to scan.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_percent(&q, 100);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to set the priority on.
 *	@param percent		The percent to scan.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_percent(as_scan * scan, uint8_t percent);

/**
 *	Set the priority for the scan.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_priority(&q, AS_SCAN_PRIORITY_LOW);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to set the priority on.
 *	@param priority		The priority for the scan.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_priority(as_scan * scan, as_scan_priority priority);

/**
 *	Do not return bins. This will only return the metadata for the records.
 *	
 *	~~~~~~~~~~{.c}
 *	as_scan_nobins(&q, true);
 *	~~~~~~~~~~
 *
 *	@param scan 		The scan to set the priority on.
 *	@param nobins		If true, then do not return bins.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_scan_nobins(as_scan * scan, bool nobins);

/**
 *	Apply a UDF to each record scanned on the server.
 *	
 *	~~~~~~~~~~{.c}
 *	as_arraylist arglist;
 *	as_arraylist_init(&arglist, 2, 0);
 *	as_arraylist_append_int64(&arglist, 1);
 *	as_arraylist_append_int64(&arglist, 2);
 *	
 *	as_scan_foreach(&q, "module", "func", (as_list *) &arglist);
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
bool as_scan_foreach(as_scan * scan, const char * module, const char * function, as_list * arglist);

/**
 *	@}
 */
