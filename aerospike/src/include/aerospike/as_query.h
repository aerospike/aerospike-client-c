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

#include <aerospike/as_bin.h>
#include <aerospike/as_list.h>
#include <aerospike/as_udf.h>
#include <stdarg.h>

/** 
 * @defgroup Query Query API
 * @{
 */

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define string_equals(__val) AS_PREDICATE_STRING_EQUAL, __val

#define integer_equals(__val) AS_PREDICATE_INTEGER_EQUAL, __val

#define integer_range(__min, __max) AS_PREDICATE_INTEGER_RANGE, __min, __max

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Union of supported predicates
 */
typedef union as_predicate_value_u {
	
	/**
	 * String Value
	 */
	char * string;

	/**
	 * Integer Value
	 */
	int64_t integer;

	/**
	 * Integer Range Value
	 */
	struct {
		int64_t min;
		int64_t max;
	} integer_range;

} as_predicate_value;

/**
 * Predicate Identifiers
 */
typedef enum as_predicate_type_e {
	AS_PREDICATE_STRING_EQUAL,
	AS_PREDICATE_INTEGER_EQUAL,
	AS_PREDICATE_INTEGER_RANGE
} as_predicate_type;

/**
 * Predicate
 */
typedef struct as_predicate_s {

	/**
	 * Bin to apply the predicate to
	 */
	as_bin_name bin;

	/**
	 * The predicate type, dictates which value to use from the union
	 */
	as_predicate_type type;

	/**
	 * The value for the predicate.
	 */
	as_predicate_value value;

} as_predicate;

/**
 * Describes the bin to be ordered by and 
 * whether it is ascending order.
 */
typedef struct as_orderby_s {

	/**
	 * name of the bin to orderby
	 */
	as_bin_name bin;

	/**
	 * bin should be in ascending order
	 */
	bool ascending;

} as_orderby;

/**
 * Sequence of bins which should be selected during a query.
 */
typedef struct as_query_select_s {

	/**
	 * @private
	 * If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 * Number of entries used
	 */
	uint16_t size;

	/**
	 * Sequence of entries
	 */
	as_bin_name * entries;

} as_query_select_params;

/**
 * Sequence of predicates to be applied to a query.
 */
typedef struct as_query_predicates_s {

	/**
	 * @private
	 * If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 * Number of entries used
	 */
	uint16_t size;

	/**
	 * Sequence of entries
	 */
	as_predicate * 	entries;

} as_query_predicates_params;

/**
 * Sequence of ordering to be applied to a query results.
 */
typedef struct as_query_orderby_s {

	/**
	 * @private
	 * If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 * Number of entries used
	 */
	uint16_t size;

	/**
	 * Sequence of entries
	 */
	as_orderby * entries;
} as_query_orderby_params;

/**
 * Describes the query.
 */
typedef struct as_query_s {

	/**
	 * @private
	 * If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * namespace to be queried.
	 */
	char * namespace;

	/**
	 * set to be queried
	 */
	char * set;

	/**
	 * Name of bins to select.
	 *
	 * You can either have this initialized on the stack or
	 * on the heap.
	 *
	 * For Stack, use alloca() or similar:
	 *
	 *		query->select._free = false;
	 *		query->select.capacity = SZ;
	 *		query->select.size = 0;
	 *		query->select.entries = (as_bin_name *) alloca(sizeof(as_bin_name) * SZ);
	 *
	 * Alternatively, on the Stack you can use an array:
	 *
	 *		as_bin_name select[3] = { "a", "b", "c" };
	 *
	 *		query->select._free = false;
	 *		query->select.capacity = 0;
	 *		query->select.size = sizeof(select) / sizeof(as_bin_name);
	 *		query->select.entries = select;
	 *
	 * For Heap, use malloc() or similar:
	 *
	 *		query->select._free = false;
	 *		query->select.capacity = SZ;
	 *		query->select.size = 0;
	 *		query->select.entries = (as_bin_name *) malloc(sizeof(as_bin_name) * SZ);
	 *
	 * The as_query_select() function will automatically malloc() entries, if entries is NULL.
	 */
	as_query_select_params select;

	/**
	 * Predicates for filtering.
	 *
	 * You can either have this initialized on the stack or
	 * on the heap.
	 *
	 * For Stack, use alloca() or similar:
	 *
	 *		query->predicates._free = false;
	 *		query->predicates.capacity = SZ;
	 *		query->predicates.size = 0;
	 *		query->predicates.entries = (as_predicate *) alloca(sizeof(as_predicate) * SZ);
	 *
	 * Alternatively, on the stack you can use an array:
	 *
	 *		as_predicate predicates[3] = { ... };
	 *
	 *		query->predicates._free = false;
	 *		query->predicates.capacity = 0;
	 *		query->predicates.size = sizeof(predicates) / sizeof(as_predicate);
	 *		query->predicates.entries = predicates;
	 *
	 * For Heap, use malloc() or similar:
	 *
	 *		query->predicates._free = false;
	 *		query->predicates.capacity = SZ;
	 *		query->predicates.size = 0;
	 *		query->predicates.entries = (as_predicate *) malloc(sizeof(as_predicate) * SZ);
	 *
	 * The as_query_where() function will automatically malloc() entries, if entries is NULL.
	 */
	as_query_predicates_params predicates;

	/**
	 * Bins to order by.
	 *
	 * You can either have this initialized on the stack or
	 * on the heap.
	 *
	 * For Stack, use alloca() or similar:
	 *
	 *		query->orderby._free = false;
	 *		query->orderby.capacity = SZ;
	 *		query->orderby.size = 0;
	 *		query->orderby.entries = (as_orderby *) alloca(sizeof(as_orderby) * SZ);
	 *
	 * Alternatively, on the stack you can use an array:
	 *
	 *		as_orderby orderby[3] = { ... };
	 *
	 *		query->orderby._free = false;
	 *		query->orderby.capacity = 0;
	 *		query->orderby.size = sizeof(orderby) / sizeof(as_orderby);
	 *		query->orderby.entries = predicates;
	 *
	 * For Heap, use malloc() or similar:
	 *
	 *		query->orderby._free = false;
	 *		query->orderby.capacity = SZ;
	 *		query->orderby.size = 0;
	 *		query->orderby.entries = (as_orderby *) malloc(sizeof(as_orderby) * SZ);
	 *
	 * The as_query_orderby() function will automatically malloc() entries, if entries is NULL.
	 */
	as_query_orderby_params orderby;

	/**
	 * Limit the result set.
	 * If set to UINT64_MAX (default), then the query will 
	 * return all matching results.
	 */
	uint64_t limit;

	/**
	 * UDF to apply to results of the query
	 */
	as_udf_call apply;

} as_query;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize a stack allocated as_query.
 *
 * @param query 	- the query to initialize
 * @param ns 		- the namespace to query
 * @param set 		- the set to query
 *
 * @return the initialized query on success. Otherwise NULL.
 */
as_query * as_query_init(as_query * query, const char * ns, const char * set);

/**
 * Creates a new heap allocated as_query.
 *
 * @param ns 		- the namespace to query
 * @param set 		- the set to query
 *
 * @return the new query on success. Otherwise NULL.
 */
as_query * as_query_new(const char * ns, const char * set);

/**
 * Destroy the query and associated resources.
 *
 * @param query 	- the query to destroy
 */
void as_query_destroy(as_query * query);


/**
 * Select bins to be projected from matching records.
 *
 *		as_query_select(&q, "bin1");
 *		as_query_select(&q, "bin2");
 *		as_query_select(&q, "bin3");
 *
 * as_query_where() will attempt to automatically malloc() entries if
 * query.select.entries is NULL. If query.select.capacity is >0, then 
 * the first malloc() will allocate query.select.capacity entries. 
 * Otherwise, query.select.capacity will default to 10.
 *
 * @param query 	- the query to modify
 * @param bin 		- the name of the bin to select
 *
 * @return 0 on success. Otherwise an error occurred.
 */
int as_query_select(as_query * query, const char * bin);

/**
 * Add a predicate to the query.
 *
 *		as_query_where(&q, "bin1", string_eq("abc"));
 *		as_query_where(&q, "bin1", integer_eq(123));
 *		as_query_where(&q, "bin1", integer_range(0,123));
 *
 * as_query_where() will attempt to automatically malloc() entries if
 * query.predicates.entries is NULL. If query.predicates.capacity is >0, then 
 * the first malloc() will allocate query.predicates.capacity entries. 
 * Otherwise, query.predicates.capacity will default to 10.
 *
 * @param query 	- the query to modify
 * @param bin 		- the name of the bin to apply a predicate to
 * @param type 		- the name of the bin to apply a predicate to
 *
 * @return 0 on success. Otherwise an error occurred.
 */
int as_query_where(as_query * query, const char * bin, as_predicate_type type, ... );

/**
 * Add a bin to sort by to the query.
 *
 *		as_query_orderby(&q, "bin1", true);
 *
 * as_query_orderby() will attempt to automatically malloc() entries if
 * query.orderby.entries is NULL. If query.orderby.capacity is >0, then 
 * the first malloc() will allocate query.orderby.capacity entries. 
 * Otherwise, query.orderby.capacity will default to 10.
 *
 * @param query 	- the query to modify
 * @param bin 		- the name of the bin to sort by
 * @param ascending	- if true, will sort the bin in ascending order. Otherwise, descending order it used.
 *
 * @param 0 on success. Otherwise an error occurred.
 */
int as_query_orderby(as_query * query, const char * bin, bool ascending);

/**
 * Limit the number of results by `limit`. If limit is UINT64_MAX, then all matching results are returned.
 *
 *		as_query_orderby(&q, "bin1", true);
 *
 * @param query 	- the query to modify
 * @param limit 	- the number of records to limit by
 *
 * @param 0 on success. Otherwise an error occurred.
 */
int as_query_limit(as_query * query, uint64_t limit);

/**
 * Apply a function to the results of the querty.
 *
 *		as_query_apply(&q, "my_module", "my_function", NULL);
 *
 * @param query 	- the query to apply the function to
 * @param module 	- the module containing the function to invoke
 * @param function 	- the function in the module to invoke
 * @param arglist 	- the arguments to use when calling the function
 *
 * @param 0 on success. Otherwise an error occurred.
 */
int as_query_apply(as_query * query, const char * module, const char * function, const as_list * arglist);

/**
 * @}
 */
