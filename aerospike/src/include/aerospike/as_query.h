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

/** 
 *	@defgroup as_query_t Queries
 *	@copydoc as_query
 */

#pragma once 

#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_udf.h>

#include <stdarg.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Macro for setting setting the STRING_EQUAL predicate.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_where(query, "bin1", string_equals("abc"));
 *	~~~~~~~~~~
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
#define string_equals(__val) AS_PREDICATE_STRING_EQUAL, __val

/**
 *	Macro for setting setting the INTEGER_EQUAL predicate.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_where(query, "bin1", integer_equals(123));
 *	~~~~~~~~~~
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
#define integer_equals(__val) AS_PREDICATE_INTEGER_EQUAL, __val

/**
 *	Macro for setting setting the INTEGER_RANGE predicate.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_where(query, "bin1", integer_range(1,100));
 *	~~~~~~~~~~
 *	
 *	@relates as_query
 *	@ingroup as_query_t
 */
#define integer_range(__min, __max) AS_PREDICATE_INTEGER_RANGE, __min, __max

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Union of supported predicates
 */
typedef union as_predicate_value_u {
	
	/**
	 *	String Value
	 */
	char * string;

	/**
	 *	Integer Value
	 */
	int64_t integer;

	/**
	 *	Integer Range Value
	 */
	struct {

		/**
		 *	Minimum value
		 */
		int64_t min;

		/**
		 *	Maximum value
		 */
		int64_t max;

	} integer_range;

} as_predicate_value;

/**
 *	The types of predicates supported.
 */
typedef enum as_predicate_type_e {

	/**
	 *	String Equality Predicate. 
	 *	Requires as_predicate_value.string to be set.
	 */
	AS_PREDICATE_STRING_EQUAL,

	/**
	 *	Integer Equality Predicate.
	 *	Requires as_predicate_value.integer to be set.
	 */
	AS_PREDICATE_INTEGER_EQUAL,

	/**
	 *	Integer Range Predicate.
	 *	Requires as_predicate_value.integer_range to be set.
	 */
	AS_PREDICATE_INTEGER_RANGE

} as_predicate_type;

/**
 *	Defines a predicate, including the bin, type of predcate and the value
 *	for the predicate.
 */
typedef struct as_predicate_s {

	/**
	 *	Bin to apply the predicate to
	 */
	as_bin_name bin;

	/**
	 *	The predicate type, dictates which value to use from the union
	 */
	as_predicate_type type;

	/**
	 *	The value for the predicate.
	 */
	as_predicate_value value;

} as_predicate;

/**
 *	Enumerations defining the direction of an ordering.
 */
typedef enum as_order_e {

	/**
	 *	Ascending order
	 */
	AS_ORDER_ASCENDING = 0,

	/**
	 *	bin should be in ascending order
	 */
	AS_ORDER_DESCENDING = 1

} as_order;


/**
 *	Defines the direction a bin should be ordered by.
 */
typedef struct as_ordering_s {

	/**
	 *	Name of the bin to sort by
	 */
	as_bin_name bin;

	/**
	 *	Direction of the sort
	 */
	as_order order;

} as_ordering;

/**
 *	Sequence of bins which should be selected during a query.
 *
 *	Entries can either be initialized on the stack or on the heap.
 *
 *	Initialization should be performed via a query object, using:
 *	-	as_query_select_init()
 *	-	as_query_select_inita()
 */
typedef struct as_query_bins_s {

	/**
	 *	@private
	 *	If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 *	Number of entries used
	 */
	uint16_t size;

	/**
	 *	Sequence of entries
	 */
	as_bin_name * entries;

} as_query_bins;

/**
 *	Sequence of predicates to be applied to a query.
 *
 *	Entries can either be initialized on the stack or on the heap.
 *
 *	Initialization should be performed via a query object, using:
 *	-	as_query_where_init()
 *	-	as_query_where_inita()
 */
typedef struct as_query_predicates_s {

	/**
	 *	@private
	 *	If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 *	Number of entries used
	 */
	uint16_t size;

	/**
	 *	Sequence of entries
	 */
	as_predicate * 	entries;

} as_query_predicates;

/**
 *	Sequence of ordering to be applied to a query results.
 *
 *	Entries can either be initialized on the stack or on the heap.
 *	
 *	Initialization should be performed via a query object, using:
 *	-	as_query_orderby_init()
 *	-	as_query_orderby_inita()
 */
typedef struct as_query_sort_s {

	/**
	 *	@private
	 *	If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 *	Number of entries used
	 */
	uint16_t size;

	/**
	 *	Sequence of entries
	 */
	as_ordering * entries;

} as_query_ordering;


/** 
 *	Defines a query to be exeucted against an Aerospike cluster.
 *
 *	A query must be initialized via either `as_query_init()` or `as_query_new()`.
 *	Both functions require a namespace and set to query.
 *
 *	`as_query_init()` will initialize a stack allocated `as_query`:
 *
 *	~~~~~~~~~~{.c}
 *	as_query query;
 *	as_query_init(&query, "namespace", "set");
 *	~~~~~~~~~~
 *
 *	`as_query_new()` will create and initialize a new heap allocated `as_query`:
 *
 *	~~~~~~~~~~{.c}
 *	as_query * query = as_query_new("namespace", "set");
 *	~~~~~~~~~~
 *
 *	You can then populate the `as_query` instance using the functions provided:
 *
 *	- `as_query_select()` - to add bins to select from each record
 *	- `as_query_where()` - to add predicates to filter the results on
 *	- `as_query_orderby()` - to sort the results
 *	- `as_query_limit()` - to limit the number of results returned.
 *
 *	When you are finished with the query, you can destroy it and associated
 *	resources:
 *
 *	~~~~~~~~~~{.c}
 *	as_query_destroy(query);
 *	~~~~~~~~~~
 *
 *	@ingroup as_query_t
 */
typedef struct as_query_s {

	/**
	 *	@private
	 *	If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Namespace to be queried.
	 *
	 *	Should be initialized via either:
	 *	-	as_query_init() -	To initialize a stack allocated query.
	 *	-	as_query_new() -	To heap allocate and initialize a query.
	 */
	as_namespace ns;

	/**
	 *	Set to be queried.
	 *
	 *	Should be initialized via either:
	 *	-	as_query_init() -	To initialize a stack allocated query.
	 *	-	as_query_new() -	To heap allocate and initialize a query.
	 */
	as_set set;

	/**
	 *	Name of bins to select.
	 *	
	 *	Use either of the following function to initialize:
	 *	-	as_query_select_init() -	To initialize on the heap.
	 *	-	as_query_select_inita() -	To initialize on the stack.
	 *
	 *	Use as_query_select() to populate.
	 */
	as_query_bins select;

	/**
	 *	Predicates for filtering.
	 *	
	 *	Use either of the following function to initialize:
	 *	-	as_query_where_init() -		To initialize on the heap.
	 *	-	as_query_where_inita() -	To initialize on the stack.
	 *
	 *	Use as_query_where() to populate.
	 */
	as_query_predicates where;

	/**
	 *	Bins to order by.
	 *	
	 *	Use either of the following function to initialize:
	 *	-	as_query_orderby_init() -	To initialize on the heap.
	 *	-	as_query_orderby_inita() -	To initialize on the stack.
	 *
	 *	Use as_query_orderby() to populate.
	 */
	as_query_ordering orderby;

	/**
	 *	Limit the result set.
	 *
	 *	Should be set via `as_query_limit()`.
	 *
	 *	If set to UINT64_MAX (default), then the query will 
	 *	return all matching results.
	 */
	uint64_t limit;

	/**
	 *	UDF to apply to results of the query
	 *
	 *	Should be set via `as_query_limit()`.
	 */
	as_udf_call apply;

} as_query;

/******************************************************************************
 *	INSTANCE FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize a stack allocated as_query.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_init(&q, "test", "demo");
 *	~~~~~~~~~~
 *
 *	@param query 	The query to initialize.
 *	@param ns 		The namespace to query.
 *	@param set 		The set to query.
 *
 *	@return On success, the initialized query. Otherwise NULL.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
as_query * as_query_init(as_query * query, const as_namespace ns, const as_set set);

/**
 *	Create and initialize a new heap allocated as_query.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_init(&q, "test", "demo");
 *	~~~~~~~~~~
 *	
 *	@param ns 		The namespace to query.
 *	@param set 		The set to query.
 *
 *	@return On success, the new query. Otherwise NULL.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
as_query * as_query_new(const as_namespace ns, const as_set set);

/**
 *	Destroy the query and associated resources.
 *
 *	@param query 	The query to destroy.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
void as_query_destroy(as_query * query);

/******************************************************************************
 *	SELECT FUNCTIONS
 *****************************************************************************/

/** 
 *	Initializes `as_query.select` with a capacity of `n` using `alloca`
 *
 *	For heap allocation, use `as_query_select_init()`.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_select_inita(&q, 2);
 *	as_query_select(&q, "bin1");
 *	as_query_select(&q, "bin2");
 *	as_query_select(&q, "bin3");
 *	~~~~~~~~~~
 *	
 *	@param __query	The query to initialize.
 *	@param __n		The number of bins to allocate.
 *
 *	@ingroup as_query_t
 */
#define as_query_select_inita(__query, __n) \
	if ( (__query) != NULL && (__query)->select.entries == NULL ) {\
		(__query)->select.entries = (as_bin_name *) alloca(__n * sizeof(as_bin_name));\
		if ( (__query)->select.entries ) { \
			(__query)->select._free = false;\
			(__query)->select.capacity = __n;\
			(__query)->select.size = 0;\
		}\
 	}

/** 
 *	Initializes `as_query.select` with a capacity of `n` using `malloc()`.
 *	
 *	For stack allocation, use `as_query_select_inita()`.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_select_init(&q, 2);
 *	as_query_select(&q, "bin1");
 *	as_query_select(&q, "bin2");
 *	as_query_select(&q, "bin3");
 *	~~~~~~~~~~
 *
 *	@param query	The query to initialize.
 *	@param n		The number of bins to allocate.
 *
 *	@return On success, the initialized. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_select_init(as_query * query, uint16_t n);

/**
 *	Select bins to be projected from matching records.
 *
 *	You have to ensure as_query.select has sufficient capacity, prior to 
 *	adding a bin. If capacity is sufficient then false is returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_select_init(&q, 2);
 *	as_query_select(&q, "bin1");
 *	as_query_select(&q, "bin2");
 *	as_query_select(&q, "bin3");
 *	~~~~~~~~~~
 *
 *	@param query 		The query to modify.
 *	@param bin 			The name of the bin to select.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_select(as_query * query, const char * bin);

/******************************************************************************
 *	WHERE FUNCTIONS
 *****************************************************************************/

/** 
 *	Initializes `as_query.where` with a capacity of `n` using `alloca()`.
 *
 *	For heap allocation, use `as_query_where_init()`.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_where_inita(&q, 3);
 *	as_query_where(&q, "bin1", string_equals("abc"));
 *	as_query_where(&q, "bin2", integer_equals(123));
 *	as_query_where(&q, "bin3", integer_range(0,123));
 *	~~~~~~~~~~
 *
 *	@param __query	The query to initialize.
 *	@param __n		The number of as_predicate to allocate.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@ingroup as_query_t
 */
#define as_query_where_inita(__query, __n) \
	if ( (__query)  != NULL && (__query)->where.entries == NULL ) {\
		(__query)->where.entries = (as_predicate *) alloca(__n * sizeof(as_predicate));\
		if ( (__query)->where.entries ) { \
			(__query)->where._free = false;\
			(__query)->where.capacity = __n;\
			(__query)->where.size = 0;\
		}\
 	}

/** 
 *	Initializes `as_query.where` with a capacity of `n` using `malloc()`.
 *
 *	For stack allocation, use `as_query_where_inita()`.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_where_init(&q, 3);
 *	as_query_where(&q, "bin1", string_equals("abc"));
 *	as_query_where(&q, "bin1", integer_equals(123));
 *	as_query_where(&q, "bin1", integer_range(0,123));
 *	~~~~~~~~~~
 *
 *	@param query	The query to initialize.
 *	@param n		The number of as_predicate to allocate.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_where_init(as_query * query, uint16_t n);

/**
 *	Add a predicate to the query.
 *
 *	You have to ensure as_query.where has sufficient capacity, prior to 
 *	adding a predicate. If capacity is sufficient then false is returned.
 *	
 *	~~~~~~~~~~{.c}
 *	as_query_where_init(&q, 3);
 *	as_query_where(&q, "bin1", string_equals("abc"));
 *	as_query_where(&q, "bin1", integer_equals(123));
 *	as_query_where(&q, "bin1", integer_range(0,123));
 *	~~~~~~~~~~
 *
 *	@param query		The query add the predicate to.
 *	@param bin			The name of the bin the predicate will apply to.
 *	@param type			The type of predicate.
 *	@param ... 			The values for the predicate.
 *	
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_where(as_query * query, const char * bin, as_predicate_type type, ... );

/******************************************************************************
 *	ORDERBY FUNCTIONS
 *****************************************************************************/

/** 
 *	Initializes `as_query.where` with a capacity of `n` using `alloca()`.
 *
 *	For heap allocation, use `as_query_where_init()`.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_orderby_inita(&q, 1);
 *	as_query_orderby(&q, "bin1", AS_ORDER_ASCENDING);
 *	~~~~~~~~~~
 *
 *	@param __query	The query to initialize.
 *	@param __n		The number of as_orders to allocate.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@ingroup as_query_t
 */
#define as_query_orderby_inita(__query, __n) \
	if ( (__query) != NULL && (__query)->orderby.entries == NULL  ) {\
		(__query)->orderby.entries = (as_ordering *) alloca(__n * sizeof(as_ordering));\
		if ( (__query)->orderby.entries ) { \
			(__query)->orderby._free = false;\
			(__query)->orderby.capacity = __n;\
			(__query)->orderby.size = 0;\
		}\
 	}

/** 
 *	Initializes `as_query.orderby` with a capacity of `n` using `malloc()`.
 *	
 *	For stack allocation, use `as_query_orderby_inita()`.
 *	
 *	~~~~~~~~~~{.c}
 *	as_query_orderby_init(&q, 1);
 *	as_query_orderby(&q, "bin1", AS_ORDER_ASCENDING);
 *	~~~~~~~~~~
 *
 *	@param query	The query to initialize.
 *	@param n		The number of as_orders to allocate.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_orderby_init(as_query * query, uint16_t n);

/**
 *	Add a bin to sort by to the query.
 *	
 *	You have to ensure as_query.orderby has sufficient capacity, prior to 
 *	adding an ordering. If capacity is sufficient then false is returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_orderby_init(&q, 1);
 *	as_query_orderby(&q, "bin1", AS_ORDER_ASCENDING);
 *	~~~~~~~~~~
 *
 *	@param query	The query to modify.
 *	@param bin		The name of the bin to sort by.
 *	@param order	The sort order: `AS_ORDER_ASCENDING` or `AS_ORDER_DESCENDING`.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_orderby(as_query * query, const char * bin, as_order order);

/******************************************************************************
 *	QUERY MODIFIER FUNCTIONS
 *****************************************************************************/

/**
 *	Limit the number of results by `limit`. If limit is `UINT64_MAX`, then all matching results are returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_limit(&q, 100);
 *	~~~~~~~~~~
 *
 *	@param query 		The query to modify.
 *	@param limit 		The number of records to limit by.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_limit(as_query * query, uint64_t limit);

/**
 *	Apply a function to the results of the query.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_apply(&q, "my_module", "my_function", NULL);
 *	~~~~~~~~~~
 *
 *	@param query		The query to apply the function to.
 *	@param module		The module containing the function to invoke.
 *	@param function		The function in the module to invoke.
 *	@param arglist		The arguments to use when calling the function.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 *	@ingroup as_query_t
 */
bool as_query_apply(as_query * query, const char * module, const char * function, const as_list * arglist);
