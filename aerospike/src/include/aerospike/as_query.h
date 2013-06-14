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
 *	Etiam molestie, mi eget pellentesque condimentum, odio magna rutrum mauris, vel blandit nisi purus ut leo. Maecenas non arcu vitae nulla cursus venenatis vel vel nisl. Curabitur egestas lorem diam, ac porttitor augue accumsan quis. Sed ultrices in nisi ut lobortis. Etiam non venenatis tellus. Morbi luctus imperdiet arcu, non porta urna. Nam sed ullamcorper erat. Phasellus vel erat sapien. Nullam erat nisi, ornare vitae mattis nec, lobortis sed eros. Curabitur congue ut orci et suscipit. Fusce accumsan, mi et adipiscing venenatis, velit eros dignissim tortor, eu sagittis lectus lorem et purus. Ut augue ligula, accumsan nec mollis a, varius nec lectus. Nam commodo et nibh id placerat.
 *
 *		foo
 *		bar
 *
 *	Maecenas et lacus massa. Nam sagittis odio eget lobortis tempor. Vivamus hendrerit diam in nisl fermentum elementum. Vestibulum gravida mollis faucibus. Nulla facilisi. Vivamus molestie at odio sit amet tincidunt. Interdum et malesuada fames ac ante ipsum primis in faucibus.
 *
 *	@defgroup query Query API
 *	@{
 */

#pragma once 

#include <aerospike/as_bin.h>
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
 *		as_query_where(query, "bin1", string_equals("abc"));
 *	~~~~~~~~~~
 *
 */
#define string_equals(__val) AS_PREDICATE_STRING_EQUAL, __val

/**
 *	Macro for setting setting the INTEGER_EQUAL predicate.
 *
 *	~~~~~~~~~~{.c}
 *		as_query_where(query, "bin1", integer_equals(123));
 *	~~~~~~~~~~
 *
 */
#define integer_equals(__val) AS_PREDICATE_INTEGER_EQUAL, __val

/**
 *	Macro for setting setting the INTEGER_RANGE predicate.
 *
 *	~~~~~~~~~~{.c}
 *		as_query_where(query, "bin1", integer_range(1,100));
 *	~~~~~~~~~~
 *
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
 *	Predicate Identifiers
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
 *	Predicate
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
 *	Describes the bin to be ordered by and 
 *	whether it is ascending order.
 */
typedef struct as_orderby_s {

	/**
	 *	name of the bin to orderby
	 */
	as_bin_name bin;

	/**
	 *	bin should be in ascending order
	 */
	bool ascending;

} as_orderby;

/**
 *	Sequence of bins which should be selected during a query.
 *
 *	Entries can either be initialized on the stack or on the heap.
 *
 *	For Stack, use alloca() or similar:
 *
 *	~~~~~~~~~~{.c}
 *		as_query_bins bins;
 *		bins._free = false;
 *		bins.capacity = SZ;
 *		bins.size = 0;
 *		bins.entries = (as_bin_name *) alloca(sizeof(as_bin_name) * SZ);
 *	~~~~~~~~~~
 *
 *	Alternatively, on the Stack you can use an array:
 *
 *	~~~~~~~~~~{.c}
 *		as_bin_name select[3] = { "a", "b", "c" };
 *
 *		as_query_bins bins;
 *		bins._free = false;
 *		bins.capacity = 0;
 *		bins.size = sizeof(select) / sizeof(as_bin_name);
 *		bins.entries = select;
 *	~~~~~~~~~~
 *
 *	For Heap, use malloc() or similar:
 *
 *	~~~~~~~~~~{.c}
 *		as_query_bins bins;
 *		bins._free = false;
 *		bins.capacity = SZ;
 *		bins.size = 0;
 *		bins.entries = (as_bin_name *) malloc(sizeof(as_bin_name) * SZ);
 *	~~~~~~~~~~
 *
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
 *	For Stack, use alloca() or similar:
 *
 *	~~~~~~~~~~{.c}
 *		as_query_predicates predicates;
 *		predicates._free = false;
 *		predicates.capacity = SZ;
 *		predicates.size = 0;
 *		predicates.entries = (as_predicate *) alloca(sizeof(as_predicate) * SZ);
 *	~~~~~~~~~~
 *
 *	Alternatively, on the stack you can use an array:
 *
 *	~~~~~~~~~~{.c}
 *		as_predicate p[3] = { ... };
 *
 *		as_query_predicates predicates;
 *		predicates._free = false;
 *		predicates.capacity = 0;
 *		predicates.size = sizeof(p) / sizeof(as_predicate);
 *		predicates.entries = p;
 *	~~~~~~~~~~
 *
 *	For Heap, use malloc() or similar:
 *
 *	~~~~~~~~~~{.c}
 *		as_query_predicates predicates;
 *		predicates._free = false;
 *		predicates.capacity = SZ;
 *		predicates.size = 0;
 *		predicates.entries = (as_predicate *) malloc(sizeof(as_predicate) * SZ);
 *	~~~~~~~~~~
 *
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
 *	For Stack, use alloca() or similar:
 *
 *	~~~~~~~~~~{.c}
 *	    as_query_ordering ordering;
 *	    ordering._free = false;
 *	    ordering.capacity = SZ;
 *	    ordering.size = 0;
 *	    ordering.entries = (as_orderby *) alloca(sizeof(as_orderby) * SZ);
 *	~~~~~~~~~~
 *
 *	Alternatively, on the stack you can use an array:
 *
 *	~~~~~~~~~~{.c}
 *	    as_orderby orderby[3] = { ... };
 *	    
 *	    as_query_ordering orderby;
 *	    ordering._free = false;
 *	    ordering.capacity = 0;
 *	    ordering.size = sizeof(orderby) / sizeof(as_orderby);
 *	    ordering.entries = predicates;
 *	~~~~~~~~~~
 *
 *	For Heap, use malloc() or similar:
 *
 *	~~~~~~~~~~{.c}
 *	    as_query_ordering orderby;
 *	    ordering._free = false;
 *	    ordering.capacity = SZ;
 *	    ordering.size = 0;
 *	    ordering.entries = (as_orderby *) malloc(sizeof(as_orderby) * SZ);
 *	~~~~~~~~~~
 *
 */
typedef struct as_query_ordering_s {

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
	as_orderby * entries;

} as_query_ordering;

/**
 *	Describes the query.
 *
 *	To create a new query, you must either use `as_query_init()` or `as_query_new()`.
 *	Both functions require a namespace and set to query.
 *
 *	`as_query_init()` will initialize a stack allocated `as_query`:
 *
 *	~~~~~~~~~~{.c}
 *		as_query query;
 *		as_query_init(&query, "namespace", "set");
 *	~~~~~~~~~~
 *
 *	`as_query_new()` will create a new heap allocated `as_query`:
 *
 *	~~~~~~~~~~{.c}
 *		as_query * query = as_query_new("namespace", "set");
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
 *	    as_query_destroy(query);
 *	~~~~~~~~~~
 *
 */
typedef struct as_query_s {

	/**
	 *	@private
	 *	If true, then as_query_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	namespace to be queried.
	 */
	char * namespace;

	/**
	 *	set to be queried
	 */
	char * set;

	/**
	 *	Name of bins to select.
	 */
	as_query_bins select;

	/**
	 *	Predicates for filtering.
	 */
	as_query_predicates predicates;

	/**
	 *	Bins to order by.
	 */
	as_query_ordering orderby;

	/**
	 *	Limit the result set.
	 *	If set to UINT64_MAX (default), then the query will 
	 *	return all matching results.
	 */
	uint64_t limit;

	/**
	 *	UDF to apply to results of the query
	 */
	as_udf_call apply;

} as_query;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize a stack allocated as_query.
 *
 *	@param query 	The query to initialize.
 *	@param ns 		The namespace to query.
 *	@param set 		The set to query.
 *
 *	@return the initialized query on success. Otherwise NULL.
 */
as_query * as_query_init(as_query * query, const char * ns, const char * set);

/**
 *	Creates a new heap allocated as_query.
 *
 *	@param ns 		The namespace to query.
 *	@param set 		The set to query.
 *
 *	@return the new query on success. Otherwise NULL.
 */
as_query * as_query_new(const char * ns, const char * set);

/**
 *	Destroy the query and associated resources.
 *
 *	@param query 	The query to destroy.
 */
void as_query_destroy(as_query * query);


/**
 *	Select bins to be projected from matching records.
 *
 *	~~~~~~~~~~{.c}
 *		as_query_select(&q, "bin1");
 *		as_query_select(&q, "bin2");
 *		as_query_select(&q, "bin3");
 *	~~~~~~~~~~
 *
 *
 *	The `as_query_select()` function will attempt to automatically `malloc()` 
 *	entries if `query.select.entries` is `NULL`. 
 *	If `query.select.capacity > 0`, then the first `malloc()` will 
 *	allocate `query.select.capacity` entries. 
 *	Otherwise, `query.select.capacity` will default to 10.
 *
 *	@param query 		The query to modify.
 *	@param bin 			The name of the bin to select.
 *
 *	@return 0 on success. Otherwise an error occurred.
 */
bool as_query_select(as_query * query, const char * bin);

/**
 *	Add a predicate to the query.
 *
 *	~~~~~~~~~~{.c}
 *		as_query_where(&q, "bin1", string_equals("abc"));
 *		as_query_where(&q, "bin1", integer_equals(123));
 *		as_query_where(&q, "bin1", integer_range(0,123));
 *	~~~~~~~~~~
 *
 *
 *	The `as_query_where()` function will attempt to automatically `malloc()` 
 *	entries if `query.predicates.entries` is `NULL`. 
 *	If `query.predicates.capacity > 0`, then the first `malloc()` will 
 *	allocate `query.predicates.capacity` entries. 
 *	Otherwise, `query.predicates.capacity` will default to 10.
 *
 *	@param query			The query to modify.
 *	@param bin			The name of the bin the predicate will apply to.
 *	@param type			The type of predicate.
 *	@param ... 			The values for the predicate.
 *
 *	@return 0 on success. Otherwise an error occurred.
 */
bool as_query_where(as_query * query, const char * bin, as_predicate_type type, ... );

/**
 *	Add a bin to sort by to the query.
 *
 *	~~~~~~~~~~{.c}
 *		as_query_orderby(&q, "bin1", true);
 *	~~~~~~~~~~
 *
 *
 *	The `as_query_orderby()` function will attempt to automatically `malloc()` 
 *	entries if `query.orderby.entries` is `NULL`. 
 *	If `query.orderby.capacity > 0`, then the first `malloc()` will 
 *	allocate `query.orderby.capacity` entries. 
 *	Otherwise, `query.orderby.capacity` will default to 10.
 *
 *	@param query			The query to modify.
 *	@param bin			The name of the bin to sort by.
 *	@param ascending		If true, will sort the bin in ascending order. Otherwise, descending order it used.
 *
 *	@param 0 on success. Otherwise an error occurred.
 */
bool as_query_orderby(as_query * query, const char * bin, bool ascending);

/**
 *	Limit the number of results by `limit`. If limit is `UINT64_MAX`, then all matching results are returned.
 *
 *	~~~~~~~~~~{.c}
 *		as_query_limit(&q, 100);
 *	~~~~~~~~~~
 *
 *	@param query 		The query to modify.
 *	@param limit 		The number of records to limit by.
 *
 *	@param 0 on success. Otherwise an error occurred.
 */
bool as_query_limit(as_query * query, uint64_t limit);

/**
 *	Apply a function to the results of the querty.
 *
 *	~~~~~~~~~~{.c}
 *		as_query_apply(&q, "my_module", "my_function", NULL);
 *	~~~~~~~~~~
 *
 *	@param query			The query to apply the function to.
 *	@param module		The module containing the function to invoke.
 *	@param function		The function in the module to invoke.
 *	@param arglist		The arguments to use when calling the function.
 *
 *	@param 0 on success. Otherwise an error occurred.
 */
bool as_query_apply(as_query * query, const char * module, const char * function, const as_list * arglist);

/**
 *	@}
 */
