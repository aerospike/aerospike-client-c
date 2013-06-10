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

#include <aerospike/as_list.h>
#include <aerospike/as_udf.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define string_eq(__val) AS_PREDICATE_STRING_EQ, __val

#define integer_eq(__val) AS_PREDICATE_INTEGER_EQ, __val

#define integer_range(__min, __max) AS_PREDICATE_INTEGER_RANGE, __min, __max

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Union of supported predicates
 */
union as_predicate_value_u {
	char * string;
	int64_t integer;
	struct {
		int64_t min;
		int64_t max;
	} integer_range;
};

typedef union as_predicate_value_u as_predicate_value;

/**
 * Predicate Identifiers
 */
enum as_predicate_type_e {
	AS_PREDICATE_STRING_EQUAL,
	AS_PREDICATE_INTEGER_EQUAL,
	AS_PREDICATE_INTEGER_RANGE
};

typedef enum as_predicate_type_e as_predicate_type;

/**
 * Predicate
 */
struct as_predicate_s {

	/**
	 * Bin to apply the predicate to
	 */
	char * bin;

	/**
	 * The predicate type, dictates which value to use from the union
	 */
	as_predicate_type type;

	/**
	 * The value for the predicate.
	 */
	as_predicate_value value;
};

typedef struct as_predicate_s as_predicate;

/**
 * Describes the bin to be ordered by and 
 * whether it is ascending order.
 */
struct as_orderby_s {
	/**
	 * name of the bin to orderby
	 */
	char * bin;

	/**
	 * bin should be in ascending order
	 */
	bool ascending;
};

typedef struct as_orderby_s as_orderby;

/**
 * Describes the query.
 */
struct as_query_s {

	/**
	 * Object can be free()'d
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
	 * A NULL terminated sequence of strings.
	 *
	 *		char * select[3] = { "bin1", "bin2", NULL }
	 *
	 */
	char ** select;

	/**
	 * Predicates for filtering.
	 *
	 * A {0} terminated sequence of as_predicate.
	 *
	 *		as_predicate predicates[2] = {
	 *			{ .bin = "bin1", .type = AS_PREDICATE_STRING_EQ, .data.string_eq.value = "abc" }
	 *			{ 0 }
	 *		}
	 */
	as_predicate * predicates;

	/**
	 * Bins to order by.
	 *
	 * A {0} terminated sequence of as_orderby.
	 *
	 *		as_orderby * orderby[3] = { 
	 *			{ .bin = "bin1", .ascending = true },
	 *			{ .bin = "bin1", .ascending = true },
	 *			{ 0 }
	 *		}
	 *
	 */
	as_orderby * orderby;

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

};

typedef struct as_query_s as_query;

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
 *
 * @param query 	- the query to modify
 * @param bin 		- the name of the bin to apply a predicate to
 * @param type 		- the name of the bin to apply a predicate to
 *
 * @return 0 on success. Otherwise an error occurred.
 */
int as_query_where(as_query * query, const char * bin, as_predicate_type type, as_predicate_value value);

/**
 * Add a bin to sort by to the query.
 *
 *		as_query_orderby(&q, "bin1", true);
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
