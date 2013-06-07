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

#include <aerospike/as_predicate.h>
#include <aerospike/as_udf.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define string_eq(__val) AS_PREDICATE_STRING_EQ, __val

#define integer_eq(__val) AS_PREDICATE_INTEGER_EQ, __val

#define string_eq(__min, __max) AS_PREDICATE_INTEGER_RANGE, __min, __max

/******************************************************************************
 * TYPES
 *****************************************************************************/

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
	 * To get all results, then set to -1.
	 */
	int32_t limit;

	/**
	 * UDF to apply to results of the query
	 */
	as_udf_call apply;

};

typedef struct as_query_s as_query;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_query * as_query_init(as_query * query, const char * ns, const char * set);

as_query * as_query_new(const char * ns, const char * set);

/**
 * Destroy the query and associated resources.
 */
void as_query_destroy(as_query * query);

/**
 * Select bins to be projected from matching records.
 *
 *		as_query_select(&q, "bin1");
 *		as_query_select(&q, "bin2");
 *		as_query_select(&q, "bin3");
 *
 */
int as_query_select(as_query * query, const char * bin);

/**
 * Add a predicate to the query.
 *
 *		as_query_where(&q, "bin1", string_eq("abc"));
 *		as_query_where(&q, "bin1", integer_eq(123));
 *		as_query_where(&q, "bin1", integer_range(0,123));
 *
 */
int as_query_where(as_query * query, const char * bin, as_predicate_type type, ...);

/**
 * Add a predicate to the query.
 *
 *		as_predicate p = { .bin = "bin1", .type = AS_PREDICATE_STRING_EQ, .data.string_eq.value = "abc" };
 *		as_query_filter(&q, &p);
 *
 */
int as_query_filter(as_query * query, as_predicate * predicate);

int as_query_orderby(as_query * query, const char * bin, bool ascending);

int as_query_limit(as_query * query, int32_t limit);

int as_query_then(as_query * query, const char * module, const char * function const as_list * arglist);
