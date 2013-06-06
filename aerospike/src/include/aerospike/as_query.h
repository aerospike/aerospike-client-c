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
 * TYPES
 *****************************************************************************/

struct as_query_orderby_s {
	/**
	 * name of the bin to orderby
	 */
	char * bin;

	/**
	 * bin should be in ascending order
	 */
	bool ascending;
};

typedef struct as_query_orderby_s as_query_orderby;

struct as_query_s {

	/**
	 * Object can be free()'d
	 */
	bool __free;

	/**
	 * namespace to be queried.
	 */
	char * namespace;

	/**
	 * set to be queried
	 */
	char * set;

	/**
	 * name of bins to select
	 * A NULL terminated sequence of strings.
	 */
	char ** select;

	/**
	 * predicates for filtering.
	 * vector of as_predicate
	 */
	char ** predicates;

	/**
	 * bins to order by
	 * vector of as_query_orderby
	 */
	char ** ordering;

	/**
	 * Limit the result set
	 */
	int32_t limit;

	/**
	 * UDF to apply to results of the query
	 */
	as_udf_call then;

	/**
	 * Queue for streamed results
	 */
	void * streamq;

};

typedef struct as_query_s as_query;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_query * as_query_init(as_query * query, const char * ns, const char * set);

as_query * as_query_new(const char * ns, const char * set);

void as_query_destroy(as_query * query);



int as_query_select(as_query * query, const char * bin);

int as_query_where(as_query * query, as_predicate_type type, ...);

int as_query_filter(as_query * query, as_predicate * predicate);

int as_query_orderby(as_query * query, const char * bin, bool ascending);

int as_query_limit(as_query * query, int32_t limit);

int as_query_then(as_query * query, const char * module, const char * function const as_list * arglist);
