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


#include <aerospike/as_bin.h>
#include <aerospike/as_query.h>
#include <aerospike/as_udf.h>
#include <stdarg.h>

#include "log.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static as_query * as_query_defaults(as_query * query, bool free, const char * ns, const char * set) 
{
	query->_free = free;
	query->namespace = ns ? strdup(ns) : NULL;
	query->set = set ? strdup(set) : NULL;

	query->select._free = false;
	query->select.capacity = 0;
	query->select.size = 0;
	query->select.entries = NULL;
	
	query->predicates._free = false;
	query->predicates.capacity = 0;
	query->predicates.size = 0;
	query->predicates.entries = NULL;
	
	query->orderby._free = false;
	query->orderby.capacity = 0;
	query->orderby.size = 0;
	query->orderby.entries = NULL;
	
	query->limit = UINT64_MAX;

	query->apply.module = NULL;
	query->apply.function = NULL;
	query->apply.arglist = NULL;

	return query;
}

/**
 * Initialize a stack allocated as_query.
 *
 * @param query 	- the query to initialize
 * @param ns 		- the namespace to query
 * @param set 		- the set to query
 *
 * @return the initialized query on success. Otherwise NULL.
 */
as_query * as_query_init(as_query * query, const char * ns, const char * set)
{
	if ( !query ) return query;
	return as_query_defaults(query, false, ns, set);
}

/**
 * Creates a new heap allocated as_query.
 *
 * @param ns 		- the namespace to query
 * @param set 		- the set to query
 *
 * @return the new query on success. Otherwise NULL.
 */
as_query * as_query_new(const char * ns, const char * set)
{
	as_query * query = (as_query *) malloc(sizeof(as_query));
	if ( !query ) return query;
	return as_query_defaults(query, true, ns, set);
}

/**
 * Destroy the query and associated resources.
 *
 * @param query 	- the query to destroy
 */
void as_query_destroy(as_query * query) 
{
	if ( query ) {

		if ( query->namespace ) {
			free(query->namespace);
			query->namespace = NULL;
		}

		if ( query->set ) {
			free(query->set);
			query->set = NULL;
		}

		if ( query->select.entries && query->select._free ) {
			free(query->select.entries);
		}

		query->select._free = false;
		query->select.capacity = 0;
		query->select.size = 0;
		query->select.entries = NULL;
		
		if ( query->predicates.entries && query->predicates._free ) {
			free(query->select.entries);
		}

		query->predicates._free = false;
		query->predicates.capacity = 0;
		query->predicates.size = 0;
		query->predicates.entries = NULL;
		
		if ( query->orderby.entries && query->orderby._free ) {
			free(query->select.entries);
		}

		query->orderby._free = false;
		query->orderby.capacity = 0;
		query->orderby.size = 0;
		query->orderby.entries = NULL;

		if ( query->_free ) {
			free(query);
		}
	}
}

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
int as_query_select(as_query * query, const char * bin)
{
	if ( !query ) {
		return 1;
	}
	else if ( query->select.entries == NULL ) {
		// entries is NULL, so we will malloc() it.
		if ( query->select.capacity == 0 ) {
			// capacity can be preset, but if not, we default to 10.
			query->select.capacity = 10;
		}
		query->select.entries = (as_bin_name *) malloc(sizeof(as_bin_name) * query->select.capacity);
		query->select._free = true;
		query->select.size = 0;
	}
	else if ( query->select.size > 0 && query->select.size == query->select.capacity ) {
		if ( query->select._free ) {
			// if previously malloc'd, we will grow by 10 entries.
			query->select.capacity += 10;
			query->select.entries = (as_bin_name *) realloc(query->select.entries, sizeof(as_bin_name) * query->select.capacity);
		}
		else {
			// we will not touch stack allocated entries.
			// so we bail
			return 2;
		}
	}
	
	memcpy(query->select.entries[query->select.size], bin, AS_BIN_NAME_MAX);
	query->select.size++;

	return 0;
}

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
int as_query_where(as_query * query, const char * bin, as_predicate_type type, ... )
{
	if ( !query ) {
		return 1;
	}
	else if ( query->predicates.entries == NULL ) {
		// entries is NULL, so we will malloc() it.
		if ( query->predicates.capacity == 0 ) {
			// capacity can be preset, but if not, we default to 10.
			query->predicates.capacity = 10;
		}
		query->predicates.entries = (as_predicate *) malloc(sizeof(as_predicate) * query->predicates.capacity);
		query->predicates._free = true;
		query->predicates.size = 0;
	}
	else if ( query->predicates.size > 0 && query->predicates.size == query->predicates.capacity ) {
		if ( query->predicates._free ) {
			// if previously malloc'd, we will grow by 10 entries.
			query->predicates.capacity += 10;
			query->predicates.entries = (as_predicate *) realloc(query->predicates.entries, sizeof(as_predicate) * query->predicates.capacity);
		}
		else {
			// we will not touch stack allocated entries.
			// so we bail
			return 2;
		}
	}

	as_predicate * p = &query->predicates.entries[query->predicates.size];

	memcpy(p->bin, bin, AS_BIN_NAME_MAX);
	p->type = type;

    va_list ap;
    va_start(ap, type);

    switch(type) {
    	case AS_PREDICATE_STRING_EQUAL:
    		p->value.string = va_arg(ap, char *);
    		break;
    	case AS_PREDICATE_INTEGER_EQUAL:
    		p->value.integer = va_arg(ap, int64_t);
    		break;
    	case AS_PREDICATE_INTEGER_RANGE:
    		p->value.integer_range.min = va_arg(ap, int64_t);
    		p->value.integer_range.max = va_arg(ap, int64_t);
    		break;
    }

    va_end(ap);
	
	query->predicates.size++;

	return 0;
}

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
int as_query_orderby(as_query * query, const char * bin, bool ascending)
{
	if ( !query ) {
		return 1;
	}
	else if ( query->orderby.entries == NULL ) {
		// entries is NULL, so we will malloc() it.
		if ( query->orderby.capacity == 0 ) {
			// capacity can be preset, but if not, we default to 10.
			query->orderby.capacity = 10;
		}
		query->orderby.entries = (as_orderby *) malloc(sizeof(as_orderby) * query->orderby.capacity);
		query->orderby._free = true;
		query->orderby.size = 0;
	}
	else if ( query->orderby.size > 0 && query->orderby.size == query->orderby.capacity ) {
		if ( query->orderby._free ) {
			// if previously malloc'd, we will grow by 10 entries.
			query->orderby.capacity += 10;
			query->orderby.entries = (as_orderby *) realloc(query->orderby.entries, sizeof(as_orderby) * query->orderby.capacity);
		}
		else {
			// we will not touch stack allocated entries.
			// so we bail
			return 2;
		}
	}

	as_orderby * o = &query->orderby.entries[query->orderby.size];
	memcpy(o->bin, bin, AS_BIN_NAME_MAX);
	o->ascending = ascending;

	query->orderby.size++;

	return 0;
}

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
int as_query_limit(as_query * query, uint64_t limit)
{
	if ( !query ) {
		return 1;
	}

	query->limit = limit;
}

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
int as_query_apply(as_query * query, const char * module, const char * function, const as_list * arglist)
{
	if ( !query ) {
		return 1;
	}

	query->apply.module = module;
	query->apply.function = function;
	query->apply.arglist = arglist;
}
