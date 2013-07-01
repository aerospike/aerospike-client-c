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
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_udf.h>
#include <stdarg.h>

#include "_log.h"

/******************************************************************************
 *	INSTANCE FUNCTIONS
 *****************************************************************************/

static as_query * as_query_defaults(as_query * query, bool free, const as_namespace ns, const as_set set) 
{
	query->_free = free;

	if ( strlen(ns) < AS_KEY_NAMESPACE_MAX_SIZE ) {
		strcpy(query->ns, ns);
	}
	else {
		query->ns[0] = '\0';
	}
	
	if ( strlen(set) < AS_KEY_SET_MAX_SIZE ) {
		strcpy(query->set, set);
	}
	else {
		query->set[0] = '\0';
	}

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
	
	as_udf_call_init(&query->apply, NULL, NULL, NULL);

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
as_query * as_query_init(as_query * query, const as_namespace ns, const as_set set)
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
as_query * as_query_new(const as_namespace ns, const as_set set)
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
	if ( !query ) return;

	query->ns[0] = '\0';
	query->set[0] = '\0';

	if ( query->select.entries && query->select._free ) {
		free(query->select.entries);
	}

	query->select._free = false;
	query->select.capacity = 0;
	query->select.size = 0;
	query->select.entries = NULL;
	
	if ( query->where.entries && query->where._free ) {
		free(query->where.entries);
	}

	query->where._free = false;
	query->where.capacity = 0;
	query->where.size = 0;
	query->where.entries = NULL;
	
	if ( query->orderby.entries && query->orderby._free ) {
		free(query->orderby.entries);
	}

	query->orderby._free = false;
	query->orderby.capacity = 0;
	query->orderby.size = 0;
	query->orderby.entries = NULL;
	
	as_udf_call_destroy(&query->apply);

	if ( query->_free ) {
		free(query);
	}
}

/******************************************************************************
 *	SELECT FUNCTIONS
 *****************************************************************************/

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
 */
bool as_query_select_init(as_query * query, uint16_t n)
{
	if ( !query ) return false;
	if ( query->select.entries ) return false;

	query->select.entries = (as_bin_name *) calloc(n, sizeof(as_bin_name));
	if ( !query->select.entries ) return false;

	query->select._free = true;
	query->select.capacity = n;
	query->select.size = 0;

	return true;
}

/**
 *	Select bins to be projected from matching records.
 *	
 *	You have to ensure as_query.select has sufficient capacity, prior to 
 *	adding a bin. If capacity is sufficient then false is returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_select_init(&q, 3);
 *	as_query_select(&q, "bin1");
 *	as_query_select(&q, "bin2");
 *	as_query_select(&q, "bin3");
 *	~~~~~~~~~~
 *	
 *	@param query 		The query to modify.
 *	@param bin 			The name of the bin to select.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_query_select(as_query * query, const char * bin)
{
	// test preconditions
	if ( !query || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( query->select.size >= query->select.capacity ) return false;

	strcpy(query->select.entries[query->select.size], bin);
	query->select.size++;

	return true;
}

/******************************************************************************
 *	WHERE FUNCTIONS
 *****************************************************************************/

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
 *	@param n		The number of as_query_predicate to allocate.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_query_where_init(as_query * query, uint16_t n)
{
	if ( !query ) return false;
	if ( query->where.entries ) return false;

	query->where.entries = (as_predicate *) calloc(n, sizeof(as_predicate));
	if ( !query->where.entries ) return false;

	query->where._free = true;
	query->where.capacity = n;
	query->where.size = 0;

	return true;
}

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
 */
bool as_query_where(as_query * query, const char * bin, as_predicate_type type, ... )
{
	// test preconditions
	if ( !query || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( query->where.size >= query->where.capacity ) return false;

	as_predicate * p = &query->where.entries[query->where.size++];

	strcpy(p->bin, bin);
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

	return true;
}

/******************************************************************************
 *	WHERE FUNCTIONS
 *****************************************************************************/

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
 *	@param n		The number of as_query_ordering to allocate.
 *
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_query_orderby_init(as_query * query, uint16_t n)
{
	if ( !query ) return false;
	if ( query->where.entries ) return false;

	query->where.entries = (as_query_predicates *) calloc(n, sizeof(as_query_predicates));
	if ( !query->where.entries ) return false;

	query->where._free = true;
	query->where.capacity = n;
	query->where.size = 0;
	
	return true;
}

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
 *	@param query		The query to modify.
 *	@param bin			The name of the bin to sort by.
 *	@param ascending	The direction to order by: `AS_ORDER_ASCENDING` or `AS_ORDER_DESCENDING`.
 *	
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_query_orderby(as_query * query, const char * bin, as_order_direction direction)
{
	// test preconditions
	if ( !query || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( query->orderby.size >= query->orderby.capacity ) return false;

	as_order * o = &query->orderby.entries[query->orderby.size++];

	strcpy(o->bin, bin);
	o->direction = direction;

	return true;
}

/******************************************************************************
 *	QUERY MODIFIER FUNCTIONS
 *****************************************************************************/

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
bool as_query_limit(as_query * query, uint64_t limit)
{
	if ( !query ) return false;
	query->limit = limit;
	return true;
}

/**
 * Apply a function to the results of the query.
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
bool as_query_apply(as_query * query, const char * module, const char * function, const as_list * arglist)
{
	if ( !query ) return false;
	as_udf_call_init(&query->apply, module, function, (as_list *) arglist);
	return true;
}
