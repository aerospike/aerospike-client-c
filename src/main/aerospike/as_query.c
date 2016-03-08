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
#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log.h>
#include <aerospike/as_query.h>
#include <aerospike/as_udf.h>

#include <citrusleaf/alloc.h>

#include <stdarg.h>

/******************************************************************************
 *	INSTANCE FUNCTIONS
 *****************************************************************************/

static as_query * as_query_defaults(as_query * query, bool free, const as_namespace ns, const as_set set) 
{
	query->_free = free;

	as_strncpy(query->ns, ns, AS_NAMESPACE_MAX_SIZE);
	as_strncpy(query->set, set, AS_SET_MAX_SIZE);

	query->select._free = false;
	query->select.capacity = 0;
	query->select.size = 0;
	query->select.entries = NULL;
	
	query->where._free = false;
	query->where.capacity = 0;
	query->where.size = 0;
	query->where.entries = NULL;
	
	query->orderby._free = false;
	query->orderby.capacity = 0;
	query->orderby.size = 0;
	query->orderby.entries = NULL;
	
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
	as_query * query = (as_query *) cf_malloc(sizeof(as_query));
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
		cf_free(query->select.entries);
	}

	query->select._free = false;
	query->select.capacity = 0;
	query->select.size = 0;
	query->select.entries = NULL;
	
	if ( query->where.entries && query->where._free ) {
		cf_free(query->where.entries);
	}

	query->where._free = false;
	query->where.capacity = 0;
	query->where.size = 0;
	query->where.entries = NULL;
	
	if ( query->orderby.entries && query->orderby._free ) {
		cf_free(query->orderby.entries);
	}

	query->orderby._free = false;
	query->orderby.capacity = 0;
	query->orderby.size = 0;
	query->orderby.entries = NULL;
	
	as_udf_call_destroy(&query->apply);

	if ( query->_free ) {
		cf_free(query);
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

	query->select.entries = (as_bin_name *) cf_calloc(n, sizeof(as_bin_name));
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
 *	as_query_where(&q, "bin1", as_string_equals("abc"));
 *	as_query_where(&q, "bin1", as_integer_equals(123));
 *	as_query_where(&q, "bin1", as_integer_range(0,123));
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

	query->where.entries = (as_predicate *) cf_calloc(n, sizeof(as_predicate));
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
 *	adding a predicate. If capacity is insufficient then false is returned.
 *
 *	String predicates are not owned by as_query.  If the string is allocated 
 *	on the heap, the caller is responsible for freeing the string after the query 
 *	has been executed.  as_query_destroy() will not free this string predicate.
 *
 *	~~~~~~~~~~{.c}
 *	as_query_where_init(&query, 3);
 *	as_query_where(&query, "bin1", as_string_equals("abc"));
 *	as_query_where(&query, "bin1", as_integer_equals(123));
 *	as_query_where(&query, "bin1", as_integer_range(0,123));
 *	~~~~~~~~~~
 *
 *	@param query		The query add the predicate to.
 *	@param bin			The name of the bin the predicate will apply to.
 *	@param type			The type of predicate.
 *	@param itype		The type of index.
 *	@param dtype		The underlying data type that the index is based on.
 *	@param ... 			The values for the predicate.
 *
 *	@return On success, true. Otherwise an error occurred.
 *
 *	@relates as_query
 */
bool as_query_where(as_query * query, const char * bin, as_predicate_type type, as_index_type itype, as_index_datatype dtype, ... )
{
	// test preconditions
	if ( !query || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( query->where.size >= query->where.capacity ) return false;

	as_predicate * p = &query->where.entries[query->where.size++];
	bool status = true;

	strcpy(p->bin, bin);
	p->type  = type;
	p->dtype = dtype;
	p->itype = itype;
    va_list ap;
    va_start(ap, dtype);

    switch(type) {
    	case AS_PREDICATE_EQUAL:
			if (dtype == AS_INDEX_STRING) {
    			p->value.string = va_arg(ap, char *);
			}
			else if (dtype == AS_INDEX_NUMERIC) {
				p->value.integer = va_arg(ap, int64_t);
			}
			else {
				status = false;
			}
    		break;
    	case AS_PREDICATE_RANGE:
    		if (dtype == AS_INDEX_NUMERIC) {
				p->value.integer_range.min = va_arg(ap, int64_t);
    			p->value.integer_range.max = va_arg(ap, int64_t);
    		}
			else if (dtype == AS_INDEX_GEO2DSPHERE) {
    			p->value.string = va_arg(ap, char *);
			}
			else {
				status = false;
			}
			break;
    }

    va_end(ap);
	return status;
}

/******************************************************************************
 *	ORDERBY FUNCTIONS
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
	if ( query->orderby.entries ) return false;

	query->orderby.entries = (as_ordering *) cf_calloc(n, sizeof(as_ordering));
	if ( !query->orderby.entries ) return false;

	query->orderby._free = true;
	query->orderby.capacity = n;
	query->orderby.size = 0;
	
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
 *	@param query	The query to modify.
 *	@param bin		The name of the bin to sort by.
 *	@param order	The direction to order by: `AS_ORDER_ASCENDING` or `AS_ORDER_DESCENDING`.
 *	
 *	@return On success, true. Otherwise an error occurred.
 */
bool as_query_orderby(as_query * query, const as_bin_name bin, as_order order)
{
	// test preconditions
	if ( !query || !bin || strlen(bin) >= AS_BIN_NAME_MAX_SIZE ) {
		return false;
	}

	// insufficient capacity
	if ( query->orderby.size >= query->orderby.capacity ) return false;

	as_ordering * o = &query->orderby.entries[query->orderby.size++];

	strcpy(o->bin, bin);
	o->order = order;

	return true;
}

/******************************************************************************
 *	QUERY MODIFIER FUNCTIONS
 *****************************************************************************/


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
