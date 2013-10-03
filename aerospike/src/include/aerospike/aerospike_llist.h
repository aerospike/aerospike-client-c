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
 * Functionality related to Large List Data Type
 */

#pragma once 

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_key.h>
#include <aerospike/as_val.h>
#include <aerospike/as_boolean.h>

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Add a value into the llist.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *	
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *	if ( aerospike_llist_add(&as, &err, NULL, &key, &llist, (as_val *) &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The ldt bin to insert values to.
 *	@param val			The value to insert into the llist.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_add(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val);

/**
 *	Add a list of values into the llist.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *
 *	as_arraylist vals;
 *	as_arraylist_inita(&vals, 2);
 *	as_string s;
 *	as_string_init(s,"a string",false);
 *	as_arraylist_append_string(&vals, s);
 *	as_arraylist_append_int64(&vals, 35);
 *
 *	if ( aerospike_llist_add_all(&as, &err, NULL, &key, &llist, (as_list *)vals) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The ldt bin to insert values to.
 *	@param vals			The list of values to insert into the llist.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_add_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * vals);

/**
 *	Search for a value in the llist.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *	
 *	as_integer search_val;
 *	as_integer_init(&search_val, 42);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_find(&as, &err, NULL, &key, &llist, &ival, &search_val, &result_list ) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// do logic because element exists
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The llist bin to lookup from. If not an llist bin, will return error.
 *	@param search_val 	The search value
 *	@param result_list	The returned list of values
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *	
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_find(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * search_val,
	as_list ** elements );


/**
 *	Given an llist bin, return all values in the list.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_filter(&as, &err, NULL, &key, &llist,
 *			 &result_list) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// process the returned elements
 *		as_arraylist_destroy(result_list);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The llist bin to search from. If not an llist bin, will return error.
 *	@param list			The pointer to a list of elements returned from search function. Pointer should
 *						be NULL passed in.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_scan(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, as_list ** elements );

/**
 *	Given an llist bin, filter the collection of objects using the given
 *	filter function. If no filter function is specified, return all values.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_filter(&as, &err, NULL, &key, &llist,
 *			"search_filter", NULL, &result_list) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// process the returned elements
 *		as_arraylist_destroy(result_list);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The llist bin to search from. If not an llist bin, will return error.
 *	@param filter		The name of the User-Defined-Function to use as a search filter.
 *	@param fargs		The list of parameters passed in to the User-Defined-Function filter.
 *	@param list			The pointer to a list of elements returned from search function. Pointer should
 *						be NULL passed in.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_udf_function_name filter, const as_list *filter_args,
	as_list ** elements );

/**
 *	Look up a llist and find how many elements it contains
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *	uint32_t llist_size = 0;
 *
 *	if ( aerospike_llist_size(&as, &err, NULL, &key, &llist, &llist_size) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The llist to operate on. If not an llist bin, will return error.
 *	@param n			Return the number of elements in the llist.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *n
	);

/**
 *	Delete the given value from the llist
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "llist", AS_LDT_LLIST, NULL);
 *
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *	if ( aerospike_llist_remove(&as, &err, NULL, &key, &llist, &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The llist bin to delete from. If not an llist bin, will return error.
 *	@param val			The value to delete from the set.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_remove(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val *element
	);

/**
 *	Destroy the llist bin
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	if ( aerospike_llist_destroy(&as, &err, NULL, &key, &llist) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The llist bin to destroy. If not an llist bin, will return error.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_destroy(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt
	);

