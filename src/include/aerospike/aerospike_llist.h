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
#pragma once

/**
 * Functionality related to Large List Data Type
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_key.h>
#include <aerospike/as_val.h>
#include <aerospike/as_boolean.h>

#ifdef __cplusplus
extern "C" {
#endif

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
 *	Add / update a value into the llist. Existing value will be overwritten.
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
 *	if ( aerospike_llist_update(&as, &err, NULL, &key, &llist, (as_val *) &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The ldt bin to insert values to.
 *	@param val			The value to update/insert into the llist.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_update(
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
 *	Add / update a list of values into the llist.
 *	If the value in input list already exists it will be overwritten.
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
 *	if ( aerospike_llist_update_all(&as, &err, NULL, &key, &llist, (as_list *)vals) != AEROSPIKE_OK ) {
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
 *	@param vals			The list of values to update/insert into the llist.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_update_all(
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
 *	as_integer search_val;
 *	as_integer_init(&search_val, 42);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_find(&as, &err, NULL, &key, &llist, &search_val, &result_list ) != AEROSPIKE_OK ) {
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
 *	@param elements		The returned list of values
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
 *	Select values from the beginning of list up to a maximum count.
 *	Supported by server versions >= 3.5.8.
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
 *	if ( aerospike_llist_find_first(&as, &err, NULL, &key, &llist, 5, &result_list ) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The llist bin to lookup from. If not an llist bin, will return error.
 *	@param count		Maximum number of values to return.
 *	@param elements		The returned list of values.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_find_first(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count, as_list ** elements);
	
/**
 *	Select values from the beginning of list up to a maximum count after applying lua filter.
 *	Supported by server versions >= 3.5.8.
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
 *	if ( aerospike_llist_find_first_filter(&as, &err, NULL, &key, &llist, 5, "search_filter", NULL, &result_list ) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The llist bin to lookup from. If not an llist bin, will return error.
 *	@param count		Maximum number of values to return.
 *	@param filter		The name of the User-Defined-Function to use as a search filter.
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter.
 *	@param elements		The returned list of values.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_find_first_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count,
	const as_udf_function_name filter, const as_list *filter_args, as_list ** elements);

/**
 *	Select values from the end of list up to a maximum count.
 *	Supported by server versions >= 3.5.8.
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
 *	if ( aerospike_llist_find_last(&as, &err, NULL, &key, &llist, 5, &result_list ) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The llist bin to lookup from. If not an llist bin, will return error.
 *	@param count		Maximum number of values to return.
 *	@param elements		The returned list of values.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_find_last(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count, as_list ** elements);
	
/**
 *	Select values from the end of list up to a maximum count after applying lua filter.
 *	Supported by server versions >= 3.5.8.
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
 *	if ( aerospike_llist_find_last_filter(&as, &err, NULL, &key, &llist, 5, "search_filter", NULL, &result_list ) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The llist bin to lookup from. If not an llist bin, will return error.
 *	@param count		Maximum number of values to return.
 *	@param filter		The name of the User-Defined-Function to use as a search filter.
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter.
 *	@param elements		The returned list of values.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_find_last_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count,
	const as_udf_function_name filter, const as_list *filter_args, as_list ** elements);

/**
 *	Select values from the begin key up to a maximum count.
 *	Supported by server versions >= 3.5.8.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	as_integer from_val;
 *	as_integer_init(&from_val, 42);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_find_from(&as, &err, NULL, &key, &llist, &from_val, 5, &result_list ) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The llist bin to lookup from. If not an llist bin, will return error.
 *	@param from_val		Value from which to start.
 *	@param count		Maximum number of values to return.
 *	@param elements		The returned list of values.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_find_from(
	aerospike * as, as_error * err, const as_policy_apply * policy, const as_key * key,
	const as_ldt * ldt, const as_val * from_val, uint32_t count, as_list ** elements);

/**
 *	Select values from the begin key up to a maximum count after applying lua filter.
 *	Supported by server versions >= 3.5.8.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	as_integer from_val;
 *	as_integer_init(&from_val, 42);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_find_from_filter(&as, &err, NULL, &key, &llist, &from_val, 5, "search_filter", NULL, &result_list ) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The llist bin to lookup from. If not an llist bin, will return error.
 *	@param from_val		Value from which to start.
 *	@param count		Maximum number of values to return.
 *	@param filter		The name of the User-Defined-Function to use as a search filter.
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter.
 *	@param elements		The returned list of values.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_find_from_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy, const as_key * key,
	const as_ldt * ldt, const as_val * from_val, uint32_t count,
	const as_udf_function_name filter, const as_list *filter_args, as_list ** elements);

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
 *	@param elements		The pointer to a list of elements returned from search function. Pointer should
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
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter.
 *	@param elements		The pointer to a list of elements returned from search function. Pointer should
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
 *	Given an llist bin, return the key values from MIN to MAX, and then
 *	filter the returned collection of objects using the given
 *	filter function. If no filter function is specified, return all values.
 *	Limit returned values size to given count.  If count is zero, do not
 *	limit returned values.  Count > 0 are supported by server versions >= 3.5.8.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	as_integer min_value;
 *	as_integer_init(&min_value, 18);
 *
 *	as_integer max_value;
 *	as_integer_init(&max_value, 99);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_range_limit(&as, &err, NULL, &key, &llist, &min_value, &max_value, 20,
 *	    "search_filter", NULL, &result_list) != AEROSPIKE_OK ) {
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
 *	@param min_value	The minimum range value (or null to be LEAST value)
 *	@param max_value	The maximum range value (or null to be the GREATEST value)
 *	@param count		The maximum number of values to return, pass in zero to obtain all values within range.
 *	@param filter		The name of the User-Defined-Function to use as a search filter (or null if no filter)
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter (or null)
 *	@param elements		The pointer to a list of elements returned from search function. Pointer should
 *						be NULL passed in.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_range_limit(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_val * min_value, const as_val * max_value, uint32_t count,
	const as_udf_function_name filter, const as_list *filter_args,
	as_list ** elements );
	
/**
 *	Given an llist bin, return the key values from MIN to MAX, and then
 *	filter the returned collection of objects using the given
 *	filter function. If no filter function is specified, return all values.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	as_integer min_value;
 *	as_integer_init(&min_value, 18);
 *
 *	as_integer max_value;
 *	as_integer_init(&max_value, 99);
 *
 *	as_list *result_list = NULL;
 *
 *	if ( aerospike_llist_range(&as, &err, NULL, &key, &llist, &min_value, &max_value,
 *	    "search_filter", NULL, &result_list) != AEROSPIKE_OK ) {
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
 *	@param min_value	The minimum range value (or null to be LEAST value)
 *	@param max_value	The maximum range value (or null to be the GREATEST value)
 *	@param filter		The name of the User-Defined-Function to use as a search filter (or null if no filter)
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter (or null)
 *	@param elements		The pointer to a list of elements returned from search function. Pointer should
 *						be NULL passed in.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
static inline as_status aerospike_llist_range(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_val * min_value, const as_val * max_value,
	const as_udf_function_name filter, const as_list *filter_args,
	as_list ** elements )
{
	return aerospike_llist_range_limit(as, err, policy, key, ldt, min_value, max_value, 0, filter, filter_args, elements);
}

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
 *	@param element		The value to delete from the set.
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

/**
 *	SET the storage capacity for this LDT.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *	uint32_t ldt_capacity = 5000;
 *
 *	if ( aerospike_llist_set_capacity(&as, &err, NULL, &key, &llist, ldt_capacity) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to operate on. If not an LLIST bin, will return error.
 *	@param ldt_capacity Set by function to 1 for true, 0 for false
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_set_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t ldt_capacity
	);

/**
 *	Check the storage capacity for this LDT.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *	uint32_t ldt_capacity = 0;
 *
 *	if ( aerospike_llist_get_capacity(&as, &err, NULL, &key, &llist, &ldt_capacity) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to operate on. If not an LLIST bin, will return error.
 *	@param ldt_capacity Set by function to 1 for true, 0 for false
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_get_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *ldt_capacity
	);

/**
 *	Check to see if an LLIST object exists in this record bin.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *	uint32_t ldt_exists = 0;
 *
 *	if ( aerospike_llist_size(&as, &err, NULL, &key, &llist, &ldt_exists) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to operate on. If not an LLIST bin, will return error.
 *	@param ldt_exists	Ptr to as_boolean: Set to TRUE if ldt exists, otherwise false.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_ldt_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	as_boolean *ldt_exists
	);

/**
 *	Set page size for LLIST bin.
 *	Supported by server versions >= 3.5.8.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt llist;
 *	as_ldt_init(&llist, "myllist", AS_LDT_LLIST, NULL);
 *
 *	if ( aerospike_llist_set_page_size(&as, &err, NULL, &key, &llist, 8192) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to operate on. If not an LLIST bin, will return error.
 *	@param page_size	The new llist page size.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_llist_set_page_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t page_size);

#ifdef __cplusplus
} // end extern "C"
#endif
