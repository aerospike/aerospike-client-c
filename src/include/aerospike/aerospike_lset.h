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
 * Functionality related to Large Set Data Type
 */

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
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
 *	Add a value into the lset.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *	
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *	if ( aerospike_lset_add(&as, &err, NULL, &key, &lset, (as_val *) &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The ldt bin to insert values to.
 *	@param val			The value to insert into the lset.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_add(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val);

/**
 *	Add a list of values into the lset.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *
 *
 *	as_arraylist vals;
 *	as_arraylist_inita(&vals, 2);
 *	as_string s;
 *	as_string_init(s,"a string",false);
 *	as_arraylist_append_string(&vals, s);
 *	as_arraylist_append_int64(&vals, 35);
 *
 *	if ( aerospike_lset_add_all(&as, &err, NULL, &key, &lset, (as_list *)vals) != AEROSPIKE_OK ) {
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
 *	@param vals			The list of values to insert into the lset.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_add_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * vals);

/**
 *	See if a value exists in an lset
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *  boolean exists = false;
 *	
 *	if ( aerospike_lset_exists(&as, &err, NULL, &key, &lset, &ival, &exists) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The lset bin to lookup from. If not an lset bin, will return error.
 *	@param val          The value we're searching for.
 *	@param exists 		Returned boolean value to indicate value exists.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *	
 *	@ingroup ldt_operations
 */

as_status aerospike_lset_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val,
	as_boolean *exists);


/**
 *	Fetch (get) a value from the lset.
 *	Note that this is useful mainly in the case where the search criteria for
 *	an object is less than the entire object -- and that is when the standard
 *	defaults are overridden and the unique_identifier() function is employed
 *	to use only part of the object for search and compare. 
 *	The unique_identifier() function is defined on create -- and declared in
 *	the USER_MODULE.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *  as_val * p_return_val;
 *	
 *	if ( aerospike_lset_exists(&as, &err, NULL, &key, &lset, &ival, &p_return_value) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The lset bin to lookup from. If not an lset bin, will return error.
 *	@param val          The value we're searching for.
 *	@param pp_return_val   Returned value.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *	
 *	@ingroup ldt_operations
 */

as_status aerospike_lset_get(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val,
	as_val ** pp_return_val );

/**
 *	Given an lset bin, filter the set of objects using the given filter function.
 *	If no filter function is specified, all values in the set will be returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *
 *	as_list *list = NULL;
 *
 *	if ( aerospike_lset_filter(&as, &err, NULL, &key, &lset,
 *			"search_filter", NULL, (as_list *) &list) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// process the returned elements
 *		as_arraylist_destroy(list);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lset bin to search from. If not an lset bin, will return error.
 *	@param filter		The name of the User-Defined-Function to use as a search filter.
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter.
 *	@param elements		The pointer to a list of elements returned from search function. Pointer should
 *						be NULL passed in.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_udf_function_name filter, const as_list *filter_args,
	as_list ** elements );

/**
 *	Given an lset bin, scan for all the values in the set
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *
 *	as_list *list = NULL;
 *
 *	if ( aerospike_lset_scan(&as, &err, NULL, &key, &lset,
 *			"search_filter", NULL, (as_list *) &list) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// process the returned elements
 *		as_arraylist_destroy(list);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lset bin to search from. If not an lset bin, will return error.
 *	@param elements		The pointer to a list of elements returned from search function. Pointer should
 *						be NULL passed in.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_scan(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	as_list ** elements );

/**
 *	Look up a lset and find how many elements it contains
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *	uint32_t lset_size = 0;
 *
 *	if ( aerospike_lset_size(&as, &err, NULL, &key, &lset, &lset_size) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lset to operate on. If not an lset bin, will return error.
 *	@param n			Return the number of elements in the lset.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *n
	);

/**
 *	Delete the given value from the lset
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "lset", AS_LDT_LSET, NULL);
 *
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *	if ( aerospike_lset_remove(&as, &err, NULL, &key, &lset, &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lset bin to delete from. If not an lset bin, will return error.
 *	@param element		The value to delete from the set.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_remove(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val *element
	);

/**
 *	Destroy the lset bin
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *
 *	if ( aerospike_lset_destroy(&as, &err, NULL, &key, &lset) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lset bin to destroy. If not an lset bin, will return error.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_destroy(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt
	);

/**
 *	Change an LDT storage capacity (in number of elements)
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LMAP, NULL);
 *	uint32_t ldt_capacity = 0;
 *
 *	if ( aerospike_lset_set_capacity(&as, &err, NULL, &key, &lset, ldt_capacity) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to check
 *	@param ldt_capacity	The number of elements cap for the LDT.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_set_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t ldt_capacity
	);

/**
 *	Get an LDTs storage capacity (in number of elements)
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LMAP, NULL);
 *	uint32_t ldt_capacity = 0;
 *
 *	if ( aerospike_lset_get_capacity(&as, &err, NULL, &key, &lset, &ldt_capacity) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT bin to operate on
 *	@param ldt_capacity The LDT Capacity, in terms of elements, not bytes.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_get_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *ldt_capacity
	);

/**
 *	Check to see if an LDT object exists in this record bin.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lset;
 *	as_ldt_init(&lset, "mylset", AS_LDT_LSET, NULL);
 *	uint32_t ldt_exists = 0;
 *
 *	if ( aerospike_lset_ldt_exists(&as, &err, NULL, &key, &lset, &ldt_exists) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to operate on. If not an LMAP bin, will return error.
 *	@param ldt_exists	Ptr to as_boolean: Set to TRUE if ldt exists, otherwise false.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lset_ldt_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	as_boolean *ldt_exists
	);

#ifdef __cplusplus
} // end extern "C"
#endif
