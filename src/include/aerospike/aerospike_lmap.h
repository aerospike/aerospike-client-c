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
 * Functionality related to Large Map Data Type
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
 *	Add a value into the lmap.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *	
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *	if ( aerospike_lmap_put(&as, &err, NULL, &key, &lmap, (const as_val *) &ival, (as_val *) &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The ldt bin to insert values to.
 *	@param mkey			The map-key.
 *	@param mval			The map-value associated with mkey.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lmap_put(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_val * mkey, const as_val * mval
	);

/**
 *	Add multiple entries into the lmap.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *
 *
 *	as_arraylist vals;
 *	as_arraylist_inita(&vals, 2);
 *	as_string s;
 *	as_string_init(s,"a string",false);
 *	as_arraylist_append_string(&vals, s);
 *	as_arraylist_append_int64(&vals, 35);
 *
 *	if ( aerospike_lmap_put_all(&as, &err, NULL, &key, &lmap, (const as_map *)vals) != AEROSPIKE_OK ) {
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
 *	@param vals			A map containing the entries to add to the lmap.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lmap_put_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_map * vals
	);

/**
 *	Get the value of an entry in the Lmap, using the given map-key
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *  boolean exists = false;
 *	
 *	if ( aerospike_lmap_get(&as, &err, NULL, &key, &lmap, &ikey, &p_val) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The lmap bin to lookup from. If not an lmap bin, will return error.
 *	@param mkey 		The map key.
 *	@param mval 		Returned map value.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *	
 *	@ingroup ldt_operations
 */

as_status aerospike_lmap_get(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * mkey,
	as_val ** mval
	);

/**
 *	Get all the entries in an lmap
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *
 *  as_map *p_map = NULL;
 *
 *	if ( aerospike_lmap_get_all(&as, &err, NULL, &key, &lmap, &p_map) != AEROSPIKE_OK ) {
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
 *	@param ldt 			The lmap bin to lookup from. If not an lmap bin, will return error.
 *	@param elements 	Returned pointer to the map of entries.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */

as_status aerospike_lmap_get_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	as_map ** elements
	);

/**
 *	Given an lmap bin, scan through all entries in the map, and apply the
 *	given filter function. If no filter function is specified, all values
 *	in the lmap will be returned.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *
 *	as_map *p_map = NULL;
 *
 *	if ( aerospike_lmap_filter(&as, &err, NULL, &key, &lmap,
 *			"counter_filter", NULL, (as_map *) &p_map) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// process the returned elements
 *		as_map_destroy(p_map);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lmap bin to operate on. If not an lmap bin, will return error.
 *	@param filter		The name of the User-Defined-Function to use as a read-filter.
 *						The UDF should either return the entry, or nil, if filtered out.
 *	@param filter_args	The list of parameters passed in to the User-Defined-Function filter.
 *	@param elements		The pointer to a map of entries returned from the function. Pointer should
 *						be NULL passed in.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lmap_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_udf_function_name filter, const as_list *filter_args,
	as_map ** elements
	);

/**
 *	Look up a lmap and find how many elements it contains
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *	uint32_t lmap_size = 0;
 *
 *	if ( aerospike_lmap_size(&as, &err, NULL, &key, &lmap, &lmap_size) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lmap to operate on. If not an lmap bin, will return error.
 *	@param n			Return the number of elements in the lmap.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lmap_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *n
	);

/**
 *	Delete the given value from the lmap
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "lmap", AS_LDT_LMAP, NULL);
 *
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *	if ( aerospike_lmap_remove(&as, &err, NULL, &key, &lmap, (const as_val*)&ikey) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lmap bin to delete from. If not an lmap bin, will return error.
 *	@param mkey			The key to delete from the set.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lmap_remove(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val *mkey
	);

/**
 *	Destroy the lmap bin
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *
 *	if ( aerospike_lmap_destroy(&as, &err, NULL, &key, &lmap) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The lmap bin to destroy. If not an lmap bin, will return error.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lmap_destroy(
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
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *	uint32_t ldt_capacity = 0;
 *
 *	if ( aerospike_lmap_set_capacity(&as, &err, NULL, &key, &lmap, ldt_capacity) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to check
 *	@param ldt_capacity	The new capacity for this LDT, in terms of elements, not bytes
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lmap_set_capacity(
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
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *	uint32_t ldt_capacity = 0;
 *
 *	if ( aerospike_lmap_get_capacity(&as, &err, NULL, &key, &lmap, &ldt_capacity) != AEROSPIKE_OK ) {
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
as_status aerospike_lmap_get_capacity(
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
 *	as_ldt lmap;
 *	as_ldt_init(&lmap, "mylmap", AS_LDT_LMAP, NULL);
 *	uint32_t ldt_exists = 0;
 *
 *	if ( aerospike_lmap_ldt_exists(&as, &err, NULL, &key, &lmap, &ldt_exists) != AEROSPIKE_OK ) {
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
as_status aerospike_lmap_ldt_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	as_boolean *ldt_exists
	);


#ifdef __cplusplus
} // end extern "C"
#endif
