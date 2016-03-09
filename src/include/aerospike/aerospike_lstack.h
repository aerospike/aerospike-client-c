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
 *	@defgroup ldt_operations Large Data Type Operations
 *	@ingroup client_operations
 *
 *	The ldt_operations module provides API to manipulate
 *	Large Data Types. Currently supported types include:
 *	   - lstack = large stack
 *	   - lset = large set
 *
 *	Forthcoming API:
 * 	   - llist = large list
 * 	   - lmap = large map
 *
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
 *	Push a value onto the lstack.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *	
 *	as_integer ival;
 *	as_integer_init(&ival, 123);
 *
 *	if ( aerospike_lstack_push(&as, &err, NULL, &key, &stack, (as_val *) &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The ldt bin to push values to.
 *	@param val			The value to push on to the lstack.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_push(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val);


/**
 *	Push a value onto the lstack.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *
 *	as_arraylist vals;
 *	as_arraylist_inita(&vals, 2);
 *	as_string s;
 *	as_string_init(s,"a string",false);
 *	as_arraylist_append_string(&vals, s);
 *	as_arraylist_append_int64(&vals, 35);
 *
 *	if ( aerospike_lstack_push_all(&as, &err, NULL, &key, &stack, (as_val *) &ival) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The ldt bin to push values to.
 *	@param vals			The list of values to push on to the lstack. list[0] is the first to push on the stack.
 *						list[n] is top of the stack.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_push_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * vals);

/**
 *	Look up an lstack, then peek to get the top n values from the stack.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *
 *  uint32_t peek_count = 3;
 *
 *	as_arraylist list = as_arraylist_init(&list, peek_count, 0);
 *	
 *	if ( aerospike_lstack_peek(&as, &err, NULL, &key, &stack, peek_count, (as_list *) &list) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// process the returned stack elements
 *		as_arraylist_destroy(list);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The stack bin to peek values from. If not a stack bin, will return error.
 *	@param peek_count	The number of elements to peek from the lstack.
 *	@param elements		Pointer to a list of elements peeked from the lstack.
 *						Pointer should be NULL passed in.
 *						If stack_size shorter than n, only stack_size is returned.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *	
 *	@ingroup ldt_operations
 */

as_status aerospike_lstack_peek(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t peek_count,
	as_list ** elements );

/**
 *	Look up a record by key, then peek into a stack bin, and do UDF post processing
 *	to filter for only the desired values.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *
 *  uint32_t peek_count = 3;
 *
 *	as_arraylist list = as_arraylist_init(&list, peek_count, 0);
 *
 *	if ( aerospike_lstack_peek_with_filter(&as, &err, NULL, &key, &stack, peek_count,
 *			"myfilter", NULL, (as_list *) &list) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		// process the returned stack elements
 *		as_arraylist_destroy(list);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The stack bin to peek values from. If not a stack bin, will return error.
 *	@param peek_count	The number of elements to peek from the lstack.
 *	@param filter		The name of the User-Defined-Function to use as a stack element filter.
 *	@param filter_args	The list of parameters to the User-Defined-Function filter.
 *	@param elements		Pointer to list of elements peeked from the lstack.
 *						Pointer should be initialized to NULL when passed in;
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t peek_count,
	const as_udf_function_name filter, const as_list *filter_args,
	as_list ** elements );

/**
 *	Destroys an existing lstack
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *	uint32_t cap_elements = 0;
 *
 *	if ( aerospike_lstack_destroy(&as, &err, NULL, &key, &stack) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The stack bin to peek values from. If not a stack bin, will return error.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */

as_status aerospike_lstack_destroy(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt
	);

/**
 *	Find how many elements are on the lstack
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *	uint32_t stack_size = 0;
 *
 *	if ( aerospike_lstack_size(&as, &err, NULL, &key, &stack, &stack_size) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The stack bin to peek values from. If not a stack bin, will return error.
 *	@param n			Return the number of elements on the lstack.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *n
	);

/**
 *	Change an LDT storage capacity (in number of elements)
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *	uint32_t ldt_capacity = 0;
 *
 *	if ( aerospike_lstack_set_capacity(&as, &err, NULL, &key, &stack, ldt_capacity) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The stack bin to peek values from. If not a stack bin, will return error.
 *	@param ldt_capacity	The number of elements cap for the lstack.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_set_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t ldt_capacity
	);

/**
 *	Get an lstack's  storage capacity (in number of elements)
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *	uint32_t ldt_capacity = 0;
 *
 *	if ( aerospike_lstack_get_capacity(&as, &err, NULL, &key, &stack, &ldt_capacity) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The stack bin to peek values from. If not a stack bin, will return error.
 *	@param ldt_capacity The LDT Capacity, in terms of elements, not bytes.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_get_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *ldt_capacity
	);

/**
 *	Check to see if an LSTACK object exists in this record bin.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt lstack;
 *	as_ldt_init(&lstack, "mylstack", AS_LDT_LSTACK, NULL);
 *	uint32_t ldt_exists = 0;
 *
 *	if ( aerospike_lstack_size(&as, &err, NULL, &key, &lstack, &ldt_exists) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The LDT to operate on. If not an LSTACK bin, will return error.
 *	@param ldt_exists	Ptr to as_boolean: Set to TRUE if ldt exists, otherwise false.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_ldt_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	as_boolean *ldt_exists
	);


#ifdef __cplusplus
} // end extern "C"
#endif
