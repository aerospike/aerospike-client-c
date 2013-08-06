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
 *	@defgroup ldt_operations Large Data Type Operations (3.0 only)
 *	@ingroup client_operations
 *
 *	The ldt_operations module provides API to manipulate
 *	Large Data Types. Currently supported types include:
 *	   - lstack = large stack
 *	   - lset = large set
 *	Forthcoming are:
 * 	   - llist = large list
 * 	   - lmap = large map
 *
 */

#pragma once 

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_key.h>
#include <aerospike/as_val.h>

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
 *	Look up a record by key, then peek into a stack bin.
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
 *	@param n			The number of elements to peek from the lstack.
 *	@param list			The elements peeked from the lstack. If stack_size shorter than n, only stack_size is returned.
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
 *	@param n			The number of elements to peek from the lstack.
 *	@param filter		The name of the User-Defined-Function to use as a stack element filter.
 *	@param fargs		The list of parameters to the User-Defined-Function filter.
 *	@param list			The elements peeked from the lstack. If stack_size shorter than n, only stack_size is returned
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_peek_with_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t peek_count,
	const as_udf_function_name filter, const as_list *filter_args,
	as_list ** elements );

/**
 *	Look up a large stack and find its stack size
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
	const as_key * key, const as_ldt * ldt, uint32_t *n
	);

/**
 *	Look up a large stack and change its storage capacity (in number of elements)
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "myns", "myset", "mykey");
 *
 *	as_ldt stack;
 *	as_ldt_init(&stack, "mystack", AS_LDT_LSTACK, NULL);
 *	uint32_t cap_elements = 0;
 *
 *	if ( aerospike_lstack_set_capacity(&as, &err, NULL, &key, &stack, &cap_elements) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ldt 			The stack bin to peek values from. If not a stack bin, will return error.
 *	@param n			The number of elements cap for the lstack.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup ldt_operations
 */
as_status aerospike_lstack_set_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t n
	);


