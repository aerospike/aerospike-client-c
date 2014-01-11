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
 *	@defgroup key_operations Key Operations
 *	@ingroup client_operations
 *
 *	Aerospike provides a key based API to access and modify data into the 
 *	cluster. 
 *
 *	The Key API is a collection of APIs that use as_key as for looking up
 *	records for accessing and modifying in the cluster. 
 *	
 */

#pragma once 

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Look up a record by key, then return all bins.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *	
 *	as_record * rec = NULL;
 *	if ( aerospike_key_get(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		as_record_destroy(rec);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param rec 			The record to be populated with the data from request.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup key_operations
 */
as_status aerospike_key_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, 
	as_record ** rec
	);

/**
 *	Lookup a record by key, then return specified bins.
 *
 *	~~~~~~~~~~{.c}
 *	char * select[] = {"bin1", "bin2", "bin3", NULL};
 *	
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *	
 *	as_record * rec = NULL;
 *	if ( aerospike_key_select(&as, &err, NULL, &key, select, &rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		as_record_destroy(rec);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param bins			The bins to select. A NULL terminated array of NULL terminated strings.
 *	@param rec 			The record to be populated with the data from request.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup key_operations
 */
as_status aerospike_key_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, const char * bins[], 
	as_record ** rec
	);

/**
 *	Check if a record exists in the cluster via its key. The record's metadata 
 * 	will be populated if the record exists.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *	
 *	bool exists = true;
 *	if ( aerospike_key_exists(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		fprintf(stdout, "Record %s", exists ? "exists." : "doesn't exist.");
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param rec    		The metadata will be populated if the record exists.
 *
 *	@return AEROSPIKE_OK if successful. AEROSPIKE_ERR_RECORD_NOT_FOUND if not found. Otherwise an error.
 *
 *	@ingroup key_operations
 */
as_status aerospike_key_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, 
	as_record ** rec	
	);

/**
 *	Store a record in the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *
 *	as_record rec;
 *	as_record_init(&rec, 2);
 *	as_record_set_str(&rec, "bin1", "abc");
 *	as_record_set_int64(&rec, "bin2", 123);
 *	
 *	if ( aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	
 *	as_record_destroy(&rec);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param rec 			The record containing the data to be written.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup key_operations
 */
as_status aerospike_key_put(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const as_key * key, as_record * rec
	);

/**
 *	Remove a record from the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *
 *	if ( aerospike_key_remove(&as, &err, NULL, &key) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup key_operations
 */
as_status aerospike_key_remove(
	aerospike * as, as_error * err, const as_policy_remove * policy, 
	const as_key * key
	);

/**
 *	Lookup a record by key, then perform specified operations.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *
 *	as_operations ops;
 *	as_operations_inita(&ops,3);
 *	as_operations_add_incr(&ops, "bin1", 456);
 *	as_operations_add_append_str(&ops, "bin2", "def");
 *	as_operations_add_read(&ops, "bin1")
 *
 *	as_record * rec = NULL;
 *
 *	if ( aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		as_record_destroy(rec);
 *	}
 *
 *	as_operations_destroy(&ops);
 *	~~~~~~~~~~
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ops			The operations to perform on the record.
 *	@param rec			The record to be populated with the data from AS_OPERATOR_READ operations.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup key_operations
 */
as_status aerospike_key_operate(
	aerospike * as, as_error * err, const as_policy_operate * policy, 
	const as_key * key, const as_operations * ops,
	as_record ** rec
	);

/**
 *	Lookup a record by key, then apply the UDF.
 *
 *	~~~~~~~~~~{.c}
 *	as_key key;
 *	as_key_init(&key, "ns", "set", "key");
 *
 *	as_arraylist args;
 *	as_arraylist_inita(&args, 2);
 *	as_arraylist_append_int64(&args, 1);
 *	as_arraylist_append_int64(&args, 2);
 *	
 *	as_val * res = NULL;
 *	
 *	if ( aerospike_key_apply(&as, &err, NULL, &key, "math", "add", &args, &res) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		as_val_destroy(res);
 *	}
 *	
 *	as_arraylist_destroy(&args);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param module		The module containing the function to execute.
 *	@param function 	The function to execute.
 *	@param arglist 		The arguments for the function.
 *	@param result 		The return value from the function.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 *
 *	@ingroup key_operations
 */
as_status aerospike_key_apply(
	aerospike * as, as_error * err, const as_policy_apply * policy, 
	const as_key * key,
	const char * module, const char * function, as_list * arglist, 
	as_val ** result
	);
