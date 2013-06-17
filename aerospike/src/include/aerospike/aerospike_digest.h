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
 *	The Digest API provides functions which use an `as_digest` as the primary
 *	means to locate a record in the cluster. 
 *
 *	Digests are 160 bit hash values, which are generated from the set and
 *	key of a record. The digest is then used to locate where the record should
 *	be stored in the cluster. 
 *
 *	Unlike the Key API, the Digest API allows you to utilize a String or Integer
 *	value as the key, for the digest. The Key API, is basically a digest using
 *	only a string for key value of the digest.
 *
 *	@defgroup digest Digest API
 *	@{
 */

#pragma once 

#include <aerospike/aerospike.h>

#include <aerospike/as_bin.h>
#include <aerospike/as_binop.h>
#include <aerospike/as_digest.h>
#include <aerospike/as_error.h>
#include <aerospike/as_list.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Get a record using a digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		as_record * rec = NULL;
 *		if ( aerospike_key_get(&as, &err, NULL, "test", &digest, &rec) != AEROSPIKE_OK ) {
 *		    fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace of the record.
 *	@param set			The set of the record.
 *	@param digest		The digest of the record. 
 *	@param rec 			The record to be created and populated with the data.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const as_digest * digest, 
	as_record ** rec
	);

/**
 *	Lookup a record by digest, then select specific bins.
 *
 *	The bins arguments must be a sequence of strings, terminated by a NULL:
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		char * select[] = {"bin1", "bin2", "bin3", NULL};
 *		
 *		as_record * rec = NULL;
 *		if ( aerospike_key_select(&as, &err, NULL, "test", &digest, select, &rec) != AEROSPIKE_OK ) {
 *		    fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *		    as_record_destroy(rec);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace of the record.
 *	@param digest		The digest of the record. 
 *	@param bins			The bins to select. 
 *	@param rec 			The record to be created and populated with the data.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const as_digest * digest, 
	const char * bins[], 
	as_record ** rec
	);

/**
 *	Check if a record exists in the cluster using its digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		bool exists = true;
 *		if ( aerospike_digest_exists(&as, &err, NULL, "test", &digest, &exists) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			fprintf(stdout, "Record %s", exists ? "exists." : "doesn't exist.");
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace of the record.
 *	@param digest		The digest of the record. 
 *	@param exists    	The variable to populate with `true` if the record exists, otherwise `false`.
 *
 *	@return AEROSPIKE_OK if exists. AEROSPIKE_DOES_NOT_EXIST if doesn't exists. Otherwise an error.
 */
as_status aerospike_digest_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const as_digest * digest,
	bool * exists
	);

/**
 *	Put a record in the cluster using a digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		as_record rec;
 *		as_record_init(&rec, 2);
 *		as_record_set_string(&rec, "bin1", "abc");
 *		as_record_set_integer(&rec, "bin2", 123);
 *		
 *		if ( aerospike_key_put(&as, &err, NULL, "test", &digest, &rec) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_record_destroy(&rec);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace of the record.
 *	@param digest		The digest of the record. 
 *	@param rec 			The record containing the data to be written.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_put(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const char * ns, const as_digest * digest, 
	as_record * rec
	);

/**
 *	Remove a record from the cluster using a digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		if ( aerospike_key_remove(&as, &err, NULL, "test", &digest) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace of the record.
 *	@param digest		The digest of the record.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_remove(
	aerospike * as, as_error * err, const as_policy_remove * policy, 
	const char * ns, const as_digest * digest
	);

/**
 *	Lookup a record by digest, then apply the UDF
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		as_list args;
 *		as_arraylist_init(&args, 2, 0);
 *		as_list_append_int64(&args, 1);
 *		as_list_append_int64(&args, 2);
 *		
 *		as_val * res = NULL;
 *		
 *		if ( aerospike_digest_apply(&as, &err, NULL, "test", &digest, "math", "add", &args, &res) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_val_destroy(res);
 *		}
 *		
 *		as_list_destroy(&args);
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace of the record.
 *	@param digest		The digest of the record.
 *	@param module		The module containing the function to execute.
 *	@param function 		The function to execute.
 *	@param arglist 		The arguments for the function.
 *	@param result 		The return value from the function.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_apply(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const as_digest * digest, 
	const char * module, const char * function, const as_list * arglist, 
	as_val ** result
	);

/**
 *	Lookup a record by key, then perform specified operations.
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace of the record.
 *	@param digest		The digest of the record.
 *	@param ops 			An array of as_bin_operation, which specify the operation to perform on bins of the record.
 *	@param nops 			The number of operations.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_operate(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const char * ns, const as_digest * digest, 
	as_binops * binops
	);


/** 
 *	@}
 */