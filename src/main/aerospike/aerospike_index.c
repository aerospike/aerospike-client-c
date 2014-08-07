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

#include <aerospike/aerospike_index.h>

#include <citrusleaf/cl_sindex.h>

#include "_log.h"
#include "_policy.h"
#include "_shim.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Create a new secondary index.
 *
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace to be indexed
 * @param set       - the set to be indexed
 * @param bin       - the bin to be indexed
 * @param type      - the type of the bin to be indexed
 * @param name      - the name of the index
 *
 * @return AEROSPIKE_OK if successful or index already exists. Otherwise an error.
 */
static as_status aerospike_index_create(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * ns, const char * set, const char * bin, const char * type, const char * name)
{
	as_error_reset(err);

	char * response = NULL;
	int rc = citrusleaf_secondary_index_create(as->cluster, ns, set, name, bin, type, &response);
	
	switch ( rc ) {
		case CITRUSLEAF_OK:
		case CITRUSLEAF_FAIL_INDEX_FOUND:
			break;

		default:
			as_strncpy(err->message, response, sizeof(err->message));
			as_error_fromrc(err, rc);
			break;
	}

	free(response);
	return err->code;
}

/**
 *	Create a new secondary index on an integer bin.
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_index_integer_create(&as, &err, NULL, "test", "demo", "bin1", "idx_test_demo_bin1") != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace to be indexed.
 *	@param set			The set to be indexed.
 *	@param bin			The bin to be indexed.
 *	@param name			The name of the index.
 *
 *	@return AEROSPIKE_OK if successful or index already exists. Otherwise an error.
 *
 *	@ingroup index_operations
 */
as_status aerospike_index_integer_create(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * ns, const char * set, const char * bin, const char * name)
{
	return aerospike_index_create(as, err, policy, ns, set, bin, "NUMERIC", name);
}

/**
 *	Create a new secondary index on a string bin.
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_index_string_create(&as, &err, NULL, "test", "demo", "bin1", "idx_test_demo_bin1") != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace to be indexed.
 *	@param set			The set to be indexed.
 *	@param bin			The bin to be indexed.
 *	@param name			The name of the index.
 *
 *	@return AEROSPIKE_OK if successful or index already exists. Otherwise an error.
 *
 *	@ingroup index_operations
 */
as_status aerospike_index_string_create(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * ns, const char * set, const char * bin, const char * name)
{
	return aerospike_index_create(as, err, policy, ns, set, bin, "STRING", name);
}

/**
 *	Removes (drops) a secondary index.
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_index_remove(&as, &err, NULL, "test", idx_test_demo_bin1") != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param ns			The namespace containing the index to be removed.
 *	@param name			The name of the index to be removed.
 *
 *	@return AEROSPIKE_OK if successful or index does not exist. Otherwise an error.
 *
 *	@ingroup index_operations
 */
as_status aerospike_index_remove(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * ns, const char * name)
{
	as_error_reset(err);

	char * response = NULL;
	int rc = citrusleaf_secondary_index_drop(as->cluster, ns, name, &response);

	switch (rc) {
		case CITRUSLEAF_OK:
		case CITRUSLEAF_FAIL_INDEX_NOTFOUND:
			break;

		default:
			as_strncpy(err->message, response, sizeof(err->message));
			as_error_fromrc(err, rc);
			break;
	}
	
	free(response);
	return err->code;
}
