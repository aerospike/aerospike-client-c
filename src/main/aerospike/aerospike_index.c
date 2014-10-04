/*
 * Copyright 2008-2014 Aerospike, Inc.
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
		case AEROSPIKE_OK:
		case AEROSPIKE_ERR_INDEX_FOUND:
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
		case AEROSPIKE_OK:
		case AEROSPIKE_ERR_INDEX_NOT_FOUND:
			break;

		default:
			as_strncpy(err->message, response, sizeof(err->message));
			as_error_fromrc(err, rc);
			break;
	}
	
	free(response);
	return err->code;
}
