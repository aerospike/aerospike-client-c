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

/** 
 * @defgroup index Index Management API
 * @{
 */

#pragma once 

#include <aerospike/aerospike.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Create a new sparse secondary index.
 *
 * A sparse secondary indexes omits references to records which do not include
 * the indexed bin.
 *
 *		if ( aerospike_index_sparse_create(&as, &err, NULL, 
 *				"test", "demo", "bin1", AS_TYPE_INTEGER, "idx_test_demo_bin1") != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns			The namespace to be indexed.
 * @param set			The set to be indexed.
 * @param bin			The bin to be indexed.
 * @param type			The type of the bin to be indexed.
 * @param name			The name of the index.
 *
 * @return AEROSPIKE_OK if successful. AEROSPIKE_EXISTS if the index already exists. Otherwise an error.
 */
as_status aerospike_index_sparse_create(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * ns, const char * set, const char * bin, as_type type, const char * name);

/**
 * Removes (drops) a secondary index.
 *
 *		if ( aerospike_index_remove(&as, &err, NULL, "idx_test_demo_bin1") != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as			The aerospike instance to use for this operation.
 * @param err			The as_error to be populated if an error occurs.
 * @param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns			The namespace containing the index to be removed.
 * @param name			The name of the index to be removed.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_index_remove(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * ns, const char * name); 


/** 
 * @}
 */