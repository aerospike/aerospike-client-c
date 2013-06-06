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

#include <aerospike/aerospike_batch.h>

/******************************************************************************
 * FUNCTION DECLS
 *****************************************************************************/

as_status aerospike_batch_init(aerospike * as, as_error * err);
as_status aerospike_batch_destroy(aerospike * as, as_error * err);

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Lookup multiple records by digest and return 
 *
 *      as_rec * rec = NULL;
 *      if ( aerospike_key_get(&as, &err, NULL, "test", "demo", "foo", &rec) != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set-name of the record.p
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param rec       - the record to be populated with the data from request.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred and the fields of err will be populated.
 */
as_status aerospike_batch_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const char * set, const char * key, 
	aerospike_batch_get_callback callback, void * udata) 
{
	if ( aerospike_batch_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}
	return AEROSPIKE_OK;
}

/**
 * Test whether multiple records exist.
 *
 *      as_rec * rec = NULL;
 *      char * select[] = {"bin1", "bin2", "bin3", NULL};
 *      if ( aerospike_batch_exists(&as, &err, NULL, "test", "demo", "foo", select, &rec) != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param bins      - the bins to select. A NULL terminated array of NULL terminated strings.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred and the fields of err will be populated.
 */
as_status aerospike_batch_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const char * set, const char * key, 
	aerospike_batch_exists_callback callback, void * udata) 
{
	if ( aerospike_batch_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}
	return AEROSPIKE_OK;
}

/**
 * Initialize batch environment
 */
as_status aerospike_batch_init(aerospike * as, as_error * err) 
{
	extern cf_atomic32 batch_initialized;
	if ( batch_initialized > 0 ) {
		return AEROSPIKE_OK;
	}
	citrusleaf_batch_init();
	return AEROSPIKE_OK;
}

/**
 * Destroy batch environment
 */
as_status aerospike_batch_destroy(aerospike * as, as_error * err) 
{
	extern cf_atomic32 batch_initialized;
	if ( batch_initialized == 0 ) {
		return AEROSPIKE_OK;
	}
	citrusleaf_batch_shutdown();
	return AEROSPIKE_OK;
}