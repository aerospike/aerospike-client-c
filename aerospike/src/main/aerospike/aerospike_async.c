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

#include <aerospike/aerospike_async.h>

/******************************************************************************
 * FUNCTION DECLS
 *****************************************************************************/

as_status aerospike_async_init(aerospike * as, as_error * err);
as_status aerospike_async_destroy(aerospike * as, as_error * err);

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Asynchronous put via key.
 *
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param udata     - user-data to be passed to the callback.
 * @param callback  - callback to be invoked when a response is received.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_async_key_put(
	aerospike * as, as_error * err, const as_policy_write * p, 
	const char * ns, const char * set, const char * key, 
	as_record * rec, 
	aerospike_async_put_callback callback, void * udata)
{
	// if ( aerospike_async_init(as, err) != AEROSPIKE_OK ) {
	// 	return err->code;
	// }
	return AEROSPIKE_ERR;
}

/**
 * Asynchronous put via digest.
 *
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param policy    - dictates the behavior of a write.
 * @param ns        - the namespace of the record.
 * @param digest    - the digest of the record.
 * @param rec       - the record to be stored.
 * @param udata     - user-data to be passed to the callback.
 * @param callback  - callback to be invoked when a response is received.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_async_digest_put(
	aerospike * as, as_error * err, const as_policy_write * p, 
	const char * ns, const as_digest * digest, 
	as_record * rec, 
	aerospike_async_put_callback callback, void * udata) 
{
	// if ( aerospike_async_init(as, err) != AEROSPIKE_OK ) {
	// 	return err->code;
	// }
	return AEROSPIKE_ERR;
}

/**
 * Asynchronous remove via key.
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param udata     - user-data to be passed to the callback.
 * @param callback  - callback to be invoked when a response is received.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_async_key_remove(
	aerospike * as, as_error * err, const as_policy_remove * p, 
	const char * ns, const char * set, const char * key, 
	aerospike_async_remove_callback callback, void * udata) 
{
	// if ( aerospike_async_init(as, err) != AEROSPIKE_OK ) {
	// 	return err->code;
	// }
	return AEROSPIKE_ERR;
}

/**
 * Asynchronous remove via digest.
 *
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param policy    - dictates the behavior of a delete.
 * @param ns        - the namespace of the record.
 * @param digest    - the digest of the record.
 * @param udata     - user-data to be passed to the callback.
 * @param callback  - callback to be invoked when a response is received.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_async_digest_remove(
	aerospike * as, as_error * err, const as_policy_remove * p, 
	const char * ns, const as_digest * digest, 
	aerospike_async_remove_callback callback, void * udata) 
{
	// if ( aerospike_async_init(as, err) != AEROSPIKE_OK ) {
	// 	return err->code;
	// }
	return AEROSPIKE_ERR;
}

/**
 * Initialize async environment
 */
as_status aerospike_async_init(aerospike * as, as_error * err) 
{
	extern cf_atomic32 g_async_initialized;
	if ( g_async_initialized > 0 ) {
		return AEROSPIKE_OK;
	}
	citrusleaf_async_init();
	return AEROSPIKE_ERR;
}

/**
 * Destroy async environment
 */
as_status aerospike_async_destroy(aerospike * as, as_error * err) 
{
	return AEROSPIKE_ERR;
}
