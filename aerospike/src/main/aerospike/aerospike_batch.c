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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
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
 *	Look up multiple records by key, then return all bins.
 */
as_status aerospike_batch_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_batch * batch, 
	aerospike_batch_read_callback callback, void * udata
	)
{
	return AEROSPIKE_OK;
}

/**
 *	Look up multiple records by key, then return selected bins.
 */
as_status aerospike_batch_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_batch * batch, const char * bins[], 
	aerospike_batch_read_callback callback, void * udata
	)
{
	return AEROSPIKE_OK;
}

/**
 *	Test whether multiple records exist in the cluster.
 */
as_status aerospike_batch_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_batch * batch, 
	aerospike_batch_exists_callback callback, void * udata
	)
{
	return AEROSPIKE_OK;
}
