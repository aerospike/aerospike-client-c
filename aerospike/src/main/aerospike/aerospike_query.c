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

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stream.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/cl_query.h>

#include <stdint.h>

#include "_log.h"
#include "_policy.h"
#include "_shim.h"
 
/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct clquery_bridge_s {
	void * udata;
	aerospike_query_foreach_callback callback;
} clquery_bridge;

/******************************************************************************
 * FUNCTION DECLS
 *****************************************************************************/

as_status aerospike_query_init(aerospike * as, as_error * err);
as_status aerospike_query_destroy(aerospike * as, as_error * err);

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static cl_query * as_query_toclquery(const as_query * query)
{
	cl_query * clquery = cl_query_new(query->ns, query->set);

	for ( int i = 0; i < query->select.size; i++ ) {
		as_bin_name * bin = &query->select.entries[i];
		char * b = *bin;
		cl_query_select(clquery,b);
	}

	for ( int i = 0; i < query->where.size; i++ ) {
		as_predicate * p = &query->where.entries[i];
		switch(p->type) {
			case AS_PREDICATE_STRING_EQUAL:
				cl_query_where(clquery, p->bin, CL_EQ, CL_STR, p->value.string);
				break;
			case AS_PREDICATE_INTEGER_EQUAL:
				cl_query_where(clquery, p->bin, CL_EQ, CL_INT, p->value.integer);
				break;
			case AS_PREDICATE_INTEGER_RANGE:
				cl_query_where(clquery, p->bin, CL_RANGE, CL_INT, p->value.integer_range.min, p->value.integer_range.max);
				break;
		}
	}

	for ( int i = 0; i < query->orderby.size; i++ ) {
		as_ordering * o = &query->orderby.entries[i];
		cl_query_orderby(clquery, o->bin, o->order == AS_ORDER_ASCENDING ? CL_ORDERBY_ASC : CL_ORDERBY_DESC);
	}

	if ( query->apply.module[0] != '\0' && query->apply.function[0] != '\0' ) {
		cl_query_aggregate(clquery, query->apply.module, query->apply.function, query->apply.arglist);
	}

	return clquery;
}

static bool clquery_callback(as_val * val, void * udata)
{
	clquery_bridge * bridge = (clquery_bridge *) udata;
	if ( bridge->callback(val, bridge->udata) == true ) {
		return true;
	}
	else {
		return false;
	}
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Execute a query and call the callback function for each result item.
 *
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param query     - the query to execute against the cluster
 * @param udata     - user-data to be passed to the callback
 * @param callback  - the callback function to call for each result item.
 *
 * @return AEROSPIKE_OK on success, otherwise an error.
 */
as_status aerospike_query_foreach(
	aerospike * as, as_error * err, const as_policy_query * policy, 
	const as_query * query, 
	aerospike_query_foreach_callback callback, void * udata) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	// as_policy_query p;
	// as_policy_query_resolve(&p, &as->config.policies, policy);
	
	if ( aerospike_query_init(as, err) != AEROSPIKE_OK ) {
		return err->code;
	}

	cl_query * clquery = as_query_toclquery(query);

	clquery_bridge bridge = {
		.udata = udata,
		.callback = callback
	};

	cl_rv rc = citrusleaf_query_foreach(as->cluster, clquery, &bridge, clquery_callback);

	cl_query_destroy(clquery);

	return as_error_fromrc(err, rc);
}

/**
 * Initialize query environment
 */
as_status aerospike_query_init(aerospike * as, as_error * err) 
{
	extern cf_atomic32 query_initialized;
	if ( query_initialized > 0 ) {
		return AEROSPIKE_OK;
	}
	citrusleaf_query_init();
	return AEROSPIKE_OK;
}

/**
 * Destroy query environment
 */
as_status aerospike_query_destroy(aerospike * as, as_error * err) 
{
	extern cf_atomic32 query_initialized;
	if ( query_initialized == 0 ) {
		return AEROSPIKE_OK;
	}
	citrusleaf_query_shutdown();
	return AEROSPIKE_OK;
}
