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
#include <aerospike/aerospike_query.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_query.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stream.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_query.h>

#include <stdint.h>

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
    as_val *  err_val = NULL;
	
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
	cl_rv rc = citrusleaf_query_foreach(as->cluster, clquery, &bridge, clquery_callback, &err_val);
    as_status ret = as_error_fromrc(err, rc);

    if (AEROSPIKE_OK != rc && err_val) {
        char * err_str = as_val_tostring(err_val);
        if(err_str) {
            strncat(err->message," : ",sizeof(err->message) - strlen(err->message));
            strncat(err->message,err_str,sizeof(err->message) - strlen(err->message));
            cf_free(err_str);
        }
        as_val_destroy(err_val);
    }
	cl_query_destroy(clquery);
	return ret;
}

/**
 * Initialize query environment
 */
as_status aerospike_query_init(aerospike * as, as_error * err) 
{
	// TODO - failure cases?
	cl_cluster_query_init(as->cluster);
	return AEROSPIKE_OK;
}
