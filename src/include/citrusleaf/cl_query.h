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
#pragma once

#include <citrusleaf/cl_types.h>
#include <citrusleaf/cl_sindex.h>
 
#include <aerospike/as_rec.h>
#include <aerospike/as_map.h>
#include <aerospike/as_list.h>
#include <aerospike/as_result.h>
#include <aerospike/as_stream.h>

/******************************************************************************
 * TYPES
 *******************************************************************************/

typedef enum cl_query_op         { CL_EQ, CL_LT, CL_GT, CL_LE, CL_GE, CL_RANGE } cl_query_op;
typedef enum cl_query_orderby_op { CL_ORDERBY_ASC, CL_ORDERBY_DESC } cl_query_orderby_op;

// These are the types of UDF calls that go over the wire from the client to
// the server.
typedef enum cl_query_udf_type_s { 
    AS_UDF_CALLTYPE_NONE, // Regular UDF call, no query involved.
    AS_UDF_CALLTYPE_RECORD, 
    AS_UDF_CALLTYPE_STREAM
} cl_query_udf_type;


typedef struct cl_query_udf_s {
    cl_query_udf_type   type;
    char *              filename;
    char *              function;
    as_list *           arglist;
} cl_query_udf;

typedef struct cl_query {
    char            * ns;
    char            * indexname;
    char            * setname;
    cf_vector       * binnames;  // Select
    cf_vector       * ranges;    // Where
    cf_vector       * filters;
    cf_vector       * orderbys;
    cl_query_udf    udf;
    void            * res_streamq;
    int             limit;  
    uint64_t        job_id;
} cl_query;

typedef struct cl_query_response_record_t {
    char *      ns;
    cf_digest   keyd;
    char *      set;
    uint32_t    generation;
    uint32_t    record_ttl;
    cl_bin *    bins;
    int         n_bins;
    as_map *    values;  
    bool        ismalloc;
	bool        free_bins;
} cl_query_response_rec;

typedef bool (* cl_query_cb) (as_val * val, void * udata);


/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/
#define cl_integer_equals(val) CL_EQ, CL_INT, val 
#define cl_integer_range(start, end) CL_RANGE, CL_INT, start, end
#define cl_string_equals(val) CL_EQ, CL_STR, val


/**
 * Allocates and initializes a new cl_query
 */
cl_query * cl_query_new(const char * ns, const char * setname);

/**
 * Initializes an cl_query
 */
cl_query * cl_query_init(cl_query * query, const char * ns, const char * setname);

/**
 * Destroy and free an cl_query
 */
void  cl_query_destroy(cl_query * query);

/**
 * Query Builders
 */

int   cl_query_select (cl_query * query, const char * binname);
int   cl_query_where(cl_query * query, const char * binname, cl_query_op, ...);
int   cl_query_where_function(cl_query * query, const char * finame, cl_query_op, ...);
int   cl_query_filter(cl_query * query, const char * binname, cl_query_op op, ...);
int   cl_query_orderby(cl_query * query, const char * binname, cl_query_orderby_op order);
cl_rv cl_query_aggregate(cl_query * query, const char * filename, const char * function, as_list * arglist);
cl_rv cl_query_foreach(cl_query * query, const char * filename, const char * function, as_list * arglist);
int   cl_query_limit(cl_query * query, uint64_t limit);


cl_rv citrusleaf_query_foreach(cl_cluster * cluster, const cl_query * query, void * udata, bool (*foreach)(as_val *, void *));


/*
 * Init and destroy for client query environment. Should be called for once per cluster
 * instance before performing citrusleaf query
 */
int    cl_cluster_query_init(cl_cluster* asc);
void   cl_cluster_query_shutdown(cl_cluster* asc);
