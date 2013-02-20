/*
 *      cl_query.h
 *
 * Copyright Aerospike, 2013
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

#pragma once

#include "cl_types.h"
#include "cl_sindex.h"
#include "as_rec.h"
#include "as_result.h"
#include "as_stream.h"

/******************************************************************************
 * TYPES
 *******************************************************************************/

typedef enum as_query_op         { CL_EQ, CL_LT, CL_GT, CL_LE, CL_GE, CL_RANGE } as_query_op;
typedef enum as_query_orderby_op { CL_ORDERBY_ASC, CL_ORDERBY_DESC } as_query_orderby_op;


typedef enum as_query_udf_type_s { 
    AS_QUERY_UDF_NONE,
    AS_QUERY_UDF_RECORD, 
    AS_QUERY_UDF_STREAM
} as_query_udf_type;


typedef struct as_query_udf_s {
    as_query_udf_type   type;
    char *              filename;
    char *              function;
    as_list *           arglist;
} as_query_udf;

typedef struct as_query {
    char            * ns;
    char            * indexname;
    char            * setname;
    cf_vector       * binnames;  // Select
    cf_vector       * ranges;    // Where
    cf_vector       * filters;
    cf_vector       * orderbys;
    as_query_udf    udf;
    void            * res_streamq;
    // byte          udf_op;
    int             limit;  
    uint64_t        job_id;
} as_query;

typedef struct as_query_response_record_t {
    char        * ns;
    cf_digest   * keyd;
    char        * set;
    uint32_t      generation;
    uint32_t      record_ttl;
    cl_bin      * bins;
    int           n_bins;
} as_query_response_rec;

typedef int (* as_query_cb) (as_query_response_rec *rec, void *udata);


/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/
#define integer_equals(val) CL_EQ, CL_INT, val 
#define integer_range(start, end) CL_RANGE, CL_INT, start, end
#define string_equals(val) CL_EQ, CL_STR, val


/**
 * Allocates and initializes a new as_query
 */
as_query * as_query_new(const char * ns, const char * setname);

/**
 * Initializes an as_query
 */
as_query * as_query_init(as_query * query, const char * ns, const char * setname);

/**
 * Destroy and free an as_query
 */
void  as_query_destroy(as_query * query);

/**
 * Query Builders
 */

int   as_query_select (as_query * query, const char * binname);
int   as_query_where(as_query * query, const char * binname, as_query_op, ...);
int   as_query_where_function(as_query * query, const char * finame, as_query_op, ...);
int   as_query_filter(as_query * query, const char * binname, as_query_op op, ...);
int   as_query_orderby(as_query * query, const char * binname, as_query_orderby_op order);
cl_rv as_query_aggregate(as_query * query, const char * filename, const char * function, as_list * arglist);
int   as_query_limit(as_query * query, uint64_t limit);


// cl_rv as_query_foreach(cl_cluster *asc, const as_query *query_obj, as_query_cb cb, void *udata, as_stream *s);

cl_rv citrusleaf_query_execute(cl_cluster * cluster, const as_query * query, as_stream * stream);


/*
 * Init and destroy for client query environment. Should be called for once per client
 * instance before performing citrusleaf query
 */
int    citrusleaf_query_init();
void   citrusleaf_query_shutdown();
