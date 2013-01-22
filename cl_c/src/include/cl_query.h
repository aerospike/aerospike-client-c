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

/******************************************************************************
 * TYPES
 *******************************************************************************/

typedef enum cl_query_op         { CL_EQ, CL_LT, CL_GT, CL_LE, CL_GE, CL_RANGE } cl_query_op;
typedef enum cl_query_orderby_op { CL_ORDERBY_ASC, CL_ORDERBY_DESC } cl_query_orderby_op;

typedef struct cl_query {
    char        * ns;
    char        * indexname;
    char        * setname;
    cf_vector   * binnames;  // Select
    cf_vector   * ranges;    // Where
    cf_vector   * filters;
    cf_vector   * orderbys;
    int           limit;  
    uint64_t      job_id;
} cl_query;

typedef int (* citrusleaf_query_cb) (char *ns, cf_digest *keyd, char *set, uint32_t generation, uint32_t record_ttl, cl_bin *bins, int num_bins, bool is_last, void *udata);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/
int   cl_query_init(cl_query **query_obj, const char *ns, const char *setname);
void  cl_query_destroy(cl_query *query_obj);
int   cl_query_select (cl_query *query_obj, const char *binname);
int   cl_query_where(cl_query *query_obj, const char *binname, cl_query_op, ...);
int   cl_query_where_function(cl_query *query_obj, const char *finame, cl_query_op, ...);
int   cl_query_filter(cl_query *query_obj, const char *binname, cl_query_op op, ...);
int   cl_query_orderby(cl_query *query_obj, const char *binname, cl_query_orderby_op order);
int   cl_query_limit(cl_query *query_obj, uint64_t limit);

/*
 * Init and destroy for client query environment. Should be called for once per client
 * instance before performing citrusleaf query
 */
int    citrusleaf_query_init();
void   citrusleaf_query_shutdown();
cl_rv  citrusleaf_query(cl_cluster *asc, const cl_query *query_obj, citrusleaf_query_cb cb, void *udata);
