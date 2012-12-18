/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"
#include "scan.h"

// Range indicates start/end condition for the columns of the indexes.
// Example1: (index on "last_activity" bin) where last_activity < start_time and last_activity > end_time
// Example2: (index on "last_activity" bin for equality) where last_activity == start_time
// Example3: (compound index on "last_activity","state","age") 
//    where last_activity < start_time and last_activity > end_time
//    and state in ["ca","wa","or"]
//    and age == 28
typedef struct cl_query_range {
    char        bin_name[CL_BINNAME_SIZE];
    cl_object   start_obj;
    cl_object   end_obj;
} cl_query_range;

typedef enum cl_query_filter_op { CL_FLTR_EQ, CL_FLTR_LT, CL_FLTR_GT, CL_FLTR_LE, CL_FLTR_GE, CL_FLTR_NE, CL_FLTR_EXISTS} cl_query_filter_op;

// Filter indicate a series of post look-up condition in an equivalent "where" clause
// applied to bins other than the indexed bins
typedef struct cl_query_filter {
    char        bin_name[CL_BINNAME_SIZE];
    cl_object   compare_obj;
    cl_query_filter_op  ftype;
} cl_query_filter;

typedef enum cl_query_orderby_op { CL_ORDERBY_ASC, CL_ORDERBY_DESC} cl_query_orderby_op;

// Order-by indicate a post look-up result ordering
typedef struct cl_query_orderby {
    char        bin_name[CL_BINNAME_SIZE];
    cl_query_orderby_op ordertype;
} cl_query_orderby;

typedef struct cl_query {
    char        indexname[CL_MAX_SINDEX_NAME_SIZE];
    char        setname[CL_MAX_SETNAME_SIZE];
    cf_vector   *binnames;
    cf_vector   *ranges;
    cf_vector   *filters;
    cf_vector   *orderbys;
    int         limit;  
    uint64_t    job_id;
} cl_query;
 

// Query  
cl_query *citrusleaf_query_create(const char *indexname, const char *setname);
void citrusleaf_query_destroy(cl_query *query_obj);
cl_rv citrusleaf_query_add_binname(cl_query *query_obj, const char *binname);
cl_rv citrusleaf_query_add_range_numeric(cl_query *query_obj, const char *binname,int64_t start,int64_t end);
cl_rv citrusleaf_query_add_range_string(cl_query *query_obj, const char *binname, const char *start, const char *end);
cl_rv citrusleaf_query_add_filter_numeric(cl_query *query_obj, const char *binname, int64_t comparer, cl_query_filter_op op);
cl_rv citrusleaf_query_add_filter_string(cl_query *query_obj, const char *binname, const char *comparer, cl_query_filter_op op);
cl_rv citrusleaf_query_add_orderby(cl_query *query_obj, const char *binname, cl_query_orderby_op order);
cl_rv citrusleaf_query_set_limit(cl_query *query_obj, uint64_t limit);

cl_rv citrusleaf_query(cl_cluster *asc, const char *ns, const cl_query *query_obj,const cl_mr_job *mr_job, citrusleaf_get_many_cb cb, void *udata);
