/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "cluster.h"
#include "as_result.h"
#include "arglist.h"

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

cl_rv citrusleaf_udf_record_apply(cl_cluster *, const char *, const char *, const cl_object *, const char *, const char *, as_list *, int, as_result *);

cl_rv citrusleaf_udf_list(cl_cluster *, char ***, int *, char **);
cl_rv citrusleaf_udf_get(cl_cluster *, const char *, char **, int *, char **);
cl_rv citrusleaf_udf_get_with_gen(cl_cluster *, const char *, char **, int *, char **, char **) ;
cl_rv citrusleaf_udf_put(cl_cluster *, const char *, const char *, char **);
cl_rv citrusleaf_udf_remove(cl_cluster *, const char *, char **);