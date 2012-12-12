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

cl_rv citrusleaf_udf_record_apply(cl_cluster * cl, const char * ns, const char * set, const cl_object * key, const char * filename, const char * function, as_list * arglist, int timeout_ms, as_result * r);
