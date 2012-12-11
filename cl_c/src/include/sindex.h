/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"

// 
// Query related structures:
#define CL_MAX_SINDEX_NAME_SIZE 128
#define CL_MAX_SETNAME_SIZE     32  
// indicate metadata needed to create a secondary index
typedef struct sindex_metadata_t {
    char    iname[CL_MAX_SINDEX_NAME_SIZE];
    char    binname[CL_BINNAME_SIZE];
    char    type[32];
    uint8_t   isuniq;
    uint8_t   istime;
} sindex_metadata_t;


// Create and Delete Secondary indexes for the entire cluster
cl_rv citrusleaf_secondary_index_create(cl_cluster *asc, const char *ns, 
        const char *set, struct sindex_metadata_t *simd);
cl_rv citrusleaf_secondary_index_delete(cl_cluster *asc, const char *ns, 
        const char *set, const char *indexname);
