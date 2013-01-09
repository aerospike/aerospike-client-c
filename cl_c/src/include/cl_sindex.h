/*
 *      cl_sindex.h
 *
 * Copyright 2008-2013 by Aerospike.
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
#include "cluster.h"

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
