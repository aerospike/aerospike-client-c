/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"

//
// cl_object calls
// 

// fill out the object structure with the string in question - no allocs
void citrusleaf_object_init(cl_object *o);
void citrusleaf_object_init_str(cl_object *o, char const *str);
void citrusleaf_object_init_str2(cl_object *o, char const *str, size_t str_len);
void citrusleaf_object_init_blob(cl_object *o, void const *buf, size_t buf_len);
void citrusleaf_object_init_blob2(cl_object *o, void const *buf, size_t buf_len, cl_type type); // several blob types
void citrusleaf_object_init_int(cl_object *o, int64_t i);
void citrusleaf_object_init_null(cl_object *o);
void citrusleaf_object_free(cl_object *o);

// frees all the memory in a bin array that would be returned from get_all but not the bin array itself
void citrusleaf_bins_free(cl_bin *bins, int n_bins);

int citrusleaf_copy_bins(cl_bin **destbins, cl_bin *srcbins, int n_bins);
