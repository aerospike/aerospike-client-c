/*
 *      cl_udf.h
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

#include "cluster.h"
#include "as_result.h"
#include "cl_arglist.h"

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

cl_rv citrusleaf_udf_record_apply(cl_cluster *, const char *, const char *, const cl_object *, const char *, const char *, as_list *, int, as_result *);

cl_rv citrusleaf_udf_list(cl_cluster *, char ***, int *, char **);
cl_rv citrusleaf_udf_get(cl_cluster *, const char *, char **, int *, char **);
cl_rv citrusleaf_udf_get_with_gen(cl_cluster *, const char *, char **, int *, char **, char **) ;
cl_rv citrusleaf_udf_put(cl_cluster *, const char *, const char *, char **);
cl_rv citrusleaf_udf_remove(cl_cluster *, const char *, char **);
