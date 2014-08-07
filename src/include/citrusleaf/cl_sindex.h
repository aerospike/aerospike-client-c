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

#include <aerospike/as_cluster.h>
#include <citrusleaf/cl_types.h>

#include <aerospike/as_list.h>

/******************************************************************************
 * TYPES
 *******************************************************************************/

typedef struct cl_sindex_t {
	char       * ns;
	char       * set;
	char       * iname;
	char       * binname;
	char       * file;
	char       * func;
	char       * type;
	as_list    * args;
} cl_sindex;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

cl_rv citrusleaf_secondary_index_create(as_cluster *asc, const char *ns, const char *set, const char *iname, const char *binname, const char *type, char **response);

cl_rv citrusleaf_secondary_index_drop(as_cluster *asc, const char *ns, const char *iname, char **response);
