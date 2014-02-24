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

#ifdef __cplusplus
extern "C" {
#endif


#include <openssl/sha.h>

#include <citrusleaf/cl_cluster.h>
#include <citrusleaf/cf_crypto.h>

#include <aerospike/as_result.h>
#include <aerospike/as_types.h>

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

#define AS_UDF_LUA 0

/******************************************************************************
 * DATA TYPES
 ******************************************************************************/

typedef uint8_t cl_udf_type;

typedef struct cl_udf_file_s {
  char			name[128];
  uint8_t		hash[CF_SHA_HEX_BUFF_LEN];
  cl_udf_type	type;
  as_bytes *	content;
} cl_udf_file;

typedef struct cl_udf_info_s {
    char *		error;
    char		filename[128];
    as_bytes	content;
    char *		gen;
    uint8_t		hash[CF_SHA_HEX_BUFF_LEN];
} cl_udf_info;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * Apply a UDF to a record.
 * @param result will be updated with the result of the call. The as_result.value member should be freed by the user.
 */
cl_rv citrusleaf_udf_record_apply(cl_cluster * cluster, const char * ns, const char * set, 
	const cl_object * key, const char * filename, const char * function, as_list * arglist, 
	int timeout, as_result * result);

/**
 * @param files - An array of filenames. Each entry is string. The array and each entry must be freed by the user.
 * @param count - Number of entries.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_list(cl_cluster * cluster, cl_udf_file ** files, int * count, char ** error);

/**
 * @param filename - The name of the file to download from the cluster.
 * @param contents - The contents of the file. The contents must be freed by the user.
 * @param size - The size of the contents of the file.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_get(cl_cluster * cluster, const char * filename, cl_udf_file * file, 
	cl_udf_type udf_type, char ** error);

/**
 * @param filename - The name of the file to download from the cluster.
 * @param contents - The contents of the file. The value must be freed by the user.
 * @param size - The size of the contents of the file.
 * @param gen - The generation value of the file. The value must be freed by the user.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_get_with_gen(cl_cluster * cluster, const char * filename, cl_udf_file * file, 
	cl_udf_type udf_type, char ** gen, char ** error) ;

/**
 * @param filename - The name of the file being uploaded to the cluster.
 * @param contents - The contents of the file being uploaded to the cluster.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_put(cl_cluster * cluster, const char * filename, as_bytes *content, 
	cl_udf_type udf_type, char ** error);

/**
 * @param filename - The file to be removed from the cluster.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_remove(cl_cluster * cluster, const char * filename, char ** error);

/******************************************************************************
 * UTILITY FUNCTIONS
 ******************************************************************************/

void cl_udf_info_destroy(cl_udf_info * info);

#ifdef __cplusplus
} // end extern "C"
#endif

