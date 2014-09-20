/*
 * Copyright 2008-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif


#include <openssl/sha.h>

#include <aerospike/as_cluster.h>
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
cl_rv citrusleaf_udf_record_apply(as_cluster * cluster, const char * ns, const char * set, 
	const cl_object * key, const char * filename, const char * function, as_list * arglist, 
	int timeout, as_result * result);

/**
 * @param files - An array of filenames. Each entry is string. The array and each entry must be freed by the user.
 * @param count - Number of entries.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_list(as_cluster * cluster, cl_udf_file ** files, int * count, char ** error);

/**
 * @param filename - The name of the file to download from the cluster.
 * @param contents - The contents of the file. The contents must be freed by the user.
 * @param size - The size of the contents of the file.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_get(as_cluster * cluster, const char * filename, cl_udf_file * file, 
	cl_udf_type udf_type, char ** error);

/**
 * @param filename - The name of the file to download from the cluster.
 * @param contents - The contents of the file. The value must be freed by the user.
 * @param size - The size of the contents of the file.
 * @param gen - The generation value of the file. The value must be freed by the user.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_get_with_gen(as_cluster * cluster, const char * filename, cl_udf_file * file, 
	cl_udf_type udf_type, char ** gen, char ** error) ;

/**
 * @param filename - The name of the file being uploaded to the cluster.
 * @param contents - The contents of the file being uploaded to the cluster.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_put(as_cluster * cluster, const char * filename, as_bytes *content, 
	cl_udf_type udf_type, char ** error);

/**
 * @param filename - The file to be removed from the cluster.
 * @param error - Contains an error message, if the return value was non-zero. The value must be freed by the user.
 */
cl_rv citrusleaf_udf_remove(as_cluster * cluster, const char * filename, char ** error);

/******************************************************************************
 * UTILITY FUNCTIONS
 ******************************************************************************/

void cl_udf_info_destroy(cl_udf_info * info);

#ifdef __cplusplus
} // end extern "C"
#endif

