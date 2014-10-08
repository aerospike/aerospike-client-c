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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

#include <citrusleaf/cl_udf.h>

#include "_policy.h"
#include "_shim.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static void clfile_to_asfile(cl_udf_file * clfile, as_udf_file * asfile) {

	strncpy(asfile->name, clfile->name, AS_UDF_FILE_NAME_LEN);
	asfile->name[AS_UDF_FILE_NAME_LEN] = '\0';

	memcpy(asfile->hash, clfile->hash, AS_UDF_FILE_HASH_SIZE);
	asfile->type = clfile->type;
	if ( clfile->content ) {
		asfile->content._free = clfile->content->free;
		asfile->content.size = clfile->content->size;
		asfile->content.capacity = clfile->content->capacity;
		asfile->content.bytes = clfile->content->value;
	}
	else {
		asfile->content._free = false;
		asfile->content.size = 0;
		asfile->content.capacity = 0;
		asfile->content.bytes = NULL;
	}
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_list(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	as_udf_files * files)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	// as_policy_info p;
	// as_policy_info_resolve(&p, &as->config.policies, policy);

	char * 			error = NULL;
	cl_udf_file * 	clfiles = NULL;
	int 			count = 0;
	
	cl_rv rc = citrusleaf_udf_list(as->cluster, &clfiles, &count, &error);

	if (rc) {
		as_strncpy(err->message, error, sizeof(err->message));
		free(error);
	}
	else if ( clfiles != NULL ) {
	
		if ( files->capacity == 0 && files->entries == NULL ) {
			as_udf_files_init(files, count);
		}

		uint32_t limit = count < files->capacity ? count : files->capacity;
	
		for ( int i = 0; i < limit; i++ ) {

			as_udf_file * asfile = &files->entries[i];
			cl_udf_file * clfile = &clfiles[i];
			clfile_to_asfile(clfile, asfile);
			files->size++;
		}

		free(clfiles);
	}
	
	return as_error_fromrc(err, rc);
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_get(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename, as_udf_type type, as_udf_file * file)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	// as_policy_info p;
	// as_policy_info_resolve(&p, &as->config.policies, policy);
	
	char * error = NULL;
	cl_udf_file clfile;
    memset(&clfile,0,sizeof(cl_udf_file));

	cl_rv rc = citrusleaf_udf_get(as->cluster, filename, &clfile, type, &error);
	
	if (rc) {
		as_strncpy(err->message, error, sizeof(err->message));
		free(error);
	}
	else {
		clfile_to_asfile(&clfile, file);

		clfile.content->value = NULL;
		clfile.content->capacity = 0;
		clfile.content->size = 0;
		as_bytes_destroy(clfile.content);
		clfile.content = NULL;
	}
	return as_error_fromrc(err, rc);
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_put(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename, as_udf_type type, as_bytes * content)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);

	// resolve policies
	// as_policy_info p;
	// as_policy_info_resolve(&p, &as->config.policies, policy);
	
	char * error = NULL;

	int rc = citrusleaf_udf_put(as->cluster, filename, content, type, &error);

	if (rc) {
		as_strncpy(err->message, error, sizeof(err->message));
		free(error);
	}

	return as_error_fromrc(err, rc);
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_remove(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	// as_policy_info p;
	// as_policy_info_resolve(&p, &as->config.policies, policy);
	
	char * error = NULL;
	
	cl_rv rc = citrusleaf_udf_remove(as->cluster, filename, &error);

	if (rc) {
		as_strncpy(err->message, error, sizeof(err->message));
		free(error);
	}

	return as_error_fromrc(err, rc);
}
