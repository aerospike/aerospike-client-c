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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>

#include <citrusleaf/cl_udf.h>

#include "shim.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static void clfile_to_asfile(cl_udf_file * clfile, as_udf_file * asfile) {
	strncpy(asfile->name, clfile->name, AS_UDF_FILE_NAME_LEN);
	memcpy(asfile->hash, clfile->hash, AS_UDF_FILE_HASH_LEN);
	asfile->type = clfile->type;
	asfile->content.size = clfile->content->len;
	asfile->content.capacity = clfile->content->capacity;
	asfile->content.bytes = clfile->content->value;
}

/**
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred.
 */
as_status aerospike_udf_list(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	as_udf_list * list)
{
	char * 			error = NULL;
	cl_udf_file ** 	files = NULL;
	int 			count = 0;
	
	int rc =  citrusleaf_udf_list(as->cluster, &files, &count, &error);
	
	if ( error != NULL ) {
		as_error_update(err, AEROSPIKE_ERR, error);
		free(error);
		error = NULL;
	}
	else if ( files != NULL ) {
		if ( list->capacity == 0 && list->files == NULL ) {
			list->_free = true;
			list->capacity = count;
			list->size = 0;
			list->files = (as_udf_file *) malloc(sizeof(as_udf_file) * list->capacity);
		}

		uint32_t limit = count < list->capacity ? count : list->capacity;

		for ( int i = 0; i < limit; i++ ) {
			as_udf_file * asfile = &list->files[i];
			cl_udf_file * clfile = files[i];
			clfile_to_asfile(clfile, asfile);

			clfile->content->value = NULL;
			clfile->content->capacity = 0;
			clfile->content->len = 0;
			as_bytes_destroy(clfile->content);
			free(clfile);
			files[i] = NULL;

			list->size++;
		}

		free(files);
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
	char * error = NULL;
	cl_udf_file clfile;

	int rc = citrusleaf_udf_get(as->cluster, filename, &clfile, type, &error);
	
	if ( error != NULL ) {
		as_error_update(err, AEROSPIKE_ERR, error);
		free(error);
		error = NULL;
	}
	else {
		clfile_to_asfile(&clfile, file);

		clfile.content->value = NULL;
		clfile.content->capacity = 0;
		clfile.content->len = 0;
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
	char * error = NULL;

	int rc = citrusleaf_udf_put(as->cluster, filename, content, type, &error);

	if ( error != NULL ) {
		as_error_update(err, AEROSPIKE_ERR, error);
		free(error);
		error = NULL;
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
	char * error = NULL;
	
	int rc = citrusleaf_udf_remove(as->cluster, filename, &error);

	if ( error != NULL ) {
		as_error_update(err, AEROSPIKE_ERR, error);
		free(error);
		error = NULL;
	}
	return as_error_fromrc(err, rc);
}
