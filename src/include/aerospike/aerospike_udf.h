/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

/** 
 *	@defgroup udf_operations UDF Operations (3.0 only)
 *	@ingroup client_operations
 *
 *	The UDF API provides the ability to manage UDFs in the cluster.
 *
 *	Management capabilities include:
 *	- aerospike_udf_list() - 	List the UDF modules in the cluster.
 *	- aerospike_udf_get() -		Download a UDF module.
 *	- aerospike_udf_put() -		Upload a UDF module.
 *	- aerospike_udf_remove() -	Remove a UDF module.
 *
 */

#pragma once 

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_udf.h>

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	List the UDF files in the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	as_udf_files files;
 *	as_udf_files_init(&files, 0);
 *	
 *	if ( aerospike_udf_list(&as, &err, NULL, &files) != AEROSPIKE_OK ) {
 *		fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *		printf("files[%d]:\n", files.size);
 *		for( int i = 0; i < files.size; i++ ) {
 *			as_udf_file * file = &files.entries[i];
 *			printf("  - %s (%d) [%s]\n", file->name, file->type, file->hash);
 *		}
 *	}
 *	
 *	as_udf_files_destroy(&files);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param files 		The list to populate with the results from the request.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error occurred.
 *
 *	@ingroup udf_operations
 */
as_status aerospike_udf_list(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	as_udf_files * files
	);


/**
 *	Get specified UDF file from the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	as_udf_file file;
 *	as_udf_file_init(&file);
 *	
 *	if ( aerospike_udf_get(&as, &err, NULL, "my.lua", AS_UDF_TYPE_LUA, &file) != AEROSPIKE_OK ) {
 *	    fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	else {
 *	    printf("%s type=%d hash=%s size=%d:\n", file.name, file.type. file.hash, file.content.size);
 *	    if ( file.type == AS_UDF_TYPE_UDF ) {
 *	        printf("%s", file.content.bytes)
 *	    }
 *	}
 *	
 *	as_udf_file_destroy(&file);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param filename		The name of the UDF file.
 *	@param type			The type of UDF file.
 *	@param file			The file from the cluster.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error occurred.
 *
 *	@ingroup udf_operations
 */
as_status aerospike_udf_get(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename, as_udf_type type, as_udf_file * file
	);

/**
 *	Put a UDF file into the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	as_bytes content;
 *	as_bytes_init(&content);
 *	...
 *	
 *	if ( aerospike_udf_put(&as, &err, NULL, "my.lua", AS_UDF_TYPE_LUA, &content) != AEROSPIKE_OK ) {
 *	    fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	
 *	as_bytes_destroy(&content);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param filename		The name of the UDF file.
 *	@param type			The type of UDF file.
 *	@param content		The file of the UDF file.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error occurred.
 *
 *	@ingroup udf_operations
 */
as_status aerospike_udf_put(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename, as_udf_type type, as_bytes * content
	);

/**
 *	Remove a UDF file from the cluster.
 *
 *	~~~~~~~~~~{.c}
 *	if ( aerospike_udf_remove(&as, &err, NULL, "my.lua") != AEROSPIKE_OK ) {
 *	    fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *	}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param filename 		The name of the UDF file.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error occurred.
 *
 *	@ingroup udf_operations
 */
as_status aerospike_udf_remove(
	aerospike * as, as_error * err, const as_policy_info * policy, 
	const char * filename
	);
