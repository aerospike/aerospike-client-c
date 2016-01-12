/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_udf.h>

#include <citrusleaf/alloc.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/******************************************************************************
 * UDF CALL FUNCTIONS
 *****************************************************************************/

/**
 * Initialize default values for given as_udf_call.
 */
static as_udf_call * as_udf_call_defaults(as_udf_call * call, bool free, const as_udf_module_name module, const as_udf_function_name function, as_list * arglist) 
{
	if ( !call ) return call;

	call->_free = free;
	call->arglist = arglist;

	if ( module ) {
		strcpy(call->module, module);
	}
	else {
		call->module[0] = '\0';
	}
	if ( function ) {
		strcpy(call->function, function);
	}
	else {
		call->function[0] = '\0';
	}
	return call;
}

/**
 * Initialize a stack allocated as_udf_call.
 */
as_udf_call * as_udf_call_init(as_udf_call * call, const as_udf_module_name module, const as_udf_function_name function, as_list * arglist)
{
	if ( ( module && strlen(module) > AS_UDF_MODULE_MAX_LEN ) ||
		 ( function && strlen(function) > AS_UDF_FUNCTION_MAX_LEN ) ) {
		return NULL;
	}
	return as_udf_call_defaults(call, false, module, function, arglist);
}

/**
 * Creates a new heap allocated as_udf_call.
 */
as_udf_call * as_udf_call_new(const as_udf_module_name module, const as_udf_function_name function, as_list * arglist)
{
	if ( ( module && strlen(module) > AS_UDF_MODULE_MAX_LEN ) ||
		 ( function && strlen(function) > AS_UDF_FUNCTION_MAX_LEN ) ) {
		return NULL;
	}
	as_udf_call * call = (as_udf_call *) cf_malloc(sizeof(as_udf_call));
	return as_udf_call_defaults(call, true, module, function, arglist);
}

/**
 * Destroy an as_udf_call.
 */
void as_udf_call_destroy(as_udf_call * call)
{
	if ( call ) {

		call->module[0] = '\0';
		call->function[0] = '\0';

		if (call->arglist) {
			as_list_destroy(call->arglist);
			call->arglist = NULL;
		}

		if ( call->_free ) {
			cf_free(call);
		}
	}
}

/******************************************************************************
 * UDF FILE FUNCTIONS
 *****************************************************************************/

/**
 * Initialize default values for given as_udf_call.
 */
static as_udf_file * as_udf_file_defaults(as_udf_file * file, bool free) {
	file->_free = free;
	file->name[0] = '\0';
	memset(file->hash, 0, AS_UDF_FILE_HASH_SIZE);
	file->content._free = false;
	file->content.capacity = 0;
	file->content.size = 0;
	file->content.bytes = 0;
	return file;
}
/**
 * Initialize a stack allocated as_udf_file.
 */
as_udf_file * as_udf_file_init(as_udf_file * file)
{
	if ( !file ) return file;
	return as_udf_file_defaults(file, false);
}

/**
 * Creates a new heap allocated as_udf_file.
 */
as_udf_file * as_udf_file_new()
{
	as_udf_file * file = (as_udf_file *) cf_malloc(sizeof(as_udf_file));
	if ( !file ) return file;
	return as_udf_file_defaults(file, true);
}

/**
 * Destroy an as_udf_file.
 */
void as_udf_file_destroy(as_udf_file * file)
{
	if ( file ) {
		if ( file->content.bytes && file->content._free ) {
			cf_free(file->content.bytes);
		}
		file->content._free = false;
		file->content.capacity = 0;
		file->content.size = 0;
		file->content.bytes = NULL;
		if ( file->_free ) {
			cf_free(file);
			file = NULL;
		}
	}
}

/******************************************************************************
 * UDF LIST FUNCTIONS
 *****************************************************************************/

as_udf_files * as_udf_files_defaults(as_udf_files * files, bool free, uint32_t capacity)
{
	if ( !files ) return files;
	
	files->_free = free;
	files->capacity = capacity;
	files->size = 0;

	if ( capacity > 0 ) {
		files->entries = (as_udf_file *) cf_malloc(sizeof(as_udf_file) * files->capacity);
	}
	else {
		files->entries = NULL;
	}

	return files;
}

/**
 *	Initialize a stack allocated as_udf_files.
 *
 *	@param files
 *	@param capacity The number of entries to allocate.
 *
 *	@returns The initialized udf list on success. Otherwise NULL.
 */
as_udf_files * as_udf_files_init(as_udf_files * files, uint32_t capacity)
{
	if ( !files ) return files;
	return as_udf_files_defaults(files, false, capacity);
}

/**
 *	Create and initialize a new heap allocated `as_udf_files`.
 *	
 *	@param capacity The number of entries to allocate.
 *
 *	@returns The newly allocated udf list on success. Otherwise NULL.
 */
as_udf_files * as_udf_files_new(uint32_t capacity)
{
	as_udf_files * files = (as_udf_files *) cf_malloc(sizeof(as_udf_files));
	if ( !files ) return files;
	return as_udf_files_defaults(files, true, capacity);
}

/**
 * Destroy an as_udf_list.
 */
void as_udf_files_destroy(as_udf_files * files)
{
	if ( files ) {

		// entries is malloced in both init and new
		// so we can directly free it
		cf_free(files->entries);

		// files can be malloced or not.
		// So handle both cases.
		if ( files->_free ) {
			cf_free(files);
			files = NULL;
			return;
		}
		files->_free = false;
		files->capacity = 0;
		files->size = 0;
		files->entries = NULL;
	}
}



