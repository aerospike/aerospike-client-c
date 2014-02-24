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

#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_udf.h>

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
	as_udf_call * call = (as_udf_call *) malloc(sizeof(as_udf_call));
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
			free(call);
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
	as_udf_file * file = (as_udf_file *) malloc(sizeof(as_udf_file));
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
			free(file->content.bytes);
		}
		file->content._free = false;
		file->content.capacity = 0;
		file->content.size = 0;
		file->content.bytes = NULL;
		if ( file->_free ) {
			free(file);
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
		files->entries = (as_udf_file *) malloc(sizeof(as_udf_file) * files->capacity);
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
	as_udf_files * files = (as_udf_files *) malloc(sizeof(as_udf_files));
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
		free(files->entries);

		// files can be malloced or not.
		// So handle both cases.
		if ( files->_free ) {
			free(files);
			files = NULL;
			return;
		}
		files->_free = false;
		files->capacity = 0;
		files->size = 0;
		files->entries = NULL;
	}
}



