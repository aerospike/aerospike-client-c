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

/******************************************************************************
 * UDF CALL FUNCTIONS
 *****************************************************************************/

/**
 * Initialize default values for given as_udf_call.
 */
static as_udf_call * as_udf_call_defaults(as_udf_call * call, bool free, const char * module, const char * function, as_list * arglist) {
	call->_free = free;
	call->module = module ? strdup(module) : NULL;
	call->function = function ? strdup(function) : NULL;
	call->arglist = arglist;
	return call;
}

/**
 * Initialize a stack allocated as_udf_call.
 */
as_udf_call * as_udf_call_init(as_udf_call * call, const char * module, const char * function, as_list * arglist)
{
	if ( !call ) return call;
	return as_udf_call_defaults(call, false, module, function, arglist);
}

/**
 * Creates a new heap allocated as_udf_call.
 */
as_udf_call * as_udf_call_new(const char * module, const char * function, as_list * arglist)
{
	as_udf_call * call = (as_udf_call *) malloc(sizeof(as_udf_call));
	if ( !call ) return call;
	return as_udf_call_defaults(call, true, module, function, arglist);
}

/**
 * Destroy an as_udf_call.
 */
void as_udf_call_destroy(as_udf_call * call)
{
	if ( call ) {

		if (call->module) {
			free(call->module);
			call->module = NULL;
		}

		if (call->function) {
			free(call->function);
			call->function = NULL;
		}

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
		for ( int i = 0; i < files->size; i++ ) {
			files->entries[i]._free = false;
			as_udf_file_destroy(&files->entries[i]);
		}
		if ( files->_free ) {
			free(files->entries);
		}
		files->_free = false;
		files->capacity = 0;
		files->size = 0;
		files->entries = NULL;
	}
}



