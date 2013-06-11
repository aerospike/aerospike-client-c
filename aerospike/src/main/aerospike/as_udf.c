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
 * Initialize a stack allocated as_udf_call.
 */
as_udf_call * as_udf_call_init(as_udf_call * call, const char * module, const char * function, as_list * arglist)
{
	call->_free = false;
	call->module = module;
	call->function = function;
	call->arglist = arglist;
	return call;
}

/**
 * Creates a new heap allocated as_udf_call.
 */
as_udf_call * as_udf_call_new(const char * module, const char * function, as_list * arglist)
{
	as_udf_call * call = (as_udf_call *) malloc(sizeof(as_udf_call));
	call->_free = true;
	call->module = module;
	call->function = function;
	call->arglist = arglist;
	return call;
}

/**
 * Destroy an as_udf_call.
 */
void as_udf_call_destroy(as_udf_call * call)
{
	if ( call ) {
		if ( call->_free ) {
			free(call);
		}
	}
}

/******************************************************************************
 * UDF FILE FUNCTIONS
 *****************************************************************************/

/**
 * Initialize a stack allocated as_udf_file.
 */
as_udf_file * as_udf_file_init(as_udf_file * file)
{
	file->_free = false;
	memset(file->name, 0, AS_UDF_FILE_NAME_LEN);
	memset(file->hash, 0, AS_UDF_FILE_HASH_LEN);
	file->content._free = false;
	file->content.capacity = 0;
	file->content.size = 0;
	file->content.bytes = 0;
	return file;
}

/**
 * Creates a new heap allocated as_udf_file.
 */
as_udf_file * as_udf_file_new()
{
	as_udf_file * file = (as_udf_file *) malloc(sizeof(as_udf_file));
	file->_free = true;
	memset(file->name, 0, AS_UDF_FILE_NAME_LEN);
	memset(file->hash, 0, AS_UDF_FILE_HASH_LEN);
	file->content._free = false;
	file->content.capacity = 0;
	file->content.size = 0;
	file->content.bytes = 0;
	return file;
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


/**
 * Initialize a stack allocated as_udf_list.
 * If size > 0, then malloc the files.entries to the size, also setting 
 * files.capacity=size.
 */
as_udf_list * as_udf_list_init(as_udf_list * list)
{
	list->_free = false;
	list->capacity = 0;
	list->size = 0;
	list->files = NULL;
	return list;
}

/**
 * Destroy an as_udf_list.
 */
void as_udf_list_destroy(as_udf_list * list)
{
	if ( list ) {
		if ( list->_free ) {
			free(list->files);
		}
		list->_free = false;
		list->capacity = 0;
		list->size = 0;
		list->files = NULL;
	}
}



