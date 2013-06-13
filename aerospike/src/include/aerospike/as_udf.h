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

#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define AS_UDF_FILE_NAME_LEN 128
#define AS_UDF_FILE_HASH_LEN 20

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Defines a call to a UDF
 */
typedef struct as_udf_call_s {

	/**
	 * If true, then as_udf_call_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * UDF Module containing the function to be called.
	 */
	char * module;

	/**
	 * UDF Function to be called
	 */
	char * function;

	/**
	 * Argument List
	 */
	as_list * arglist;
	
} as_udf_call;

/**
 * Enumeration of UDF types
 */
typedef enum as_udf_type_e {
	AS_UDF_TYPE_LUA
} as_udf_type;

/**
 * UDF File
 */
typedef struct as_udf_file_s {

	/**
	 * If true, then as_udf_file_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Name of the UDF file
	 */
	char name[AS_UDF_FILE_NAME_LEN];

	/** 
	 * Hash value of the file contents
	 */
	uint8_t hash[AS_UDF_FILE_HASH_LEN];

	/**
	 * The type of UDF
	 */
	as_udf_type type;

	/**
	 * UDF File contents
	 */
	struct {

		/**
		 * If true, then as_udf_file_destroy() will free bytes()
		 */
		bool _free;

		/**
		 * Number of bytes allocated to bytes.
		 */
		uint32_t capacity;

		/**
		 * Number of bytes used in bytes.
		 */
		uint32_t size;

		/**
		 * Sequence of bytes
		 */
		uint8_t * bytes;

	} content;
} as_udf_file;

/**
 * List of UDF Files
 */
typedef struct as_udf_list_s {

	/**
	 * If true, then as_udf_list_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Number of file entries allocated to files.
	 */
	uint32_t capacity;

	/**
	 * Number of used file entries in files.
	 */
	uint32_t size;

	/**
	 * Sequence of files.
	 */
	as_udf_file * files;

} as_udf_list;

/******************************************************************************
 * UDF CALL FUNCTIONS
 *****************************************************************************/

/**
 * Initialize a stack allocated as_udf_call.
 */
as_udf_call * as_udf_call_init(as_udf_call * call, const char * module, const char * function, as_list * arglist);

/**
 * Creates a new heap allocated as_udf_call.
 */
as_udf_call * as_udf_call_new(const char * module, const char * function, as_list * arglist);

/**
 * Destroy an as_udf_call.
 */
void as_udf_call_destroy(as_udf_call * call);

/******************************************************************************
 * UDF FILE FUNCTIONS
 *****************************************************************************/

/**
 * Initialize a stack allocated as_udf_file.
 */
as_udf_file * as_udf_file_init(as_udf_file * file);

/**
 * Creates a new heap allocated as_udf_file.
 */
as_udf_file * as_udf_file_new();

/**
 * Destroy an as_udf_file.
 */
void as_udf_file_destroy(as_udf_file * file);

/******************************************************************************
 * UDF LIST FUNCTIONS
 *****************************************************************************/

/**
 * Initialize a stack allocated as_udf_list.
 */
as_udf_list * as_udf_list_init(as_udf_list * list);

/**
 * Creates a new heap allocated as_udf_list.
 */
as_udf_list * as_udf_list_new();

/**
 * Destroy an as_udf_list.
 */
void as_udf_list_destroy(as_udf_list * list);

