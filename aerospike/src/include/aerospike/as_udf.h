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

#define CF_SHA_HEX_BUFF_LEN 20


/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Defines a call to a UDF
 */
struct as_udf_call_s {

	/**
	 * Object can be free()'d
	 */
	bool __free;

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
};

typedef struct as_udf_call_s as_udf_call;


typedef uint8_t as_udf_type;

struct as_udf_file_s {
	char 			name[128];
	unsigned char 	hash[CF_SHA_HEX_BUFF_LEN];
	as_udf_type 	type;
	as_bytes * 		content;
};

typedef struct as_udf_file_s as_udf_file;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_udf_call * as_udf_call_init(as_udf_call * call, const char * module, const char * function, as_list * arglist);

as_udf_call * as_udf_call_new(const char * module, const char * function, as_list * arglist);

void as_udf_call_destroy(as_udf_call * call);