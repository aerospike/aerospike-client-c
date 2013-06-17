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

#pragma once 

#include <aerospike/as_buffer.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_val.h>

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	The length for the bin name.
 */
#define AS_BIN_NAME_LEN 16

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Bin Types
 */
typedef enum as_type_e { 
	AS_TYPE_NULL            = 0,
	AS_TYPE_INT             = 1,
	AS_TYPE_FLOAT           = 2,
	AS_TYPE_STR             = 3,
	AS_TYPE_BLOB            = 4,
	AS_TYPE_TIMESTAMP       = 5,
	AS_TYPE_DIGEST          = 6,
	AS_TYPE_JAVA_BLOB       = 7,
	AS_TYPE_CSHARP_BLOB     = 8,
	AS_TYPE_PYTHON_BLOB     = 9,
	AS_TYPE_RUBY_BLOB       = 10,
	AS_TYPE_PHP_BLOB        = 11,
	AS_TYPE_ERLANG_BLOB     = 12,
	AS_TYPE_APPEND          = 13,
	AS_TYPE_RTA_LIST        = 14,	//!< @deprecated
	AS_TYPE_RTA_DICT        = 15,	//!< @deprecated
	AS_TYPE_RTA_APPEND_DICT = 16,	//!< @deprecated
	AS_TYPE_RTA_APPEND_LIST = 17,	//!< @deprecated
	AS_TYPE_LUA_BLOB        = 18,
	AS_TYPE_MAP             = 19,
	AS_TYPE_LIST            = 20,
	AS_TYPE_UNKNOWN         = 666666
} as_type;

/**
 *	Bin Name
 */
typedef char as_bin_name[AS_BIN_NAME_LEN];

/**
 *	Bin Value
 */
typedef union as_bin_value_s {
	as_val 		nil;
	as_integer 	integer;
	as_string 	string;
	as_bytes 	bytes;
	as_list 	list;
	as_map 		map;
} as_bin_value;

/**
 *	Bin Structure
 */
typedef struct as_bin_s {

	/**
	 *	Bin name.
	 */
	as_bin_name name;

	/**
	 *	Bin value.
	 */
	as_bin_value * value;
	
} as_bin;
