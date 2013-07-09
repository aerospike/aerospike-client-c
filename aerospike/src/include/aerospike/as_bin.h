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
 *	Maximum bin name size
 */
#define AS_BIN_NAME_MAX_SIZE 15

/**
 *	Maximum bin name length
 */
#define AS_BIN_NAME_MAX_LEN (AS_BIN_NAME_MAX_SIZE - 1)

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Bin Name
 */
typedef char as_bin_name[AS_BIN_NAME_MAX_SIZE];

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
	as_bin_value value;

	/**
	 *	Bin value pointer.
	 *	If NULL, then there is no value.
	 *	It can point to as_bin.value or a different value.
	 */
	as_bin_value * valuep;
	
} as_bin;

/**
 * Sequence of bins.
 */
typedef struct as_bins_s {

	/**
	 *	@private
	 *	If true, then as_record_destroy() will free data
	 */
	bool _free;

	/**
	 *	Number of entries allocated to data.
	 */
	uint16_t capacity;

	/**
	 *	Number of entries currently holding data.
	 */
	uint16_t size;

	/**
	 *	Storage for bins
	 */
	as_bin * entries;

} as_bins;
