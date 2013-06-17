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

#include <aerospike/as_bin.h>
#include <aerospike/as_buffer.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Operation Identifiers
 */
typedef enum as_binop_op_e { 

	/**
	 *	Return the bin from the cluster.
	 */
	AS_BINOP_READ       = 1,

	/**
	 *	Update the bin.
	 */
	AS_BINOP_WRITE      = 2, 

	/**
	 *	Increment a bin containing an
	 *	integer value.
	 */
	AS_BINOP_INCR       = 5, 

	/**
	 *	Append bytes to the bin containing
	 *	either a string or blob.
	 */
	AS_BINOP_APPEND     = 9, 

	/**
	 *	Prepend bytes to the bin containing
	 *	either a string or blob.
	 */
	AS_BINOP_PREPEND    = 10, 

	/**
	 *	Touch the record's ttl.
	 */
	AS_BINOP_TOUCH      = 11
	
} as_binop_op;

/**
 *	Operation on a bin.
 *	The value for the bin will be applied according to the operation.
 */
typedef struct as_binop_s {
	
	/**
	 *	Operation to be performed on a bin.
	 */
	as_binop_op operation;

	/**
	 *	Bin to perform operation on.
	 */
	as_bin bin;
	
} as_binop;
