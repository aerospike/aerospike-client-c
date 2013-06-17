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
typedef enum as_operator_e { 

	/**
	 *	Return the bin from the cluster.
	 */
	AS_OPERATOR_READ       = 1,

	/**
	 *	Update the bin.
	 */
	AS_OPERATOR_WRITE      = 2, 

	/**
	 *	Increment a bin containing an
	 *	integer value.
	 */
	AS_OPERATOR_INCR       = 5, 

	/**
	 *	Append bytes to the bin containing
	 *	either a string or blob.
	 */
	AS_OPERATOR_APPEND     = 9, 

	/**
	 *	Prepend bytes to the bin containing
	 *	either a string or blob.
	 */
	AS_OPERATOR_PREPEND    = 10, 

	/**
	 *	Touch the record's ttl.
	 */
	AS_OPERATOR_TOUCH      = 11
	
} as_operator;

/**
 *	Operation on a bin.
 *	The value for the bin will be applied according to the operation.
 */
typedef struct as_binop_s {
	
	/**
	 *	The operation to be performed on the bin.
	 */
	as_operator operator;

	/**
	 *	The bin the operation will be performed on.
	 */
	as_bin bin;
	
} as_binop;

/**
 *	Sequence of as_binop.
 *
 *	as_binops binops;
 *	as_binops_inita(&binops, 2);
 *	as_binops_append(&binops, "bin1", integer_incr(123));
 *	as_binops_append(&binops, "bin2", string_append(123));
 */
typedef struct as_binops_s {

	/**
	 *	@private
	 *	If true, then as_ops_destroy() will free this instance.
	 */
	bool _free;

	/**
	 *	Number of entries allocated
	 */
	uint16_t capacity;

	/**
	 *	Number of entries used
	 */
	uint16_t size;

	/**
	 *	Sequence of entries
	 */
	as_binop * entries;

} as_binops;


