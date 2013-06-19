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
	 *	Update the bin.
	 */
	AS_OPERATOR_WRITE      = 0, 

	/**
	 *	Return the bin from the cluster.
	 */
	AS_OPERATOR_READ       = 1,

	/**
	 *	Increment a bin containing an
	 *	integer value.
	 */
	AS_OPERATOR_INCR       = 3, 

	/**
	 *	Prepend bytes to the bin containing
	 *	either a string or blob.
	 */
	AS_OPERATOR_PREPEND    = 4, 

	/**
	 *	Append bytes to the bin containing
	 *	either a string or blob.
	 */
	AS_OPERATOR_APPEND     = 5, 

	/**
	 *	Touch the record's ttl.
	 */
	AS_OPERATOR_TOUCH      = 8
	
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
 *	...
 *	as_binops_destroy(&binops);
 */
typedef struct as_binops_s {

	/**
	 *	@private
	 *	If true, then as_binops_destroy() will free the entries.
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


typedef struct as_operations_s {

	/**
	 *	@private
	 *	If true, then as_operations_destroy() will free this instance.
	 */
	bool _free;

	/**
	 * Operations to be performed on the bins of a record.
	 */
	as_binops binops;

} as_operations;

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Initializes a stack allocated `as_operations` (as_operations) and allocates 
 *	`__nops` number of entries on the stack.
 *
 *	~~~~~~~~~~{.c}
 *		as_operations ops;
 * 		as_operations_inita(&ops, 2);
 *		as_operations_append_int64(&ops, AS_OPERATOR_INCR, "bin1", 123);
 *		as_operations_append_str(&ops, AS_OPERATOR_APPEND, "bin2", "abc");
 *	~~~~~~~~~~
 *
 *	@param __ops		The `as_operations *` to initialize.
 *	@param __nops		The number of `as_binops.entries` to allocate on the 
 *						stack.
 */
#define as_operations_inita(__ops, __nops) \
	(__ops)->_free = false;\
	(__ops)->binops._free = false;\
	(__ops)->binops.capacity = __nops;\
	(__ops)->binops.size = 0;\
	(__ops)->binops.entries = (as_binop *) alloca(sizeof(as_binop) * __nops);

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Intializes a stack allocated `as_operations`. 
 *
 *	~~~~~~~~~~{.c}
 *		as_operations ops;
 * 		as_operations_init(&ops, 2);
 *		as_operations_append_int64(&ops, AS_OPERATOR_INCR, "bin1", 123);
 *		as_operations_append_str(&ops, AS_OPERATOR_APPEND, "bin2", "abc");
 *	~~~~~~~~~~
 *
 *	Use `as_operations_destroy()` to free the resources allocated to the
 *	`as_operations`.
 *
 *	@param ops 		The `as_operations` to initialize.
 *	@param nops		The number of `as_operations.binops.entries` to allocate on the heap.
 *
 *	@return The initialized `as_operations` on success. Otherwise NULL.
 */
as_operations * as_operations_init(as_operations * ops, uint16_t nops);

/**
 *	Creates and initializes a heap allocated `as_operations`.
 *
 *	~~~~~~~~~~{.c}
 *		as_operations ops;
 * 		as_operations_init(&ops, 2);
 *		as_operations_append_int64(&ops, AS_OPERATOR_INCR, "bin1", 123);
 *		as_operations_append_str(&ops, AS_OPERATOR_APPEND, "bin2", "abc");
 *	~~~~~~~~~~
 *
 *	Use `as_operations_destroy()` to free the resources allocated to the
 *	`as_operations`.
 *
 *	@param ops 		The `as_operations` to initialize.
 *	@param nops		The number of `as_operations.binops.entries` to allocate on the heap.
 *
 *	@return The new `as_operations` on success. Otherwise NULL.
 */
as_operations * as_operations_new(uint16_t nops);

/**
 *	Releases the `as_operations` and associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 		as_operations_destroy(binops);
 *	~~~~~~~~~~
 *
 *	@param bins 	The `as_binops` to destroy.
 */
void as_operations_destroy(as_operations * ops);

/**
 *	Append a bin to the sequence of bins.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param operator 	The operator to be used on the bin.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_append(as_operations * ops, as_operator operator, const as_bin_name name, as_bin_value * value);

/**
 *	Append a as_binop with an int64_t value to the as_binops.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param operator 	The operator to be used on the bin.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_append_int64(as_operations * ops, as_operator operator, const as_bin_name name, int64_t value);

/**
 *	Append a as_binop with a NULL-terminated string value to the as_binops.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param operator 	The operator to be used on the bin.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_append_str(as_operations * ops, as_operator operator, const as_bin_name name, const char * value);

/**
 *	Append a as_binop with raw bytes value to the as_binops.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param operator 	The operator to be used on the bin.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The bytes to be used in the operation.
 *	@param size 		The number of bytes in value.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_append_raw(as_operations * ops, as_operator operator, const as_bin_name name, uint8_t * value, uint32_t size);
