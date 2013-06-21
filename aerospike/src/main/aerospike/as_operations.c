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

#include <aerospike/as_bin.h>
#include <aerospike/as_operations.h>

#include <stdbool.h>
#include <stdint.h>

#include "_bin.h"

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

static as_operations * as_operations_default(as_operations * ops, bool free, uint16_t nops)
{
	if ( !ops ) return ops;
	ops->_free = free;

	as_binop * entries = NULL;
	if ( nops > 0 ) {
		entries = (as_binop *) malloc(sizeof(as_binop) * nops);
	}

	if ( entries ) {
		ops->binops._free = true;
		ops->binops.capacity = nops;
		ops->binops.size = 0;
		ops->binops.entries = entries;
	}
	else {
		ops->binops._free = false;
		ops->binops.capacity = 0;
		ops->binops.size = 0;
		ops->binops.entries = NULL;
	}

	return ops;
}

/**
 *	Find the as_binop to update when appending.
 *	Returns an as_binop ready for bin initialization.
 *	If no more entries available or precondition failed, then returns NULL.
 */
static as_binop * as_binop_forappend(as_operations * ops, as_operator operator, const as_bin_name name)
{
	if ( ! (ops && ops->binops.size < ops->binops.capacity &&
			name && strlen(name) < AS_BIN_NAME_MAX_SIZE) ) {
		return NULL;
	}

	// Note - caller must successfully populate bin once we increment size.
	as_binop * binop = &ops->binops.entries[ops->binops.size++];
	binop->operator = operator;

	return binop;
}

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
as_operations * as_operations_init(as_operations * ops, uint16_t nops)
{
	if ( !ops ) return ops;
	return as_operations_default(ops, false, nops);
}

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
as_operations * as_operations_new(uint16_t nops)
{
	as_operations *	ops = (as_operations *) malloc(sizeof(as_operations));
	if ( !ops ) return ops;
	return as_operations_default(ops, false, nops);
}

/**
 *	Releases the `as_operations` and associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 		as_operations_destroy(binops);
 *	~~~~~~~~~~
 *
 *	@param bins 	The `as_binops` to destroy.
 */
void as_operations_destroy(as_operations * ops)
{
	if ( !ops ) return;
	
	// destroy each bin in binops
	for(int i = 0; i < ops->binops.size; i++) {
		as_bin_destroy(&ops->binops.entries[i].bin);
	}

	// free binops
	if ( ops->binops._free ) {
		free(ops->binops.entries);
	}

	// reset values 
	ops->binops._free = false;
	ops->binops.capacity = 0;
	ops->binops.size = 0;
	ops->binops.entries = NULL;

	if ( ops->_free ) {
		free(ops);
	}
}

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
bool as_operations_append(as_operations * ops, as_operator operator, const as_bin_name name, as_bin_value * value)
{
	as_binop * binop = as_binop_forappend(ops, operator, name);
	if ( !binop ) return false;
	as_bin_init(&binop->bin, name, value);
	return true;
}

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
bool as_operations_append_int64(as_operations * ops, as_operator operator, const as_bin_name name, int64_t value)
{
	as_binop * binop = as_binop_forappend(ops, operator, name);
	if ( !binop ) return false;
	as_bin_init_int64(&binop->bin, name, value);
	return true;
}

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
bool as_operations_append_str(as_operations * ops, as_operator operator, const as_bin_name name, const char * value)
{
	as_binop * binop = as_binop_forappend(ops, operator, name);
	if ( !binop ) return false;
	as_bin_init_str(&binop->bin, name, value);
	return true;
}

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
bool as_operations_append_raw(as_operations * ops, as_operator operator, const as_bin_name name, uint8_t * value, uint32_t size)
{
	as_binop * binop = as_binop_forappend(ops, operator, name);
	if ( !binop ) return false;
	as_bin_init_raw(&binop->bin, name, value, size);
	return true;
}
