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

/**
 *	The `aerospike_key_operate()` function performs multiple operations on a
 *	record in the database. The `as_operations` object is used to define the
 *	operations to be performed on the record.
 *	
 *	@addtogroup operate Operate API
 *	@{
 */

#pragma once

#include <aerospike/as_bin.h>

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
	AS_OPERATOR_INCR       = 2,

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
 *	Sequence of operations.
 *
 *	~~~~~~~~~~{.c}
 *	as_operations ops;
 *	as_operations_inita(&ops, 2);
 *	as_operations_add_incr(&ops, "bin1", 123);
 *	as_operations_add_append_str(&ops, "bin2", "abc");
 *	...
 *	as_operations_destroy(&ops);
 *	~~~~~~~~~~
 *
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
	 *	The generation of the record.
	 */
	uint16_t gen;

	/**
	 *	The time-to-live (expiration) of the record in seconds.
	 */
	uint32_t ttl;

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
 *	as_operations ops;
 * 	as_operations_inita(&ops, 2);
 *	as_operations_add_incr(&ops, "bin1", 123);
 *	as_operations_add_append_str(&ops, "bin2", "abc");
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
 *	as_operations ops;
 * 	as_operations_init(&ops, 2);
 *	as_operations_add_incr(&ops, "bin1", 123);
 *	as_operations_add_append_str(&ops, "bin2", "abc");
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
 *	Create and initialize a heap allocated `as_operations`.
 *
 *	~~~~~~~~~~{.c}
 *	as_operations ops = as_operations_new(2);
 *	as_operations_add_incr(ops, "bin1", 123);
 *	as_operations_add_append_str(ops, "bin2", "abc");
 *	~~~~~~~~~~
 *
 *	Use `as_operations_destroy()` to free the resources allocated to the
 *	`as_operations`.
 *
 *	@param nops		The number of `as_operations.binops.entries` to allocate on the heap.
 *
 *	@return The new `as_operations` on success. Otherwise NULL.
 */
as_operations * as_operations_new(uint16_t nops);

/**
 *	Destroy an `as_operations` and release associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 	as_operations_destroy(binops);
 *	~~~~~~~~~~
 *
 *	@param ops 	The `as_operations` to destroy.
 */
void as_operations_destroy(as_operations * ops);

/**
 *	Add a `AS_OPERATOR_WRITE` bin operation.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write(as_operations * ops, const as_bin_name name, as_bin_value * value);

/**
 *	Add a `AS_OPERATOR_WRITE` bin operation with an int64_t value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_int64(as_operations * ops, const as_bin_name name, int64_t value);

/**
 *	Add a `AS_OPERATOR_WRITE` bin operation with a NULL-terminated string value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_str(as_operations * ops, const as_bin_name name, const char * value);

/**
 *	Add a `AS_OPERATOR_WRITE` bin operation with a raw bytes value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_raw(as_operations * ops, const as_bin_name name, uint8_t * value, uint32_t size);

/**
 *	Add a `AS_OPERATOR_READ` bin operation.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_read(as_operations * ops, const as_bin_name name);

/**
 *	Add a `AS_OPERATOR_INCR` bin operation with (required) int64_t value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_incr(as_operations * ops, const as_bin_name name, int64_t value);

/**
 *	Add a `AS_OPERATOR_PREPEND` bin operation with a NULL-terminated string value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_prepend_str(as_operations * ops, const as_bin_name name, const char * value);

/**
 *	Add a `AS_OPERATOR_PREPEND` bin operation with a raw bytes value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_prepend_raw(as_operations * ops, const as_bin_name name, uint8_t * value, uint32_t size);

/**
 *	Add a `AS_OPERATOR_APPEND` bin operation with a NULL-terminated string value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_append_str(as_operations * ops, const as_bin_name name, const char * value);

/**
 *	Add a `AS_OPERATOR_APPEND` bin operation with a raw bytes value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_append_raw(as_operations * ops, const as_bin_name name, uint8_t * value, uint32_t size);

/**
 *	Add a `AS_OPERATOR_TOUCH` record operation.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_touch(as_operations * ops);

/**
 *	@}
 */
