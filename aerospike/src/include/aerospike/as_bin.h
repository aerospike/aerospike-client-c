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
#define AS_BIN_NAME_SIZE 16
#define AS_BIN_NAME_LEN AS_BIN_NAME_SIZE - 1

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Bin Name
 */
typedef char as_bin_name[AS_BIN_NAME_SIZE];

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
	 *	Number of entries allocated to data.
	 */
	uint16_t size;

	/**
	 *	Storage for bins
	 */
	as_bin * entries;

} as_bins;

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 *	Initializes a stack allocated `as_bins` (__bins) and allocates `__capacity`
 *	number of entries on the stack.
 *
 *	~~~~~~~~~~{.c}
 *		as_bins bins;
 * 		as_bins_init(&bins, 2);
 *		as_bins_append(&bins, "bin1", as_integer_new(123));
 *		as_bins_append(&bins, "bin2", as_integer_new(456));
 *	~~~~~~~~~~
 *
 *	@param __bins		The `as_bins *` to initialize.
 *	@param __capacity	The number of `as_bins.entries` to allocate on the 
 *						stack.
 */
#define as_bins_inita(__bins, __capacity) \
	(__bins)->_free = false;\
	(__bins)->capacity = __capacity;\
	(__bins)->size = 0;\
	(__bins)->entries = (as_bin *) alloca(sizeof(as_bin) * __capacity);

/******************************************************************************
 *	as_bin FUNCTIONS
 *****************************************************************************/

/**
 *	Intializes an `as_bin` with the given name and value.
 *
 *	~~~~~~~~~~{.c}
 *		as_bin bin;
 * 		as_bin_init(&bin, "bin1", as_integer_new(123));
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to free the resources allocated by this function.
 *
 *	@param bin 		The `as_bin` to initialize.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return The initialized `as_bin` on success. Otherwsie NULL.
 */
as_bin * as_bin_init(as_bin * bin, const as_bin_name name, as_bin_value * value);

/**
 *	Initialize a stack allocated `as_bin` to a int64_t value.
 *
 *	~~~~~~~~~~{.c}
 *		as_bin bin;
 *	    as_bin_init_int64(&key, "abc", 123);
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name 	The name of the bin.
 *	@param value	The value of the value.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_int64(as_bin * bin, const as_bin_name name, int64_t value);

/**
 *	Initialize a stack allocated `as_bin` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *		as_bin bin;
 *	    as_bin_init_str(&key, "abc", "def");
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name 	The name of the bin.
 *	@param value	The value of the value.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_str(as_bin * bin, const as_bin_name name, const char * value);

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 *
 *	~~~~~~~~~~{.c}
 *		uint8_t rgb[3] = {254,254,120};
 *
 *		as_bin bin;
 *	    as_bin_init_str(&key, "abc", rgb, 3);
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name 	The name of the bin.
 *	@param value	The value of the value.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_raw(as_bin * bin, const as_bin_name name, const uint8_t * value, uint32_t size);

/**
 *	Initialize a stack allocated `as_key` to a an `as_key_value`.
 *
 *	~~~~~~~~~~{.c}
 *		as_string str;
 *		as_string_init(&str, "abc", false);
 *
 *		as_bin bin;
 *	    as_bin_init_str(&key, "abc", (as_key_value *) str);
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name 	The name of the bin.
 *	@param value	The value of the value.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_nil(as_bin * bin, const as_bin_name name);

/**
 *	Destroy the given `as_bin` and associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 		as_bin_destroy(bin);
 *	~~~~~~~~~~
 *
 *	@param bin The `as_bin` to destroy.
 */
void as_bin_destroy(as_bin * bin);

/******************************************************************************
 *	as_bins FUNCTIONS
 *****************************************************************************/

/**
 *	Intializes a stack allocated `as_bins`. The capacity specifies the number 
 *	of `as_bins.entries` to allocate on the heap. 
 *
 *	~~~~~~~~~~{.c}
 *		as_bins bins;
 * 		as_bins_init(&bins, 2);
 *		as_bins_append(&bins, "bin1", as_integer_new(123));
 *		as_bins_append(&bins, "bin2", as_integer_new(456));
 *	~~~~~~~~~~
 *
 *	Use `as_bins_destroy()` to free the resources allocated by this function.
 *
 *	@param bins 		The `as_bins` to initialize.
 *	@param capacity		The number of `as_bins.entries` to allocate on the heap.
 *
 *	@return The initialized `as_bins` on success. Otherwsie NULL.
 */
as_bins * as_bins_init(as_bins * bins, uint16_t capacity);

/**
 *	Destroy the `as_bins` and associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 		as_bins_destroy(bins);
 *	~~~~~~~~~~
 *
 *	@param bins 	The `as_bins` to destroy.
 */
void as_bins_destroy(as_bins * bins);

/**
 *	Append a bin to the sequence of bins.
 *
 *	@param bins 		The `as_bins` to append the bin to.
 *	@param name 		The name of the bin to append.
 *	@param value 		The value of the bin to append.
 *
 *	@return true on success. Otherswise an error occurred.
 */
bool as_bins_append(as_bins * bins, as_bin_name name, as_bin_value * value);

