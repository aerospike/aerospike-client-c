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

#include <stdio.h>
#include <string.h>

#include <aerospike/as_bin.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_val.h>

#include "_bin.h"

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_bin * as_bin_defaults(as_bin * bin, const as_bin_name name, as_bin_value * valuep)
{
	strncpy(bin->name, name, AS_BIN_NAME_MAX_LEN);
	bin->name[AS_BIN_NAME_MAX_LEN] = '\0';
	bin->valuep = valuep;
	return bin;
}

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
as_bin * as_bin_init(as_bin * bin, const as_bin_name name, as_bin_value * value)
{
	if ( !bin ) return bin;
	return as_bin_defaults(bin, name, value);
}

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
as_bin * as_bin_init_int64(as_bin * bin, const as_bin_name name, int64_t value)
{
	if ( !bin ) return bin;
	as_integer_init((as_integer *) &bin->value, value);
	return as_bin_defaults(bin, name, &bin->value);
}

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
as_bin * as_bin_init_str(as_bin * bin, const as_bin_name name, const char * value)
{
	if ( !bin ) return bin;
	as_string_init((as_string *) &bin->value, (char *) value, false);
	return as_bin_defaults(bin, name, &bin->value);
}

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
as_bin * as_bin_init_raw(as_bin * bin, const as_bin_name name, const uint8_t * value, uint32_t size)
{
	if ( !bin ) return bin;
	as_bytes_init((as_bytes *) &bin->value, (uint8_t *) value, size, false);
	return as_bin_defaults(bin, name, &bin->value);
}

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
as_bin * as_bin_init_nil(as_bin * bin, const as_bin_name name)
{
	if ( !bin ) return bin;
	as_val * nil = (as_val *) &bin->value;
	nil->type = as_nil.type;
	nil->free = as_nil.free;
	nil->count = as_nil.count;
	return as_bin_defaults(bin, name, &bin->value);
}

/**
 *	Destroy the given `as_bin` and associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 		as_bin_destroy(bin);
 *	~~~~~~~~~~
 *
 *	@param bin The `as_bin` to destroy.
 */
void as_bin_destroy(as_bin * bin) 
{
	if ( !bin ) return;

	bin->name[0] = '\0';

	if ( bin->valuep ) {
		as_val_destroy((as_val *) bin->valuep);
		bin->valuep = NULL;
	}
}

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
as_bins * as_bins_init(as_bins * bins, uint16_t capacity)
{
	if ( !bins ) return bins;

	as_bin * entries = (as_bin *) malloc(sizeof(as_bin) * capacity);
	if ( entries ) {
		bins->_free = true;
		bins->capacity = capacity;
		bins->size = 0;
		bins->entries = entries;
	}
	else {
		bins->_free = false;
		bins->capacity = 0;
		bins->size = 0;
		bins->entries = NULL;
	}

	return bins;
}

/**
 *	Destroy the `as_bins` and associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 		as_bins_destroy(bins);
 *	~~~~~~~~~~
 *
 *	@param bins 	The `as_bins` to destroy.
 */
void as_bins_destroy(as_bins * bins)
{
	if ( !bins ) return;

	if ( bins->_free && bins->entries ) {
		free(bins->entries);
	}

	bins->capacity = 0;
	bins->size = 0;
	bins->entries = NULL;
}

/**
 *	Append a bin to the sequence of bins.
 *
 *	@param bins 		The `as_bins` to append the bin to.
 *	@param name 		The name of the bin to append.
 *	@param value 		The value of the bin to append.
 *
 *	@return true on success. Otherswise an error occurred.
 */
bool as_bins_append(as_bins * bins, as_bin_name name, as_bin_value * value)
{
	if ( !bins ) return false;
	if ( bins->size >= bins->capacity ) return false;
	as_bin_init(&bins->entries[bins->size], name, value);
	bins->size++;
	return true;
}




