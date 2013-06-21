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
 *	[TOC]
 *
 *	Records in Aerospike are collections of named bins. 
 *
 *	The bins in a record are analogous to columns in relational databases. 
 *	However, unlike columns, the bins themselves are not typed. Instead, bins 
 *	contain values which are typed. So, it is possible to have multiple records 
 *	with bins of the same name but different types for values.
 *
 *	The bin's value can only be of the types defined in `as_bin_value`.
 *	
 *	## Creating and Initializing
 *	
 *	There are several ways to use an `as_record`. 
 *
 *	You can create the `as_record` on the stack:
 *	
 *	~~~~~~~~~~{.c}
 *		as_record rec;
 *	~~~~~~~~~~
 *	
 *	Then initialize it using either the `as_record_init()` function or 
 *	`as_record_inita()` macro.
 *
 *	The `as_record_init()` function will initialize the variable, then 
 *	allocate the specified number of bins using `malloc()`. The following
 *	initializes `rec` with 2 bins.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_init(&rec, 2);
 *	~~~~~~~~~~
 *
 *	The `as_record_inita()` macro will initialize the variable, then allocate
 *	the specified number of bins using `alloca()`. The following initializes 
 *	`rec` with 2 bins.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_inita(&rec, 2);
 *	~~~~~~~~~~
 *	
 *	The `as_record_new()` function will allocate an `as_record` on the heap
 *	using `malloc()` then allocate the specified number of bins using 
 *	`malloc()`. The following creates a new `as_record` with 2 bins.
 *
 *	~~~~~~~~~~{.c}
 *		as_record * rec = as_record_new(2);
 *	~~~~~~~~~~
 *	
 *	## Setting Bin Values
 *
 *   Function                    |  Description
 *	---------------------------- | ----------------------------------------------
 *	 `as_record_set_int64()`     | Set the bin value to a 64-bit integer.
 *	 `as_record_set_str()`       | Set the bin value to a NULL-terminated string.
 *	 `as_record_set_integer()`   | Set the bin value to an `as_integer`.
 *	 `as_record_set_string()`    | Set the bin value to an `as_string`.
 *	 `as_record_set_bytes()`     | Set the bin value to an `as_bytes`.
 *	 `as_record_set_list()`      | Set the bin value to an `as_list`.                    
 *	 `as_record_set_map()`       | Set the bin value to an `as_map`.
 *	 `as_record_set_nil()`       | Set the bin value to an `as_nil`.
 *
 *	## Getting Bin Values
 *
 *   Function                    |  Description
 *	---------------------------- | ----------------------------------------------
 *	 `as_record_get_int64()`     | Get the bin as a 64-bit integer.
 *	 `as_record_get_str()`       | Get the bin as a NULL-terminated string.
 *	 `as_record_get_integer()`   | Get the bin as an `as_integer`.
 *	 `as_record_get_string()`    | Get the bin as an `as_string`.
 *	 `as_record_get_bytes()`     | Get the bin as an `as_bytes`.
 *	 `as_record_get_list()`      | Get the bin as an `as_list`.                    
 *	 `as_record_get_map()`       | Get the bin as an `as_map`.
 *
 *	
 *
 *	@addtogroup record Record API
 *	@{
 */

#pragma once 

#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include <stdint.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Represents a record, including:
 *	- collection of bins
 *	- the digest
 *	- the generation value
 *	- the time-to-live (expiry)
 *
 *	Should only be created or initialized using either: 
 *	- as_record_new() or
 *	- as_record_init()
 */
typedef struct as_record_s {

	/**
	 *	@private
	 *	as_record is "derived" from as_rec.
	 *	So you can actually type cast as_record to as_rec.
	 */
	as_rec _;

	/**
	 *	The key of the record.
	 *	This is populated when a record is read from the database.
	 *	This should not be set by the user.
	 */
	as_key key;

	/**
	 *	The generation of the record.
	 */
	uint16_t gen;

	/**
	 *	The time-to-live (expiration) of the record in seconds.
	 */
	uint32_t ttl;

	/**
	 *	The bins of the record.
	 */
	as_bins bins;

} as_record;

/******************************************************************************
 *	MACROS
 *****************************************************************************/

/**
 * Initialize a stack allocated `as_record` then allocate `__nbins` capacity 
 * for as_record.bins on the stack.
 *
 *	~~~~~~~~~~{.c}
 *		as_record record;
 *		as_record_inita(&record, 2);
 *		as_record_set_int64(&record, "bin1", 123);
 *		as_record_set_int64(&record, "bin2", 456);
 *	~~~~~~~~~~
 *
 *	When you are finished using the `as_record` instance, you should release the 
 *	resources allocated to it by calling `as_record_destroy()`.
 *
 *	@param __rec		The `as_record *` to initialize.
 *	@param __nbins		The number of `as_record.bins.entries` to allocate on the 
 *						stack.
 */
#define as_record_inita(__rec, __nbins) \
	as_record_init(__rec, 0);\
	(__rec)->bins._free = false;\
	(__rec)->bins.capacity = __nbins;\
	(__rec)->bins.size = 0;\
	(__rec)->bins.entries = alloca(sizeof(as_bin) * __nbins);

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Create a new as_record on the heap.
 *
 *	~~~~~~~~~~{.c}
 *		as_record * r = as_record_new(2);
 *		as_record_set_int64(r, "bin1", 123);
 *		as_record_set_str(r, "bin1", "abc");
 *	~~~~~~~~~~
 *
 *	When you are finished using the `as_record` instance, you should release the 
 *	resources allocated to it by calling `as_record_destroy()`.
 *
 *	@param nbins 	The number of bins to initialize. Set to 0, if unknown.
 *
 *	@return a pointer to the new as_record if successful, otherwise NULL.
 */
as_record * as_record_new(uint16_t nbins);

/**
 *	Initializes an as_record created on the stack.
 *
 *	~~~~~~~~~~{.c}
 *		as_record r;
 *		as_record_init(&r, 2);
 *		as_record_set_int64(&r, "bin1", 123);
 *		as_record_set_str(&r, "bin1", "abc");
 *	~~~~~~~~~~
 *
 *	When you are finished using the `as_record` instance, you should release the 
 *	resources allocated to it by calling `as_record_destroy()`.
 *
 *	@param rec		The record to initialize.
 *	@param nbins	The number of bins to initialize. Set to 0, if unknown.
 *
 *	@return a pointer to the initialized as_record if successful, otherwise NULL.
 */
as_record * as_record_init(as_record * rec, uint16_t nbins);

/**
 *	Destroy the as_record and associated resources.
 *
 *	@param rec The record to destroy.
 */
void as_record_destroy(as_record * rec);

/**
 *	Get the number of bins in the record.
 *
 *	@return the number of bins in the record.
 */
uint16_t as_record_numbins(as_record * rec);

/**
 *	Set specified bin's value to an as_val (as_integer, as_string, as_bytes, as_list, as_map).
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set(as_record * rec, const as_bin_name name, as_bin_value * value);

/**
 *	Set specified bin's value to an int64_t.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_set_int64(rec, "bin", 123);
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_int64(as_record * rec, const as_bin_name name, int64_t value);

/**
 *	Set specified bin's value to an NULL terminates string.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_set_str(rec, "bin", "abc");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_str(as_record * rec, const as_bin_name name, const char * value);

/**
 *	Set specified bin's value to an as_integer.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_set_integer(rec, "bin", as_integer_new(123));
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_integer(as_record * rec, const as_bin_name name, as_integer * value);

/**
 *	Set specified bin's value to an as_string.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_set_string(rec, "bin", as_string_new("abc", false));
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_string(as_record * rec, const as_bin_name name, as_string * value);

/**
 *	Set specified bin's value to an as_bytes.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_set_integer(rec, "bin", bytes);
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_bytes(as_record * rec, const as_bin_name name, as_bytes * value);

/**
 *	Set specified bin's value to an as_list.
 *
 *	~~~~~~~~~~{.c}
 *		as_list list;
 *		as_arraylist_init(&list);
 *		as_list_add_int64(&list, 1);
 *		as_list_add_int64(&list, 2);
 *		as_list_add_int64(&list, 3);
 *
 *		as_record_set_list(rec, "bin", &list);
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_list(as_record * rec, const as_bin_name name, as_list * value);

/**
 *	Set specified bin's value to an as_map.
 *
 *	~~~~~~~~~~{.c}
 *		as_map map;
 *		as_stringmap_init(&map);
 *		as_stringmap_set_int64(&map, "a", 1);
 *		as_stringmap_set_int64(&map, "b", 2);
 *		as_stringmap_set_int64(&map, "c", 3);
 *
 *		as_record_set_map(rec, "bin", &map);
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param value	The value of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_map(as_record * rec, const as_bin_name name, as_map * value);

/**
 *	Set specified bin's value to as_nil.
 *
 *	~~~~~~~~~~{.c}
 *		as_record_set_nil(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return true on success, false on failure.
 */
bool as_record_set_nil(as_record * rec, const as_bin_name name);

/**
 *	Get specified bin's value as an as_val (as_integer, as_string, as_bytes, as_list, as_map).
 *
 *	~~~~~~~~~~{.c}
 *		as_val * value = as_record_get(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return the value if it exists, otherwise NULL.
 */
as_val * as_record_get(as_record * rec, const as_bin_name name);

/**
 *	Get specified bin's value as an int64_t.
 *
 *	~~~~~~~~~~{.c}
 *		int64_t value = as_record_get_int64(rec, "bin", INT64_MAX);
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param fallback	The default value to use, if the bin doesn't exist or is not an integer.
 *
 *	@return the value if it exists, otherwise 0.
 */
int64_t as_record_get_int64(as_record * rec, const as_bin_name name, int64_t fallback);

/**
 *	Get specified bin's value as an NULL terminated string.
 *
 *	~~~~~~~~~~{.c}
 *		char * value = as_record_get_str(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return the value if it exists, otherwise NULL.
 */
char * as_record_get_str(as_record * rec, const as_bin_name name);

/**
 *	Get specified bin's value as an as_integer.
 *
 *	~~~~~~~~~~{.c}
 *		as_integer * value = as_record_get_integer(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return the value if it exists, otherwise NULL.
 */
as_integer * as_record_get_integer(as_record * rec, const as_bin_name name);

/**
 *	Get specified bin's value as an as_string.
 *
 *	~~~~~~~~~~{.c}
 *		as_string * value = as_record_get_string(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return the value if it exists, otherwise NULL.
 */
as_string * as_record_get_string(as_record * rec, const as_bin_name name);

/**
 *	Get specified bin's value as an as_bytes.
 *
 *	~~~~~~~~~~{.c}
 *		as_bytes * value = as_record_get_bytes(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return the value if it exists, otherwise NULL.
 */
as_bytes * as_record_get_bytes(as_record * rec, const as_bin_name name);

/**
 *	Get specified bin's value as an as_list.
 *
 *	~~~~~~~~~~{.c}
 *		as_list * value = as_record_get_list(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return the value if it exists, otherwise NULL.
 */
as_list * as_record_get_list(as_record * rec, const as_bin_name name);

/**
 *	Get specified bin's value as an as_map.
 *
 *	~~~~~~~~~~{.c}
 *		as_map * value = as_record_get_map(rec, "bin");
 *	~~~~~~~~~~
 *
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *
 *	@return the value if it exists, otherwise NULL.
 */
as_map * as_record_get_map(as_record * rec, const as_bin_name name);

/**
 *	@}
 */
