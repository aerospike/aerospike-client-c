/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
 *	as_bin bin;
 *	as_bin_init_int64(&key, "abc", 123);
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
 *	Initialize a stack allocated `as_bin` to a double value.
 *
 *	~~~~~~~~~~{.c}
 *	as_bin bin;
 *	as_bin_init_double(&key, "abc", 123.456);
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name 	The name of the bin.
 *	@param value	The value of the value.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_double(as_bin * bin, const as_bin_name name, double value);

/**
 *	Initialize a stack allocated `as_bin` to a NULL-terminated string value.
 *
 *	~~~~~~~~~~{.c}
 *	as_bin bin;
 *	as_bin_init_str(&key, "abc", "def", false);
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name 	The name of the bin.
 *	@param value	The value of the value.
 *	@param free		If true, then the value is freed when the bin is destroyed.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_str(as_bin * bin, const as_bin_name name, const char * value, bool free);

/**
 *	Initialize a stack allocated `as_bin` to a NULL-terminated GeoJSON string value.
 *
 *	~~~~~~~~~~{.c}
 *	as_bin bin;
 *	as_bin_init_geojson(&key, "abc", "def", false);
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name		The name of the bin.
 *	@param value	The value of the value.
 *	@param free		If true, then the value is freed when the bin is destroyed.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_geojson(as_bin * bin, const as_bin_name name, const char * value, bool free);

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 *
 *	~~~~~~~~~~{.c}
 *	uint8_t rgb[3] = {254,254,120};
 *
 *	as_bin bin;
 *  as_bin_init_raw(&key, "abc", rgb, 3, false);
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name 	The name of the bin.
 *	@param value	The value of the value.
 *	@param free		If true, then the value is freed when the bin is destroyed.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_raw(as_bin * bin, const as_bin_name name, const uint8_t * value, uint32_t size, bool free);

/**
 *	Initialize a stack allocated `as_key` to a an `as_key_value`.
 *
 *	~~~~~~~~~~{.c}
 *	as_bin bin;
 *	as_bin_init_nil(&key, "abc");
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
 * 	as_bin_destroy(bin);
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
