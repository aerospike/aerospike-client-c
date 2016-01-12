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
#include <aerospike/as_bin.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_val.h>

#include <citrusleaf/alloc.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "_bin.h"

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_bin * as_bin_defaults(as_bin * bin, const as_bin_name name, as_bin_value * valuep)
{
	strcpy(bin->name, name);
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
	((as_val *) &bin->value)->type = AS_UNKNOWN;
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

as_bin * as_bin_init_double(as_bin * bin, const as_bin_name name, double value)
{
	if ( !bin ) return bin;
	as_double_init((as_double *) &bin->value, value);
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
as_bin * as_bin_init_str(as_bin * bin, const as_bin_name name, const char * value, bool free)
{
	if ( !bin ) return bin;
	as_string_init((as_string *) &bin->value, (char *) value, free);
	return as_bin_defaults(bin, name, &bin->value);
}

/**
 *	Initialize a stack allocated `as_bin` to a NULL-terminated GeoJSON string value.
 *
 *	~~~~~~~~~~{.c}
 *		as_bin bin;
 *		as_bin_init_geojson(&key, "abc", "def");
 *	~~~~~~~~~~
 *
 *	Use `as_bin_destroy()` to release resources allocated to `as_bin`.
 *
 *	@param name		The name of the bin.
 *	@param value	The value of the value.
 *
 *	@return The initialized `as_bin` on success. Otherwise NULL.
 */
as_bin * as_bin_init_geojson(as_bin * bin, const as_bin_name name, const char * value, bool free)
{
	if ( !bin ) return bin;
	as_geojson_init((as_geojson *) &bin->value, (char *) value, free);
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
as_bin * as_bin_init_raw(as_bin * bin, const as_bin_name name, const uint8_t * value, uint32_t size, bool free)
{
	if ( !bin ) return bin;
	as_bytes_init_wrap((as_bytes *) &bin->value, (uint8_t *) value, size, free);
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

	as_bin * entries = (as_bin *) cf_malloc(sizeof(as_bin) * capacity);
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
		cf_free(bins->entries);
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
