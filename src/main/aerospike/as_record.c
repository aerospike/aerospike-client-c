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
#include <aerospike/as_bytes.h>
#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>

#include <citrusleaf/alloc.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "_bin.h"

/******************************************************************************
 *	CONSTANTS
 *****************************************************************************/

extern const as_rec_hooks as_record_rec_hooks;

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

extern inline as_val * as_record_toval(const as_record * rec);
extern inline as_record * as_record_fromval(const as_val * v);

extern inline bool as_record_set_str(as_record * rec, const as_bin_name name, const char * value);
extern inline bool as_record_set_raw(as_record * rec, const as_bin_name name, const uint8_t * value, uint32_t size);

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_record * 	as_record_defaults(as_record * rec, bool free, uint16_t nbins);
static as_bin * 	as_record_bin_forupdate(as_record * rec, const as_bin_name name);

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_record * as_record_defaults(as_record * rec, bool free, uint16_t nbins) 
{
	as_rec * r = &rec->_;
	as_val_init(&r->_, AS_REC, free);
	r->data = rec;
	r->hooks = &as_record_rec_hooks;

	rec->key._free = false;
	rec->key.ns[0] = '\0';
	rec->key.set[0] = '\0';
	rec->key.valuep = NULL;

	rec->key.digest.init = false;
	memset(rec->key.digest.value, 0, AS_DIGEST_VALUE_SIZE);

	rec->gen = 0;
	rec->ttl = 0;

	if ( nbins > 0 ) {
		rec->bins._free = true;
		rec->bins.capacity = nbins;
		rec->bins.size = 0;
		rec->bins.entries = (as_bin *) cf_malloc(sizeof(as_bin) * nbins);
	}
	else {
		rec->bins._free = false;
		rec->bins.capacity = 0;
		rec->bins.size = 0;
		rec->bins.entries = NULL;
	}

	return rec;
}

/**
 *	Find a bin for updating.
 *	Either return an existing bin of given name, or return an empty entry.
 *	If no more entries available or precondition failed, then returns NULL.
 */ 
static as_bin * as_record_bin_forupdate(as_record * rec, const as_bin_name name) 
{
	if ( ! (rec && name && strlen(name) < AS_BIN_NAME_MAX_SIZE) ) {
		return NULL;
	}

	// look for bin of same name
	for(int i = 0; i < rec->bins.size; i++) {
		if ( strcmp(rec->bins.entries[i].name, name) == 0 ) {
			as_val_destroy(rec->bins.entries[i].valuep);
			rec->bins.entries[i].valuep = NULL;
			return &rec->bins.entries[i];
		}
	}

	// bin not found, then append
	if ( rec->bins.size < rec->bins.capacity ) {
		// Note - caller must successfully populate bin once we increment size.
		return &rec->bins.entries[rec->bins.size++];
	}

	return NULL;
}

/******************************************************************************
 *	INSTANCE FUNCTIONS
 *****************************************************************************/

void as_record_release(as_record * rec) 
{
	if ( rec ) {

		if ( rec->bins.entries ) {
			for ( int i = 0; i < rec->bins.size; i++ ) {
				as_val_destroy((as_val *) rec->bins.entries[i].valuep);
				rec->bins.entries[i].valuep = NULL;
			}
			if ( rec->bins._free ) {
				cf_free(rec->bins.entries);
			}
		}
		rec->bins.entries = NULL;
		rec->bins.capacity = 0;
		rec->bins.size = 0;

		rec->key.ns[0] = '\0';
		rec->key.set[0] = '\0';

		as_val_destroy((as_val *) rec->key.valuep);
		rec->key.valuep = NULL;

		rec->key.digest.init = false;
	}
}


/**
 *	Create a new as_record on the heap.
 *	as_record * r = as_record_new(2);
 *	as_record_set_int64(r, "bin1", 123);
 *	as_record_set_str(r, "bin1", "abc");
 *	@param nbins - the number of bins to initialize. Set to 0, if unknown.
 *	@return a pointer to the new as_record if successful, otherwise NULL.
 */
as_record * as_record_new(uint16_t nbins) 
{
	as_record * rec = (as_record *) cf_malloc(sizeof(as_record));
	if ( !rec ) return rec;
	return as_record_defaults(rec, true, nbins);
}

/**
 *	Initializes an as_record created on the stack.
 *	as_record r;
 *	as_record_init(&r, 2);
 *	as_record_set_int64(&r, "bin1", 123);
 *	as_record_set_str(&r, "bin1", "abc");
 *	@param rec 	- the record to initialize
 *	@param nbins - the number of bins to initialize. Set to 0, if unknown.
 *	@return a pointer to the initialized as_record if successful, otherwise NULL.
 */
as_record * as_record_init(as_record * rec, uint16_t nbins) 
{
	if ( !rec ) return rec;
	return as_record_defaults(rec, false, nbins);
}

/**
 *	Destroy the as_record and associated resources.
 */
void as_record_destroy(as_record * rec) 
{
	as_rec_destroy((as_rec *) rec);
}

/******************************************************************************
 *	VALUE FUNCTIONS
 *****************************************************************************/

/**
 *	Get the number of bins in the record.
 *	@param rec - the record
 *	@return the number of bins in the record.
 */
uint16_t as_record_numbins(const as_record * rec) 
{
	return rec ? rec->bins.size : 0;
}

/******************************************************************************
 *	SETTER FUNCTIONS
 *****************************************************************************/

/**
 *	Set specified bin's value to an as_val (as_integer, as_string, as_bytes, as_list, as_map).
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set(as_record * rec, const as_bin_name name, as_bin_value * value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, value);
	return true;
}


/**
 *	Set specified bin's value to an int64_t.
 *	as_record_set_int64(rec, "bin", 123);
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_int64(as_record * rec, const as_bin_name name, int64_t value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_int64(bin, name, value);
	return true;
}

bool as_record_set_double(as_record * rec, const as_bin_name name, double value)
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_double(bin, name, value);
	return true;
}

/**
 *	Set specified bin's value to an NULL terminated string.
 *	as_record_set_str(rec, "bin", "abc");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_strp(as_record * rec, const as_bin_name name, const char * value, bool free) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_str(bin, name, value, free);
	return true;
}

/**
 *	Set specified bin's value to an NULL terminated GeoJSON string.
 *	as_record_set_geojson_str(rec, "bin", "abc");
 *	@param rec	- the record containing the bin
 *	@param name		- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_geojson_strp(as_record * rec, const as_bin_name name, const char * value, bool free) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_geojson(bin, name, value, free);
	return true;
}

/**
 *	Set specified bin's value to raw bytes of given length.
 *	uint8_t bytes[3] = {1,2,3}
 *	as_record_set_raw(rec, "bin", bytes, 3);
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@param size 	- the size of the value
 *	@return true on success, false on failure.
 */
bool as_record_set_rawp(as_record * rec, const as_bin_name name, const uint8_t * value, uint32_t size, bool free) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_raw(bin, name, value, size, free);
	return true;
}

/**
 *	Set specified bin's value to raw bytes of given length.
 *	uint8_t bytes[3] = {1,2,3}
 *	as_record_set_raw(rec, "bin", bytes, 3);
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@param size 	- the size of the value
 *	@param type 	- the as_bytes_type designation (AS_BYTES_*)
 *	@return true on success, false on failure.
 */
bool as_record_set_raw_typep(as_record * rec, const as_bin_name name, const uint8_t * value, uint32_t size, as_bytes_type type, bool free)
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_raw(bin, name, value, size, free);
	as_bytes *bytes_val = (as_bytes*) &bin->value;
	bytes_val->type = type;
	return true;
}

/**
 *	Set specified bin's value to an as_integer.
 *	as_record_set_integer(rec, "bin", as_integer_new(123));
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_integer(as_record * rec, const as_bin_name name, as_integer * value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool as_record_set_as_double(as_record * rec, const as_bin_name name, as_double * value)
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

/**
 *	Set specified bin's value to an as_string.
 *	as_record_set_string(rec, "bin", as_string_new("abc", false));
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_string(as_record * rec, const as_bin_name name, as_string * value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

/**
 *	Set specified bin's value to an as_geojson.
 *	as_record_set_geojson(rec, "bin", as_geojson_new("abc", false));
 *	@param rec	- the record containing the bin
 *	@param name		- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_geojson(as_record * rec, const as_bin_name name, as_geojson * value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

/**
 *	Set specified bin's value to an as_bytes.
 *	as_record_set_integer(rec, "bin", bytes);
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_bytes(as_record * rec, const as_bin_name name, as_bytes * value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

/**
 *	Set specified bin's value to an as_list.
 *	as_arraylist list;
 *	as_arraylist_init(&list);
 *	as_arraylist_add_int64(&list, 1);
 *	as_arraylist_add_int64(&list, 2);
 *	as_arraylist_add_int64(&list, 3);
 *	as_record_set_list(rec, "bin", (as_list *) &list);
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_list(as_record * rec, const as_bin_name name, as_list * value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

/**
 *	Set specified bin's value to an as_map.
 *	as_stringmap map;
 *	as_stringmap_init(&map);
 *	as_stringmap_set_int64(&map, "a", 1);
 *	as_stringmap_set_int64(&map, "b", 2);
 *	as_stringmap_set_int64(&map, "c", 3);
 *	as_record_set_map(rec, "bin", &map);
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@param value - the value of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_map(as_record * rec, const as_bin_name name, as_map * value) 
{
	as_bin * bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

/**
 *	Set specified bin's value to as_nil.
 *	as_record_set_nil(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return true on success, false on failure.
 */
bool as_record_set_nil(as_record * rec, const as_bin_name name)
{
	return as_record_set(rec, name, (as_bin_value *) &as_nil);
}

/******************************************************************************
 *	GETTER FUNCTIONS
 *****************************************************************************/

/**
 *	Get specified bin's value as an as_val (as_integer, as_string, as_bytes, as_list, as_map).
 *	as_val * value = as_record_get(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
as_bin_value * as_record_get(const as_record * rec, const as_bin_name name) 
{
	for(int i=0; i<rec->bins.size; i++) {
		if ( strcmp(rec->bins.entries[i].name, name) == 0 ) {
			return (as_bin_value *) rec->bins.entries[i].valuep;
		}
	}
	return NULL;
}

/**
 *	Get specified bin's value as an int64_t.
 *	~~~~~~~~~~{.c}
 *	int64_t value = as_record_get_int64(rec, "bin", INT64_MAX);
 *	~~~~~~~~~~
 *	@param rec		The record containing the bin.
 *	@param name		The name of the bin.
 *	@param fallback	The default value to use, if the bin doesn't exist or is not an integer.
 *	@return the value if it exists, otherwise 0.
 */
int64_t as_record_get_int64(const as_record * rec, const as_bin_name name, int64_t fallback) 
{
	as_integer * val = as_integer_fromval((as_val *) as_record_get(rec, name));
	return val ? as_integer_toint(val) : fallback;
}

double as_record_get_double(const as_record * rec, const as_bin_name name, double fallback)
{
	as_double * val = as_double_fromval((as_val *) as_record_get(rec, name));
	return val ? val->value : fallback;
}

/**
 *	Get specified bin's value as an NULL terminated string.
 *	char * value = as_record_get_str(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
char * as_record_get_str(const as_record * rec, const as_bin_name name) 
{
	as_string * val = as_string_fromval((as_val *) as_record_get(rec, name));
	return val ? as_string_tostring(val) : NULL;
}

/**
 *	Get specified bin's value as an NULL terminated GeoJSON string.
 *	char * value = as_record_get_geojson_str(rec, "bin");
 *	@param rec	- the record containing the bin
 *	@param name		- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
char * as_record_get_geojson_str(const as_record * rec, const as_bin_name name) 
{
	as_geojson * val = as_geojson_fromval((as_val *) as_record_get(rec, name));
	return val ? as_geojson_get(val) : NULL;
}

/**
 *	Get specified bin's value as an as_integer.
 *	as_integer * value = as_record_get_integer(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
as_integer * as_record_get_integer(const as_record * rec, const as_bin_name name)
{
	return as_integer_fromval((as_val *) as_record_get(rec, name));
}

as_double * as_record_get_as_double(const as_record * rec, const as_bin_name name)
{
	return as_double_fromval((as_val *) as_record_get(rec, name));
}

/**
 *	Get specified bin's value as an as_string.
 *	as_string * value = as_record_get_string(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
as_string * as_record_get_string(const as_record * rec, const as_bin_name name)
{
	return as_string_fromval((as_val *) as_record_get(rec, name));
}

/**
 *	Get specified bin's value as an as_geojson.
 *	as_string * value = as_record_get_geojson(rec, "bin");
 *	@param rec	- the record containing the bin
 *	@param name		- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
as_geojson * as_record_get_geojson(const as_record * rec, const as_bin_name name)
{
	return as_geojson_fromval((as_val *) as_record_get(rec, name));
}

/**
 *	Get specified bin's value as an as_bytes.
 *	as_bytes * value = as_record_get_bytes(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
as_bytes * as_record_get_bytes(const as_record * rec, const as_bin_name name) 
{
	return as_bytes_fromval((as_val *) as_record_get(rec, name));
}

/**
 *	Get specified bin's value as an as_list.
 *	as_list * value = as_record_get_list(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
as_list * as_record_get_list(const as_record * rec, const as_bin_name name) 
{
	return as_list_fromval((as_val *) as_record_get(rec, name));
}

/**
 *	Get specified bin's value as an as_map.
 *	as_map * value = as_record_get_map(rec, "bin");
 *	@param rec 	- the record containing the bin
 *	@param name 	- the name of the bin
 *	@return the value if it exists, otherwise NULL.
 */
as_map * as_record_get_map(const as_record * rec, const as_bin_name name) 
{
	return as_map_fromval((as_val *) as_record_get(rec, name));
}

/******************************************************************************
 *	ITERATION FUNCTIONS
 *****************************************************************************/

bool as_record_foreach(const as_record * rec, as_rec_foreach_callback callback, void * udata)
{
	if ( rec->bins.entries ) {
		for ( int i = 0; i < rec->bins.size; i++ ) {
			if ( callback(rec->bins.entries[i].name, (as_val *) rec->bins.entries[i].valuep, udata) == false ) {
				return false;
			}
		}
	}
	return true;
}
