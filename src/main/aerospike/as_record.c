/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <stdlib.h>
#include <string.h>

#include "_bin.h"

/******************************************************************************
 * CONSTANTS
 *****************************************************************************/

extern const as_rec_hooks as_record_rec_hooks;

/******************************************************************************
 * INLINE FUNCTIONS
 *****************************************************************************/

extern inline as_val* as_record_toval(const as_record* rec);
extern inline as_record* as_record_fromval(const as_val * v);

extern inline bool as_record_set_str(as_record* rec, const char* name, const char* value);
extern inline bool as_record_set_raw(as_record* rec, const char* name, const uint8_t* value, uint32_t size);

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_record* as_record_defaults(as_record* rec, bool free, uint16_t nbins);
static as_bin* as_record_bin_forupdate(as_record* rec, const char* name);

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_record*
as_record_defaults(as_record* rec, bool free, uint16_t nbins)
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
 * Find a bin for updating.
 * Either return an existing bin of given name, or return an empty entry.
 * If no more entries available or precondition failed, then returns NULL.
 */ 
static as_bin*
as_record_bin_forupdate(as_record* rec, const char* name)
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
 * INSTANCE FUNCTIONS
 *****************************************************************************/

void
as_record_release(as_record* rec)
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

as_record*
as_record_new(uint16_t nbins)
{
	as_record* rec = (as_record *) cf_malloc(sizeof(as_record));
	if ( !rec ) return rec;
	return as_record_defaults(rec, true, nbins);
}

as_record*
as_record_init(as_record* rec, uint16_t nbins)
{
	if ( !rec ) return rec;
	return as_record_defaults(rec, false, nbins);
}

void
as_record_destroy(as_record* rec)
{
	as_rec_destroy((as_rec *) rec);
}

/******************************************************************************
 * VALUE FUNCTIONS
 *****************************************************************************/

uint16_t
as_record_numbins(const as_record* rec)
{
	return rec ? rec->bins.size : 0;
}

/******************************************************************************
 * SETTER FUNCTIONS
 *****************************************************************************/

bool
as_record_set(as_record* rec, const char* name, as_bin_value* value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, value);
	return true;
}

bool
as_record_set_bool(as_record* rec, const char* name, bool value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_bool(bin, name, value);
	return true;
}

bool
as_record_set_int64(as_record* rec, const char* name, int64_t value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_int64(bin, name, value);
	return true;
}

bool
as_record_set_double(as_record* rec, const char* name, double value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_double(bin, name, value);
	return true;
}

bool
as_record_set_strp(as_record* rec, const char* name, const char* value, bool free)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_str(bin, name, value, free);
	return true;
}

bool
as_record_set_geojson_strp(as_record* rec, const char* name, const char* value, bool free)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_geojson(bin, name, value, free);
	return true;
}

bool
as_record_set_rawp(as_record* rec, const char* name, const uint8_t* value, uint32_t size, bool free)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_raw(bin, name, value, size, free);
	return true;
}

bool
as_record_set_raw_typep(as_record* rec, const char* name, const uint8_t* value, uint32_t size, as_bytes_type type, bool free)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init_raw(bin, name, value, size, free);
	as_bytes *bytes_val = (as_bytes*) &bin->value;
	bytes_val->type = type;
	return true;
}

bool
as_record_set_integer(as_record* rec, const char* name, as_integer * value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool
as_record_set_as_double(as_record* rec, const char* name, as_double * value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool
as_record_set_string(as_record* rec, const char* name, as_string * value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool
as_record_set_geojson(as_record* rec, const char* name, as_geojson * value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool
as_record_set_bytes(as_record* rec, const char* name, as_bytes * value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool
as_record_set_list(as_record* rec, const char* name, as_list * value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool
as_record_set_map(as_record* rec, const char* name, as_map * value)
{
	as_bin* bin = as_record_bin_forupdate(rec, name);
	if ( !bin ) return false;
	as_bin_init(bin, name, (as_bin_value *) value);
	return true;
}

bool
as_record_set_nil(as_record* rec, const char* name)
{
	return as_record_set(rec, name, (as_bin_value *) &as_nil);
}

/******************************************************************************
 * GETTER FUNCTIONS
 *****************************************************************************/

as_bin_value*
as_record_get(const as_record* rec, const char* name)
{
	for(int i=0; i<rec->bins.size; i++) {
		if ( strcmp(rec->bins.entries[i].name, name) == 0 ) {
			return (as_bin_value *) rec->bins.entries[i].valuep;
		}
	}
	return NULL;
}

bool
as_record_get_bool(const as_record* rec, const char* name)
{
	as_bin_value* bv = as_record_get(rec, name);

	if (! bv) {
		return false;
	}

	as_val_t type = bv->boolean._.type;

	if (type == AS_BOOLEAN) {
		return bv->boolean.value;
	}

	if (type == AS_INTEGER) {
		int64_t v = bv->integer.value;
		return (v == 0)? false : true;
	}
	return false;
}

int64_t
as_record_get_int64(const as_record* rec, const char* name, int64_t fallback)
{
	as_integer * val = as_integer_fromval((as_val *) as_record_get(rec, name));
	return val ? as_integer_toint(val) : fallback;
}

double
as_record_get_double(const as_record* rec, const char* name, double fallback)
{
	as_double * val = as_double_fromval((as_val *) as_record_get(rec, name));
	return val ? val->value : fallback;
}

char*
as_record_get_str(const as_record* rec, const char* name)
{
	as_string * val = as_string_fromval((as_val *) as_record_get(rec, name));
	return val ? as_string_tostring(val) : NULL;
}

char*
as_record_get_geojson_str(const as_record* rec, const char* name)
{
	as_geojson * val = as_geojson_fromval((as_val *) as_record_get(rec, name));
	return val ? as_geojson_get(val) : NULL;
}

as_integer*
as_record_get_integer(const as_record* rec, const char* name)
{
	return as_integer_fromval((as_val *) as_record_get(rec, name));
}

as_double*
as_record_get_as_double(const as_record* rec, const char* name)
{
	return as_double_fromval((as_val *) as_record_get(rec, name));
}

as_string*
as_record_get_string(const as_record* rec, const char* name)
{
	return as_string_fromval((as_val *) as_record_get(rec, name));
}

as_geojson*
as_record_get_geojson(const as_record* rec, const char* name)
{
	return as_geojson_fromval((as_val *) as_record_get(rec, name));
}

as_bytes*
as_record_get_bytes(const as_record* rec, const char* name)
{
	return as_bytes_fromval((as_val *) as_record_get(rec, name));
}

as_list*
as_record_get_list(const as_record* rec, const char* name)
{
	return as_list_fromval((as_val *) as_record_get(rec, name));
}

as_map*
as_record_get_map(const as_record* rec, const char* name)
{
	return as_map_fromval((as_val *) as_record_get(rec, name));
}

as_val*
as_record_get_udf_result(const as_record* rec)
{
	return (as_val*)as_record_get(rec, "SUCCESS");
}

char*
as_record_get_udf_error(const as_record* rec)
{
	return as_record_get_str(rec, "FAILURE");
}

/******************************************************************************
 * ITERATION FUNCTIONS
 *****************************************************************************/

bool
as_record_foreach(const as_record* rec, as_rec_foreach_callback callback, void* udata)
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
