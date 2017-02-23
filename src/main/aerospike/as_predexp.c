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

#include <aerospike/aerospike_index.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_predexp.h>

#include <citrusleaf/cf_byte_order.h>

#define AS_PREDEXP_AND					1
#define AS_PREDEXP_OR					2
#define AS_PREDEXP_NOT					3

#define AS_PREDEXP_INTEGER_VALUE		10
#define AS_PREDEXP_STRING_VALUE			11
#define AS_PREDEXP_GEOJSON_VALUE		12

#define AS_PREDEXP_INTEGER_BIN			100
#define AS_PREDEXP_STRING_BIN			101
#define AS_PREDEXP_GEOJSON_BIN			102
#define AS_PREDEXP_LIST_BIN				103
#define AS_PREDEXP_MAP_BIN				104

#define AS_PREDEXP_INTEGER_VAR			120
#define AS_PREDEXP_STRING_VAR			121
#define AS_PREDEXP_GEOJSON_VAR			122

#define AS_PREDEXP_RECSIZE				150
#define AS_PREDEXP_LAST_UPDATE			151
#define AS_PREDEXP_VOID_TIME			152

#define AS_PREDEXP_INTEGER_EQUAL		200
#define AS_PREDEXP_INTEGER_UNEQUAL		201
#define AS_PREDEXP_INTEGER_GREATER		202
#define AS_PREDEXP_INTEGER_GREATEREQ	203
#define AS_PREDEXP_INTEGER_LESS			204
#define AS_PREDEXP_INTEGER_LESSEQ		205

#define AS_PREDEXP_STRING_EQUAL			210
#define AS_PREDEXP_STRING_UNEQUAL		211
#define AS_PREDEXP_STRING_REGEX			212

#define AS_PREDEXP_GEOJSON_WITHIN		220
#define AS_PREDEXP_GEOJSON_CONTAINS		221

#define AS_PREDEXP_LIST_ITERATE_OR		250
#define AS_PREDEXP_MAPKEY_ITERATE_OR	251
#define AS_PREDEXP_MAPVAL_ITERATE_OR	252
#define AS_PREDEXP_LIST_ITERATE_AND		253
#define AS_PREDEXP_MAPKEY_ITERATE_AND	254
#define AS_PREDEXP_MAPVAL_ITERATE_AND	255


// ----------------------------------------------------------------
// as_predexp_and
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	uint16_t			nexpr;
} as_predexp_and_t;

void as_predexp_and_dtor(as_predexp_base * bp)
{
	as_predexp_and_t * dp = (as_predexp_and_t *) bp;
	cf_free(dp);
}

size_t as_predexp_and_size(as_predexp_base * bp)
{
	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += sizeof(uint16_t);								// nexpr
	return sz;
}

uint8_t * as_predexp_and_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_and_t * dp = (as_predexp_and_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_AND);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(sizeof(uint16_t));

	// nexpr
	uint16_t * nexpr_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*nexpr_ptr = cf_swap_to_be16(dp->nexpr);

	return p;
}

as_predexp_base * as_predexp_and(uint16_t nexpr)
{
	as_predexp_and_t * dp =
		(as_predexp_and_t *)
		cf_malloc(sizeof(as_predexp_and_t));
	dp->base.dtor_fn = as_predexp_and_dtor;
	dp->base.size_fn = as_predexp_and_size;
	dp->base.write_fn = as_predexp_and_write;
	dp->nexpr = nexpr;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_or
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	uint16_t			nexpr;
} as_predexp_or_t;

void as_predexp_or_dtor(as_predexp_base * bp)
{
	as_predexp_or_t * dp = (as_predexp_or_t *) bp;
	cf_free(dp);
}

size_t as_predexp_or_size(as_predexp_base * bp)
{
	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += sizeof(uint16_t);								// nexpr
	return sz;
}

uint8_t * as_predexp_or_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_or_t * dp = (as_predexp_or_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_OR);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(sizeof(uint16_t));

	// nexpr
	uint16_t * nexpr_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*nexpr_ptr = cf_swap_to_be16(dp->nexpr);

	return p;
}

as_predexp_base * as_predexp_or(uint16_t nexpr)
{
	as_predexp_or_t * dp =
		(as_predexp_or_t *)
		cf_malloc(sizeof(as_predexp_or_t));
	dp->base.dtor_fn = as_predexp_or_dtor;
	dp->base.size_fn = as_predexp_or_size;
	dp->base.write_fn = as_predexp_or_write;
	dp->nexpr = nexpr;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_not
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
} as_predexp_not_t;

void as_predexp_not_dtor(as_predexp_base * bp)
{
	as_predexp_not_t * dp = (as_predexp_not_t *) bp;
	cf_free(dp);
}

size_t as_predexp_not_size(as_predexp_base * bp)
{
	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	return sz;
}

uint8_t * as_predexp_not_write(as_predexp_base * bp, uint8_t * p)
{
	// as_predexp_not_t * dp = (as_predexp_not_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_NOT);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(0);

	return p;
}

as_predexp_base * as_predexp_not()
{
	as_predexp_not_t * dp =
		(as_predexp_not_t *)
		cf_malloc(sizeof(as_predexp_not_t));
	dp->base.dtor_fn = as_predexp_not_dtor;
	dp->base.size_fn = as_predexp_not_size;
	dp->base.write_fn = as_predexp_not_write;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_integer_value
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	int64_t				value;
} as_predexp_integer_value_t;

void as_predexp_integer_value_dtor(as_predexp_base * bp)
{
	as_predexp_integer_value_t * dp = (as_predexp_integer_value_t *) bp;
	cf_free(dp);
}

size_t as_predexp_integer_value_size(as_predexp_base * bp)
{
	// as_predexp_integer_value_t * dp = (as_predexp_integer_value_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += sizeof(int64_t);			// value

	return sz;
}

uint8_t * as_predexp_integer_value_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_integer_value_t * dp = (as_predexp_integer_value_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_INTEGER_VALUE);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(sizeof(int64_t));
	
	// value
	int64_t * value_ptr = (int64_t *) p;
	p += sizeof(int64_t);
	*value_ptr = cf_swap_to_be64(dp->value);

	return p;
}

as_predexp_base * as_predexp_integer_value(int64_t value)
{
	as_predexp_integer_value_t * dp =
		(as_predexp_integer_value_t *)
		cf_malloc(sizeof(as_predexp_integer_value_t));
	dp->base.dtor_fn = as_predexp_integer_value_dtor;
	dp->base.size_fn = as_predexp_integer_value_size;
	dp->base.write_fn = as_predexp_integer_value_write;
	dp->value = value;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_string_value
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	char const *		value;
} as_predexp_string_value_t;

void as_predexp_string_value_dtor(as_predexp_base * bp)
{
	as_predexp_string_value_t * dp = (as_predexp_string_value_t *) bp;
	if (dp->value) {
		cf_free((char *) dp->value);
	}
	cf_free(dp);
}

size_t as_predexp_string_value_size(as_predexp_base * bp)
{
	as_predexp_string_value_t * dp = (as_predexp_string_value_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += strlen(dp->value);							// value

	return sz;
}

uint8_t * as_predexp_string_value_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_string_value_t * dp = (as_predexp_string_value_t *) bp;

	uint32_t vlen = (uint32_t)strlen(dp->value);

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_STRING_VALUE);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(vlen);

	// value
	char * vptr = (char *) p;
	p += vlen;
	memcpy(vptr, dp->value, vlen);

	return p;
}

as_predexp_base * as_predexp_string_value(char const * value)
{
	as_predexp_string_value_t * dp =
		(as_predexp_string_value_t *)
		cf_malloc(sizeof(as_predexp_string_value_t));
	dp->base.dtor_fn = as_predexp_string_value_dtor;
	dp->base.size_fn = as_predexp_string_value_size;
	dp->base.write_fn = as_predexp_string_value_write;
	dp->value = strdup(value);
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_geojson_value
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	char const *		value;
} as_predexp_geojson_value_t;

void as_predexp_geojson_value_dtor(as_predexp_base * bp)
{
	as_predexp_geojson_value_t * dp = (as_predexp_geojson_value_t *) bp;
	if (dp->value) {
		cf_free((char *) dp->value);
	}
	cf_free(dp);
}

size_t as_predexp_geojson_value_size(as_predexp_base * bp)
{
	as_predexp_geojson_value_t * dp = (as_predexp_geojson_value_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += sizeof(uint8_t);								// flags
	sz += sizeof(uint16_t);								// ncells
	sz += strlen(dp->value);							// value

	return sz;
}

uint8_t * as_predexp_geojson_value_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_geojson_value_t * dp = (as_predexp_geojson_value_t *) bp;

	size_t slen = strlen(dp->value);
	uint32_t vlen = (uint32_t)(sizeof(uint8_t) + sizeof(uint16_t) + slen);

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_GEOJSON_VALUE);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(vlen);

	// flags
	uint8_t * flags_ptr = (uint8_t *) p;
	p += sizeof(uint8_t);
	*flags_ptr = 0;
	
	// ncells
	uint16_t * ncells_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*ncells_ptr = cf_swap_to_be16(0);
	
	// value
	char * vptr = (char *) p;
	p += slen;
	memcpy(vptr, dp->value, slen);

	return p;
}

as_predexp_base * as_predexp_geojson_value(char const * value)
{
	as_predexp_geojson_value_t * dp =
		(as_predexp_geojson_value_t *)
		cf_malloc(sizeof(as_predexp_geojson_value_t));
	dp->base.dtor_fn = as_predexp_geojson_value_dtor;
	dp->base.size_fn = as_predexp_geojson_value_size;
	dp->base.write_fn = as_predexp_geojson_value_write;
	dp->value = strdup(value);
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_*_bin
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	char *				binname;
	uint16_t			tag;		// Not written to wire
} as_predexp_bin_t;

void as_predexp_bin_dtor(as_predexp_base * bp)
{
	as_predexp_bin_t * dp = (as_predexp_bin_t *) bp;
	if (dp->binname)
		cf_free(dp->binname);
	cf_free(dp);
}

size_t as_predexp_bin_size(as_predexp_base * bp)
{
	as_predexp_bin_t * dp = (as_predexp_bin_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += strlen(dp->binname);							// binname

	return sz;
}

uint8_t * as_predexp_bin_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_bin_t * dp = (as_predexp_bin_t *) bp;

	size_t len = strlen(dp->binname);
	uint8_t bnlen = len;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(dp->tag);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(bnlen);

	// binname value
	char * bnptr = (char *) p;
	p += bnlen;
	memcpy(bnptr, dp->binname, bnlen);

	return p;
}

static as_predexp_base * as_predexp_bin(char const * binname, uint16_t tag)
{
	if (!binname) {
		as_log_error("missing bin name argument");
		return NULL;
	}
	else if (strlen(binname) >= AS_BIN_NAME_MAX_SIZE) {
		as_log_error("bin name \"%s\" too long", binname);
		return NULL;
	}

	as_predexp_bin_t * dp =
		(as_predexp_bin_t *)
		cf_malloc(sizeof(as_predexp_bin_t));
	dp->base.dtor_fn = as_predexp_bin_dtor;
	dp->base.size_fn = as_predexp_bin_size;
	dp->base.write_fn = as_predexp_bin_write;
	dp->binname = strdup(binname);
	dp->tag = tag;
	return (as_predexp_base *) dp;
}

as_predexp_base * as_predexp_integer_bin(char const * binname)
{
	return as_predexp_bin(binname, AS_PREDEXP_INTEGER_BIN);
}

as_predexp_base * as_predexp_string_bin(char const * binname)
{
	return as_predexp_bin(binname, AS_PREDEXP_STRING_BIN);
}

as_predexp_base * as_predexp_geojson_bin(char const * binname)
{
	return as_predexp_bin(binname, AS_PREDEXP_GEOJSON_BIN);
}

as_predexp_base * as_predexp_list_bin(char const * binname)
{
	return as_predexp_bin(binname, AS_PREDEXP_LIST_BIN);
}

as_predexp_base * as_predexp_map_bin(char const * binname)
{
	return as_predexp_bin(binname, AS_PREDEXP_MAP_BIN);
}

// ----------------------------------------------------------------
// as_predexp_*_var
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	char *				varname;
	uint16_t			tag;		// Not written to wire
} as_predexp_var_t;

void as_predexp_var_dtor(as_predexp_base * bp)
{
	as_predexp_var_t * dp = (as_predexp_var_t *) bp;
	if (dp->varname)
		cf_free(dp->varname);
	cf_free(dp);
}

size_t as_predexp_var_size(as_predexp_base * bp)
{
	as_predexp_var_t * dp = (as_predexp_var_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += strlen(dp->varname);							// varname

	return sz;
}

uint8_t * as_predexp_var_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_var_t * dp = (as_predexp_var_t *) bp;

	size_t len = strlen(dp->varname);
	uint8_t bnlen = len;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(dp->tag);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(bnlen);

	// varname value
	char * bnptr = (char *) p;
	p += bnlen;
	memcpy(bnptr, dp->varname, bnlen);

	return p;
}

static as_predexp_base * as_predexp_var(char const * varname, uint16_t tag)
{
	if (!varname) {
		as_log_error("missing var name argument");
		return NULL;
	}
	else if (strlen(varname) >= AS_BIN_NAME_MAX_SIZE) {
		as_log_error("var name \"%s\" too long", varname);
		return NULL;
	}

	as_predexp_var_t * dp =
		(as_predexp_var_t *)
		cf_malloc(sizeof(as_predexp_var_t));
	dp->base.dtor_fn = as_predexp_var_dtor;
	dp->base.size_fn = as_predexp_var_size;
	dp->base.write_fn = as_predexp_var_write;
	dp->varname = strdup(varname);
	dp->tag = tag;
	return (as_predexp_base *) dp;
}

as_predexp_base * as_predexp_integer_var(char const * varname)
{
	return as_predexp_var(varname, AS_PREDEXP_INTEGER_VAR);
}

as_predexp_base * as_predexp_string_var(char const * varname)
{
	return as_predexp_var(varname, AS_PREDEXP_STRING_VAR);
}

as_predexp_base * as_predexp_geojson_var(char const * varname)
{
	return as_predexp_var(varname, AS_PREDEXP_GEOJSON_VAR);
}

// ----------------------------------------------------------------
// as_predexp_recsize
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
} as_predexp_recsize_t;

void as_predexp_recsize_dtor(as_predexp_base * bp)
{
	as_predexp_recsize_t * dp = (as_predexp_recsize_t *) bp;
	cf_free(dp);
}

size_t as_predexp_recsize_size(as_predexp_base * bp)
{
	// as_predexp_recsize_t * dp = (as_predexp_recsize_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN

	return sz;
}

uint8_t * as_predexp_recsize_write(as_predexp_base * bp, uint8_t * p)
{
	// as_predexp_recsize_t * dp = (as_predexp_recsize_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_RECSIZE);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(0);

	return p;
}

as_predexp_base * as_predexp_recsize()
{
	as_predexp_recsize_t * dp =
		(as_predexp_recsize_t *)
		cf_malloc(sizeof(as_predexp_recsize_t));
	dp->base.dtor_fn = as_predexp_recsize_dtor;
	dp->base.size_fn = as_predexp_recsize_size;
	dp->base.write_fn = as_predexp_recsize_write;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_last_update
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
} as_predexp_last_update_t;

void as_predexp_last_update_dtor(as_predexp_base * bp)
{
	as_predexp_last_update_t * dp = (as_predexp_last_update_t *) bp;
	cf_free(dp);
}

size_t as_predexp_last_update_size(as_predexp_base * bp)
{
	// as_predexp_last_update_t * dp = (as_predexp_last_update_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN

	return sz;
}

uint8_t * as_predexp_last_update_write(as_predexp_base * bp, uint8_t * p)
{
	// as_predexp_last_update_t * dp = (as_predexp_last_update_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_LAST_UPDATE);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(0);

	return p;
}

as_predexp_base * as_predexp_last_update()
{
	as_predexp_last_update_t * dp =
		(as_predexp_last_update_t *)
		cf_malloc(sizeof(as_predexp_last_update_t));
	dp->base.dtor_fn = as_predexp_last_update_dtor;
	dp->base.size_fn = as_predexp_last_update_size;
	dp->base.write_fn = as_predexp_last_update_write;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_void_time
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
} as_predexp_void_time_t;

void as_predexp_void_time_dtor(as_predexp_base * bp)
{
	as_predexp_void_time_t * dp = (as_predexp_void_time_t *) bp;
	cf_free(dp);
}

size_t as_predexp_void_time_size(as_predexp_base * bp)
{
	// as_predexp_void_time_t * dp = (as_predexp_void_time_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN

	return sz;
}

uint8_t * as_predexp_void_time_write(as_predexp_base * bp, uint8_t * p)
{
	// as_predexp_void_time_t * dp = (as_predexp_void_time_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_VOID_TIME);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(0);

	return p;
}

as_predexp_base * as_predexp_void_time()
{
	as_predexp_void_time_t * dp =
		(as_predexp_void_time_t *)
		cf_malloc(sizeof(as_predexp_void_time_t));
	dp->base.dtor_fn = as_predexp_void_time_dtor;
	dp->base.size_fn = as_predexp_void_time_size;
	dp->base.write_fn = as_predexp_void_time_write;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_*_compare
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	uint16_t			tag;	// Not written to the wire
} as_predexp_compare_t;

void as_predexp_compare_dtor(as_predexp_base * bp)
{
	as_predexp_compare_t * dp = (as_predexp_compare_t *) bp;
	cf_free(dp);
}

size_t as_predexp_compare_size(as_predexp_base * bp)
{
	// as_predexp_compare_t * dp = (as_predexp_compare_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN

	return sz;
}

uint8_t * as_predexp_compare_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_compare_t * dp = (as_predexp_compare_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(dp->tag);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(0);

	return p;
}

// This routine is static, helpers below are used.
static as_predexp_base * as_predexp_compare(uint16_t tag)
{
	as_predexp_compare_t * dp =
		(as_predexp_compare_t *)
		cf_malloc(sizeof(as_predexp_compare_t));
	dp->base.dtor_fn = as_predexp_compare_dtor;
	dp->base.size_fn = as_predexp_compare_size;
	dp->base.write_fn = as_predexp_compare_write;
	dp->tag = tag;
	return (as_predexp_base *) dp;
}

as_predexp_base * as_predexp_integer_equal()
{
	return as_predexp_compare(AS_PREDEXP_INTEGER_EQUAL);
}

as_predexp_base * as_predexp_integer_unequal()
{
	return as_predexp_compare(AS_PREDEXP_INTEGER_UNEQUAL);
}

as_predexp_base * as_predexp_integer_greater()
{
	return as_predexp_compare(AS_PREDEXP_INTEGER_GREATER);
}

as_predexp_base * as_predexp_integer_greatereq()
{
	return as_predexp_compare(AS_PREDEXP_INTEGER_GREATEREQ);
}

as_predexp_base * as_predexp_integer_less()
{
	return as_predexp_compare(AS_PREDEXP_INTEGER_LESS);
}

as_predexp_base * as_predexp_integer_lesseq()
{
	return as_predexp_compare(AS_PREDEXP_INTEGER_LESSEQ);
}

as_predexp_base * as_predexp_string_equal()
{
	return as_predexp_compare(AS_PREDEXP_STRING_EQUAL);
}

as_predexp_base * as_predexp_string_unequal()
{
	return as_predexp_compare(AS_PREDEXP_STRING_UNEQUAL);
}

as_predexp_base * as_predexp_geojson_within()
{
	return as_predexp_compare(AS_PREDEXP_GEOJSON_WITHIN);
}

as_predexp_base * as_predexp_geojson_contains()
{
	return as_predexp_compare(AS_PREDEXP_GEOJSON_CONTAINS);
}

// ----------------------------------------------------------------
// as_predexp_string_regex
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	uint32_t			cflags;
} as_predexp_string_regex_t;

void as_predexp_string_regex_dtor(as_predexp_base * bp)
{
	as_predexp_string_regex_t * dp = (as_predexp_string_regex_t *) bp;
	cf_free(dp);
}

size_t as_predexp_string_regex_size(as_predexp_base * bp)
{
	// as_predexp_string_regex_t * dp = (as_predexp_string_regex_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += sizeof(uint32_t);								// cflags
	
	return sz;
}

uint8_t * as_predexp_string_regex_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_string_regex_t * dp = (as_predexp_string_regex_t *) bp;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(AS_PREDEXP_STRING_REGEX);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(sizeof(uint32_t));

	// value
	uint32_t * cflags_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*cflags_ptr = cf_swap_to_be32(dp->cflags);

	return p;
}

as_predexp_base * as_predexp_string_regex(uint32_t cflags)
{
	as_predexp_string_regex_t * dp =
		(as_predexp_string_regex_t *)
		cf_malloc(sizeof(as_predexp_string_regex_t));
	dp->base.dtor_fn = as_predexp_string_regex_dtor;
	dp->base.size_fn = as_predexp_string_regex_size;
	dp->base.write_fn = as_predexp_string_regex_write;
	dp->cflags = cflags;
	return (as_predexp_base *) dp;
}

// ----------------------------------------------------------------
// as_predexp_*_iterate_*
// ----------------------------------------------------------------

typedef struct {
	as_predexp_base		base;
	char *				varname;
	uint16_t			tag;		// Not written to wire
} as_predexp_iter_t;

void as_predexp_iter_dtor(as_predexp_base * bp)
{
	as_predexp_iter_t * dp = (as_predexp_iter_t *) bp;
	if (dp->varname)
		cf_free(dp->varname);
	cf_free(dp);
}

size_t as_predexp_iter_size(as_predexp_base * bp)
{
	as_predexp_iter_t * dp = (as_predexp_iter_t *) bp;

	size_t sz = sizeof(uint16_t) + sizeof(uint32_t);	// TAG + LEN
	sz += strlen(dp->varname);							// varname

	return sz;
}

uint8_t * as_predexp_iter_write(as_predexp_base * bp, uint8_t * p)
{
	as_predexp_iter_t * dp = (as_predexp_iter_t *) bp;

	size_t len = strlen(dp->varname);
	uint8_t bnlen = len;

	// TAG
	uint16_t * tag_ptr = (uint16_t *) p;
	p += sizeof(uint16_t);
	*tag_ptr = cf_swap_to_be16(dp->tag);

	// LEN
	uint32_t * len_ptr = (uint32_t *) p;
	p += sizeof(uint32_t);
	*len_ptr = cf_swap_to_be32(bnlen);

	// varname value
	char * bnptr = (char *) p;
	p += bnlen;
	memcpy(bnptr, dp->varname, bnlen);

	return p;
}

static as_predexp_base * as_predexp_iter(char const * varname, uint16_t tag)
{
	if (!varname) {
		as_log_error("missing var name argument");
		return NULL;
	}
	else if (strlen(varname) >= AS_BIN_NAME_MAX_SIZE) {
		as_log_error("var name \"%s\" too long", varname);
		return NULL;
	}

	as_predexp_iter_t * dp =
		(as_predexp_iter_t *)
		cf_malloc(sizeof(as_predexp_iter_t));
	dp->base.dtor_fn = as_predexp_iter_dtor;
	dp->base.size_fn = as_predexp_iter_size;
	dp->base.write_fn = as_predexp_iter_write;
	dp->varname = strdup(varname);
	dp->tag = tag;
	return (as_predexp_base *) dp;
}

as_predexp_base * as_predexp_list_iterate_or(char const * varname)
{
	return as_predexp_iter(varname, AS_PREDEXP_LIST_ITERATE_OR);
}

as_predexp_base * as_predexp_mapkey_iterate_or(char const * varname)
{
	return as_predexp_iter(varname, AS_PREDEXP_MAPKEY_ITERATE_OR);
}

as_predexp_base * as_predexp_mapval_iterate_or(char const * varname)
{
	return as_predexp_iter(varname, AS_PREDEXP_MAPVAL_ITERATE_OR);
}

as_predexp_base * as_predexp_list_iterate_and(char const * varname)
{
	return as_predexp_iter(varname, AS_PREDEXP_LIST_ITERATE_AND);
}

as_predexp_base * as_predexp_mapkey_iterate_and(char const * varname)
{
	return as_predexp_iter(varname, AS_PREDEXP_MAPKEY_ITERATE_AND);
}

as_predexp_base * as_predexp_mapval_iterate_and(char const * varname)
{
	return as_predexp_iter(varname, AS_PREDEXP_MAPVAL_ITERATE_AND);
}
