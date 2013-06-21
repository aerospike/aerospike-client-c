/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_record * as_record_defaults(as_record * rec, bool free, uint16_t nbins);

static bool			as_record_rec_destroy(as_rec * r);
static uint32_t		as_record_rec_hashcode(const as_rec * r);

static as_val *		as_record_rec_get(const as_rec * r, const char * name) ;
static int 			as_record_rec_set(const as_rec * r, const char * name, const as_val * value) ;
static int 			as_record_rec_remove(const as_rec * r, const char * name) ;
static uint32_t 	as_record_rec_ttl(const as_rec * r);
static uint16_t 	as_record_rec_gen(const as_rec * r) ;
static as_bytes * 	as_record_rec_digest(const as_rec * r) ;
static uint16_t 	as_record_rec_numbins(const as_rec * r) ;

/******************************************************************************
 * CONSTANTS
 ******************************************************************************/

const as_rec_hooks as_record_rec_hooks = {
	.destroy	= as_record_rec_destroy,
	.hashcode	= as_record_rec_hashcode,

	.get		= as_record_rec_get,
	.set		= as_record_rec_set,
	.remove		= as_record_rec_remove,
	.ttl		= as_record_rec_ttl,
	.gen		= as_record_rec_gen,
	.numbins	= as_record_rec_numbins,
	.digest		= as_record_rec_digest
};

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_record * as_record_defaults(as_record * rec, bool free, uint16_t nbins) 
{
	as_rec * r = &rec->_;
	as_val_init(&r->_, AS_REC, free);
	r->data = rec;
	r->hooks = &as_record_rec_hooks;

	rec->key._free = false;
	rec->key.namespace = NULL;
	rec->key.set = NULL;
	rec->key.valuep = NULL;

	rec->key.digest.init = false;
	memset(rec->key.digest.value, 0, AS_DIGEST_VALUE_SIZE);

	rec->gen = 0;
	rec->ttl = 0;

	if ( nbins > 0 ) {
		rec->bins._free = true;
		rec->bins.capacity = nbins;
		rec->bins.size = 0;
		rec->bins.entries = (as_bin *) malloc(sizeof(as_bin) * nbins);
	}
	else {
		rec->bins._free = false;
		rec->bins.capacity = 0;
		rec->bins.size = 0;
		rec->bins.entries = NULL;
	}

	return rec;
}

static void as_record_release(as_record * rec) 
{
	if ( rec ) {

		if ( rec->bins.entries && rec->bins._free ) {
			free(rec->bins.entries);
		}
		rec->bins.entries = NULL;
		rec->bins.capacity = 0;
		rec->bins.size = 0;

		rec->key.namespace = NULL;
		rec->key.set = NULL;

		as_val_destroy((as_val *) rec->key.valuep);
		rec->key.valuep = NULL;

		rec->key.digest.init = false;
	}
}

static bool as_record_rec_destroy(as_rec * r) 
{
	as_record_release((as_record *) r);
	return true;
}

static uint32_t as_record_rec_hashcode(const as_rec * r) 
{
	as_record * rec = (as_record *) r;

	uint32_t hash = 0;

	for(int i = 0; i < rec->bins.size; i++) {
		int c;
		char * str = rec->bins.entries[i].name;
		while ( (c = *str++) ) {
			hash += c + (hash << 6) + (hash << 16) - hash;
		}
		if ( rec->bins.entries[i].valuep != NULL ) {
			hash += as_val_hashcode(rec->bins.entries[i].valuep);
		}
	}

	return hash;
}


static as_val * as_record_rec_get(const as_rec * r, const char * name) 
{
	return r && name ? as_record_get((as_record *) r, name) : NULL;
}

static int as_record_rec_set(const as_rec * r, const char * name, const as_val * value) 
{
	return r && name ? as_record_set((as_record *) r, name, (as_bin_value *)value) : 1;
}

static int as_record_rec_remove(const as_rec * r, const char * name) 
{
	return r && name ? as_record_remove((as_record *) r, name) : 1;
}

static uint32_t as_record_rec_ttl(const as_rec * r) 
{
	return r ? ((as_record *) r)->ttl : 0;
}

static uint16_t  as_record_rec_gen(const as_rec * r) 
{
	return r ? ((as_record *) r)->gen : 0;
}

static as_bytes * as_record_rec_digest(const as_rec * r) 
{
	return r ? as_bytes_new(((as_record *) r)->key.digest.value, AS_DIGEST_VALUE_SIZE, false) : NULL;
}

static uint16_t as_record_rec_numbins(const as_rec * r) 
{
	return r ? as_record_numbins((as_record *) r) : 0;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Create a new as_record on the heap.
 *
 *		as_record * r = as_record_new(2);
 *		as_record_set_int64(r, "bin1", 123);
 *		as_record_set_str(r, "bin1", "abc");
 *
 * @param nbins - the number of bins to initialize. Set to 0, if unknown.
 *
 * @return a pointer to the new as_record if successful, otherwise NULL.
 */
as_record * as_record_new(uint16_t nbins) 
{
	as_record * rec = (as_record *) malloc(sizeof(as_record));
	if ( !rec ) return rec;
	return as_record_defaults(rec, true, nbins);
}

/**
 * Initializes an as_record created on the stack.
 *
 *		as_record r;
 *		as_record_init(&r, 2);
 *		as_record_set_int64(&r, "bin1", 123);
 *		as_record_set_str(&r, "bin1", "abc");
 *
 * @param rec 	- the record to initialize
 * @param nbins - the number of bins to initialize. Set to 0, if unknown.
 *
 * @return a pointer to the initialized as_record if successful, otherwise NULL.
 */
as_record * as_record_init(as_record * rec, uint16_t nbins) 
{
	if ( !rec ) return rec;
	return as_record_defaults(rec, false, nbins);
}

/**
 * Destroy the as_record and associated resources.
 */
void as_record_destroy(as_record * rec) 
{
	as_rec_destroy((as_rec *) rec);
}

/**
 * Get the number of bins in the record.
 *
 * @param rec - the record
 *
 * @return the number of bins in the record.
 */
uint16_t as_record_numbins(as_record * rec) 
{
	return rec ? rec->bins.size : 0;
}


/**
 * Set specified bin's value to an as_val (as_integer, as_string, as_bytes, as_list, as_map).
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set(as_record * rec, const char * name, as_bin_value * value) 
{
	// TODO - perhaps check bin name length in here, so deeper copies don't
	// strncpy and truncate... this can work if we can hide the whole as_bin API

	// replace
	for(int i = 0; i < rec->bins.size; i++) {
		if ( strcmp(rec->bins.entries[i].name, name) == 0 ) {
			as_val_destroy(rec->bins.entries[i].valuep);
			rec->bins.entries[i].valuep = value;
			return 0;
		}
	}
	// not found, then append
	if ( rec->bins.size < rec->bins.capacity ) {
		as_bin_init(&rec->bins.entries[rec->bins.size], name, value);
		rec->bins.size++;
		return 0;
	}
	// capacity exceeded
	// raise an error
	return 1;
}

/**
 * Set specified bin's value to an int64_t.
 *
 *		as_record_set_int64(rec, "bin", 123);
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set_int64(as_record * rec, const char * name, int64_t value) 
{
	as_integer * val = as_integer_new(value);
	return as_record_set(rec, name, (as_bin_value *) val);
}

/**
 * Set specified bin's value to an NULL terminates string.
 *
 *		as_record_set_str(rec, "bin", "abc");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set_str(as_record * rec, const char * name, const char * value) 
{
	as_string * val = as_string_new(strdup(value), true);
	return as_record_set(rec, name, (as_bin_value *) val);
}

/**
 * Set specified bin's value to an as_integer.
 *
 *		as_record_set_integer(rec, "bin", as_integer_new(123));
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set_integer(as_record * rec, const char * name, as_integer * value) 
{
	return as_record_set(rec, name, (as_bin_value *) value);
}

/**
 * Set specified bin's value to an as_string.
 *
 *		as_record_set_string(rec, "bin", as_string_new("abc", false));
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set_string(as_record * rec, const char * name, as_string * value) 
{
	return as_record_set(rec, name, (as_bin_value *) value);
}

/**
 * Set specified bin's value to an as_bytes.
 *
 *		as_record_set_integer(rec, "bin", bytes);
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set_bytes(as_record * rec, const char * name, as_bytes * value) 
{
	return as_record_set(rec, name, (as_bin_value *) value);
}

/**
 * Set specified bin's value to an as_list.
 *
 *		as_list list;
 *		as_arraylist_init(&list);
 *		as_list_add_int64(&list, 1);
 *		as_list_add_int64(&list, 2);
 *		as_list_add_int64(&list, 3);
 *
 *		as_record_set_list(rec, "bin", &list);
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set_list(as_record * rec, const char * name, as_list * value) 
{
	return as_record_set(rec, name, (as_bin_value *) value);
}

/**
 * Set specified bin's value to an as_map.
 *
 *		as_map map;
 *		as_stringmap_init(&map);
 *		as_stringmap_set_int64(&map, "a", 1);
 *		as_stringmap_set_int64(&map, "b", 2);
 *		as_stringmap_set_int64(&map, "c", 3);
 *
 *		as_record_set_map(rec, "bin", &map);
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 */
int as_record_set_map(as_record * rec, const char * name, as_map * value) 
{
	return as_record_set(rec, name, (as_bin_value *) value);
}

/**
 * Set specified bin's value to as_nil.
 *
 *		as_record_set_nil(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set_nil(as_record * rec, const char * name) {
	return as_record_set(rec, name, (as_bin_value *) &as_nil);
}

/**
 * Get specified bin's value as an as_val (as_integer, as_string, as_bytes, as_list, as_map).
 *
 *		as_val * value = as_record_get(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise NULL.
 */
as_val * as_record_get(as_record * rec, const char * name) 
{
	for(int i=0; i<rec->bins.size; i++) {
		if ( strcmp(rec->bins.entries[i].name, name) == 0 ) {
			return (as_val *) rec->bins.entries[i].valuep;
		}
	}
	return (as_val *) &as_nil;
}

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
int64_t as_record_get_int64(as_record * rec, const char * name, int64_t fallback) 
{
	as_integer * val = as_integer_fromval(as_record_get(rec, name));
	return val ? as_integer_toint(val) : fallback;
}

/**
 * Get specified bin's value as an NULL terminated string.
 *
 *		char * value = as_record_get_str(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise NULL.
 */
char * as_record_get_str(as_record * rec, const char * name) 
{
	as_string * val = as_string_fromval(as_record_get(rec, name));
	return val ? as_string_tostring(val) : NULL;
}

/**
 * Get specified bin's value as an as_integer.
 *
 *		as_integer * value = as_record_get_integer(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise NULL.
 */
as_integer * as_record_get_integer(as_record * rec, const char * name)
{
	return as_integer_fromval(as_record_get(rec, name));
}

/**
 * Get specified bin's value as an as_string.
 *
 *		as_string * value = as_record_get_string(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise NULL.
 */
as_string * as_record_get_string(as_record * rec, const char * name)
{
	return as_string_fromval(as_record_get(rec, name));
}

/**
 * Get specified bin's value as an as_bytes.
 *
 *		as_bytes * value = as_record_get_bytes(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise NULL.
 */
as_bytes * as_record_get_bytes(as_record * rec, const char * name) 
{
	return as_bytes_fromval(as_record_get(rec, name));
}

/**
 * Get specified bin's value as an as_list.
 *
 *		as_list * value = as_record_get_list(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise NULL.
 */
as_list * as_record_get_list(as_record * rec, const char * name) 
{
	return as_list_fromval(as_record_get(rec, name));
}

/**
 * Get specified bin's value as an as_map.
 *
 *		as_map * value = as_record_get_map(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise NULL.
 */
as_map * as_record_get_map(as_record * rec, const char * name) 
{
	return as_map_fromval(as_record_get(rec, name));
}

/**
 * Remove a bin from the record
 *
 *		as_record_remove(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return 0 on success. Otherwise a failure.
 */
int as_record_remove(as_record * rec, const char * name)
{
	return as_record_set_nil(rec, name);
}
