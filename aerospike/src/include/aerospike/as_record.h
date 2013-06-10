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

#pragma once 

#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_digest.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include <stdint.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Represents a record, including:
 * - collection of bins
 * - the digest
 * - the generation value
 * - the time-to-live (expiry)
 *
 * Should only be created or initialized using either: 
 * - as_record_new() or
 * - as_record_init()
 *
 */
struct as_record_s {
	as_rec 		_;
	as_digest   digest;
	uint16_t    gen;
	uint32_t    ttl;
	struct {
		bool     _free;
		uint16_t capacity;
		uint16_t size;
		as_bin * data;
	} bins;
};

typedef struct as_record_s as_record;

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
as_record * as_record_new(uint16_t nbins);

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
as_record * as_record_init(as_record * rec, uint16_t nbins);

/**
 * Destroy the as_record and associated resources.
 */
void as_record_destroy(as_record * rec);

/**
 * Get the digest for the record.
 *
 * @param rec - the record containing the digest.
 *
 * @return the digest if exists, otherwise NULL.
 */
as_digest * as_record_get_digest(as_record * rec);

/**
 * Get the ttl for the record.
 *
 * @param rec - the record containing the ttl.
 *
 * @return the ttl value
 */
uint32_t as_record_get_ttl(as_record * rec);

/**
 * Get the generation value for the record.
 *
 * @param rec - the record containing the digest.
 *
 * @return the generation value
 */
uint16_t as_record_get_gen(as_record * rec);

/**
 * Get the number of bins in the record.
 *
 * @param rec - the record
 *
 * @return the number of bins in the record.
 */
uint16_t as_record_numbins(as_record * rec);

/**
 * Set specified bin's value to an as_val (as_integer, as_string, as_bytes, as_list, as_map).
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 * @param value - the value of the bin
 *
 * @return 0 on success. 1 on failure.
 */
int as_record_set(as_record * rec, const char * name, const as_val * value);

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
int as_record_set_int64(as_record * rec, const char * name, int64_t value);

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
int as_record_set_str(as_record * rec, const char * name, const char * value);

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
int as_record_set_integer(as_record * rec, const char * name, as_integer * value);

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
int as_record_set_string(as_record * rec, const char * name, as_string * value);

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
int as_record_set_bytes(as_record * rec, const char * name, as_bytes * value);

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
int as_record_set_list(as_record * rec, const char * name, as_list * value);

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
int as_record_set_map(as_record * rec, const char * name, as_map * value);

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
int as_record_set_nil(as_record * rec, const char * name);

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
as_val * as_record_get(as_record * rec, const char * name);

/**
 * Get specified bin's value as an int64_t.
 *
 *		int64_t value = as_record_get_int64(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return the value if it exists, otherwise 0.
 */
int64_t as_record_get_int64(as_record * rec, const char * name);

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
char * as_record_get_str(as_record * rec, const char * name);

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
as_integer * as_record_get_integer(as_record * rec, const char * name);

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
as_string * as_record_get_string(as_record * rec, const char * name);

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
as_bytes * as_record_get_bytes(as_record * rec, const char * name);

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
as_list * as_record_get_list(as_record * rec, const char * name);

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
as_map * as_record_get_map(as_record * rec, const char * name);

/**
 * Remove a bin from the record
 *
 *		as_record_remove(rec, "bin");
 *
 * @param rec 	- the record containing the bin
 * @param name 	- the name of the bin
 *
 * @return true if bin was removed, otherwise false.
 */
int as_record_remove(as_record * rec, const char * name);
