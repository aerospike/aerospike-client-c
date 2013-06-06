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
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include <stdint.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

struct as_record_s {
	bool        _free;
	as_digest   digest;
	uint32_t    gen;
	uint32_t    ttl;
	struct {
		bool     _free;
		uint32_t capacity;
		uint32_t size;
		as_bin * data;
	} bins;
};

typedef struct as_record_s as_record;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_record * as_record_new(uint32_t nbins);

as_record * as_record_init(as_record * rec, uint32_t nbins);

void as_record_destroy(as_record * rec);


void as_record_set(as_record * rec, const char * name, as_val * val);

void as_record_set_integer(as_record * rec, const char * name, int64_t i);

void as_record_set_string(as_record * rec, const char * name, const char * str);

void as_record_set_bytes(as_record * rec, const char * name, as_bytes * bytes);

void as_record_set_list(as_record * rec, const char * name, as_list * list);

void as_record_set_map(as_record * rec, const char * name, as_map * map);

void as_record_set_nil(as_record * rec, const char * name);


as_val * as_record_get(as_record * rec, const char * name);

int64_t as_record_get_integer(as_record * rec, const char * name);

char * as_record_get_string(as_record * rec, const char * name);

as_bytes * as_record_get_bytes(as_record * rec, const char * name);

as_list * as_record_get_list(as_record * rec, const char * name);

as_map * as_record_get_map(as_record * rec, const char * name);


void as_record_remove(as_record * rec, const char * name);

