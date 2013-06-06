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

#include <aerospike/as_record.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_nil.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_record * as_record_new(uint32_t nbins) 
{
	as_record * rec = (as_record *) malloc(sizeof(as_record));
	rec->_free = true;
	rec->bins._free = true;
	rec->bins.capacity = nbins;
	rec->bins.size = 0;
	if ( nbins > 0 ) {
		rec->bins.data = (as_bin *) malloc(sizeof(as_bin) * nbins);
	}
	else {
		rec->bins.data = NULL;
	}
	return rec;
}

as_record * as_record_init(as_record * rec, uint32_t nbins) 
{
	rec->_free = false;
	rec->bins._free = true;
	rec->bins.capacity = nbins;
	rec->bins.size = 0;
	if ( nbins > 0 ) {
		rec->bins.data = (as_bin *) malloc(sizeof(as_bin) * nbins);
	}
	else {
		rec->bins.data = NULL;
	}
	return rec;
}

void as_record_destroy(as_record * rec) 
{
	if ( rec ) {
		if ( rec->bins.data && rec->bins._free ) {
			free(rec->bins.data);
			rec->bins.data = NULL;
			rec->bins.capacity = 0;
			rec->bins.size = 0;
		}
		if ( rec->_free ) {
			free(rec);
		}
	}
}


void as_record_set(as_record * rec, const char * name, as_val * val) 
{
	// replace
	for(int i = 0; i < rec->bins.size; i++) {
		if ( strcmp(rec->bins.data[i].name, name) == 0 ) {
			as_val_destroy(rec->bins.data[i].value);
			rec->bins.data[i].value = val;
			return;
		}
	}
	// not found, then append
	if ( rec->bins.size < rec->bins.capacity ) {
		strncpy(rec->bins.data[rec->bins.size].name,name,AS_BIN_NAME_MAX);
		rec->bins.data[rec->bins.size].value = val;
		rec->bins.size++;
	}
	// capacity exceeded
	// raise an error
}

void as_record_set_integer(as_record * rec, const char * name, int64_t i) 
{
	as_integer * val = as_integer_new(i);
	as_record_set(rec, name, (as_val *) val);
}

void as_record_set_string(as_record * rec, const char * name, const char * str) 
{
	as_string * val = as_string_new(strdup(str), true);
	as_record_set(rec, name, (as_val *) val);
}

void as_record_set_bytes(as_record * rec, const char * name, as_bytes * bytes) 
{
	as_record_set(rec, name, (as_val *) bytes);
}

void as_record_set_list(as_record * rec, const char * name, as_list * list) 
{
	as_record_set(rec, name, (as_val *) list);
}

void as_record_set_map(as_record * rec, const char * name, as_map * map) 
{
	as_record_set(rec, name, (as_val *) map);
}

void as_record_set_nil(as_record * rec, const char * name) {
	as_record_set(rec, name, (as_val *) &as_nil);
}


as_val * as_record_get(as_record * rec, const char * name) 
{
	for(int i=0; i<rec->bins.size; i++) {
		if ( strcmp(rec->bins.data[i].name, name) == 0 ) {
			return rec->bins.data[i].value;
		}
	}
	return &as_nil;
}

int64_t as_record_get_integer(as_record * rec, const char * name) 
{
	as_integer * val = as_integer_fromval(as_record_get(rec, name));
	return val ? as_integer_toint(val) : 0;
}

char * as_record_get_string(as_record * rec, const char * name) 
{
	as_string * val = as_string_fromval(as_record_get(rec, name));
	return val ? as_string_tostring(val) : NULL;
}

as_bytes * as_record_get_bytes(as_record * rec, const char * name) 
{
	return as_bytes_fromval(as_record_get(rec, name));
}

as_list * as_record_get_list(as_record * rec, const char * name) 
{
	return as_list_fromval(as_record_get(rec, name));
}

as_map * as_record_get_map(as_record * rec, const char * name) 
{
	return as_map_fromval(as_record_get(rec, name));
}
