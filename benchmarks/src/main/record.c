/*******************************************************************************
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
 ******************************************************************************/
#include "benchmark.h"
#include "aerospike/aerospike_key.h"
#include <citrusleaf/cf_clock.h>

static const char alphanum[] =
	"0123456789"
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	"abcdefghijklmnopqrstuvwxyz";

static int alphanum_len = sizeof(alphanum) - 1;

uint32_t cf_get_rand32();
int cf_get_rand_buf(uint8_t *buf, int len);

int
gen_value(arguments* args, as_bin_value* val)
{
	switch (args->bintype) {
		case 'I': {
			// Generate integer.
			uint32_t v = cf_get_rand32();
			as_integer_init((as_integer*)val, v);
			break;
		}
			
		case 'B': {
			// Generate byte array on heap.
			int len = args->binlen;
			uint8_t* buf = cf_malloc(len);
			cf_get_rand_buf(buf, len);
			as_bytes_init_wrap((as_bytes*)val, buf, len, true);
			break;
		}
			
		case 'S': {
			// Generate random bytes on heap and convert to alphanumeric string.
			int len = args->binlen;
			uint8_t* buf = cf_malloc(len+1);
			cf_get_rand_buf(buf, len);
			
			for (int i = 0; i < len; i++) {
				buf[i] = alphanum[buf[i] % alphanum_len];
			}
			buf[len] = 0;
			as_string_init((as_string*)val, (char*)buf, true);
			break;
		}
			
		default: {
			blog_error("Unknown type %c", args->bintype);
			return -1;
		}
	}
	return 0;
}

static as_status
put_record(int keyval, as_record* rec, clientdata* data)
{
	as_key key;
	as_key_init_int64(&key, data->namespace, data->set, keyval);
	
	as_status status;
	as_error err;

	if (data->latency) {
		uint64_t begin = cf_getms();
		status = aerospike_key_put(&data->client, &err, 0, &key, rec);
		uint64_t end = cf_getms();
		
		if (status == AEROSPIKE_OK) {
			cf_atomic32_incr(&data->write_count);
			latency_add(&data->write_latency, end - begin);
			return status;
		}
	}
	else {
		status = aerospike_key_put(&data->client, &err, 0, &key, rec);
		
		if (status == AEROSPIKE_OK) {
			cf_atomic32_incr(&data->write_count);
			return status;
		}
	}

	// Handle error conditions.
	if (status == AEROSPIKE_ERR_TIMEOUT) {
		cf_atomic32_incr(&data->write_timeout_count);
	}
	else {
		cf_atomic32_incr(&data->write_error_count);
		
		if (data->debug) {
			blog_error("Write error: ns=%s set=%s key=%d bin=%s code=%d message=%s",
				data->namespace, data->set, keyval, data->bin_name, status, err.message);
		}
	}
	return status;
}

int
write_record(int keyval, clientdata* data)
{
	as_record rec;
	as_record_inita(&rec, 1);
	
	as_status status;

	if (data->random) {
		// Generate random value.
		switch (data->bintype)
		{
			case 'I': {
				// Generate integer.
				uint32_t i = cf_get_rand32();
				as_record_set_int64(&rec, data->bin_name, i);
				status = put_record(keyval, &rec, data);
				break;
			}
				
			case 'B': {
				// Generate byte array on stack.
				int len = data->binlen;
				uint8_t buf[len];
				cf_get_rand_buf(buf, len);
				as_record_set_rawp(&rec, data->bin_name, buf, len, false);
				status = put_record(keyval, &rec, data);
				break;
			}
				
			case 'S': {
				// Generate random bytes on stack and convert to alphanumeric string.
				int len = data->binlen;
				uint8_t buf[len+1];
				cf_get_rand_buf(buf, len);
				
				for (int i = 0; i < len; i++) {
					buf[i] = alphanum[buf[i] % alphanum_len];
				}
				buf[len] = 0;
				as_record_set_strp(&rec, data->bin_name, (char*)buf, false);
				status = put_record(keyval, &rec, data);
				break;
			}
				
			default: {
				blog_error("Unknown type %c", data->bintype);
				status = AEROSPIKE_ERR_CLIENT;
				break;
			}
		}
	}
	else {
		// Use fixed value.
		as_record_set(&rec, data->bin_name, &data->fixed_value);
		status = put_record(keyval, &rec, data);
	}
	return (int)status;
}

int
read_record(int keyval, clientdata* data)
{
	as_key key;
	as_key_init_int64(&key, data->namespace, data->set, keyval);
	
	as_record* rec = 0;
	as_status status;
	as_error err;
	
	if (data->latency) {
		uint64_t begin = cf_getms();
		status = aerospike_key_get(&data->client, &err, 0, &key, &rec);
		uint64_t end = cf_getms();
		
		// Record may not have been initialized, so not found is ok.
		if (status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			cf_atomic32_incr(&data->read_count);
			latency_add(&data->read_latency, end - begin);
			return status;
		}
	}
	else {
		status = aerospike_key_get(&data->client, &err, 0, &key, &rec);
		
		// Record may not have been initialized, so not found is ok.
		if (status == AEROSPIKE_OK|| status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			cf_atomic32_incr(&data->read_count);
			return status;
		}
	}
	
	// Handle error conditions.
	if (status == AEROSPIKE_ERR_TIMEOUT) {
		cf_atomic32_incr(&data->read_timeout_count);
	}
	else {
		cf_atomic32_incr(&data->read_error_count);
		
		if (data->debug) {
			blog_error("Read error: ns=%s set=%s key=%d bin=%s code=%d message=%s",
				data->namespace, data->set, keyval, data->bin_name, status, err.message);
		}
	}
	
	as_record_destroy(rec);
	return status;
}
