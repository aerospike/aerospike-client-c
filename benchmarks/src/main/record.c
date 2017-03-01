/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_random.h>
#include <citrusleaf/cf_clock.h>

extern as_monitor monitor;

static const char alphanum[] =
	"0123456789"
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	"abcdefghijklmnopqrstuvwxyz";

static int alphanum_len = sizeof(alphanum) - 1;

static int
calc_list_or_map_ele_count(char bintype, int binlen, len_type binlen_type, int expected_ele_size)
{
	int len = binlen;

	switch (binlen_type) {
	case LEN_TYPE_KBYTES:
		len *= 1024;
		/* no break */
	case LEN_TYPE_BYTES:
		len /= expected_ele_size;
		if (bintype == 'M') {
			len /= 2;
		}
		break;
	default:
		break;
	}

	return len;
}

// Expected msgpack size is 9 bytes.
static as_val *
random_element_9b(as_random *ran)
{
	int type = (int)(as_random_next_uint64(ran) % 2);

	if (type == 0) {
		return (as_val *)as_integer_new(as_random_next_uint64(ran));
	}

	// Len is 4 to 10, average 7 with 2 bytes of msgpack
	// string header results in an expected value of 9 bytes.
	int len = (int)(as_random_next_uint64(ran) % 6) + 4;
	uint8_t buf[len + 1];
	as_random_next_bytes(ran, buf, len);

	for (int i = 0; i < len; i++) {
		buf[i] = alphanum[buf[i] % alphanum_len];
	}
	buf[len] = 0;

	return (as_val *)as_string_new_strdup((char *)buf);
}

int
gen_value(arguments* args, as_val** valpp)
{
	switch (args->bintype) {
		case 'I': {
			// Generate integer.
			uint32_t v = as_random_get_uint32();
			*valpp = (as_val *) as_integer_new(v);
			break;
		}
			
		case 'B': {
			// Generate byte array on heap.
			int len = args->binlen;
			uint8_t* buf = cf_malloc(len);
			as_random_get_bytes(buf, len);
			*valpp = (as_val *) as_bytes_new_wrap(buf, len, true);
			break;
		}
			
		case 'S': {
			// Generate random bytes on heap and convert to alphanumeric string.
			int len = args->binlen;
			uint8_t* buf = cf_malloc(len+1);
			as_random_get_bytes(buf, len);
			
			for (int i = 0; i < len; i++) {
				buf[i] = alphanum[buf[i] % alphanum_len];
			}
			buf[len] = 0;
			*valpp = (as_val *) as_string_new((char *)buf, true);
			break;
		}

		case 'L': {
			int len = calc_list_or_map_ele_count(args->bintype, args->binlen, args->binlen_type, 9);
			*valpp = (as_val *)as_arraylist_new(len, 0);

			for (size_t i = 0; i < len; i++) {
				as_list_append((as_list *)(*valpp), random_element_9b(as_random_instance()));
			}
			break;
		}

		case 'M': {
			int len = calc_list_or_map_ele_count(args->bintype, args->binlen, args->binlen_type, 9);
			*valpp = (as_val *)as_hashmap_new(len);

			for (size_t i = 0; i < len; i++) {
				as_map_set((as_map *)(*valpp), random_element_9b(as_random_instance()), random_element_9b(as_random_instance()));
			}
			break;
		}

		default: {
			blog_error("Unknown type %c", args->bintype);
			return -1;
		}
	}
	return 0;
}

threaddata*
create_threaddata(clientdata* cdata, uint64_t key)
{
	int len = 0;
	
	// Only random bin values need a thread local buffer.
	if (cdata->random) {
		switch (cdata->bintype)
		{
			case 'I': {
				// Integer does not use buffer.
				break;
			}
				
			case 'B': {
				// Create thread local byte buffer.
				len = cdata->binlen;
				break;
			}
				
			case 'S': {
				// Create thread local string buffer.
				len = cdata->binlen + 1;
				break;
			}

			case 'L':
			case 'M':
				// Does not need buffer.
				break;
				
			default: {
				blog_error("Unknown type %c", cdata->bintype);
				return 0;
			}
		}
	}

	threaddata* tdata = malloc(sizeof(threaddata));
	tdata->cdata = cdata;
	tdata->random = as_random_instance();
	tdata->buffer = len != 0 ? malloc(len) : NULL;
	tdata->begin = 0;
	
	// Initialize a thread local key, record.
	as_key_init_int64(&tdata->key, cdata->namespace, cdata->set, key);
	as_record_init(&tdata->rec, cdata->numbins);
	for (int i = 0; i < cdata->numbins; i++) {
		tdata->rec.bins.entries[i].valuep = NULL;
	}
	tdata->rec.bins.size = cdata->numbins;
	
	as_val_reserve(cdata->fixed_value);
	return tdata;
}

void
destroy_threaddata(threaddata* tdata)
{
	as_key_destroy(&tdata->key);
	as_record_destroy(&tdata->rec);
	free(tdata->buffer);
	free(tdata);
}

static void
init_write_record(clientdata* cdata, threaddata* tdata)
{
	if (cdata->del_bin) {
		for (int i = 0; i < cdata->numbins; i++) {
			as_bin* bin = &tdata->rec.bins.entries[i];
			if (i==0) {
				strcpy(bin->name, cdata->bin_name);
			} else {
				sprintf(bin->name, "%s_%d", cdata->bin_name, i);
			}
			as_record_set_nil(&tdata->rec, bin->name);
		}
		return;
	}
	
	for (int i = 0; i <cdata->numbins; i++) {
		as_bin* bin = &tdata->rec.bins.entries[i];
		if (i==0) {
			strcpy(bin->name, cdata->bin_name);
		} else {
			sprintf(bin->name, "%s_%d", cdata->bin_name, i);
		}

		if (cdata->random) {
			// Generate random value.
			switch (cdata->bintype)
			{
				case 'I': {
							  // Generate integer.
							  uint32_t i = as_random_next_uint32(tdata->random);
							  as_integer_init((as_integer*)&bin->value, i);
							  bin->valuep = &bin->value;
							  break;
						  }

				case 'B': {
							  // Generate byte array in thread local buffer.
							  uint8_t* buf = tdata->buffer;
							  int len = cdata->binlen;
							  as_random_next_bytes(tdata->random, buf, len);
							  as_bytes_init_wrap((as_bytes*)&bin->value, buf, len, false);
							  bin->valuep = &bin->value;
							  break;
						  }

				case 'S': {
							  // Generate random bytes on stack and convert to alphanumeric string.
							  uint8_t* buf = tdata->buffer;
							  int len = cdata->binlen;
							  as_random_next_bytes(tdata->random, buf, len);

							  for (int i = 0; i < len; i++) {
								  buf[i] = alphanum[buf[i] % alphanum_len];
							  }
							  buf[len] = 0;
							  as_string_init((as_string *)&bin->value, (char*)buf, false);
							  bin->valuep = &bin->value;
							  break;
						  }

				case 'L': {
							  int len = calc_list_or_map_ele_count(cdata->bintype, cdata->binlen, cdata->binlen_type, 9);
							  as_list *list = (as_list *)as_arraylist_new((uint32_t)len, 0);

							  for (int i = 0; i < len; i++) {
								  as_list_append(list, random_element_9b(tdata->random));
							  }

							  as_record_set_list(&tdata->rec, cdata->bin_name, list);
							  break;
						  }

				case 'M': {
							  int len = calc_list_or_map_ele_count(cdata->bintype, cdata->binlen, cdata->binlen_type, 9);
							  as_map *map = (as_map *)as_hashmap_new((uint32_t)len);

							  for (int i = 0; i < len; i++) {
								  as_val *k = random_element_9b(tdata->random);
								  as_val *v = random_element_9b(tdata->random);
								  as_map_set(map, k, v);
							  }

							  as_record_set_map(&tdata->rec, cdata->bin_name, map);
							  break;
						  }

				default: {
							 blog_error("Unknown type %c", cdata->bintype);
							 break;
						 }
			}
		}
		else {
			// Use fixed value.
			((as_val*)&bin->value)->type = cdata->fixed_value->type;
			bin->valuep = (as_bin_value *)cdata->fixed_value;
		}
	}
}

void
write_record_sync(clientdata* cdata, threaddata* tdata, uint64_t key)
{
	// Initialize key
	tdata->key.value.integer.value = key;
	tdata->key.digest.init = false;

	// Initialize record
	init_write_record(cdata, tdata);
	
	as_status status;
	as_error err;
	
	if (cdata->latency) {
		uint64_t begin = cf_getms();
		status = aerospike_key_put(&cdata->client, &err, 0, &tdata->key, &tdata->rec);
		uint64_t end = cf_getms();
		
		if (status == AEROSPIKE_OK) {
			ck_pr_inc_32(&cdata->write_count);
			latency_add(&cdata->write_latency, end - begin);
			return;
		}
	}
	else {
		status = aerospike_key_put(&cdata->client, &err, 0, &tdata->key, &tdata->rec);
		
		if (status == AEROSPIKE_OK) {
			ck_pr_inc_32(&cdata->write_count);
			return;
		}
	}
	
	// Handle error conditions.
	if (status == AEROSPIKE_ERR_TIMEOUT) {
		ck_pr_inc_32(&cdata->write_timeout_count);
	}
	else {
		ck_pr_inc_32(&cdata->write_error_count);
		
		if (cdata->debug) {
			blog_error("Write error: ns=%s set=%s key=%d bin=%s code=%d message=%s",
					   cdata->namespace, cdata->set, key, cdata->bin_name, status, err.message);
		}
	}
}

int
read_record_sync(uint64_t keyval, clientdata* data)
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
			ck_pr_inc_32(&data->read_count);
			latency_add(&data->read_latency, end - begin);
			as_record_destroy(rec);
			return status;
		}
	}
	else {
		status = aerospike_key_get(&data->client, &err, 0, &key, &rec);
		
		// Record may not have been initialized, so not found is ok.
		if (status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			ck_pr_inc_32(&data->read_count);
			as_record_destroy(rec);
			return status;
		}
	}
	
	// Handle error conditions.
	if (status == AEROSPIKE_ERR_TIMEOUT) {
		ck_pr_inc_32(&data->read_timeout_count);
	}
	else {
		ck_pr_inc_32(&data->read_error_count);
		
		if (data->debug) {
			blog_error("Read error: ns=%s set=%s key=%d bin=%s code=%d message=%s",
					   data->namespace, data->set, keyval, data->bin_name, status, err.message);
		}
	}
	
	as_record_destroy(rec);
	return status;
}

void
throttle(clientdata* cdata) {
	if (cdata->throughput > 0) {
		int transactions = cdata->write_count + cdata->read_count;

		if (transactions >= cdata->throughput) {
			int64_t millis = (int64_t)cdata->period_begin + 1000L -
					(int64_t)cf_getms();

			if (millis > 0) {
				usleep((uint32_t)millis * 1000);
			}
		}
	}
}

static void linear_write_listener(as_error* err, void* udata, as_event_loop* event_loop);

void
linear_write_async(clientdata* cdata, threaddata* tdata, as_event_loop* event_loop)
{
	init_write_record(cdata, tdata);
	
	if (cdata->latency) {
		tdata->begin = cf_getms();
	}
	
	as_error err;
	
	if (aerospike_key_put_async(&cdata->client, &err, NULL, &tdata->key, &tdata->rec, linear_write_listener, tdata, event_loop, NULL) != AEROSPIKE_OK) {
		linear_write_listener(&err, tdata, event_loop);
	}
}

static void
linear_write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	threaddata* tdata = udata;
	clientdata* cdata = tdata->cdata;
	
	if (!err) {
		if (cdata->latency) {
			uint64_t end = cf_getms();
			latency_add(&cdata->write_latency, end - tdata->begin);
		}
		ck_pr_inc_32(&cdata->write_count);
	}
	else {
		if (err->code == AEROSPIKE_ERR_TIMEOUT) {
			ck_pr_inc_32(&cdata->write_timeout_count);
		}
		else {
			ck_pr_inc_32(&cdata->write_error_count);
			
			if (cdata->debug) {
				blog_error("Write error: ns=%s set=%s key=%d bin=%s code=%d message=%s",
						   cdata->namespace, cdata->set, tdata->key.value.integer.value,
						   cdata->bin_name, err->code, err->message);
			}
		}
	}
	
	// Reuse tdata structures.
	uint64_t key = ck_pr_faa_64(&cdata->key_count, 1) + cdata->key_min;
	
	if (key == cdata->key_max) {
		// We have reached max number of records.
		destroy_threaddata(tdata);
		as_monitor_notify(&monitor);
		return;
	}
	
	key += cdata->async_max_commands;
	
	if (key > cdata->key_max) {
		// We already have enough records in progress, so do not issue any more puts.
		destroy_threaddata(tdata);
		return;
	}
	
	tdata->key.value.integer.value = key;
	tdata->key.digest.init = false;
	linear_write_async(cdata, tdata, event_loop);
}

static void random_write_listener(as_error* err, void* udata, as_event_loop* event_loop);
static void random_read_listener(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop);

void
random_read_write_async(clientdata* cdata, threaddata* tdata, as_event_loop* event_loop)
{
	uint64_t n_keys = cdata->key_max - cdata->key_min;

	// Choose key at random.
	uint64_t key = as_random_next_uint64(tdata->random) % n_keys + cdata->key_min;
	tdata->key.value.integer.value = key;
	tdata->key.digest.init = false;
	
	int die = as_random_next_uint32(tdata->random) % 100;
	as_error err;
	
	if (die < cdata->read_pct) {
		if (cdata->latency) {
			tdata->begin = cf_getms();
		}
		
		if (aerospike_key_get_async(&cdata->client, &err, NULL, &tdata->key, random_read_listener, tdata, event_loop, NULL) != AEROSPIKE_OK) {
			random_read_listener(&err, NULL, tdata, event_loop);
		}
	}
	else {
		init_write_record(cdata, tdata);
		
		if (cdata->latency) {
			tdata->begin = cf_getms();
		}
		
		if (aerospike_key_put_async(&cdata->client, &err, NULL, &tdata->key, &tdata->rec, random_write_listener, tdata, event_loop, NULL) != AEROSPIKE_OK) {
			random_write_listener(&err, tdata, event_loop);
		}
	}
}

static void
random_write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
	threaddata* tdata = udata;
	clientdata* cdata = tdata->cdata;
	
	if (!err) {
		if (cdata->latency) {
			uint64_t end = cf_getms();
			latency_add(&cdata->write_latency, end - tdata->begin);
		}
		ck_pr_inc_32(&cdata->write_count);
	}
	else {
		if (err->code == AEROSPIKE_ERR_TIMEOUT) {
			ck_pr_inc_32(&cdata->write_timeout_count);
		}
		else {
			ck_pr_inc_32(&cdata->write_error_count);
			
			if (cdata->debug) {
				blog_error("Write error: ns=%s set=%s key=%d bin=%s code=%d message=%s",
						   cdata->namespace, cdata->set, tdata->key.value.integer.value,
						   cdata->bin_name, err->code, err->message);
			}
		}
	}
	
	ck_pr_inc_64(&cdata->transactions_count);

	if (cdata->valid) {
		// Start a new command on same event loop to keep the queue full.
		random_read_write_async(cdata, tdata, event_loop);
	}
	else {
		// We have reached max number of records.
		destroy_threaddata(tdata);
		as_monitor_notify(&monitor);
	}
}

static void
random_read_listener(as_error* err, as_record* rec, void* udata, as_event_loop* event_loop)
{
	threaddata* tdata = udata;
	clientdata* cdata = tdata->cdata;
	
	if (!err || err->code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		if (cdata->latency) {
			uint64_t end = cf_getms();
			latency_add(&cdata->read_latency, end - tdata->begin);
		}
		ck_pr_inc_32(&cdata->read_count);
	}
	else {
		if (err->code == AEROSPIKE_ERR_TIMEOUT) {
			ck_pr_inc_32(&cdata->read_timeout_count);
		}
		else {
			ck_pr_inc_32(&cdata->read_error_count);
			
			if (cdata->debug) {
				blog_error("Read error: ns=%s set=%s key=%d bin=%s code=%d message=%s",
						   cdata->namespace, cdata->set, tdata->key.value.integer.value,
						   cdata->bin_name, err->code, err->message);
			}
		}
	}
	ck_pr_inc_64(&cdata->transactions_count);

	if (cdata->valid) {
		// Start a new command on same event loop to keep the queue full.
		random_read_write_async(cdata, tdata, event_loop);
	}
	else {
		// We have reached max number of records.
		destroy_threaddata(tdata);
		as_monitor_notify(&monitor);
	}
}
