/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_record.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_tls.h>
#include <aerospike/as_val.h>
#include <pthread.h>
#include "../test.h"
#include "../util/log_helper.h"

//---------------------------------
// Globals
//---------------------------------

#define NAMESPACE "test"
#define SET "test_batch"
#define LIST_BIN "listbin"
#define N_KEYS 200

extern aerospike* as;
extern bool g_has_ttl;

uint32_t num_threads = 0;
pthread_rwlock_t rwlock;

static char bin1[] = "bin1";
static char bin2[] = "bin2";
static char bin3[] = "bin3";

//---------------------------------
// Types
//---------------------------------

typedef struct batch_stats_s {
	uint32_t thread_id;
	uint32_t total;
	uint32_t found;
	uint32_t errors;
	uint32_t last_error;
} batch_stats;

//---------------------------------
// Static Functions
//---------------------------------

static as_status
insert_record(as_error* err, int i)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, i);

	as_status status;

	// Do not write some records to test not found logic too.
	if (i % 20 == 0) {
		status = aerospike_key_remove(as, err, NULL, &key);
		return status == AEROSPIKE_ERR_RECORD_NOT_FOUND? AEROSPIKE_OK : status;
	}

	as_arraylist list;
	as_arraylist_inita(&list, i);

	for (int j = 0; j < i; j++) {
		as_arraylist_append_int64(&list, j * i);
	}

	as_record rec;
	
	// Some records should be missing bins to test bin filters.
	if (i % 25 == 0) {
		as_record_inita(&rec, 2);
		as_record_set_int64(&rec, bin1, i);
		as_record_set_list(&rec, LIST_BIN, (as_list*)&list);
	}
	else {
		as_record_inita(&rec, 3);
		as_record_set_int64(&rec, bin1, i);
		as_record_set_int64(&rec, bin2, i);
		as_record_set_list(&rec, LIST_BIN, (as_list*)&list);
	}

	status = aerospike_key_put(as, err, NULL, &key, &rec);
	as_record_destroy(&rec);
	return status;
}

static bool
before(atf_suite* suite)
{
	as_error err;
	as_status status;

	for (int i = 0; i < N_KEYS; i++) {
		status = insert_record(&err, i);

		if (status != AEROSPIKE_OK) {
			info("command[%d]: error(%d): %s", i, err.code, err.message);
			return false;
		}
	}

	// Add records that will be removed.
	for (int i = 10000; i <= 10002; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, i);

		as_record rec;
		as_record_init(&rec, 1);
		as_record_set_int64(&rec, bin1, i);

		status = aerospike_key_put(as, &err, NULL, &key, &rec);

		if (status != AEROSPIKE_OK) {
			info("error(%d): %s", err.code, err.message);
			as_record_destroy(&rec);
			return false;
		}
		as_record_destroy(&rec);
	}
	return true;
}

static bool
after(atf_suite* suite)
{
	as_error err;
	
	for (int i = 1; i < N_KEYS; i++) {
		as_key key;
		as_key_init_int64(&key, NAMESPACE, SET, i);
		
		as_status status = aerospike_key_remove(as, &err, NULL, &key);
		
		if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			info("error(%d): %s", err.code, err.message);
			return false;
		}
	}
	return true;
}

static bool
batch_get_1_callback(const as_batch_read* results, uint32_t n, void* udata)
{
	batch_stats* data = (batch_stats *) udata;
	
	data->total = n;

	for (uint32_t i = 0; i < n; i++) {

		if (results[i].result == AEROSPIKE_OK) {
			data->found++;

			int64_t key = as_integer_getorelse((as_integer *) results[i].key->valuep, -1);
			int64_t val = as_record_get_int64(&results[i].record, bin1, -1);
			if (key != val) {
				warn("key(%d) != val(%d)",key,val);
				data->errors++;
				data->last_error = -2;
			}
		}
		else if (results[i].result != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			data->errors++;
			data->last_error = results[i].result;
			warn("batch callback thread(%d) error(%d)", data->thread_id, data->last_error);
		}
	}

	info("total: %d, found: %d, errors: %d", data->total, data->found, data->errors);

	return true;
}

//---------------------------------
// Tests
//---------------------------------

TEST(batch_get_1, "Simple")
{
	as_error err;

	as_batch batch;
	as_batch_inita(&batch, N_KEYS);

	for (uint32_t i = 0; i < N_KEYS; i++) {
		as_key_init_int64(as_batch_keyat(&batch,i), NAMESPACE, SET, i+1);
	}

	batch_stats data = {0};

	aerospike_batch_get(as, &err, NULL, &batch, batch_get_1_callback, &data);
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq(err.code, AEROSPIKE_OK);

	assert_int_eq(data.found, N_KEYS - N_KEYS/20);
	assert_int_eq(data.errors, 0);
}

static void*
batch_get_function(void* thread_id)
{
	int thread_num = *(int*)thread_id;
	as_error err;
	
	as_batch batch;
	as_batch_inita(&batch, 20);
	
	int start_index = thread_num * 20;
	int end_index = start_index + 20;
	int j = 0;

	for (int i = start_index; i < end_index; i++) {
		as_key_init_int64(as_batch_keyat(&batch,j++), NAMESPACE, SET, i+1);
	}
	
	batch_stats data = {thread_num, 0, 0, 0, 0};
	
	as_incr_uint32(&num_threads);
	pthread_rwlock_rdlock(&rwlock);
	
	aerospike_batch_get(as, &err, NULL, &batch, batch_get_1_callback, &data);
	
	pthread_rwlock_unlock(&rwlock);

	if (err.code != AEROSPIKE_OK) {
		info("multi-thread error(%d): %s", err.code, err.message);
	}

	as_tls_thread_cleanup();
	return(0);
}

TEST(multithreaded_batch_get, "Batch Get - with multiple threads ")
{
	uint32_t threads = 10;
	
	pthread_t* batch_thread = alloca(sizeof(pthread_t) * threads);
	int* ids = alloca(sizeof(int) * threads);
	pthread_rwlock_init(&rwlock, NULL);
	pthread_rwlock_wrlock(&rwlock);
	for (uint32_t i = 0; i < threads; i++) {
		ids[i] = i;
		pthread_create(&batch_thread[i], 0, batch_get_function, &ids[i]);
	}
	
	while (as_load_uint32(&num_threads) < threads) {
		as_sleep(1 * 1000);
	}
	pthread_rwlock_unlock(&rwlock);
	
	for (uint32_t i = 0; i < threads; i++) {
		pthread_join(batch_thread[i], NULL);
	}
	pthread_rwlock_destroy(&rwlock);
}

static bool
batch_get_bins_callback(const as_batch_read* results, uint32_t n, void* udata)
{
	batch_stats* data = udata;

	data->total = n;
	
	for (uint32_t i = 0; i < n; i++) {
		
		if (results[i].result == AEROSPIKE_OK) {
			data->found++;
			
			int64_t val = as_record_get_int64(&results[i].record, bin1, -1);
			if (val != -1) {
				warn("val(%d) should not have been returned!", val);
				data->errors++;
				data->last_error = -2;
			}
			
			int64_t key = as_integer_getorelse((as_integer *) results[i].key->valuep, -1);
			int64_t val2 = as_record_get_int64(&results[i].record, bin2, -1);

			if (i % 25 == 0) {
				if (val2 != -1) {
					data->errors++;
					warn("val2(%d) exists when it shouldn't exist!", i, val2);
				}
			}
			else {
				if (val2 == -1 || key != val2) {
					warn("key(%d) != val2(%d)", key, val2);
					data->errors++;
					data->last_error = -2;
				}
			}
		}
		else if (results[i].result != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			data->errors++;
			data->last_error = results[i].result;
			warn("batch callback thread(%d) error(%d)", data->thread_id, data->last_error);
		}
	}
	
	info("total: %d, found: %d, errors: %d", data->total, data->found, data->errors);
	
	return true;
}

TEST(batch_get_bins, "Batch Get - with bin name filters")
{
	as_error err;
	
	as_batch batch;
	as_batch_inita(&batch, N_KEYS);
	
	for (uint32_t i = 0; i < N_KEYS; i++) {
		as_key_init_int64(as_batch_keyat(&batch,i), NAMESPACE, SET, i);
	}
	
	batch_stats data = {0};
	const char* bins[] = {bin2};
	
	aerospike_batch_get_bins(as, &err, NULL, &batch, bins, 1, batch_get_bins_callback, &data);
	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq(err.code, AEROSPIKE_OK);
	assert_int_eq(data.found, N_KEYS - N_KEYS/20);
	assert_int_eq(data.errors, 0);
}

TEST(batch_read_complex, "Batch read complex")
{
	// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
	as_batch_records records;
	as_batch_records_inita(&records, 9);
	
	char* bins[] = {bin1};
	uint32_t n_bins = 1;
	
	// get specified bins
	as_batch_read_record* record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 1);
	record->bin_names = bins;
	record->n_bin_names = n_bins;
	
	// get all bins
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 2);
	record->read_all_bins = true;
	
	// get all bins
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 3);
	record->read_all_bins = true;

	// exists
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 4);
	
	// get all bins
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 5);
	record->read_all_bins = true;
	
	// get all bins
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 6);
	record->read_all_bins = true;

	// get specified bins
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 7);
	record->bin_names = bins;
	record->n_bin_names = n_bins;

	// This record should be found, but the requested bin will not be found.
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 8);
	char* bins2[] = {"binnotfound"};
	record->bin_names = bins2;
	record->n_bin_names = 1;
	
	// This record should not be found.
	record = as_batch_read_reserve(&records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 20);
	record->bin_names = bins;
	record->n_bin_names = n_bins;

	as_error err;

	as_status status = aerospike_batch_read(as, &err, NULL, &records);
	
	uint32_t found = 0;
	uint32_t errors = 0;
	
	if (status == AEROSPIKE_OK) {
		as_vector* list = &records.list;
		for (uint32_t i = 0; i < list->size; i++) {
			as_batch_read_record* batch = as_vector_get(list, i);
			as_key* key = &batch->key;
			
			if (batch->result == AEROSPIKE_OK) {
				found++;
				
				if (batch->read_all_bins || batch->n_bin_names > 0) {
					int64_t val = as_record_get_int64(&batch->record, bin1, -1);
					
					if (val != -1) {
						info("Record: ns=%s set=%s key=%d bin=%d",
							 key->ns, key->set, (int)key->valuep->integer.value, (int)val);
					}
					else {
						info("Record: ns=%s set=%s key=%d bin=null",
							 key->ns, key->set, (int)key->valuep->integer.value);
					}
				}
				else {
					info("Record: ns=%s set=%s key=%d exists=true",
						 key->ns, key->set, (int)key->valuep->integer.value);
				}
			}
			else if (batch->result == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
				info("Record not found: ns=%s set=%s key=%d",
					 key->ns, key->set, (int)key->valuep->integer.value);
			}
			else {
				errors++;
				error("Unexpected error(%u): %s", i, as_error_string(batch->result));
			}
		}
	}
	as_batch_records_destroy(&records);
	
	if (status != AEROSPIKE_OK) {
		if (status == AEROSPIKE_ERR_UNSUPPORTED_FEATURE) {
			info("aerospike_batch_read() not supported by connected cluster");
			return;
		}
		error("error(%d): %s", err.code, err.message);
	}
	
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(found, 8);
	assert_int_eq(errors, 0);
}

static bool
batch_read_operate_cb(const as_batch_read* results, uint32_t n, void* udata)
{
	batch_stats* data = udata;
	data->total = n;

	for (uint32_t i = 0; i < n; i++) {
		const as_batch_read* r = &results[i];
		int k = (int)r->key->valuep->integer.value;

		if (r->result == AEROSPIKE_OK) {
			data->found++;

			as_bin* results = r->record.bins.entries;
			int v2 = (int)results[1].valuep->integer.value;
			int expected = k * (k - 1);

			if (v2 != expected) {
				data->errors++;
				warn("Result[%d]: v2(%d) != expected(%d)", k, v2, expected);
			}
		}
		else if (r->result != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			data->errors++;
			data->last_error = r->result;
			warn("Result[%d]: not found", k);
		}
	}
	return true;
}

TEST(batch_read_list_operate, "Batch read list operate")
{
	as_batch batch;
	as_batch_inita(&batch, N_KEYS);
	
	for (uint32_t i = 0; i < N_KEYS; i++) {
		as_key_init_int64(as_batch_keyat(&batch,i), NAMESPACE, SET, i);
	}

	// Get size and last element of list bin for all records.
	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_list_size(&ops, LIST_BIN, NULL);
	as_operations_list_get_by_index(&ops, LIST_BIN, NULL, -1, AS_LIST_RETURN_VALUE);

	batch_stats data = {0};
	as_error err;
	as_status status = aerospike_batch_get_ops(as, &err, NULL, &batch, &ops, batch_read_operate_cb,
											   &data);

	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.found, N_KEYS - N_KEYS/20);
	assert_int_eq(data.errors, 0);
}

void example_dump_record(const as_record* p_rec);

static bool
batch_write_operate_cb(const as_batch_result* results, uint32_t n, void* udata)
{
	batch_stats* data = udata;

	for (uint32_t i = 0; i < n; i++) {
		const as_batch_result* r = &results[i];
		int k = (int)r->key->valuep->integer.value;

		if (r->result == AEROSPIKE_OK) {
			data->found++;
			//example_dump_record(&r->record);
			as_bin* bins = r->record.bins.entries;
			int sz = (int)bins[1].valuep->integer.value;
			int sz_expect = (i % 20 == 0)? 1 : i + 1;

			if (sz != sz_expect) {
				warn("Result[%d]: size(%d) != expected(%d) %d", k, sz, sz_expect, i);
				data->errors++;
				continue;
			}

			int val = (int)bins[2].valuep->integer.value;
			int val_expect = (i % 20 == 0)? 1000 : k * (k - 1);

			if (val != val_expect) {
				warn("Result[%d]: last(%d) != expected(%d)", k, val, val_expect);
				data->errors++;
			}
		}
	}
	return true;
}

TEST(batch_write_list_operate, "Batch write list operate with default ttl")
{
	as_batch batch;
	as_batch_inita(&batch, N_KEYS);
	
	for (uint32_t i = 0; i < N_KEYS; i++) {
		as_key_init_int64(as_batch_keyat(&batch,i), NAMESPACE, SET, i);
	}

	// Add integer to list and get size and last element of list bin for all records.
	as_integer val;
	as_integer_init(&val, 1000);

	as_operations ops;
	as_operations_inita(&ops, 3);
	as_operations_list_insert(&ops, LIST_BIN, NULL, NULL, 0, (as_val*)&val);
	as_operations_list_size(&ops, LIST_BIN, NULL);
	as_operations_list_get_by_index(&ops, LIST_BIN, NULL, -1, AS_LIST_RETURN_VALUE);
	ops.ttl = AS_RECORD_CLIENT_DEFAULT_TTL;

	as_policy_batch_write p;
	as_policy_batch_write_init(&p);
	p.ttl = 5000;

	batch_stats data = {0};
	as_error err;
	as_status status = aerospike_batch_operate(as, &err, NULL, &p, &batch, &ops,
		batch_write_operate_cb, &data);

	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.found, N_KEYS);
	assert_int_eq(data.errors, 0);
}

TEST(batch_write_complex, "Batch write complex")
{
	as_exp_build(wexp1, as_exp_add(as_exp_bin_int(bin1), as_exp_int(1000)));

	as_operations wops1;
	as_operations_inita(&wops1, 1);
	as_operations_add_write_int64(&wops1, bin2, 100);
	wops1.ttl = AS_RECORD_CLIENT_DEFAULT_TTL;

	as_operations wops2;
	as_operations_inita(&wops2, 1);
	as_operations_exp_write(&wops2, bin3, wexp1, AS_EXP_WRITE_DEFAULT);
	wops2.ttl = AS_RECORD_CLIENT_DEFAULT_TTL;

	as_policy_batch_write wp;
	as_policy_batch_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;
	wp.ttl = 500;

	as_batch_records recs;
	as_batch_records_inita(&recs, 3);

	as_batch_write_record* wr1 = as_batch_write_reserve(&recs);
	as_key_init_int64(&wr1->key, NAMESPACE, SET, 1);
	wr1->ops = &wops1;

	as_batch_write_record* wr2 = as_batch_write_reserve(&recs);
	as_key_init_int64(&wr2->key, NAMESPACE, SET, 6);
	wr2->policy = &wp;
	wr2->ops = &wops2;

	as_batch_remove_record* rm = as_batch_remove_reserve(&recs);
	as_key_init_int64(&rm->key, NAMESPACE, SET, 10002);

	// Test default ttl.
	as->config.policies.batch_write.ttl = 1000;

	as_error err;
	as_status status = aerospike_batch_write(as, &err, NULL, &recs);

	// Reset default ttl.
	as->config.policies.batch_write.ttl = 0;

	assert_int_eq(status, AEROSPIKE_OK);

	assert_int_eq(wr1->result, AEROSPIKE_OK);
	assert_int_eq(wr1->record.bins.entries[0].valuep->nil.type, AS_NIL);

	assert_int_eq(wr2->result, AEROSPIKE_OK);
	assert_int_eq(wr2->record.bins.entries[0].valuep->nil.type, AS_NIL);

	assert_int_eq(rm->result, AEROSPIKE_OK);

	as_operations_destroy(&wops1);
	as_operations_destroy(&wops2);
	as_exp_destroy(wexp1);
	as_batch_records_destroy(&recs);

	as_batch_records_inita(&recs, 3);

	as_batch_read_record* rr1 = as_batch_read_reserve(&recs);
	as_key_init_int64(&rr1->key, NAMESPACE, SET, 1);
	char* bins2[] = {bin2};
	rr1->bin_names = bins2;
	rr1->n_bin_names = 1;

	as_batch_read_record* rr2 = as_batch_read_reserve(&recs);
	as_key_init_int64(&rr2->key, NAMESPACE, SET, 6);
	char* bins3[] = {bin3};
	rr2->bin_names = bins3;
	rr2->n_bin_names = 1;

	as_batch_read_record* rr3 = as_batch_read_reserve(&recs);
	as_key_init_int64(&rr3->key, NAMESPACE, SET, 10002);
	rr3->read_all_bins = true;

	status = aerospike_batch_write(as, &err, NULL, &recs);

	assert_int_eq(status, AEROSPIKE_OK);

	assert_int_eq(rr1->result, AEROSPIKE_OK);
	int64_t v = as_record_get_int64(&rr1->record, bin2, -1);
	assert_int_eq(v, 100);

	assert_int_eq(rr2->result, AEROSPIKE_OK);
	v = as_record_get_int64(&rr2->record, bin3, -1);
	assert_int_eq(v, 1006);

	// Read of deleted record causes error.
	assert_int_eq(rr3->result, AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_batch_records_destroy(&recs);
}

static bool
batch_write_read_all_cb(const as_batch_result* results, uint32_t n, void* udata)
{
	batch_stats* data = udata;

	for (uint32_t i = 0; i < n; i++) {
		const as_batch_result* r = &results[i];
		int k = (int)r->key->valuep->integer.value;

		if (r->result == AEROSPIKE_OK) {
			data->found++;
			//example_dump_record(&r->record);
			int64_t val = as_record_get_int64(&r->record, bin1, -1);
			int64_t val_expect = (i % 20 == 0)? 1 : k + 1;

			if (val != val_expect) {
				warn("Result[%d]: val(%d) != expected(%d)", k, val, val_expect);
				data->errors++;
				continue;
			}
		}
		else {
			warn("Result[%d] failed: %d", k, r->result);
		}
	}
	return true;
}

TEST(batch_write_read_all_bins, "Batch write/read all bins")
{
	as_batch batch;
	as_batch_inita(&batch, N_KEYS);
	
	for (uint32_t i = 0; i < N_KEYS; i++) {
		as_key_init_int64(as_batch_keyat(&batch,i), NAMESPACE, SET, i);
	}

	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_add_incr(&ops, bin1, 1);
	as_operations_add_read_all(&ops);

	batch_stats data = {0};
	as_error err;
	as_status status = aerospike_batch_operate(as, &err, NULL, NULL, &batch, &ops,
		batch_write_read_all_cb, &data);

	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(data.found, N_KEYS);
	assert_int_eq(data.errors, 0);
}

static bool
result_cb(const as_batch_result* results, uint32_t n, void* udata)
{
	uint32_t* errors = udata;

	for (uint32_t i = 0; i < n; i++) {
		if (results[i].result != AEROSPIKE_OK) {
			(*errors)++;
		}
	}
	return true;
}

static bool
not_exists_cb(const as_batch_result* results, uint32_t n, void* udata)
{
	uint32_t* errors = udata;

	for (uint32_t i = 0; i < n; i++) {
		if (results[i].result != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			(*errors)++;
		}
	}
	return true;
}

TEST(batch_remove, "Batch remove")
{
	as_error err;
	as_status status;
	uint32_t errors;

	// Define keys
	as_batch batch;
	as_batch_inita(&batch, 2);
	as_key_init_int64(as_batch_keyat(&batch, 0), NAMESPACE, SET, 10000);
	as_key_init_int64(as_batch_keyat(&batch, 1), NAMESPACE, SET, 10001);

	// Ensure keys exists
	errors = 0;
	status = aerospike_batch_exists(as, &err, NULL, &batch, result_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);

	// Delete keys
	errors = 0;
	status = aerospike_batch_remove(as, &err, NULL, NULL, &batch, result_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);

	// Ensure keys do not exist
	errors = 0;
	status = aerospike_batch_exists(as, &err, NULL, &batch, not_exists_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);
}

TEST(batch_reset_read_ttl, "Batch reset read ttl")
{
	as_error err;
	as_status status;
	uint32_t errors;

	// Define keys
	as_batch batch;
	as_batch_inita(&batch, 2);
	as_key_init_int64(as_batch_keyat(&batch, 0), NAMESPACE, SET, 8888);
	as_key_init_int64(as_batch_keyat(&batch, 1), NAMESPACE, SET, 8889);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_write_int64(&ops, "a", 1);
	ops.ttl = 2;

	errors = 0;
	status = aerospike_batch_operate(as, &err, NULL, NULL, &batch, &ops, result_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);

	// Read the records before they expire and reset read ttl.
	as_sleep(1010);
	as_policy_batch pb;
	as_policy_batch_init(&pb);
	pb.read_touch_ttl_percent = 80;
	
	errors = 0;
	status = aerospike_batch_exists(as, &err, &pb, &batch, result_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);

	// Read the records again, but don't reset read ttl.
	as_sleep(1010);
	pb.read_touch_ttl_percent = -1;
	errors = 0;
	status = aerospike_batch_exists(as, &err, &pb, &batch, result_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);

	// Read the record after it expires, showing it's gone.
	as_sleep(2000);
	errors = 0;
	status = aerospike_batch_exists(as, &err, NULL, &batch, not_exists_cb, &errors);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(errors, 0);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(batch, "aerospike batch tests")
{
	suite_before(before);
	suite_after(after);
	suite_add(batch_get_1);
	suite_add(multithreaded_batch_get);
	suite_add(batch_get_bins);
	suite_add(batch_read_complex);
	suite_add(batch_read_list_operate);
	suite_add(batch_write_list_operate);
	suite_add(batch_write_complex);
	suite_add(batch_write_read_all_bins);
	suite_add(batch_remove);
	
	if (g_has_ttl) {
		suite_add(batch_reset_read_ttl);
	}
}
