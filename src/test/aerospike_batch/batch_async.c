/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_monitor.h>

#include "../test.h"
#include "../util/log_helper.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;
static as_monitor monitor;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "batchasync"
#define LIST_BIN "listbin"
#define N_KEYS 200

static char bin1[] = "bin1";
static char bin2[] = "bin2";
static char bin3[] = "bin3";

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_status
insert_record(as_error* err, int i)
{
	// Do not write some records to test not found logic too.
	if (i % 20 == 0) {
		return AEROSPIKE_OK;
	}
		
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, i);

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

	as_status status = aerospike_key_put(as, err, NULL, &key, &rec);
	as_record_destroy(&rec);
	return status;
}

static bool
before(atf_suite* suite)
{
	as_monitor_init(&monitor);

	as_error err;

	for (int i = 0; i < N_KEYS; i++) {
		as_status status = insert_record(&err, i);

		if (status != AEROSPIKE_OK) {
			info("error(%d): %s", err.code, err.message);
			return false;
		}
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
	as_monitor_destroy(&monitor);
	return true;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

static void
batch_callback(
	as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop
	)
{
	if (err) {
		as_batch_records_destroy(records);
	}
	assert_success_async(&monitor, err, udata);

	as_vector* list = &records->list;
	uint32_t found = 0;
	uint32_t errors = 0;
	
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
	as_batch_records_destroy(records);
	
	assert_int_eq_async(&monitor, found, 8);
	assert_int_eq_async(&monitor, errors, 0);
	as_monitor_notify(&monitor);
}

TEST(batch_async_read_complex, "Batch Async Read Complex")
{
	// Batch allows multiple namespaces in one call,
	// but example test environment may only have one namespace.
	as_batch_records* records = as_batch_records_create(9);
	
	char* bins[] = {bin1};
	uint32_t n_bins = 1;
	
	// get specified bins
	as_batch_read_record* record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 1);
	record->bin_names = bins;
	record->n_bin_names = n_bins;
	
	// get all bins
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 2);
	record->read_all_bins = true;
	
	// get all bins
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 3);
	record->read_all_bins = true;

	// exists
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 4);
	
	// get all bins
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 5);
	record->read_all_bins = true;
	
	// get all bins
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 6);
	record->read_all_bins = true;

	// get specified bins
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 7);
	record->bin_names = bins;
	record->n_bin_names = n_bins;

	// This record should be found, but the requested bin will not be found.
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 8);
	char* bins2[] = {"binnotfound"};
	record->bin_names = bins2;
	record->n_bin_names = 1;
	
	// This record should not be found.
	record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, 20);
	record->bin_names = bins;
	record->n_bin_names = n_bins;

	as_monitor_begin(&monitor);

	as_error err;
	as_status status = aerospike_batch_read_async(as, &err, NULL, records, batch_callback,
												  __result__, NULL);
	
	if (status != AEROSPIKE_OK) {
		as_batch_records_destroy(records);
	}
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

static void
batch_async_list_operate_cb(
	as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop
	)
{
	if (err) {
		as_batch_records_destroy(records);
	}
	assert_success_async(&monitor, err, udata);

	as_vector* list = &records->list;
	uint32_t found = 0;
	uint32_t errors = 0;
	
	for (uint32_t i = 0; i < list->size; i++) {
		as_batch_read_record* batch = as_vector_get(list, i);
		as_key* key = &batch->key;
		int k = (int)key->valuep->integer.value;
		
		if (batch->result == AEROSPIKE_OK) {
			found++;
			
			as_bin* results = batch->record.bins.entries;
			int v2 = (int)results[1].valuep->integer.value;
			int expected = k * (k - 1);

			if (v2 != expected) {
				errors++;
				warn("Result[%d]: v2(%d) != expected(%d)", k, v2, expected);
			}
		}
		else if (batch->result != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			errors++;
			error("Unexpected error(%u): %s", i, as_error_string(batch->result));
		}
	}
	as_batch_records_destroy(records);
	
	assert_int_eq_async(&monitor, found, N_KEYS - N_KEYS/20);
	assert_int_eq_async(&monitor, errors, 0);
	as_monitor_notify(&monitor);
}

TEST(batch_async_list_operate, "Batch Async List Operate")
{
	as_batch_records* records = as_batch_records_create(N_KEYS);
	
	// Get size and last element of list bin for all records.
	as_operations ops;
	as_operations_inita(&ops, 2);
	as_operations_list_size(&ops, LIST_BIN, NULL);
	as_operations_list_get_by_index(&ops, LIST_BIN, NULL, -1, AS_LIST_RETURN_VALUE);

	for (uint32_t i = 0; i < N_KEYS; i++) {
		as_batch_read_record* r = as_batch_read_reserve(records);
		as_key_init_int64(&r->key, NAMESPACE, SET, i);
		r->ops = &ops;
	}

	as_monitor_begin(&monitor);

	as_error err;
	as_status status = aerospike_batch_read_async(as, &err, NULL, records,
												  batch_async_list_operate_cb, __result__, NULL);

	as_operations_destroy(&ops);
	
	if (status != AEROSPIKE_OK) {
		as_batch_records_destroy(records);
	}
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

static void
batch_write_callback(
	as_error* err, as_batch_records* recs, void* udata, as_event_loop* event_loop
	)
{
	if (err) {
		as_batch_records_destroy(recs);
	}
	assert_success_async(&monitor, err, udata);

	as_vector* list = &recs->list;
	assert_int_eq_async(&monitor, list->size, 2);

	as_batch_write_record* r = as_vector_get(list, 0);
	assert_int_eq_async(&monitor, r->result, AEROSPIKE_OK);
	//dump_record(&r->record);
	assert_int_eq_async(&monitor, r->record.bins.size, 2);
	as_bin* bins = r->record.bins.entries;
	assert_int_eq_async(&monitor, bins[0].valuep->nil.type, AS_NIL);
	int64_t i = bins[1].valuep->integer.value;
	assert_int_eq_async(&monitor, i, 100);

	r = as_vector_get(list, 1);
	assert_int_eq_async(&monitor, r->result, AEROSPIKE_OK);
	assert_int_eq_async(&monitor, r->record.bins.size, 2);
	bins = r->record.bins.entries;
	assert_int_eq_async(&monitor, bins[0].valuep->nil.type, AS_NIL);
	i = bins[1].valuep->integer.value;
	assert_int_eq_async(&monitor, i, 1006);

	as_batch_records_destroy(recs);
	as_monitor_notify(&monitor);
}

TEST(batch_async_write_complex, "Batch Async Write Complex")
{
	as_exp_build(wexp1, as_exp_add(as_exp_bin_int(bin1), as_exp_int(1000)));

	as_operations ops1;
	as_operations_inita(&ops1, 2);
	as_operations_add_write_int64(&ops1, bin2, 100);
	as_operations_add_read(&ops1, bin2);

	as_operations ops2;
	as_operations_inita(&ops2, 2);
	as_operations_exp_write(&ops2, bin3, wexp1, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops2, bin3);

	as_batch_records* recs = as_batch_records_create(2);
	as_batch_write_record* wr;

	wr = as_batch_write_reserve(recs);
	as_key_init_int64(&wr->key, NAMESPACE, SET, 1);
	wr->ops = &ops1;

	wr = as_batch_write_reserve(recs);
	as_key_init_int64(&wr->key, NAMESPACE, SET, 6);
	wr->ops = &ops2;

	as_monitor_begin(&monitor);

	as_error err;
	as_status status = aerospike_batch_write_async(as, &err, NULL, recs, batch_write_callback,
												   __result__, NULL);

	if (status != AEROSPIKE_OK) {
		as_batch_records_destroy(recs);
	}

	as_operations_destroy(&ops1);
	as_operations_destroy(&ops2);
	as_exp_destroy(wexp1);

	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

static void
batch_one_record_not_found_cb(
	as_error* err, as_batch_records* records, void* udata, as_event_loop* event_loop
	)
{
	if (err) {
		as_batch_records_destroy(records);
	}
	assert_success_async(&monitor, err, udata);

	as_vector* list = &records->list;
	uint32_t found = 0;
	uint32_t not_found = 0;
	uint32_t errors = 0;
	
	for (uint32_t i = 0; i < list->size; i++) {
		as_batch_read_record* batch = as_vector_get(list, i);

		if (batch->result == AEROSPIKE_OK) {
			found++;
		}
		else if (batch->result == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			not_found++;
		}
		else {
			errors++;
			error("Unexpected error(%u): %s", i, as_error_string(batch->result));
		}
	}
	as_batch_records_destroy(records);
	
	assert_int_eq_async(&monitor, found, 0);
	assert_int_eq_async(&monitor, not_found, 1);
	assert_int_eq_async(&monitor, errors, 0);
	as_monitor_notify(&monitor);
}

TEST(batch_one_record_not_found, "Batch One Record Not Found")
{
	int64_t k = 888888888;

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, k);

	as_error err;
	aerospike_key_remove(as, &err, NULL, &key);

	as_batch_records* records = as_batch_records_create(1);

	as_batch_read_record* record = as_batch_read_reserve(records);
	as_key_init_int64(&record->key, NAMESPACE, SET, k);
	record->read_all_bins = true;

	as_monitor_begin(&monitor);

	as_status status = aerospike_batch_read_async(as, &err, NULL, records, batch_one_record_not_found_cb,
												  __result__, NULL);
	
	if (status != AEROSPIKE_OK) {
		as_batch_records_destroy(records);
	}
	assert_int_eq(status, AEROSPIKE_OK);
	as_monitor_wait(&monitor);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(batch_async, "aerospike batch async tests")
{
	suite_before(before);
	suite_after(after);
	suite_add(batch_async_read_complex);
	suite_add(batch_async_list_operate);
	suite_add(batch_async_write_complex);
	suite_add(batch_one_record_not_found);
}
