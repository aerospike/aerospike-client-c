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
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_hashmap_iterator.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_udf.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/udf.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike *as;

extern void example_dump_record(const as_record* p_rec);

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_cdt"
#define BIN_NAME "test-list-1"

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_testlist_s {
	aerospike *as;
	as_key key;
	as_record *rec;

	as_arraylist arraylist;
} as_testlist;

typedef struct list_order_type_s
{
	as_list_order order;
	bool pad;
} list_order_type;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

#define LUA_DIR AS_START_DIR "src/test/lua/"

static bool
load_udf(const char* filename)
{
	as_error err;
	as_bytes content;

	char namebuf[512];
	sprintf(namebuf, "%s%s", LUA_DIR, filename);
	info("reading: %s", namebuf);
	bool b = udf_readfile(namebuf, &content);

	if (! b) {
		info("read failed");
		return false;
	}

	info("uploading: %s",filename);
	aerospike_udf_put(as, &err, NULL, filename, AS_UDF_TYPE_LUA, &content);

	if (err.code != AEROSPIKE_OK) {
		info("put failed");
		return false;
	}

	aerospike_udf_put_wait(as, &err, NULL, filename, 100);
	as_bytes_destroy(&content);
	return true;
}

static bool as_testlist_compare(as_testlist *tlist);

static bool
as_val_is_equal(const as_val *v0, const as_val *v1)
{
	as_val_t type0 = as_val_type(v0);
	if (type0 == as_val_type(v1)) {
		switch (type0) {
		case AS_INTEGER:
			return as_integer_get((as_integer *)v0) == as_integer_get((as_integer *)v1);
		case AS_STRING:
			return strcmp(as_string_get((as_string *)v0), as_string_get((as_string *)v1)) == 0;
		case AS_NIL:
			return true;
		case AS_LIST:
			if (as_list_size((as_list*)v0) != as_list_size((as_list*)v1)) {
				return false;
			}

			for (uint32_t i = 0; i < as_list_size((as_list*)v0); i++) {
				as_val* v00 = as_list_get((as_list*)v0, i);
				as_val* v11 = as_list_get((as_list*)v1, i);

				if (! as_val_is_equal(v00, v11)) {
					return false;
				}
			}

			return true;
		default:
			error("Type %d not supported for is_equal.", type0);
			return false;
		}
	}
	else if (type0 == AS_NIL) {
		return v1 == NULL;
	}
	else if (as_val_type(v1) == AS_NIL) {
		return v0 == NULL;
	}

	return false;
}

static void
make_string_list(as_arraylist *list, int len)
{
	char* str = alloca(len + 1);
	for (int i = 0; i < len; i++) {
		// Random chars from 32 (space) to 126 (~)
		str[i] = (rand() % (126 - 32 + 1)) + 32;
	}
	str[len] = '\0';
	as_arraylist_append(list, (as_val *)as_string_new_strdup(str));
}

void
make_random_list(as_arraylist *list, uint32_t count)
{
	as_arraylist_init(list, count, 1);
	for (uint32_t i = 0; i < count; i++) {
		if (rand() % 2 == 0) {
			as_arraylist_append(list, (as_val *)as_integer_new(rand()%1000));
		}
		else {
			int len = rand() % 100;
			make_string_list(list, len);
		}
	}
}

static bool
as_testlist_op(as_testlist *tlist, as_operations *ops)
{
	if (tlist->rec) {
		as_record_destroy(tlist->rec);
		tlist->rec = NULL;
	}

	as_error err;

	if (aerospike_key_operate(tlist->as, &err, NULL, &tlist->key, ops, &tlist->rec) != AEROSPIKE_OK) {
		//debug("as_testlist_op() returned %d - %s", err.code, err.message);
		as_operations_destroy(ops);
		return false;
	}

	as_operations_destroy(ops);

	return true;
}

static bool
as_testlist_init(as_testlist *tlist, aerospike *as)
{
	tlist->as = as;
	tlist->rec = NULL;
	as_key_init_int64(&tlist->key, NAMESPACE, SET, 1);

	as_error err;
	aerospike_key_remove(as, &err, NULL, &tlist->key);

	as_arraylist_init(&tlist->arraylist, 100, 100);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_arraylist list;
	as_arraylist_init(&list, 1, 1);

	as_val_reserve((as_val *)&list);
	as_operations_add_write(&ops, BIN_NAME, (as_bin_value *)&list);

	if (! as_testlist_op(tlist, &ops)) {
		error("write empty list failed");
		return false;
	}

	//debug("wrote empty list");
	if (! as_testlist_compare(tlist)) {
		error("write empty list: post compare failed");
		return false;
	}

	//debug("remove record");
	if (aerospike_key_remove(as, &err, NULL, &tlist->key) != AEROSPIKE_OK) {
		error("aerospike_key_remove failed");
		return false;
	}

	as_operations_inita(&ops, 1);
	as_operations_add_list_append_items(&ops, BIN_NAME, (as_list *)&list);

	//debug("append empty list");
	if (! as_testlist_op(tlist, &ops)) {
		error("append empty list failed");
		return false;
	}

	if (! as_testlist_compare(tlist)) {
		error("append empty list: post compare failed");
		return false;
	}

	return true;
}

static void
as_testlist_destroy(as_testlist *tlist)
{
	as_arraylist_destroy(&tlist->arraylist);

	as_key_destroy(&tlist->key);

	if (tlist->rec) {
		as_record_destroy(tlist->rec);
		tlist->rec = NULL;
	}
}

static uint32_t
index2uindex(as_testlist *tlist, int64_t index)
{
	if (index < 0) {
		return as_arraylist_size(&tlist->arraylist) + (uint32_t)index;
	}
	return (uint32_t)index;
}

static bool
as_testlist_remove(as_testlist *tlist, int index, bool is_pop)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);

	as_arraylist_remove(&tlist->arraylist, uindex);
	if (is_pop) {
		as_operations_add_list_pop(&ops, BIN_NAME, index);
	}
	else {
		as_operations_add_list_remove(&ops, BIN_NAME, index);
	}

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_remove_range(as_testlist *tlist, int index, uint32_t count, bool is_pop)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);

	bool first_index_invalid = false;
	for (uint32_t i = 0; i < count; i++) {
		int ret = as_arraylist_remove(&tlist->arraylist, uindex);
		if (ret != AS_ARRAYLIST_OK && i == 0) {
			first_index_invalid = true;
		}
	}

	if (is_pop) {
		as_operations_add_list_pop_range(&ops, BIN_NAME, (int64_t)index, (uint64_t)count);
	}
	else {
		as_operations_add_list_remove_range(&ops, BIN_NAME, (int64_t)index, (uint64_t)count);
	}

	bool ret = as_testlist_op(tlist, &ops);

	if (! ret) {
		if (first_index_invalid) {
			debug("remove_range: index=%d count=%u out_of_range=%s failed as expected", index, count, first_index_invalid ? "true" : "false");
			return true;
		}

		debug("remove_range: index=%d count=%u out_of_range=%s", index, count, first_index_invalid ? "true" : "false");
		return false;
	}

	//debug("remove_range: index=%d count=%u out_of_range=%s", index, count, first_index_invalid ? "true" : "false")
	return true;
}

static bool
compare_range(as_arraylist *a, uint32_t index, as_list *list)
{
	uint32_t result_size = as_list_size(list);

	for (uint32_t i = 0; i < result_size; i++) {
		uint32_t test_index = index + i;
		as_val *v0 = as_arraylist_get(a, test_index);
		as_val *v1 = as_list_get(list, i);
		if (! as_val_is_equal(v0, v1)) {
			char *s0 = as_val_tostring(v0);
			char *s1 = as_val_tostring(v1);
			debug("compare_range: index=%u: %s != %s", i, s0, s1);
			free(s0);
			free(s1);
			return false;
		}
	}

	return true;
}

static bool
as_testlist_get_range(as_testlist *tlist, int64_t index, uint32_t count)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_operations_add_list_get_range(&ops, BIN_NAME, index, count);

	bool ret = as_testlist_op(tlist, &ops);
	if (! ret) {
		return false;
	}

	uint32_t uindex = index2uindex(tlist, index);
	as_list *list = as_record_get_list(tlist->rec, BIN_NAME);

	//uint32_t result_size = as_list_size(list);
	//debug("get_range: result_size=%u", result_size);
	return compare_range(&tlist->arraylist, uindex, list);
}

static bool
as_testlist_get_range_from(as_testlist *tlist, int64_t index)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_operations_add_list_get_range_from(&ops, BIN_NAME, index);

	bool ret = as_testlist_op(tlist, &ops);
	if (! ret) {
		if (index > as_arraylist_size(&tlist->arraylist)) {
			debug("get_range_from: index=%ld failed as expected", index);
			return false;
		}

		debug("get_range_from: index=%ld failed", index);
		return false;
	}


	uint32_t uindex = index2uindex(tlist, index);
	as_list *list = as_record_get_list(tlist->rec, BIN_NAME);
	uint32_t result_size = as_list_size(list);

	if (uindex + result_size != as_arraylist_size(&tlist->arraylist)) {
		return false;
	}

	//debug("get_range_from: result_size=%u", result_size);
	return compare_range(&tlist->arraylist, uindex, list);
}


static bool
as_testlist_append(as_testlist *tlist, as_val *val)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_val_reserve(val);
	as_arraylist_append(&tlist->arraylist, val);
	as_operations_add_list_append(&ops, BIN_NAME, val);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_append_list(as_testlist *tlist, as_arraylist *list)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_arraylist_concat(&tlist->arraylist, list);
	as_operations_add_list_append_items(&ops, BIN_NAME, (as_list *)list);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_insert(as_testlist *tlist, int64_t index, as_val *val)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);
	as_val_reserve(val);
	as_arraylist_insert(&tlist->arraylist, uindex, val);

	as_operations_add_list_insert(&ops, BIN_NAME, index, val);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_insert_list(as_testlist *tlist, int64_t index, as_arraylist *list)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);
	uint32_t size = as_arraylist_size(list);
	for (int i = size - 1; i >= 0; i--) {
		as_val *val = as_arraylist_get(list, i);
		as_val_reserve(val);
		as_arraylist_insert(&tlist->arraylist, uindex, val);
	}

	as_operations_add_list_insert_items(&ops, BIN_NAME, index, (as_list *)list);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_set(as_testlist *tlist, int64_t index, as_val *val)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);
	as_val_reserve(val);
	as_arraylist_set(&tlist->arraylist, uindex, val);

	as_operations_add_list_set(&ops, BIN_NAME, index, val);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_incr(as_testlist *tlist, int64_t index, as_val *incr)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);
	as_val *val = as_arraylist_get(&tlist->arraylist, uindex);
	as_val_t val_type = as_val_type(val);
	as_val_t incr_type = as_val_type(incr);

	if (incr_type == AS_INTEGER || incr_type == AS_DOUBLE) {
		if (val_type == AS_INTEGER) {
			int64_t val_int = as_integer_get((as_integer *)val);
			val_int += (incr_type == AS_INTEGER) ?
					as_integer_get((as_integer *)incr) :
					(int64_t)as_double_get((as_double *)incr);
			as_arraylist_set_int64(&tlist->arraylist, uindex, val_int);
		}
		else if (val_type == AS_DOUBLE) {
			double val_double = as_double_get((as_double *)val);
			val_double += (incr_type == AS_INTEGER) ?
					as_integer_get((as_integer *)incr) :
					as_double_get((as_double *)incr);
			as_arraylist_set_double(&tlist->arraylist, uindex, val_double);
		}
	}

	as_operations_add_list_increment(&ops, BIN_NAME, index, incr);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_trim(as_testlist *tlist, int64_t index, uint32_t count)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);
	as_arraylist_trim(&tlist->arraylist, uindex + count);

	for (int64_t i = 0; i < index; i++) {
		as_arraylist_remove(&tlist->arraylist, 0);
	}

	as_operations_add_list_trim(&ops, BIN_NAME, index, (uint64_t)count);

	return as_testlist_op(tlist, &ops);

}

static bool
as_testlist_clear(as_testlist *tlist)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_arraylist_trim(&tlist->arraylist, 0);

	as_operations_add_list_clear(&ops, BIN_NAME);

	return as_testlist_op(tlist, &ops);

}

static bool
as_list_compare(as_list* a, as_list* b)
{
	bool ret = as_val_is_equal((as_val*)a, (as_val*)b);

	if (! ret) {
		char *s0 = as_val_val_tostring((as_val*)a);
		char *s1 = as_val_val_tostring((as_val*)b);
		//info("a=%s", s0);
		//info("b=%s", s1);
		free(s0);
		free(s1);
	}

	return ret;
}

static bool
as_testlist_compare(as_testlist* tlist)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_operations_add_read(&ops, BIN_NAME);

	if (! as_testlist_op(tlist, &ops)) {
		return false;
	}

	as_list *list = as_record_get_list(tlist->rec, BIN_NAME);

	uint32_t server_size = as_list_size(list);
	uint32_t local_size = as_arraylist_size(&tlist->arraylist);

	if (server_size != local_size) {
		debug("as_testlist_compare() server_size: %u != local_size: %u", server_size, local_size);
		char *s = as_val_tostring(list);
		debug("as_testlist_compare() server_list = %s", s);
		free(s);
		return false;
	}

	for (uint32_t i = 0; i < local_size; i++) {
		as_val *v0 = as_arraylist_get(&tlist->arraylist, i);
		as_val *v1 = as_list_get(list, i);
		if (! as_val_is_equal(v0, v1)) {
			char *s0 = as_val_tostring(v0);
			char *s1 = as_val_tostring(v1);
			debug("as_testlist_compare() at index: %u server_value: %s != local_value: %s", i, s1, s0);
			free(s0);
			free(s1);
			return false;
		}
	}

	return true;
}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(cdt_basics_op , "CDT operations test on a single bin")
{
	as_testlist tlist;
	assert_true(as_testlist_init(&tlist, as));

	//debug("insert 1");
	for (int i = 0; i < 100; i++) {
		int ridx = rand() % 100;
		int v = rand() % 1000;

		assert_true(as_testlist_insert(&tlist, ridx, (as_val *)as_integer_new(v)));
	}

	assert_true(as_testlist_compare(&tlist));

	as_arraylist list;

	//debug("insert_list of 1 item");
	make_random_list(&list, 1);
	as_testlist_insert_list(&tlist, 1, &list);

	//debug("insert_list");
	make_random_list(&list, 5);
	for (int i = 0; i < 10; i++) {
		int ridx = rand() % 100;

		as_val_reserve((as_val *)&list);
		as_testlist_insert_list(&tlist, (uint32_t)ridx, &list);
	}
	as_arraylist_destroy(&list);
	assert_true(as_testlist_compare(&tlist));

	//debug("append 1");
	for (int i = 0; i < 20; i++) {
		int v = rand() % 1000;

		assert_true(as_testlist_append(&tlist, (as_val *)as_integer_new(v)));
	}

	//debug("append_list");
	make_random_list(&list, 10);
	for (int i = 0; i < 8; i++) {
		as_val_reserve((as_val *)&list);
		as_testlist_append_list(&tlist, &list);
	}
	as_arraylist_destroy(&list);
	assert_true(as_testlist_compare(&tlist));

	//debug("pop -1");
	for (int i = 0; i < 50; i++) {
		assert_true(as_testlist_remove(&tlist, -1, true));
	}
	assert_true(as_testlist_compare(&tlist));

	//debug("pop_range");
	for (int i = 0; i < 10; i++) {
		int ridx = rand() % 100;

		as_testlist_remove_range(&tlist, ridx, 5, true);
	}
	assert_true(as_testlist_compare(&tlist));

	//debug("remove 1");
	for (int i = 0; i < 50; i++) {
		int ridx = rand() % 100;

		assert_true(as_testlist_remove(&tlist, ridx, false));
	}
	assert_true(as_testlist_compare(&tlist));

	//debug("remove_range");
	for (int i = 0; i < 50; i++) {
		int ridx = rand() % 100;

		assert_true(as_testlist_remove_range(&tlist, ridx, 5, false));
	}
	assert_true(as_testlist_compare(&tlist));

	//debug("remove_range: negative out of range");
	assert_true(as_testlist_remove_range(&tlist, -100, 5, false));

	//debug("remove_range: negative index");
	assert_true(as_testlist_remove_range(&tlist, -1, 5, false));

	//debug("remove_range: 0 count");
	assert_true(as_testlist_remove_range(&tlist, 1, 0, false));

	//debug("get_range");
	assert_true(as_testlist_get_range(&tlist, 0, 22));
	assert_true(as_testlist_get_range(&tlist, 10, 22));
	assert_false(as_testlist_get_range_from(&tlist, 25));

	//debug("set");
	for (int i = 0; i < 100; i++) {
		int ridx = rand() % 100;
		int v = rand() % 1000;

		assert_true(as_testlist_set(&tlist, ridx, (as_val *)as_integer_new(v)));
	}

	assert_true(as_testlist_compare(&tlist));

	//debug("trim 10%% x 5");
	for (int i = 0; i < 5; i++) {
		uint32_t size = as_arraylist_size(&tlist.arraylist);
		int idx = size / 20;
		uint32_t count = size * 9 / 10;
		as_testlist_trim(&tlist, idx, count);
	}
	assert_true(as_testlist_compare(&tlist));

	//debug("clear");
	as_testlist_clear(&tlist);
	assert_true(as_testlist_compare(&tlist));

	as_testlist_destroy(&tlist);
}

TEST(cdt_incr , "CDT incr test on a single bin")
{
	as_testlist tlist;
	assert_true(as_testlist_init(&tlist, as));

	assert_true(as_testlist_append(&tlist, (as_val *)as_string_new("test", false)));
	assert_true(as_testlist_append(&tlist, (as_val *)as_integer_new(1)));
	assert_true(as_testlist_append(&tlist, (as_val *)as_integer_new(2)));
	assert_true(as_testlist_append(&tlist, (as_val *)as_integer_new(30000)));
	assert_true(as_testlist_append(&tlist, (as_val *)as_integer_new(4)));
	assert_true(as_testlist_append(&tlist, (as_val *)as_integer_new(5)));
	assert_true(as_testlist_append(&tlist, (as_val *)as_string_new("end", false)));

	assert_false(as_testlist_incr(&tlist, 0, (as_val *)as_integer_new(1)));
	assert_false(as_testlist_incr(&tlist, 6, (as_val *)as_integer_new(1)));

	for (int64_t i = 1; i < 6; i++) {
		assert_true(as_testlist_incr(&tlist, i, (as_val *)as_integer_new(1)));
	}

	assert_true(as_testlist_compare(&tlist));

	as_testlist_destroy(&tlist);
}

TEST(list_switch_sort, "List Switch Sort")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 100);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 5, 0);
	as_arraylist_append_int64(&item_list, 4);
	as_arraylist_append_int64(&item_list, 3);
	as_arraylist_append_int64(&item_list, 1);
	as_arraylist_append_int64(&item_list, 5);
	as_arraylist_append_int64(&item_list, 2);
	as_operations_add_list_append_items(&ops, BIN_NAME, (as_list*)&item_list);
	as_operations_add_list_get_by_index(&ops, BIN_NAME, 3, AS_LIST_RETURN_VALUE);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	as_record_destroy(rec);

	as_operations_inita(&ops, 8);
	as_operations_add_list_set_order(&ops, BIN_NAME, AS_LIST_ORDERED);

	as_integer v1;
	as_integer v2;

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value(&ops, BIN_NAME, (as_val*)&v1, AS_LIST_RETURN_INDEX);

	as_integer_init(&v1, -1);
	as_integer_init(&v2, 3);
	as_operations_add_list_get_by_value_range(&ops, BIN_NAME, (as_val*)&v1, (as_val*)&v2, AS_LIST_RETURN_COUNT);

	as_arraylist value_list;
	as_arraylist_init(&value_list, 2, 0);
	as_arraylist_append_int64(&value_list, 4);
	as_arraylist_append_int64(&value_list, 2);
	as_operations_add_list_get_by_value_list(&ops, BIN_NAME, (as_list*)&value_list, AS_LIST_RETURN_RANK);

	as_operations_add_list_get_by_index(&ops, BIN_NAME, 3, AS_LIST_RETURN_VALUE);
	as_operations_add_list_get_by_index_range(&ops, BIN_NAME, -2, 2, AS_LIST_RETURN_VALUE);
	as_operations_add_list_get_by_rank(&ops, BIN_NAME, 0, AS_LIST_RETURN_VALUE);
	as_operations_add_list_get_by_rank_range(&ops, BIN_NAME, 2, 3, AS_LIST_RETURN_VALUE);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	results = rec->bins.entries;
	i = 0;

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_get_int64(list, 0), 2);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 2);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 3);
	assert_int_eq(as_list_get_int64(list, 1), 1);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 4);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 4);
	assert_int_eq(as_list_get_int64(list, 1), 5);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 1);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);
	assert_int_eq(as_list_get_int64(list, 0), 3);
	assert_int_eq(as_list_get_int64(list, 1), 4);
	assert_int_eq(as_list_get_int64(list, 2), 5);

	as_record_destroy(rec);
}

TEST(list_sort, "List Sort")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 101);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 3);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_DEFAULT);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 5, 0);
	as_arraylist_append_int64(&item_list, -44);
	as_arraylist_append_int64(&item_list, 33);
	as_arraylist_append_int64(&item_list, -1);
	as_arraylist_append_int64(&item_list, 33);
	as_arraylist_append_int64(&item_list, -2);
	as_operations_add_list_append_items_with_policy(&ops, BIN_NAME, &lp, (as_list*)&item_list);
	as_operations_add_list_sort(&ops, BIN_NAME, AS_LIST_SORT_DROP_DUPLICATES);
	as_operations_add_list_size(&ops, BIN_NAME);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 4);

	as_record_destroy(rec);
}

TEST(list_remove, "List Remove")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 102);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 8);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 10, 0);
	as_arraylist_append_int64(&item_list, -44);
	as_arraylist_append_int64(&item_list, 33);
	as_arraylist_append_int64(&item_list, -1);
	as_arraylist_append_int64(&item_list, 33);
	as_arraylist_append_int64(&item_list, -2);
	as_arraylist_append_int64(&item_list, 0);
	as_arraylist_append_int64(&item_list, 22);
	as_arraylist_append_int64(&item_list, 11);
	as_arraylist_append_int64(&item_list, 14);
	as_arraylist_append_int64(&item_list, 6);
	as_operations_add_list_append_items(&ops, BIN_NAME, (as_list*)&item_list);

	as_integer v1;
	as_integer_init(&v1, 0);
	as_operations_add_list_remove_by_value(&ops, BIN_NAME, (as_val*)&v1, AS_LIST_RETURN_INDEX);

	as_arraylist remove_list;
	as_arraylist_init(&remove_list, 2, 0);
	as_arraylist_append_int64(&remove_list, -45);
	as_arraylist_append_int64(&remove_list, 14);
	as_operations_add_list_remove_by_value_list(&ops, BIN_NAME, (as_list*)&remove_list, AS_LIST_RETURN_VALUE);

	as_integer v2;
	as_integer_init(&v1, 33);
	as_integer_init(&v2, 100);
	as_operations_add_list_remove_by_value_range(&ops, BIN_NAME, (as_val*)&v1, (as_val*)&v2, AS_LIST_RETURN_VALUE);

	as_operations_add_list_remove_by_index(&ops, BIN_NAME, 1, AS_LIST_RETURN_VALUE);
	as_operations_add_list_remove_by_index_range(&ops, BIN_NAME, 100, 101, AS_LIST_RETURN_VALUE);
	as_operations_add_list_remove_by_rank(&ops, BIN_NAME, 0, AS_LIST_RETURN_VALUE);
	as_operations_add_list_remove_by_rank_range(&ops, BIN_NAME, 3, 1, AS_LIST_RETURN_VALUE);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 10);

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 5);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 14);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 33);
	assert_int_eq(as_list_get_int64(list, 1), 33);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, -1);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 0);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, -44);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 22);

	as_record_destroy(rec);
}

TEST(list_inverted, "List Inverted")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 102);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 6);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 5, 0);
	as_arraylist_append_int64(&item_list, 4);
	as_arraylist_append_int64(&item_list, 3);
	as_arraylist_append_int64(&item_list, 1);
	as_arraylist_append_int64(&item_list, 5);
	as_arraylist_append_int64(&item_list, 2);
	as_operations_add_list_append_items(&ops, BIN_NAME, (as_list*)&item_list);

	as_integer v1;
	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value(&ops, BIN_NAME, (as_val*)&v1, AS_LIST_RETURN_INDEX | AS_LIST_RETURN_INVERTED);

	as_integer v2;
	as_integer_init(&v1, -1);
	as_integer_init(&v2, 3);
	as_operations_add_list_get_by_value_range(&ops, BIN_NAME, (as_val*)&v1, (as_val*)&v2, AS_LIST_RETURN_COUNT | AS_LIST_RETURN_INVERTED);

	as_arraylist search_list;
	as_arraylist_init(&search_list, 2, 0);
	as_arraylist_append_int64(&search_list, 4);
	as_arraylist_append_int64(&search_list, 2);
	as_operations_add_list_get_by_value_list(&ops, BIN_NAME, (as_list*)&search_list, AS_LIST_RETURN_RANK | AS_LIST_RETURN_INVERTED);

	as_operations_add_list_remove_by_index_range(&ops, BIN_NAME, -2, 2, AS_LIST_RETURN_VALUE | AS_LIST_RETURN_INVERTED);
	as_operations_add_list_remove_by_rank_range(&ops, BIN_NAME, 2, 3, AS_LIST_RETURN_VALUE | AS_LIST_RETURN_INVERTED);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4);
	assert_int_eq(as_list_get_int64(list, 0), 0);
	assert_int_eq(as_list_get_int64(list, 1), 2);
	assert_int_eq(as_list_get_int64(list, 2), 3);
	assert_int_eq(as_list_get_int64(list, 3), 4);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 3);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);
	assert_int_eq(as_list_get_int64(list, 0), 0);
	assert_int_eq(as_list_get_int64(list, 1), 2);
	assert_int_eq(as_list_get_int64(list, 2), 4);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);
	assert_int_eq(as_list_get_int64(list, 0), 4);
	assert_int_eq(as_list_get_int64(list, 1), 3);
	assert_int_eq(as_list_get_int64(list, 2), 1);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 5);
	assert_int_eq(as_list_get_int64(list, 1), 2);

	as_record_destroy(rec);
}

TEST(list_insert, "List Insert")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 103);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 3);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_UNORDERED, AS_LIST_WRITE_ADD_UNIQUE);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 5, 0);
	as_arraylist_append_int64(&item_list, 5);
	as_arraylist_append_int64(&item_list, 2);
	as_arraylist_append_int64(&item_list, 9);
	as_arraylist_append_int64(&item_list, 3);
	as_arraylist_append_int64(&item_list, 0);
	as_operations_add_list_insert_items_with_policy(&ops, BIN_NAME, &lp, 0, (as_list*)&item_list);

	as_integer v1;
	as_integer_init(&v1, 6);
	as_operations_add_list_insert_with_policy(&ops, BIN_NAME, &lp, 1, (as_val*)&v1);

	as_integer v2;
	// Duplicate would cause failure.
	//as_integer_init(&v2, 3);
	as_integer_init(&v2, 1);
	as_operations_add_list_insert_with_policy(&ops, BIN_NAME, &lp, 5, (as_val*)&v2);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 6);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 7);

	as_record_destroy(rec);
}

TEST(list_increment, "List Increment")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 104);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 5);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_UNORDERED, AS_LIST_WRITE_ADD_UNIQUE);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 3, 0);
	as_arraylist_append_int64(&item_list, 1);
	as_arraylist_append_int64(&item_list, 2);
	as_arraylist_append_int64(&item_list, 3);
	as_operations_add_list_insert_items_with_policy(&ops, BIN_NAME, &lp, 0, (as_list*)&item_list);

	as_operations_add_list_increment(&ops, BIN_NAME, 2, NULL);
	as_operations_add_list_increment_with_policy(&ops, BIN_NAME, &lp, 2, NULL);

	as_integer v1;
	as_integer_init(&v1, 7);
	as_operations_add_list_increment(&ops, BIN_NAME, 1, (as_val*)&v1);

	as_integer_init(&v1, 7);
	as_operations_add_list_increment_with_policy(&ops, BIN_NAME, &lp, 1, (as_val*)&v1);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 3);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 4);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 9);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 16);

	as_record_destroy(rec);
}

TEST(list_get_relative, "List Get Relative")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 105);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_DEFAULT);

	as_operations ops;
	as_operations_inita(&ops, 13);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 6, 0);
	as_arraylist_append_int64(&item_list, 0);
	as_arraylist_append_int64(&item_list, 4);
	as_arraylist_append_int64(&item_list, 5);
	as_arraylist_append_int64(&item_list, 9);
	as_arraylist_append_int64(&item_list, 11);
	as_arraylist_append_int64(&item_list, 15);
	as_operations_add_list_append_items_with_policy(&ops, BIN_NAME, &lp, (as_list*)&item_list);

	as_integer v1;
	as_integer_init(&v1, 5);
	as_operations_add_list_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, 0, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 5);
	as_operations_add_list_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, 1, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 5);
	as_operations_add_list_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, -1, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, 0, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, 3, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, -3, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 5);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, 0, 2, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 5);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, 1, 1, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 5);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, -1, 2, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, 0, 1, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, 3, 7, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, -3, 2, AS_LIST_RETURN_VALUE);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 6);

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4);
	assert_int_eq(as_list_get_int64(list, 0), 5);
	assert_int_eq(as_list_get_int64(list, 1), 9);
	assert_int_eq(as_list_get_int64(list, 2), 11);
	assert_int_eq(as_list_get_int64(list, 3), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);
	assert_int_eq(as_list_get_int64(list, 0), 9);
	assert_int_eq(as_list_get_int64(list, 1), 11);
	assert_int_eq(as_list_get_int64(list, 2), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 5);
	assert_int_eq(as_list_get_int64(list, 0), 4);
	assert_int_eq(as_list_get_int64(list, 1), 5);
	assert_int_eq(as_list_get_int64(list, 2), 9);
	assert_int_eq(as_list_get_int64(list, 3), 11);
	assert_int_eq(as_list_get_int64(list, 4), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 5);
	assert_int_eq(as_list_get_int64(list, 0), 4);
	assert_int_eq(as_list_get_int64(list, 1), 5);
	assert_int_eq(as_list_get_int64(list, 2), 9);
	assert_int_eq(as_list_get_int64(list, 3), 11);
	assert_int_eq(as_list_get_int64(list, 4), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 11);
	assert_int_eq(as_list_get_int64(list, 1), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 6);
	assert_int_eq(as_list_get_int64(list, 0), 0);
	assert_int_eq(as_list_get_int64(list, 1), 4);
	assert_int_eq(as_list_get_int64(list, 2), 5);
	assert_int_eq(as_list_get_int64(list, 3), 9);
	assert_int_eq(as_list_get_int64(list, 4), 11);
	assert_int_eq(as_list_get_int64(list, 5), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 5);
	assert_int_eq(as_list_get_int64(list, 1), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 9);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 4);
	assert_int_eq(as_list_get_int64(list, 1), 5);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 4);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 2);
	assert_int_eq(as_list_get_int64(list, 0), 11);
	assert_int_eq(as_list_get_int64(list, 1), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 0);

	as_record_destroy(rec);
}

TEST(list_remove_relative, "List Remove Relative")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 106);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_DEFAULT);

	as_operations ops;
	as_operations_inita(&ops, 7);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 6, 0);
	as_arraylist_append_int64(&item_list, 0);
	as_arraylist_append_int64(&item_list, 4);
	as_arraylist_append_int64(&item_list, 5);
	as_arraylist_append_int64(&item_list, 9);
	as_arraylist_append_int64(&item_list, 11);
	as_arraylist_append_int64(&item_list, 15);
	as_operations_add_list_append_items_with_policy(&ops, BIN_NAME, &lp, (as_list*)&item_list);

	as_integer v1;
	as_integer_init(&v1, 5);
	as_operations_add_list_remove_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, 0, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 5);
	as_operations_add_list_remove_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, 1, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 5);
	as_operations_add_list_remove_by_value_rel_rank_range_to_end(&ops, BIN_NAME, (as_val*)&v1, -1, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, -3, 1, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, -3, 2, AS_LIST_RETURN_VALUE);

	as_integer_init(&v1, 3);
	as_operations_add_list_get_by_value_rel_rank_range(&ops, BIN_NAME, (as_val*)&v1, -3, 3, AS_LIST_RETURN_VALUE);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 6);

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 4);
	assert_int_eq(as_list_get_int64(list, 0), 5);
	assert_int_eq(as_list_get_int64(list, 1), 9);
	assert_int_eq(as_list_get_int64(list, 2), 11);
	assert_int_eq(as_list_get_int64(list, 3), 15);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 0);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 4);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 0);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 0);

	list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 1);
	assert_int_eq(as_list_get_int64(list, 0), 0);

	as_record_destroy(rec);
}

TEST(list_partial, "List Partial")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 107);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 8, 0);
	as_arraylist_append_int64(&item_list, 0);
	as_arraylist_append_int64(&item_list, 4);
	as_arraylist_append_int64(&item_list, 5);
	as_arraylist_append_int64(&item_list, 9);
	as_arraylist_append_int64(&item_list, 9);
	as_arraylist_append_int64(&item_list, 11);
	as_arraylist_append_int64(&item_list, 15);
	as_arraylist_append_int64(&item_list, 0);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_PARTIAL | AS_LIST_WRITE_NO_FAIL);
	as_operations_add_list_append_items_with_policy(&ops, BIN_NAME, &lp, (as_list*)&item_list);

	as_arraylist item_list2;
	as_arraylist_init(&item_list2, 8, 0);
	as_arraylist_append_int64(&item_list2, 0);
	as_arraylist_append_int64(&item_list2, 4);
	as_arraylist_append_int64(&item_list2, 5);
	as_arraylist_append_int64(&item_list2, 9);
	as_arraylist_append_int64(&item_list2, 9);
	as_arraylist_append_int64(&item_list2, 11);
	as_arraylist_append_int64(&item_list2, 15);
	as_arraylist_append_int64(&item_list2, 0);

	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_NO_FAIL);
	as_operations_add_list_append_items_with_policy(&ops, "bin2", &lp, (as_list*)&item_list2);

	as_record* rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	uint32_t i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 6);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 0);

	as_record_destroy(rec);

	as_operations_inita(&ops, 2);

	as_arraylist_init(&item_list, 2, 0);
	as_arraylist_append_int64(&item_list, 11);
	as_arraylist_append_int64(&item_list, 3);

	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_PARTIAL | AS_LIST_WRITE_NO_FAIL);
	as_operations_add_list_append_items_with_policy(&ops, BIN_NAME, &lp, (as_list*)&item_list);

	as_arraylist_init(&item_list2, 2, 0);
	as_arraylist_append_int64(&item_list2, 11);
	as_arraylist_append_int64(&item_list2, 3);

	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_ADD_UNIQUE | AS_LIST_WRITE_NO_FAIL);
	as_operations_add_list_append_items_with_policy(&ops, "bin2", &lp, (as_list*)&item_list2);

	rec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	results = rec->bins.entries;
	i = 0;

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 7);

	val = results[i++].valuep->integer.value;
	assert_int_eq(val, 2);

	as_record_destroy(rec);
}

TEST(list_nested, "Nested List")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 108);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist l1;
	as_arraylist_inita(&l1, 3);
	as_arraylist_append_int64(&l1, 7);
	as_arraylist_append_int64(&l1, 9);
	as_arraylist_append_int64(&l1, 5);

	as_arraylist l2;
	as_arraylist_inita(&l2, 3);
	as_arraylist_append_int64(&l2, 1);
	as_arraylist_append_int64(&l2, 2);
	as_arraylist_append_int64(&l2, 3);

	as_arraylist l3;
	as_arraylist_inita(&l3, 4);
	as_arraylist_append_int64(&l3, 6);
	as_arraylist_append_int64(&l3, 5);
	as_arraylist_append_int64(&l3, 4);
	as_arraylist_append_int64(&l3, 1);

	as_arraylist item_list;
	as_arraylist_inita(&item_list, 3);
	as_arraylist_append(&item_list, (as_val*)&l1);
	as_arraylist_append(&item_list, (as_val*)&l2);
	as_arraylist_append(&item_list, (as_val*)&l3);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_list(&rec, BIN_NAME, (as_list*)&item_list);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_cdt_ctx ctx;
	as_cdt_ctx_inita(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, -1);

	as_integer v;
	as_integer_init(&v, 11);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)&v);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;
	int i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 5);

	as_list* list = &results[i++].valuep->list;
	assert_int_eq(as_list_size(list), 3);

	as_list* l = as_list_get_list(list, 2);
	assert_int_eq(as_list_size(l), 5);
	assert_int_eq(as_list_get_int64(l, 0), 6);
	assert_int_eq(as_list_get_int64(l, 1), 5);
	assert_int_eq(as_list_get_int64(l, 2), 4);
	assert_int_eq(as_list_get_int64(l, 3), 1);
	assert_int_eq(as_list_get_int64(l, 4), 11);

	as_record_destroy(prec);
}

TEST(list_nested_map, "Nested List Map")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 109);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist l11;
	as_arraylist_inita(&l11, 3);
	as_arraylist_append_int64(&l11, 7);
	as_arraylist_append_int64(&l11, 9);
	as_arraylist_append_int64(&l11, 5);

	as_arraylist l12;
	as_arraylist_inita(&l12, 1);
	as_arraylist_append_int64(&l12, 13);

	as_arraylist l1;
	as_arraylist_inita(&l1, 2);
	as_arraylist_append(&l1, (as_val*)&l11);
	as_arraylist_append(&l1, (as_val*)&l12);

	as_arraylist l21;
	as_arraylist_inita(&l21, 1);
	as_arraylist_append_int64(&l21, 9);

	as_arraylist l22;
	as_arraylist_inita(&l22, 2);
	as_arraylist_append_int64(&l22, 2);
	as_arraylist_append_int64(&l22, 4);

	as_arraylist l23;
	as_arraylist_inita(&l23, 3);
	as_arraylist_append_int64(&l23, 6);
	as_arraylist_append_int64(&l23, 1);
	as_arraylist_append_int64(&l23, 9);

	as_arraylist l2;
	as_arraylist_inita(&l2, 3);
	as_arraylist_append(&l2, (as_val*)&l21);
	as_arraylist_append(&l2, (as_val*)&l22);
	as_arraylist_append(&l2, (as_val*)&l23);

	as_hashmap map;
	as_hashmap_init(&map, 2);
	as_string k1;
	as_string_init(&k1, "key1", false);
	as_hashmap_set(&map, (as_val*)&k1, (as_val*)&l1);
	as_string k2;
	as_string_init(&k2, "key2", false);
	as_hashmap_set(&map, (as_val*)&k2, (as_val*)&l2);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_map(&rec, BIN_NAME, (as_map*)&map);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 2);

	as_cdt_ctx ctx;
	// as_cdt_ctx_inita() is more efficient, but as_cdt_ctx_init() needs to be tested too.
	// as_cdt_ctx_inita(&ctx, 2);
	as_cdt_ctx_init(&ctx, 2);
	as_string_init(&k2, "key2", false);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)&k2);
	as_cdt_ctx_add_list_rank(&ctx, 0);

	as_integer v;
	as_integer_init(&v, 11);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)&v);
	as_cdt_ctx_destroy(&ctx);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* prec = 0;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &prec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	//example_dump_record(prec);

	as_bin* results = prec->bins.entries;
	int i = 0;

	int64_t val = results[i++].valuep->integer.value;
	assert_int_eq(val, 3);

	as_map* m = &results[i++].valuep->map;
	assert_int_eq(as_map_size(m), 2);

	as_string_init(&k2, "key2", false);
	as_list* lr = (as_list*)as_map_get(m, (as_val*)&k2);
	as_list* lr2 = as_list_get_list(lr, 1);

	assert_int_eq(as_list_size(lr2), 3);
	assert_int_eq(as_list_get_int64(lr2, 0), 2);
	assert_int_eq(as_list_get_int64(lr2, 1), 4);
	assert_int_eq(as_list_get_int64(lr2, 2), 11);

	as_record_destroy(prec);
}

TEST(list_ctx_create_noop, "Nested List ctx")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 200);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_cdt_ctx ctx;
	as_operations ops;
	as_record* rec = NULL;

	// Remove-no-op on a non-existent record fails.
	as_operations_init(&ops, 3);
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_operations_list_clear(&ops, BIN_NAME, &ctx);
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_ne(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	rec = NULL;

	// Set-no-op on a non-existent record&bin creates record&bin.
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_UNORDERED, false);
	as_operations_list_set_order(&ops, BIN_NAME, &ctx, AS_LIST_UNORDERED);
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;

	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_list* ret = as_record_get_list(rec, BIN_NAME);
	assert_not_null(ret);
	assert_int_eq(as_list_size(ret), 1);
	ret = as_list_get_list(ret, 0);
	assert_not_null(ret);
	assert_int_eq(as_list_size(ret), 0);
	as_record_destroy(rec);
	rec = NULL;

	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	// Set-no-op on a non-existent record&bin creates record&bin, same but with ORDERED top level.
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_operations_list_set_order(&ops, BIN_NAME, &ctx, AS_LIST_UNORDERED);
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	assert_not_null(ret);
	assert_int_eq(as_list_size(ret), 1);
	ret = as_list_get_list(ret, 0);
	assert_not_null(ret);
	assert_int_eq(as_list_size(ret), 0);
	as_record_destroy(rec);
	rec = NULL;

	// Remove-no-op should not create non existent bin.
	as_operations_init(&ops, 3);
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_operations_list_clear(&ops, "temp", &ctx);
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_null(as_record_get_list(rec, "temp"));
	as_record_destroy(rec);
	rec = NULL;

	// Add 2 entries.
	as_operations_init(&ops, 2);
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(1));
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(2));
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	as_list* cmp_list = as_list_take(ret, as_list_size(ret));
	as_record_destroy(rec);
	rec = NULL;

	// Remove-no-op on non existent sub-context should be no-op.
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_operations_list_clear(&ops, BIN_NAME, &ctx);
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	as_record_destroy(rec);
	rec = NULL;

	// Deep remove-no-op on non existent sub-context should be no-op.
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 3);
	as_cdt_ctx_add_list_index_create(&ctx, 4, AS_LIST_ORDERED, false);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_UNORDERED, false);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_UNORDERED, false);
	as_operations_list_clear(&ops, BIN_NAME, &ctx);
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Deep set-no-op on non existent sub-context creates context.
	as_list_append_list(cmp_list, (as_list*)as_arraylist_new(1, 1));
	as_list_append_list(as_list_get_list(cmp_list, 3), (as_list*)as_arraylist_new(1, 1));
	as_list_append_list(as_list_get_list(as_list_get_list(cmp_list, 3), 0), (as_list*)as_arraylist_new(1, 1));

	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 3);
	as_cdt_ctx_add_list_index_create(&ctx, 4, AS_LIST_ORDERED, false);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_UNORDERED, false);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_UNORDERED, false);
	as_operations_list_set_order(&ops, BIN_NAME, &ctx, AS_LIST_ORDERED);
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;
	as_list_destroy(cmp_list);

	as_key_destroy(&rkey);
}

TEST(list_ctx_create, "Nested List ctx create")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 201);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_cdt_ctx ctx;
	as_operations ops;
	as_record* rec = NULL;

	as_list_policy pol;
	as_list_policy_set(&pol, AS_LIST_ORDERED, 0);

	// Unbound test.
	as_list* cmp_list = (as_list*)as_arraylist_new(3, 3);
	as_list_set_list(cmp_list, 3, (as_list*)as_arraylist_new(1, 1));
	as_list_append_int64(as_list_get_list(cmp_list, 3), 1);
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_UNORDERED, true);
	as_operations_list_append(&ops, BIN_NAME, &ctx, &pol, (as_val*)as_integer_new(1));
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BIN_NAME);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_list* ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// 2 levels deep
	as_arraylist *single_list = as_arraylist_new(1, 1);
	as_arraylist_append_int64(single_list, 2);
	as_list_append_list(as_list_get_list(cmp_list, 3), (as_list*)single_list);
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_UNORDERED, true); // Create ignored because context exists.
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_UNORDERED, true); // Create ignored because context exists (and is an ordered list).
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(2));
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BIN_NAME);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Ordering on 2nd level.
	single_list = as_arraylist_new(1, 1);
	as_arraylist_append_int64(single_list, 1);
	as_list_insert_list(as_list_get_list(cmp_list, 3), 1, (as_list*)single_list);
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_UNORDERED, true); // Create ignored because context exists.
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_UNORDERED, true); // Create ignored because context exists (and is an ordered list).
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(1));
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BIN_NAME);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	as_list_destroy(cmp_list);
	as_key_destroy(&rkey);
}

TEST(list_ctx_create_order, "Nested List ctx create ordered")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 202);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_cdt_ctx ctx;
	as_operations ops;
	as_record* rec = NULL;

	// Double ordered context.
	as_list* single_list = (as_list*)as_arraylist_new(1, 1);
	as_list_append_int64(single_list, 10);
	as_list* cmp_list = (as_list*)as_arraylist_new(1, 1);
	as_list_append_list(cmp_list, (as_list*)as_arraylist_new(1, 1));
	as_list_append_list(as_list_get_list(cmp_list, 0), single_list);
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(10));
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BIN_NAME);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_list* ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Insert 2nd item.
	single_list = (as_list*)as_arraylist_new(1, 1);
	as_list_append_list(single_list, (as_list*)as_arraylist_new(1, 1));
	as_list_append_int64(as_list_get_list(single_list, 0), 1);
	as_list_insert_list(cmp_list, 0, single_list);
	as_operations_init(&ops, 1);
	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false); // Create ignored due to existing context.
	as_cdt_ctx_add_list_index_create(&ctx, 3, AS_LIST_ORDERED, false);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(1));
	as_cdt_ctx_destroy(&ctx);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;
	// Get and check.
	as_operations_init(&ops, 1);
	as_operations_add_read(&ops, BIN_NAME);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	ret = as_record_get_list(rec, BIN_NAME);
	assert_true(as_list_compare(ret, cmp_list));
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	as_list_destroy(cmp_list);
	as_key_destroy(&rkey);
}

TEST(list_ctx_create_double_nil, "Nested List ctx create nil filling")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 203);

	for (uint32_t index_top = 0; index_top < 129; index_top++) {
		as_error err;
		as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
		assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

		as_cdt_ctx ctx;
		as_operations ops;
		as_record* rec = NULL;

		as_operations_init(&ops, 1);
		as_cdt_ctx_init(&ctx, 2);
		as_cdt_ctx_add_list_index_create(&ctx, index_top, AS_LIST_UNORDERED, true);
		as_cdt_ctx_add_list_index_create(&ctx, index_top, AS_LIST_UNORDERED, true);
		as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(1));
		as_cdt_ctx_destroy(&ctx);

		status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
		assert_int_eq(status, AEROSPIKE_OK);
		as_operations_destroy(&ops);
		as_record_destroy(rec);
		rec = NULL;

		// Get and check.
		as_operations_init(&ops, 1);
		as_operations_add_read(&ops, BIN_NAME);
		status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
		assert_int_eq(status, AEROSPIKE_OK);
		as_operations_destroy(&ops);
		//example_dump_record(rec);
		{
			as_val* cv = (as_val*)rec->bins.entries[0].valuep;
			assert_int_eq(as_val_type(cv), AS_LIST); // top is list
			assert_int_eq(as_list_size((as_list*)cv), index_top + 1);

			for (uint32_t i = 0; i < index_top; i++) {
				as_val *v = as_list_get((as_list*)cv, i);
				assert_int_eq(as_val_type(v), AS_NIL);
			}

			as_list* list_0 = as_list_get_list((as_list*)cv, index_top);
			assert_not_null(list_0);
			assert_int_eq(as_list_size(list_0), index_top + 1);

			for (uint32_t i = 0; i < index_top; i++) {
				as_val *v = as_list_get(list_0, i);
				assert_int_eq(as_val_type(v), AS_NIL);
			}

			as_list* list_1 = as_list_get_list(list_0, index_top);
			assert_not_null(list_1);
			assert_not_null(as_list_get_integer(list_1, 0));
		}

		as_record_destroy(rec);
		rec = NULL;
	}
}

TEST(list_ctx_create_toplvl, "Nested List ctx create top level")
{
	const uint32_t data[] = {
		0, 0x80, 0x100
	};

	uint32_t offset = 0;

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 204);

	const list_order_type types[] = {
		{AS_LIST_UNORDERED, false},
		{AS_LIST_ORDERED, false}
	};

	for (int ord = 0; ord < 2; ord++) {
		list_order_type type = types[ord];
		offset = 0;

		as_error err;
		as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
		assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

		//info("type 0x%x", type);

		for (int i = 0; i < 255; i++) {
			as_cdt_ctx ctx;
			as_operations ops;
			as_record* rec = NULL;

			as_operations_init(&ops, 1);
			as_cdt_ctx_init(&ctx, 2);

			as_cdt_ctx_add_list_index_create(&ctx, i, type.order, type.pad);
			as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(data[offset % 3] + offset / 3));
			as_cdt_ctx_destroy(&ctx);
			offset++;

			status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
			assert_int_eq(status, AEROSPIKE_OK);
			as_operations_destroy(&ops);
			as_record_destroy(rec);
			rec = NULL;

			// Get and check.
			as_operations_init(&ops, 1);
			as_operations_add_read(&ops, BIN_NAME);
			status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
			assert_int_eq(status, AEROSPIKE_OK);
			as_operations_destroy(&ops);
			//example_dump_record(rec);
			{
				as_val* cv = (as_val*)rec->bins.entries[0].valuep;
				assert_int_eq(as_val_type(cv), AS_LIST); // top is list
				as_list *list_0 = (as_list*)cv;

				for (uint32_t j = 0; j < as_list_size(list_0); j++) {
					as_list *p = as_list_get_list(list_0, j);
					assert_not_null(p);
					assert_int_eq(as_list_size(p), 1);
					assert_not_null(as_list_get_integer(p, 0));
				}
			}

			as_record_destroy(rec);
			rec = NULL;
		}
	}
}

TEST(list_ctx_create_nontoplvl_map, "Nested map ctx create non top level")
{
	const uint32_t data[] = {
		0, 0x80, 0x100
	};

	uint32_t offset = 0;

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 205);

	const uint32_t top_types[] = {
		AS_MAP_UNORDERED,
		AS_MAP_KEY_ORDERED,
		AS_MAP_KEY_VALUE_ORDERED
	};

	const list_order_type list_types[] = {
		{AS_LIST_UNORDERED, false},
		{AS_LIST_ORDERED, false}
	};

	for (int top_i = 0; top_i < sizeof(top_types) / sizeof(uint32_t); top_i++) {
		uint32_t top_type = top_types[top_i];

		for (int ord = 0; ord < 2; ord++) {
			list_order_type list_type = list_types[ord];
			offset = 0;

			as_error err;
			as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
			assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

			//info("top_type 0x%x list_type 0x%x", top_type, list_type);

			for (int i = 0; i < 255; i++) {
				as_cdt_ctx ctx;
				as_operations ops;
				as_record* rec = NULL;

				as_operations_init(&ops, 3);
				as_cdt_ctx_init(&ctx, 2);

				as_integer v;
				as_integer_init(&v, 0);

				as_cdt_ctx_add_map_key_create(&ctx, (as_val*)&v, top_type);
				as_cdt_ctx_add_list_index_create(&ctx, i, list_type.order, list_type.pad);
				as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(data[offset % 3] + offset / 3));
				as_cdt_ctx_destroy(&ctx);
				offset++;

				status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
				assert_int_eq(status, AEROSPIKE_OK);
				as_operations_destroy(&ops);
				as_record_destroy(rec);
				rec = NULL;

				// Get and check.
				as_operations_init(&ops, 1);
				as_operations_add_read(&ops, BIN_NAME);
				status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
				assert_int_eq(status, AEROSPIKE_OK);
				as_operations_destroy(&ops);
				//example_dump_record(rec);
				{
					as_val* v_map = (as_val*)rec->bins.entries[0].valuep;
					assert_int_eq(as_val_type(v_map), AS_MAP); // top is map
					assert_int_eq(as_map_size((as_map*)v_map), 1);

					as_hashmap_iterator it;
					as_hashmap_iterator_init(&it, (as_hashmap*)v_map);

					const as_val* map_pair = as_hashmap_iterator_next(&it);
					assert_int_eq(as_val_type(map_pair), AS_PAIR);
					as_val* map_val = as_pair_2((as_pair*)map_pair);

					for (uint32_t j = 0; j < as_list_size((as_list*)map_val); j++) {
						as_list *p = as_list_get_list((as_list*)map_val, j);
						assert_not_null(p);
						assert_int_eq(as_list_size(p), 1);
						assert_not_null(as_list_get_integer(p, 0));
					}

					as_hashmap_iterator_destroy(&it);
				}

				as_record_destroy(rec);
				rec = NULL;
			}
		}
	}
}

TEST(list_ctx_create_nontoplvl_list, "Nested List ctx create non top level")
{
	const uint32_t data[] = {
		0, 0x80, 0x100
	};

	uint32_t offset = 0;

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 206);

	const list_order_type top_types[] = {
		{AS_LIST_UNORDERED, false},
		{AS_LIST_ORDERED, false}
	};

	const list_order_type list_types[] = {
		{AS_LIST_UNORDERED, false},
		{AS_LIST_ORDERED, false}
	};

	for (int top_i = 0; top_i < sizeof(top_types) / sizeof(list_order_type); top_i++) {
		list_order_type top_type = top_types[top_i];

		for (int list_i = 0; list_i < sizeof(list_types) / sizeof(list_order_type); list_i++) {
			list_order_type list_type = list_types[list_i];
			offset = 0;

			as_error err;
			as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
			assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

			//info("top_type 0x%x list_type 0x%x", top_type, list_type);

			for (int i = 0; i < 255; i++) {
				as_cdt_ctx ctx;
				as_operations ops;
				as_record* rec = NULL;

				as_operations_init(&ops, 3);
				as_cdt_ctx_init(&ctx, 2);

				as_cdt_ctx_add_list_index_create(&ctx, 0, top_type.order, top_type.pad);
				as_cdt_ctx_add_list_index_create(&ctx, i, list_type.order, list_type.pad);
				as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(data[offset % 3] + offset / 3));
				as_cdt_ctx_destroy(&ctx);
				offset++;

				status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
				assert_int_eq(status, AEROSPIKE_OK);
				as_operations_destroy(&ops);
				as_record_destroy(rec);
				rec = NULL;

				// Get and check.
				as_operations_init(&ops, 1);
				as_operations_add_read(&ops, BIN_NAME);
				status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
				assert_int_eq(status, AEROSPIKE_OK);
				as_operations_destroy(&ops);
				//example_dump_record(rec);
				{
					as_val* cv = (as_val*)rec->bins.entries[0].valuep;
					assert_int_eq(as_val_type(cv), AS_LIST); // top is list
					assert_int_eq(as_list_size((as_list*)cv), 1);

					as_list* list_0 = as_list_get_list((as_list*)cv, 0);
					assert_not_null(list_0);

					for (uint32_t j = 0; j < as_list_size(list_0); j++) {
						as_list *p = as_list_get_list(list_0, j);
						assert_not_null(p);
						assert_int_eq(as_list_size(p), 1);
						assert_not_null(as_list_get_integer(p, 0));
					}
				}

				as_record_destroy(rec);
				rec = NULL;
			}
		}
	}
}

TEST(list_ctx_create_pad, "List ctx create with padding")
{
	as_key rkey;
	as_key_init_str(&rkey, NAMESPACE, SET, "oplkey20");

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist *l1 = as_arraylist_new(3, 1);
	as_arraylist_append_int64(l1, 7);
	as_arraylist_append_int64(l1, 9);
	as_arraylist_append_int64(l1, 5);

	as_arraylist *l2 = as_arraylist_new(3, 1);
	as_arraylist_append_int64(l2, 1);
	as_arraylist_append_int64(l2, 2);
	as_arraylist_append_int64(l2, 3);

	as_arraylist *l3 = as_arraylist_new(4, 1);
	as_arraylist_append_int64(l3, 6);
	as_arraylist_append_int64(l3, 5);
	as_arraylist_append_int64(l3, 4);
	as_arraylist_append_int64(l3, 1);

	as_arraylist *inputList = as_arraylist_new(3, 1);
	as_arraylist_append(inputList, (as_val*)l1);
	as_arraylist_append(inputList, (as_val*)l2);
	as_arraylist_append(inputList, (as_val*)l3);

	as_record *rec = as_record_new(1);
	as_record_set_list(rec, BIN_NAME, (as_list*)inputList);
	aerospike_key_put(as, &err, NULL, &rkey, rec);
	as_record_destroy(rec);
	rec = NULL;

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index_create(&ctx, 4, AS_LIST_UNORDERED, true);
	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(111));

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, NULL);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);

	// Get and check.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_list* ret = as_record_get_list(rec, BIN_NAME);
	assert_not_null(ret);
	assert_int_eq(as_list_size(ret), 5);
	ret = as_list_get_list(ret, 4);
	assert_not_null(ret);
	assert_int_eq(as_list_size(ret), 1);
	assert_int_eq(as_list_get_int64(ret, 0), 111);
	as_record_destroy(rec);
	rec = NULL;

	as_cdt_ctx_destroy(&ctx);
	as_key_destroy(&rkey);
}

TEST(list_create, "Create list")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 207);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist l1;
	as_arraylist_init(&l1, 3, 0);
	as_arraylist_append_int64(&l1, 7);
	as_arraylist_append_int64(&l1, 9);
	as_arraylist_append_int64(&l1, 5);

	as_arraylist l2;
	as_arraylist_init(&l2, 3, 0);
	as_arraylist_append_int64(&l2, 1);
	as_arraylist_append_int64(&l2, 2);
	as_arraylist_append_int64(&l2, 3);

	as_arraylist l3;
	as_arraylist_init(&l3, 2, 0);
	as_arraylist_append_int64(&l3, 6);
	as_arraylist_append_int64(&l3, 5);

	as_arraylist l;
	as_arraylist_init(&l, 3, 0);
	as_arraylist_append(&l, (as_val*)&l1);
	as_arraylist_append(&l, (as_val*)&l2);
	as_arraylist_append(&l, (as_val*)&l3);

	as_list_policy lp;
	as_list_policy_set(&lp, AS_LIST_ORDERED, AS_LIST_WRITE_DEFAULT);

	// Write nested list.
	as_operations ops;
	as_operations_inita(&ops, 2);

	as_operations_add_list_append_items_with_policy(&ops, BIN_NAME, &lp, (as_list*)&l);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);
	as_record_destroy(rec);

	// Create new list in nested list.
	as_operations_inita(&ops, 2);

	as_cdt_ctx ctx;
	as_cdt_ctx_inita(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, 3);

	as_operations_list_create(&ops, BIN_NAME, &ctx, AS_LIST_ORDERED, false);
	as_operations_add_read(&ops, BIN_NAME);
	as_cdt_ctx_destroy(&ctx);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	//example_dump_record(rec);

	as_bin* results = rec->bins.entries;
	as_list* rlist = &results[0].valuep->list;
	assert_int_eq(as_list_size(rlist), 4);

	as_list* rl = as_list_get_list(rlist, 0);
	assert_int_eq(as_list_size(rl), 0);
	as_record_destroy(rec);
}

TEST(list_exp_mod, "List Modify Expressions")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 208);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_hashmap map;
	as_hashmap_init(&map, 1);
	as_hashmap_set(&map, (as_val*)as_integer_new(1), (as_val*)as_integer_new(1));

	as_arraylist list;
	as_arraylist_init(&list, 6, 6);
	as_arraylist_append_double(&list, 1.1); // double > map
	as_arraylist_append_map(&list, (as_map*)&map); // map > string
	as_arraylist_append_str(&list, "7"); // string > int
	as_arraylist_append_int64(&list, 9);
	as_arraylist_append_int64(&list, 5);
	as_arraylist_append_int64(&list, 3);

	as_record *rec = as_record_new(1);
	as_record_set_list(rec, BIN_NAME, (as_list*)&list);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_true(status == AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_policy_read p;
	as_policy_read_init(&p);

	as_exp_build(filter,
		as_exp_cmp_eq(
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_STR, as_exp_int(3),
				as_exp_list_sort(NULL, 0, as_exp_bin_list(BIN_NAME))),
			as_exp_str("7")));
	assert_not_null(filter);

	p.base.filter_exp = filter;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter);

	as_exp_build(filter2,
		as_exp_cmp_gt(
			as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_FLOAT, as_exp_int(5),
				as_exp_list_sort(NULL, 0, as_exp_bin_list(BIN_NAME))),
			as_exp_float(0.1)));
	assert_not_null(filter2);

	p.base.filter_exp = filter2;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter2);

	as_exp_build(filter3,
		as_exp_cmp_eq(
			as_exp_map_get_by_index(NULL, AS_MAP_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
				as_exp_list_get_by_rank(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_MAP, as_exp_int(4),
					as_exp_bin_list(BIN_NAME))),
			as_exp_int(1)));
	assert_not_null(filter3);

	p.base.filter_exp = filter3;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter3);

	as_arraylist add;
	as_arraylist_init(&add, 2, 2);
	as_arraylist_append_int64(&add, 1);
	as_arraylist_append_int64(&add, 2);

	as_exp_build(filter4,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_list_size(NULL,
					as_exp_list_insert(NULL, NULL, as_exp_int(2), as_exp_str("x"),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(7)),
			as_exp_cmp_eq(
				as_exp_list_size(NULL,
					as_exp_list_insert_items(NULL, NULL, as_exp_int(3), as_exp_val(&add),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(8))));

	as_arraylist_destroy(&add);
	assert_not_null(filter4);

	p.base.filter_exp = filter4;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter4);

	as_arraylist expected;
	as_arraylist_init(&expected, 3, 3);
	as_arraylist_append_int64(&expected, 11);
	as_arraylist_append_int64(&expected, 5);
	as_arraylist_append_int64(&expected, 3);

	as_exp_build(filter5,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_list_get_by_rank(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(0),
					as_exp_list_increment(NULL, NULL, as_exp_int(5), as_exp_int(1),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(4)),
			as_exp_cmp_eq(
				as_exp_list_get_by_index_range(NULL, AS_LIST_RETURN_VALUE, as_exp_int(3), as_exp_int(3),
					as_exp_list_set(NULL, NULL, as_exp_int(3), as_exp_int(11),
						as_exp_bin_list(BIN_NAME))),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_size(NULL,
					as_exp_list_remove_by_value(NULL, AS_LIST_RETURN_NONE, as_exp_int(5), as_exp_bin_list(BIN_NAME))),
				as_exp_int(5)),
			as_exp_cmp_eq(
				as_exp_list_size(NULL,
					as_exp_list_remove_by_value_list(NULL, AS_LIST_RETURN_NONE, as_exp_val(&expected), as_exp_bin_list(BIN_NAME))),
				as_exp_int(4))));

	as_arraylist_destroy(&expected);
	assert_not_null(filter5);

	p.base.filter_exp = filter5;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter5);

	as_exp_build(filter6,
		as_exp_not(as_exp_or(
			as_exp_cmp_gt(
				as_exp_list_size(NULL,
					as_exp_list_remove_by_value_range(NULL, AS_LIST_RETURN_NONE, as_exp_int(3), as_exp_int(6),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(4)),
			as_exp_cmp_gt(
				as_exp_list_size(NULL,
					as_exp_list_remove_by_rel_rank_range_to_end(NULL, AS_LIST_RETURN_NONE, as_exp_int(9), as_exp_int(0),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(2)),
			as_exp_cmp_gt(
				as_exp_list_size(NULL,
					as_exp_list_remove_by_rel_rank_range(NULL, AS_LIST_RETURN_NONE, as_exp_int(9), as_exp_int(0), as_exp_int(3),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(3)))));
	assert_not_null(filter6);

	p.base.filter_exp = filter6;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter6);

	as_arraylist_init(&expected, 3, 3);
	as_arraylist_append_int64(&expected, 9);
	as_arraylist_append_int64(&expected, 5);
	as_arraylist_append_int64(&expected, 3);

	as_arraylist empty;
	as_arraylist_init(&empty, 1, 1);

	as_exp_build(filter7,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_list_size(NULL,
					as_exp_list_remove_by_index(NULL, as_exp_int(3),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(5)),
			as_exp_cmp_eq(
				as_exp_list_get_by_index(NULL, AS_LIST_RETURN_VALUE, AS_EXP_TYPE_INT, as_exp_int(-1),
					as_exp_list_remove_by_rank(NULL, as_exp_int(0),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(5)),
			as_exp_cmp_eq(
				as_exp_list_remove_by_rank_range_to_end(NULL, AS_LIST_RETURN_NONE, as_exp_int(3),
					as_exp_bin_list(BIN_NAME)),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_remove_by_rank_range(NULL, AS_LIST_RETURN_NONE, as_exp_int(-3), as_exp_int(3),
					as_exp_bin_list(BIN_NAME)),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_size(NULL,
					as_exp_list_remove_by_index_range_to_end(NULL, AS_LIST_RETURN_NONE, as_exp_int(1),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(1)),
			as_exp_cmp_eq(
				as_exp_list_clear(NULL,
					as_exp_bin_list(BIN_NAME)),
				as_exp_val(&empty)),
			as_exp_cmp_eq(
				as_exp_list_remove_by_index_range(NULL, AS_LIST_RETURN_NONE, as_exp_int(0), as_exp_int(3),
						as_exp_bin_list(BIN_NAME)),
				as_exp_val(&expected))));

	as_arraylist_destroy(&empty);
	as_arraylist_destroy(&expected);
	assert_not_null(filter7);

	p.base.filter_exp = filter7;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter7);
}

TEST(list_exp_read, "List Read Expressions")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 209);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist list;
	as_arraylist_init(&list, 6, 6);
	as_arraylist_append_int64(&list, 19);
	as_arraylist_append_int64(&list, 11);
	as_arraylist_append_int64(&list, 16);
	as_arraylist_append_int64(&list, 8);
	as_arraylist_append_int64(&list, 5);
	as_arraylist_append_int64(&list, 3);

	as_record *rec = as_record_new(1);
	as_record_set_list(rec, BIN_NAME, (as_list*)&list);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_true(status == AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_policy_read p;
	as_policy_read_init(&p);

	as_arraylist expected;
	as_arraylist_init(&expected, 1, 1);
	as_arraylist_append_int64(&expected, 8);

	as_arraylist expected2;
	as_arraylist_init(&expected2, 3, 1);
	as_arraylist_append_int64(&expected2, 8);
	as_arraylist_append_int64(&expected2, 5);
	as_arraylist_append_int64(&expected2, 3);

	as_arraylist get_list;
	as_arraylist_init(&get_list, 3, 3);
	as_arraylist_append_int64(&get_list, 3);
	as_arraylist_append_int64(&get_list, 5);
	as_arraylist_append_int64(&get_list, 8);

	as_exp_build(filter,
		as_exp_and(
			as_exp_cmp_eq(
				as_exp_list_get_by_value_range(NULL, AS_LIST_RETURN_VALUE, as_exp_int(8), as_exp_int(9),
					as_exp_bin_list(BIN_NAME)),
				as_exp_val(&expected)),
			as_exp_cmp_eq(
				as_exp_list_get_by_rel_rank_range_to_end(NULL, AS_LIST_RETURN_COUNT, as_exp_int(5), as_exp_int(0),
					as_exp_list_get_by_value_list(NULL, AS_LIST_RETURN_VALUE, as_exp_val(&get_list),
						as_exp_bin_list(BIN_NAME))),
				as_exp_int(2)),
			as_exp_cmp_eq(
				as_exp_list_get_by_rel_rank_range(NULL, AS_LIST_RETURN_COUNT, as_exp_int(5), as_exp_int(0), as_exp_int(3),
					as_exp_bin_list(BIN_NAME)),
				as_exp_int(3)),
			as_exp_list_get_by_rel_rank_range(NULL, AS_LIST_RETURN_EXISTS, as_exp_int(5), as_exp_int(0), as_exp_int(3),
					as_exp_bin_list(BIN_NAME)),
			as_exp_cmp_eq(
				as_exp_list_get_by_index_range_to_end(NULL, AS_LIST_RETURN_VALUE, as_exp_int(3),
					as_exp_bin_list(BIN_NAME)),
				as_exp_val(&expected2)),
			as_exp_cmp_eq(
				as_exp_list_get_by_rank_range(NULL, AS_LIST_RETURN_VALUE, as_exp_int(0), as_exp_int(1),
					as_exp_list_get_by_rank_range_to_end(NULL, AS_LIST_RETURN_VALUE, as_exp_int(2),
						as_exp_bin_list(BIN_NAME))),
				as_exp_val(&expected))));

	as_arraylist_destroy(&get_list);
	as_arraylist_destroy(&expected);
	as_arraylist_destroy(&expected2);
	assert_not_null(filter);

	p.base.filter_exp = filter;

	status = aerospike_key_get(as, &err, &p, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_exp_destroy(filter);
}

TEST(exp_returns_list, "exp returns list")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 210);
	const char* CString = "C";

	as_arraylist list;
	make_random_list(&list, 10);

	as_exp_build(expr, as_exp_val(&list));
	assert_not_null(expr);

	as_error err;
	as_record* rec;
	as_status rc;
	as_operations ops;
	as_bin* results;

	as_operations_inita(&ops, 3);
	as_operations_exp_write(&ops, CString, expr, AS_EXP_WRITE_DEFAULT);
	as_operations_add_read(&ops, CString);
	as_operations_exp_read(&ops, "EV", expr, AS_EXP_READ_DEFAULT);
	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[1]), AS_LIST);
	assert_int_eq(as_bin_get_type(&results[2]), AS_LIST);

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	as_operations_inita(&ops, 2);
	as_operations_exp_read(&ops, "EV", expr, AS_EXP_READ_DEFAULT);
	as_operations_add_read(&ops, CString);

	rec = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_bin_get_type(&results[0]), AS_LIST);
	assert(as_list_compare((as_list*)&list, &as_bin_get_value(&results[0])->list));

	as_record_destroy(rec);
	as_operations_destroy(&ops);
	as_list_destroy((as_list*)&list);
	as_exp_destroy(expr);
}

TEST(list_exp_infinity, "test as_exp_inf()")
{
    as_arraylist list;
    as_arraylist_inita(&list, 10);
    as_arraylist_append_int64(&list, 40);
    as_arraylist_append_int64(&list, 6);
    as_arraylist_append_int64(&list, 13);
    as_arraylist_append_int64(&list, 27);
    as_arraylist_append_int64(&list, 33);
    as_arraylist_append_int64(&list, 33);
    as_arraylist_append_int64(&list, 10);
    as_arraylist_append_int64(&list, 7);
    as_arraylist_append_int64(&list, 15);
    as_arraylist_append_int64(&list, 5);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 211);

	as_record rec;
    as_record_init(&rec, 1);
    as_record_set_list(&rec, BIN_NAME, (as_list*)&list);

	as_error err;
	as_status status = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
    as_record_destroy(&rec);

	// Remove values >= 30
	as_exp_build(read_exp, as_exp_list_remove_by_value_range(NULL, AS_LIST_RETURN_NONE, as_exp_int(30), as_exp_inf(), as_exp_bin_list(BIN_NAME)));
	assert_not_null(read_exp);

	as_operations ops;
    as_operations_inita(&ops, 1);
    as_operations_exp_read(&ops, BIN_NAME, read_exp, AS_EXP_READ_DEFAULT);
    
    as_record* rec_ptr = NULL;
    status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec_ptr);
	assert_int_eq(status, AEROSPIKE_OK);

	as_list* rlist = as_record_get_list(rec_ptr, BIN_NAME);
	// Should return [6,13,27,10,7,15,5]
	assert_int_eq(as_list_size(rlist), 7);

    as_record_destroy(rec_ptr);
    as_operations_destroy(&ops);
    as_exp_destroy(read_exp);
}

// Add flag for the purpose of tests that bypass user functions and use low-level wire protocol.
#define AS_LIST_FLAG_PERSIST_INDEX 0x10

TEST(list_persist_index, "test persist index")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 212);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	uint8_t buf[4096];
	as_bytes b;

	// Test out of order.
	as_packer pk = {
			.buffer = buf,
			.capacity = sizeof(buf)
	};

	as_pack_list_header(&pk, 6);
	as_pack_ext_header(&pk, 0, AS_LIST_ORDERED | AS_LIST_FLAG_PERSIST_INDEX);

	for (int i = 0; i < 5; i++) {
		as_pack_int64(&pk, 5 - i);
	}

	as_bytes_init_wrap(&b, buf, pk.offset, false);
	as_bytes_set_type(&b, AS_BYTES_LIST);

	as_record* rec = as_record_new(1);
	as_record_set_bytes(rec, BIN_NAME, (as_bytes*)&b);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_ne(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_operations ops;
	as_cdt_ctx ctx;

	// Test ctx create.
	as_arraylist list;
	as_arraylist_init(&list, 1, 0);
	as_arraylist_append_int64(&list, 1);

	as_operations_init(&ops, 2);
	as_operations_list_create_all(&ops, BIN_NAME, NULL, AS_LIST_UNORDERED, false, true);
	as_operations_list_append(&ops, BIN_NAME, NULL, NULL, (as_val*)&list);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);

	as_list* check_list = as_record_get_list(rec, BIN_NAME);
	assert_not_null(check_list);
	as_list* check_list1 = as_list_get_list(check_list, 0);
	assert_not_null(check_list1);
	assert_int_eq(as_list_size(check_list1), 1);
	assert_int_eq(as_list_get_int64(check_list1, 0), 1);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Test ctx create UNBOUNDED.
	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK);

	as_arraylist_init(&list, 1, 0);
	as_arraylist_append_int64(&list, 1);

	as_operations_init(&ops, 2);
	as_operations_list_create_all(&ops, BIN_NAME, NULL, AS_LIST_UNORDERED, true, true);
	as_operations_list_insert(&ops, BIN_NAME, NULL, NULL, 10, (as_val*)&list);

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(rec);
	rec = NULL;

	// Get.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	check_list = as_record_get_list(rec, BIN_NAME);
	assert_int_eq(as_list_size(check_list), 11);
	check_list1 = as_list_get_list(check_list, 10);
	assert_int_eq(as_list_get_int64(check_list1, 0), 1);
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	// Test ctx create sub presist rejection.
	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK);

	as_operations_init(&ops, 1);

	as_cdt_ctx_init(&ctx, 2);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_UNORDERED, false);
	as_cdt_ctx_add_list_index_create(&ctx, 0, AS_LIST_ORDERED, false);
	as_cdt_ctx_item* hack_item = as_vector_get(&ctx.list, ctx.list.size - 1);
	hack_item->type |= 0x100; // hack in a persist flag, do not do this normally

	as_operations_list_append(&ops, BIN_NAME, &ctx, NULL, (as_val*)as_integer_new(1));

	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_ne(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_operations_destroy(&ops);
	as_cdt_ctx_destroy(&ctx);
}

TEST(list_persist_udf, "test persist udf")
{
	load_udf("client_record_lists.lua");

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 213);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist l;
	as_arraylist_init(&l, 4, 4);

	as_arraylist_append_int64(&l, 10);
	as_arraylist_append_int64(&l, 20);
	as_arraylist_append_int64(&l, 30);
	as_arraylist_append_int64(&l, 0);

	as_record* rec = as_record_new(1);
	as_record_set_list(rec, BIN_NAME, (as_list*)&l);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_operations ops;
	as_operations_init(&ops, 1);

	as_operations_list_set_order(&ops, BIN_NAME, NULL, AS_LIST_ORDERED);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_operations_destroy(&ops);

	// Get.
	as_list *check_list;
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	check_list = as_record_get_list(rec, BIN_NAME);
	assert_not_null(check_list);
	assert_int_eq(as_list_size(check_list), 4);
	int64_t check_int = as_list_get_int64(check_list, 0);
	for (uint32_t i = 1; i < as_list_size(check_list); i++) {
		int64_t num = as_list_get_int64(check_list, i);
		assert_true(check_int <= num);
		check_int = num;
	}
	//example_dump_record(rec);
	as_record_destroy(rec);
	rec = NULL;

	as_arraylist args;
	as_arraylist_init(&args,2,2);
	as_arraylist_append_str(&args, BIN_NAME);
	as_arraylist_append_int64(&args, 5);
	as_val* val = NULL;

	aerospike_key_apply(as, &err, NULL, &rkey, "client_record_lists", "append", (as_list*)&args, &val);
	assert_int_eq(err.code, AEROSPIKE_OK);
	as_val_destroy(val);
	as_arraylist_destroy(&args);

	// Get.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);
	check_list = as_record_get_list(rec, BIN_NAME);
	assert_not_null(check_list);
	assert_int_eq(as_list_size(check_list), 5);
	check_int = as_list_get_int64(check_list, 0);
	for (uint32_t i = 1; i < as_list_size(check_list); i++) {
		int64_t num = as_list_get_int64(check_list, i);
		assert_true(check_int <= num);
		check_int = num;
	}
	as_record_destroy(rec);
	rec = NULL;
}

TEST(list_ordered_udf, "test ordered udf")
{
	bool check = load_udf("list_unordered.lua");
	assert_true(check);

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 214);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	const char* test_strings[] = {
			"1", "231020204109691000", "231020204109704569"
	};
	as_arraylist l;
	as_arraylist_init(&l, 4, 4);
	as_arraylist_append_str(&l, test_strings[1]);
	as_arraylist_append_str(&l, test_strings[2]);
	as_arraylist_append_str(&l, test_strings[0]);

	as_record* rec = as_record_new(1);
	as_record_set_list(rec, "list1", (as_list*)&l);

	status = aerospike_key_put(as, &err, NULL, &rkey, rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;

	as_operations ops;
	as_operations_init(&ops, 1);
	as_operations_list_set_order(&ops, "list1", NULL, AS_LIST_ORDERED);
	status = aerospike_key_operate(as, &err, NULL, &rkey, &ops, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(rec);
	rec = NULL;
	as_operations_destroy(&ops);

	// Get.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);
	as_list* check_list = as_record_get_list(rec, "list1");
	for (int i = 0; i < 3; i++) {
		assert_string_eq(test_strings[i], as_list_get_str(check_list, i));
	}
	as_record_destroy(rec);
	rec = NULL;

	const char* test_strings2[] = {
			test_strings[0], test_strings[1],
			"231020204109704558",
			"231020204109704567", test_strings[2]
	};
	as_arraylist args;
	as_arraylist_init(&args,4,4);
	as_arraylist_append_str(&args, test_strings2[2]);
	as_arraylist_append_str(&args, test_strings2[3]);
	as_val* val = NULL;

	aerospike_key_apply(as, &err, NULL, &rkey, "list_unordered", "list_unordered", (as_list*)&args, &val);
	assert_int_eq(err.code, AEROSPIKE_OK);
//	char* s = as_val_tostring(val);
//	info("ret %s", s);
//	info(s);
//	free(s);
	as_arraylist_destroy(&args);
	as_val_destroy(val);

	// Get.
	status = aerospike_key_get(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	//example_dump_record(rec);
	check_list = as_record_get_list(rec, "list1");
	for (int i = 0; i < 5; i++) {
		assert_string_eq(test_strings2[i], as_list_get_str(check_list, i));
	}
	as_record_destroy(rec);
	rec = NULL;
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(list_basics, "aerospike list basic tests")
{
	suite_add(cdt_basics_op);
	suite_add(cdt_incr);
	suite_add(list_switch_sort);
	suite_add(list_sort);
	suite_add(list_remove);
	suite_add(list_inverted);
	suite_add(list_insert);
	suite_add(list_increment);
	suite_add(list_get_relative);
	suite_add(list_remove_relative);
	suite_add(list_partial);
	suite_add(list_nested);
	suite_add(list_nested_map);
	suite_add(list_ctx_create_noop);
	suite_add(list_ctx_create);
	suite_add(list_ctx_create_order);
	suite_add(list_ctx_create_double_nil);
	suite_add(list_ctx_create_toplvl);
	suite_add(list_ctx_create_nontoplvl_map);
	suite_add(list_ctx_create_nontoplvl_list);
	suite_add(list_ctx_create_pad);
	suite_add(list_create);
	suite_add(list_exp_mod);
	suite_add(list_exp_read);

	// Requires Aerospike 5.6.
	suite_add(exp_returns_list);

	suite_add(list_exp_infinity);

	suite_add(list_persist_index);
	suite_add(list_persist_udf);
	suite_add(list_ordered_udf);
}
