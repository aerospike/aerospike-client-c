/*
 * Copyright 2008-2018 Aerospike, Inc.
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

#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike *as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_cdt"
#define BIN_NAME "test-list-1"
#define INFO_CALL "features"

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_testlist_s {
	aerospike *as;
	as_key key;
	as_record *rec;

	as_arraylist arraylist;
} as_testlist;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

void example_dump_record(const as_record* p_rec);

static bool as_testlist_compare(as_testlist *tlist);

bool
has_cdt_list()
{
	char *res = NULL;
	as_error err;
	int rc = aerospike_info_any(as, &err, NULL, INFO_CALL, &res);

	if (rc == AEROSPIKE_OK) {
		char *st = strstr(res, "cdt-list");
		free(res);

		if (st) {
			return true;
		}
	}
	return false;
}

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
		debug("as_testlist_op() returned %d - %s", err.code, err.message);
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

	debug("wrote empty list");
	if (! as_testlist_compare(tlist)) {
		error("write empty list: post compare failed");
		return false;
	}

	debug("remove record");
	if (aerospike_key_remove(as, &err, NULL, &tlist->key) != AEROSPIKE_OK) {
		error("aerospike_key_remove failed");
		return false;
	}

	as_operations_inita(&ops, 1);
	as_operations_add_list_append_items(&ops, BIN_NAME, (as_list *)&list);

	debug("append empty list");
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

	debug("remove_range: index=%d count=%u out_of_range=%s", index, count, first_index_invalid ? "true" : "false")
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
	uint32_t result_size = as_list_size(list);

	debug("get_range: result_size=%u", result_size);
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

	debug("get_range_from: result_size=%u", result_size);
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
as_testlist_compare(as_testlist *tlist)
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

TEST( cdt_basics_op , "CDT operations test on a single bin" )
{
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}

	as_testlist tlist;
	assert_true( as_testlist_init(&tlist, as) );

	debug("insert 1");
	for (int i = 0; i < 100; i++) {
		int ridx = rand() % 100;
		int v = rand() % 1000;

		assert_true( as_testlist_insert(&tlist, ridx, (as_val *)as_integer_new(v)) );
	}

	assert_true( as_testlist_compare(&tlist) );

	as_arraylist list;

	debug("insert_list of 1 item");
	make_random_list(&list, 1);
	as_testlist_insert_list(&tlist, 1, &list);

	debug("insert_list");
	make_random_list(&list, 5);
	for (int i = 0; i < 10; i++) {
		int ridx = rand() % 100;

		as_val_reserve((as_val *)&list);
		as_testlist_insert_list(&tlist, (uint32_t)ridx, &list);
	}
	as_arraylist_destroy(&list);
	assert_true( as_testlist_compare(&tlist) );

	debug("append 1");
	for (int i = 0; i < 20; i++) {
		int v = rand() % 1000;

		assert_true( as_testlist_append(&tlist, (as_val *)as_integer_new(v)) );
	}

	debug("append_list");
	make_random_list(&list, 10);
	for (int i = 0; i < 8; i++) {
		as_val_reserve((as_val *)&list);
		as_testlist_append_list(&tlist, &list);
	}
	as_arraylist_destroy(&list);
	assert_true( as_testlist_compare(&tlist) );

	debug("pop -1");
	for (int i = 0; i < 50; i++) {
		assert_true( as_testlist_remove(&tlist, -1, true) );
	}
	assert_true( as_testlist_compare(&tlist) );

	debug("pop_range");
	for (int i = 0; i < 10; i++) {
		int ridx = rand() % 100;

		as_testlist_remove_range(&tlist, ridx, 5, true);
	}
	assert_true( as_testlist_compare(&tlist) );

	debug("remove 1");
	for (int i = 0; i < 50; i++) {
		int ridx = rand() % 100;

		assert_true( as_testlist_remove(&tlist, ridx, false) );
	}
	assert_true( as_testlist_compare(&tlist) );

	debug("remove_range");
	for (int i = 0; i < 50; i++) {
		int ridx = rand() % 100;

		assert_true( as_testlist_remove_range(&tlist, ridx, 5, false) );
	}
	assert_true( as_testlist_compare(&tlist) );

	debug("remove_range: negative out of range");
	assert_true( as_testlist_remove_range(&tlist, -100, 5, false) );

	debug("remove_range: negative index");
	assert_true( as_testlist_remove_range(&tlist, -1, 5, false) );

	debug("remove_range: 0 count");
	assert_true( as_testlist_remove_range(&tlist, 1, 0, false) );

	debug("get_range");
	as_testlist_get_range(&tlist, 0, 22);
	as_testlist_get_range(&tlist, 10, 22);
	as_testlist_get_range_from(&tlist, 20);
	as_testlist_get_range_from(&tlist, 25);

	debug("set");
	for (int i = 0; i < 100; i++) {
		int ridx = rand() % 100;
		int v = rand() % 1000;

		assert_true( as_testlist_set(&tlist, ridx, (as_val *)as_integer_new(v)) );
	}

	assert_true( as_testlist_compare(&tlist) );

	debug("trim 10%% x 5");
	for (int i = 0; i < 5; i++) {
		uint32_t size = as_arraylist_size(&tlist.arraylist);
		int idx = size / 20;
		uint32_t count = size * 9 / 10;
		as_testlist_trim(&tlist, idx, count);
	}
	assert_true( as_testlist_compare(&tlist) );

	debug("clear");
	as_testlist_clear(&tlist);
	assert_true( as_testlist_compare(&tlist) );

	as_testlist_destroy(&tlist);
}

TEST( cdt_incr , "CDT incr test on a single bin" )
{
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}

	as_testlist tlist;
	assert_true( as_testlist_init(&tlist, as) );

	as_testlist_append(&tlist, (as_val *)as_string_new("test", false));
	as_testlist_append(&tlist, (as_val *)as_integer_new(1));
	as_testlist_append(&tlist, (as_val *)as_integer_new(2));
	as_testlist_append(&tlist, (as_val *)as_integer_new(30000));
	as_testlist_append(&tlist, (as_val *)as_integer_new(4));
	as_testlist_append(&tlist, (as_val *)as_integer_new(5));
	as_testlist_append(&tlist, (as_val *)as_string_new("end", false));

	for (int64_t i = 0; i < 7; i++) {
		as_testlist_incr(&tlist, i, (as_val *)as_integer_new(1));
	}

	assert_true( as_testlist_compare(&tlist) );

	as_testlist_destroy(&tlist);
}

TEST(list_switch_sort, "List Switch Sort")
{
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}

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
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}

	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 101);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_operations ops;
	as_operations_inita(&ops, 3);

	as_arraylist item_list;
	as_arraylist_init(&item_list, 5, 0);
	as_arraylist_append_int64(&item_list, -44);
	as_arraylist_append_int64(&item_list, 33);
	as_arraylist_append_int64(&item_list, -1);
	as_arraylist_append_int64(&item_list, 33);
	as_arraylist_append_int64(&item_list, -2);
	as_operations_add_list_append_items(&ops, BIN_NAME, (as_list*)&item_list);
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
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}

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
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}

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
}
