/*
 * Copyright 2015 Aerospike, Inc.
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

static bool
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

void
make_random_list(as_arraylist *list, uint32_t count)
{
	as_arraylist_init(list, count, 1);
	for (uint32_t i = 0; i < count; i++) {
		as_arraylist_append(list, (as_val *)as_integer_new(rand()%1000));
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
		info("as_testlist_op() returned %d - %s", err.code, err.message);
		return false;
	}

	as_operations_destroy(ops);

	return true;
}

static void
as_testlist_init(as_testlist *tlist, aerospike *as)
{
	tlist->as = as;
	tlist->rec = NULL;
	as_key_init_int64(&tlist->key, NAMESPACE, SET, 1);

	as_arraylist_init(&tlist->arraylist, 100, 100);

	as_operations ops;
	as_operations_inita(&ops, 1);

	as_arraylist list;
	as_arraylist_init(&list, 1, 1);

	as_operations_add_write(&ops, BIN_NAME, (as_bin_value *)&list);

	as_testlist_op(tlist, &ops);
}

static void
as_testlist_destroy(as_testlist *tlist)
{
	as_arraylist_destroy(&tlist->arraylist);

	as_key_destroy(&tlist->key);
}

static uint32_t
index2uindex(as_testlist *tlist, int index)
{
	if (index < 0) {
		return as_arraylist_size(&tlist->arraylist) + index;
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
		AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_POP, index);
	}
	else {
		AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_REMOVE, index);
	}

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_remove_range(as_testlist *tlist, int index, uint32_t count, bool is_pop)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);

	for (uint32_t i = 0; i < count; i++) {
		as_arraylist_remove(&tlist->arraylist, uindex);
	}
	if (is_pop) {
		AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_POP_RANGE, (int64_t)index, (uint64_t)count);
	}
	else {
		AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_REMOVE_RANGE, (int64_t)index, (uint64_t)count);
	}

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_append(as_testlist *tlist, as_val *val)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_val_reserve(val);
	as_arraylist_append(&tlist->arraylist, val);
	AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_APPEND, val);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_append_list(as_testlist *tlist, as_arraylist *list)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_arraylist_concat(&tlist->arraylist, list);
	AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_APPEND_LIST, list);

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

	AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_INSERT, index, val);

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

	AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_INSERT_LIST, index, list);

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

	AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_SET, index, val);

	return as_testlist_op(tlist, &ops);
}

static bool
as_testlist_trim(as_testlist *tlist, int64_t index, uint64_t count)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	uint32_t uindex = index2uindex(tlist, index);
	as_arraylist_trim(&tlist->arraylist, uindex + count);
	for (int64_t i = 0; i < index; i++) {
		as_arraylist_remove(&tlist->arraylist, 0);
	}

	AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_TRIM, index, count);

	return as_testlist_op(tlist, &ops);

}

static bool
as_testlist_clear(as_testlist *tlist)
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	as_arraylist_trim(&tlist->arraylist, 0);

	AS_OPERATIONS_CDT_OP(&ops, BIN_NAME, AS_CDT_OP_LIST_CLEAR);

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
		info("as_testlist_compare() server_size: %u local_size: %u", server_size, local_size);
		return false;
	}

	for (int i = 0; i < local_size; i++) {
		int64_t server_value = as_list_get_int64(list, i);
		int64_t local_value = as_arraylist_get_int64(&tlist->arraylist, i);

		if (server_value != local_value) {
			info("as_testlist_compare() server_value: %u local_value: %u", server_value, local_value);
			return false;
		}
	}

	return true;
}


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST( cdt_basics_op , "CDT operations test on a single bin" ) {
	if (! has_cdt_list()) {
		info("cdt-list not enabled. skipping test");
		return;
	}

	as_testlist tlist;
	as_testlist_init(&tlist, as);

	debug("insert 1");
	for (int i = 0; i < 100; i++) {
		int ridx = rand() % 100;
		int v = rand() % 1000;

		assert_true( as_testlist_insert(&tlist, ridx, (as_val *)as_integer_new(v)) );
	}

	assert_true( as_testlist_compare(&tlist) );

	as_arraylist list;

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

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE( cdt_basics, "aerospike_cdt basic tests" ) {
    suite_add( cdt_basics_op );
}
