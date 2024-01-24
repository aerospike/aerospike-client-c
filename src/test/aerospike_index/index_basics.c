/*
 * Copyright 2008-2022 Aerospike, Inc.
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

#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#include <aerospike/as_record.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_string.h>
#include <aerospike/as_list.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_val.h>

#include "../test.h"
#include "../util/index_util.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/
#define NAMESPACE "test"
#define SET "test_index"

/******************************************************************************
 * TYPES
 *****************************************************************************/


/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/


/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(index_basics_create, "Create index on bin")
{
	as_error err;
	as_error_reset(&err);

	as_index_task task;

	// DEFAULT type index
	as_status status = aerospike_index_create(as, &err, &task, NULL, NAMESPACE, SET, "new_bin", "idx_test_new_bin", AS_INDEX_STRING);

	if (! index_process_return_code(status, &err, &task)) {
		assert_int_eq(status , AEROSPIKE_OK);
	}

	// LIST type index
	status = aerospike_index_create_complex(as, &err, &task, NULL, NAMESPACE,
			SET, "new_bin[0]", "idx_test_listbin", AS_INDEX_TYPE_LIST,
			AS_INDEX_STRING);

	if (! index_process_return_code(status, &err, &task)) {
		assert_int_eq(status , AEROSPIKE_OK);
	}
}

TEST(index_basics_drop , "Drop index")
{
	as_error err;
	as_error_reset(&err);

	// DEFAULT type index
	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_new_bin");
	if ( err.code != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq( err.code, AEROSPIKE_OK );

	// LIST type index
	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_listbin");
	if ( err.code != AEROSPIKE_OK ) {
		info("error(%d): %s", err.code, err.message);
	}
	assert_int_eq( err.code, AEROSPIKE_OK );
}

TEST(index_ctx_test , "Create ctx index on bin")
{
	as_error err;
	as_error_reset(&err);

	char* res = NULL;

	aerospike_info_any(as, &err, NULL, "sindex/test/idx_test_ctx", &res);

	if (res != NULL) {
		assert_not_null(res);
		info("sindex-info: %s", res);
		free(res);
		res = NULL;
	}

	as_index_task task;

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, 0);

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_ctx");

	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}

	assert_int_eq( err.code, AEROSPIKE_OK );

	as_status status = aerospike_index_create_ctx(as, &err, &task, NULL,
			NAMESPACE, SET, "new_bin", "idx_test_ctx", AS_INDEX_TYPE_DEFAULT,
			AS_INDEX_NUMERIC, &ctx);

	if (! index_process_return_code(status, &err, &task)) {
		assert_int_eq(status , AEROSPIKE_OK);
	}

	as_arraylist list;
	as_key rkey;
	as_record rec;

	for (uint32_t i = 0; i < 100; i++) {
		as_key_init_int64(&rkey, NAMESPACE, SET, i + 2000);

		as_arraylist_init(&list, 1, 1);
		as_arraylist_append_int64(&list, i);

		as_record_init(&rec, 1);
		as_record_set_list(&rec, "new_bin", (as_list*)&list);

		status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
		assert_int_eq(status, AEROSPIKE_OK);
		as_record_destroy(&rec);
	}

	as_cdt_ctx_destroy(&ctx);

	aerospike_info_any(as, &err, NULL, "sindex/test/idx_test_ctx", &res);
	assert_not_null(res);
	info("sindex-info: %s", res);
	free(res);
	res = NULL;

	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_ctx");

	if (err.code != AEROSPIKE_OK) {
		info("error(%d): %s", err.code, err.message);
	}
}

TEST(ctx_restore_test , "backup/restore ctx")
{
	as_cdt_ctx ctx1;
	as_cdt_ctx_init(&ctx1, 3);
	as_cdt_ctx_add_list_index(&ctx1, -1);
	as_cdt_ctx_add_map_key(&ctx1, (as_val*)as_string_new("key1", false));
	as_cdt_ctx_add_list_value(&ctx1, (as_val*)as_integer_new(937));
	assert_int_eq(ctx1.list.size, 3);

	uint32_t capacity = as_cdt_ctx_base64_capacity(&ctx1);
	char* base64 = cf_malloc(capacity);
	bool rv = as_cdt_ctx_to_base64(&ctx1, base64, capacity);
	assert_true(rv);

	as_cdt_ctx ctx2;
	rv = as_cdt_ctx_from_base64(&ctx2, base64);
	cf_free(base64);
	assert_true(rv);
	assert_int_eq(ctx2.list.size, 3);

	as_cdt_ctx_item* item1 = as_vector_get(&ctx1.list, 0);
	as_cdt_ctx_item* item2 = as_vector_get(&ctx2.list, 0);

	assert_int_eq(item2->type, item1->type);
	assert_int_eq(item2->val.ival, item1->val.ival);

	item1 = as_vector_get(&ctx1.list, 1);
	item2 = as_vector_get(&ctx2.list, 1);
	as_string* s1 = (as_string*)item1->val.pval;
	as_string* s2 = (as_string*)item2->val.pval;

	assert_int_eq(item2->type, item1->type);
	assert_string_eq(s2->value, s1->value);

	item1 = as_vector_get(&ctx1.list, 2);
	item2 = as_vector_get(&ctx2.list, 2);
	as_integer* i1 = (as_integer*)item1->val.pval;
	as_integer* i2 = (as_integer*)item2->val.pval;

	assert_int_eq(item2->type, item1->type);
	assert_int_eq(i2->value, i1->value);

	as_cdt_ctx_destroy(&ctx1);
	as_cdt_ctx_destroy(&ctx2);
}

/******************************************************************************
 * TEST SUITE
 *****************************************************************************/

SUITE(index_basics, "aerospike_sindex basic tests")
{
	suite_add(index_basics_create);
	suite_add(index_basics_drop);
	suite_add(index_ctx_test);
	suite_add(ctx_restore_test);
}
