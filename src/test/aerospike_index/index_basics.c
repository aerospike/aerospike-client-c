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
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>

#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
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

//---------------------------------
// Globals
//---------------------------------

extern aerospike* as;

//---------------------------------
// Macros
//---------------------------------

#define NAMESPACE "test"
#define SET "test_index"

//---------------------------------
// Callbacks
//---------------------------------

static bool
query_count_callback(const as_val* v, void* udata)
{
	if (v != NULL) {
		(*(uint32_t*)udata)++;
	}
	return true;
}

//---------------------------------
// Tests
//---------------------------------

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

	char command[1024];
	as_node* node = as_node_get_random(as->cluster);

	if (as_version_compare(&node->version, &as_server_version_8_1) >= 0) {
		strcpy(command, "sindex-stat:namespace=test;indexname=idx_test_ctx");
	}
	else {
		strcpy(command, "sindex/test/idx_test_ctx");
	}

	as_node_release(node);

	char* res = NULL;

	aerospike_info_any(as, &err, NULL, command, &res);

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

	aerospike_info_any(as, &err, NULL, command, &res);
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

// Verify expression-based sindex with select_by_path and nested context
// containing all_children_with_filter. Previously crashed server in
// display_call (exp.c) when sindex-stat was queried.
TEST(index_exp_select_by_path, "Exp sindex select_by_path with filter ctx")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 9000);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	// Build nested map data.
	// inventory = {
	//   "item1": {"price":221, "category":"electronics",
	//             "attributes":{121:"value1_1", 223:"value1_2"}},
	//   "item2": {"price":78,  "category":"food",
	//             "attributes":{120:"value2_1", 272:"value2_2"}},
	//   "item3": {"price":103, "category":"books",
	//             "attributes":{115:"value3_1", 232:"value3_2"}}
	// }

	// attributes maps (integer keys)
	as_hashmap attrs1;
	as_hashmap_init(&attrs1, 2);
	as_hashmap_set(&attrs1, (as_val*)as_integer_new(121), (as_val*)as_string_new("value1_1", false));
	as_hashmap_set(&attrs1, (as_val*)as_integer_new(223), (as_val*)as_string_new("value1_2", false));

	as_hashmap attrs2;
	as_hashmap_init(&attrs2, 2);
	as_hashmap_set(&attrs2, (as_val*)as_integer_new(120), (as_val*)as_string_new("value2_1", false));
	as_hashmap_set(&attrs2, (as_val*)as_integer_new(272), (as_val*)as_string_new("value2_2", false));

	as_hashmap attrs3;
	as_hashmap_init(&attrs3, 2);
	as_hashmap_set(&attrs3, (as_val*)as_integer_new(115), (as_val*)as_string_new("value3_1", false));
	as_hashmap_set(&attrs3, (as_val*)as_integer_new(232), (as_val*)as_string_new("value3_2", false));

	// item maps
	as_hashmap item1;
	as_hashmap_init(&item1, 3);
	as_hashmap_set(&item1, (as_val*)as_string_new("price", false), (as_val*)as_integer_new(221));
	as_hashmap_set(&item1, (as_val*)as_string_new("category", false), (as_val*)as_string_new("electronics", false));
	as_hashmap_set(&item1, (as_val*)as_string_new("attributes", false), (as_val*)&attrs1);

	as_hashmap item2;
	as_hashmap_init(&item2, 3);
	as_hashmap_set(&item2, (as_val*)as_string_new("price", false), (as_val*)as_integer_new(78));
	as_hashmap_set(&item2, (as_val*)as_string_new("category", false), (as_val*)as_string_new("food", false));
	as_hashmap_set(&item2, (as_val*)as_string_new("attributes", false), (as_val*)&attrs2);

	as_hashmap item3;
	as_hashmap_init(&item3, 3);
	as_hashmap_set(&item3, (as_val*)as_string_new("price", false), (as_val*)as_integer_new(103));
	as_hashmap_set(&item3, (as_val*)as_string_new("category", false), (as_val*)as_string_new("books", false));
	as_hashmap_set(&item3, (as_val*)as_string_new("attributes", false), (as_val*)&attrs3);

	// inventory map
	as_hashmap inventory;
	as_hashmap_init(&inventory, 3);
	as_hashmap_set(&inventory, (as_val*)as_string_new("item1", false), (as_val*)&item1);
	as_hashmap_set(&inventory, (as_val*)as_string_new("item2", false), (as_val*)&item2);
	as_hashmap_set(&inventory, (as_val*)as_string_new("item3", false), (as_val*)&item3);

	// Write the record.
	as_record rec;
	as_record_init(&rec, 1);
	as_record_set_map(&rec, "inventory", (as_map*)&inventory);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	// Build filter expression: category == "electronics"
	as_exp_build(filter_exp,
		as_exp_cmp_eq(
			as_exp_map_get_by_key(NULL, AS_MAP_RETURN_VALUE, AS_EXP_TYPE_STR,
				as_exp_str("category"),
				as_exp_loopvar_map(AS_EXP_LOOPVAR_VALUE)),
			as_exp_str("electronics")));
	assert_not_null(filter_exp);

	// Build CDT context:
	//   all_children_with_filter(category == "electronics")
	//   -> map_key("attributes")
	//   -> all_children()
	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 3);
	as_cdt_ctx_add_all_children_with_filter(&ctx, filter_exp);
	as_cdt_ctx_add_map_key(&ctx, (as_val*)as_string_new("attributes", false));
	as_cdt_ctx_add_all_children(&ctx);

	// Build select_by_path expression with MAP_KEY flag.
	// MAP_KEY always returns a LIST of the selected map keys.
	as_exp_build(select_exp,
		as_exp_select_by_path(&ctx, AS_EXP_TYPE_LIST,
			AS_EXP_PATH_SELECT_MAP_KEY,
			as_exp_bin_map("inventory")));
	assert_not_null(select_exp);

	// Remove index in case it exists from a prior run.
	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_exp_select_path");
	as_error_reset(&err);

	// Create expression-based secondary index. The expression returns a list
	// of integer keys, so use LIST index type with NUMERIC data type.
	as_index_task task;

	status = aerospike_index_create_exp(as, &err, &task, NULL,
		NAMESPACE, SET, "idx_test_exp_select_path",
		AS_INDEX_TYPE_LIST, AS_INDEX_NUMERIC, select_exp);

	if (! index_process_return_code(status, &err, &task)) {
		assert_int_eq(status, AEROSPIKE_OK);
	}

	// Query sindex stat. This exercises as_exp_display -> display_call for
	// the AS_CDT_OP_SELECT expression, which previously crashed the server.
	char* res = NULL;

	status = aerospike_info_any(as, &err, NULL,
		"sindex-stat:namespace=test;indexname=idx_test_exp_select_path", &res);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_not_null(res);
	info("sindex-stat: %s", res);
	free(res);

	// Query the index. The "electronics" item has attributes keys 121 and
	// 223, so querying for key 121 should find exactly 1 record.
	uint32_t count = 0;

	as_query q;
	as_query_init(&q, NAMESPACE, SET);
	as_query_where_inita(&q, 1);
	as_query_where_with_exp(&q, select_exp,
		as_contains(LIST, NUMERIC, (int64_t)121));

	status = aerospike_query_foreach(as, &err, NULL, &q, query_count_callback,
		&count);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_int_eq(count, 1);
	as_query_destroy(&q);

	// Cleanup.
	status = aerospike_index_remove(as, &err, NULL, NAMESPACE,
		"idx_test_exp_select_path");
	assert_int_eq(status, AEROSPIKE_OK);

	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_int_eq(status, AEROSPIKE_OK);

	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(filter_exp);
	as_exp_destroy(select_exp);
}

// Verify expression-based sindex with a bogus CDT context type (0x7f)
// in a sub-context CDT operation (list_append via CONTEXT_EVAL).
// The server must handle the invalid context type gracefully in display_call.
TEST(index_exp_bogus_ctx_type_fuzz, "Exp sindex with invalid ctx type 0x7f")
{
	as_key rkey;
	as_key_init_int64(&rkey, NAMESPACE, SET, 9001);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	// Write a record with a list bin.
	as_arraylist list;
	as_arraylist_init(&list, 3, 0);
	as_arraylist_append_int64(&list, 1);
	as_arraylist_append_int64(&list, 2);
	as_arraylist_append_int64(&list, 3);

	as_record rec;
	as_record_init(&rec, 1);
	as_record_set_list(&rec, "mylist", (as_list*)&list);

	status = aerospike_key_put(as, &err, NULL, &rkey, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	as_record_destroy(&rec);

	// Build a CDT context with a bogus type 0x7f.
	// Valid types have base nibble 0-4 (AS_CDT_MAX_CTX == 5 on the server).
	// 0x7f & 0x0f = 0xf which is well beyond valid range.
	// Use a valid as_val* so the client packer serializes it correctly
	// (0x7f has the AS_CDT_CTX_VALUE bit set, so packer calls as_pack_val).
	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);

	as_cdt_ctx_item bogus_item;
	bogus_item.type = 0x7f;
	bogus_item.val.pval = (as_val*)as_integer_new(0);
	as_vector_append(&ctx.list, &bogus_item);

	// Build list_append expression with the bogus context.
	// list_append with non-NULL ctx wraps in AS_CDT_OP_CONTEXT_EVAL,
	// which is the sub-context CDT operation code path in display_call.
	as_exp_build(exp,
		as_exp_list_append(&ctx, NULL, as_exp_int(999),
			as_exp_bin_list("mylist")));
	assert_not_null(exp);

	// Remove index in case it exists from a prior run.
	aerospike_index_remove(as, &err, NULL, NAMESPACE, "idx_test_bogus_ctx");
	as_error_reset(&err);

	// Create expression-based secondary index.
	as_index_task task;

	status = aerospike_index_create_exp(as, &err, &task, NULL,
		NAMESPACE, SET, "idx_test_bogus_ctx",
		AS_INDEX_TYPE_LIST, AS_INDEX_NUMERIC, exp);

	if (! index_process_return_code(status, &err, &task)) {
		assert_int_eq(status, AEROSPIKE_OK);
	}

	// Query sindex stat. This exercises as_exp_display -> display_call for
	// the AS_CDT_OP_CONTEXT_EVAL path with the bogus ctx type, which must
	// be handled gracefully (ctx-error) rather than crashing.
	char* res = NULL;

	status = aerospike_info_any(as, &err, NULL,
		"sindex-stat:namespace=test;indexname=idx_test_bogus_ctx", &res);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_not_null(res);
	info("sindex-stat: %s", res);
	free(res);

	// Cleanup.
	status = aerospike_index_remove(as, &err, NULL, NAMESPACE,
		"idx_test_bogus_ctx");
	assert_int_eq(status, AEROSPIKE_OK);

	status = aerospike_key_remove(as, &err, NULL, &rkey);
	assert_int_eq(status, AEROSPIKE_OK);

	as_cdt_ctx_destroy(&ctx);
	as_exp_destroy(exp);
}

//---------------------------------
// Test Suite
//---------------------------------

SUITE(index_basics, "aerospike_sindex basic tests")
{
	suite_add(index_basics_create);
	suite_add(index_basics_drop);
	suite_add(index_ctx_test);
	suite_add(ctx_restore_test);
	suite_add(index_exp_select_by_path);
	suite_add(index_exp_bogus_ctx_type_fuzz);
}
