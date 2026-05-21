/*
 * Copyright 2008-2026 Aerospike, Inc.
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
 * ANY WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/*
 * Integration tests for STRING bin operations (SERVER-97). Requires a server
 * that supports AS_MSG_OP_STRING_READ / STRING_MODIFY / TO_STRING.
 */
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_double.h>
#include <aerospike/as_error.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string_operations.h>

#include <math.h>
#include <stdint.h>

#include "../test.h"

/******************************************************************************
 * GLOBAL VARS
 *****************************************************************************/

extern aerospike* as;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define NAMESPACE "test"
#define SET "test_string"
#define BIN "mystring"

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(string_strlen_basic, "string strlen on hello world")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "strlen_test");

	(void)aerospike_key_remove(as, &err, NULL, &key);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hello world");
	as_status rc = aerospike_key_put(as, &err, NULL, &key, &rec);
	as_record_destroy(&rec);
	assert_int_eq(rc, AEROSPIKE_OK);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_string_strlen(&ops, BIN);

	as_record* out = NULL;
	rc = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	as_operations_destroy(&ops);
	assert_int_eq(rc, AEROSPIKE_OK);
	assert_not_null(out);

	int64_t len = as_record_get_int64(out, BIN, INT64_MIN);
	as_record_destroy(out);
	assert_int_eq((int)len, 11);

	as_key_destroy(&key);
}

TEST(string_substr_and_modify, "substr read; upper/lower/insert/snip/overwrite")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "substr_modify_test");

	(void)aerospike_key_remove(as, &err, NULL, &key);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hello world");
	as_status prc = aerospike_key_put(as, &err, NULL, &key, &rec);
	assert_int_eq(prc, AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_add_string_substr(&ops, BIN, 0, true, 5);
	as_record* out = NULL;
	as_status orc = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	assert_int_eq(orc, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_substr(&ops, BIN, 6, false, 0);
	orc = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	assert_int_eq(orc, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "world");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_substr(&ops, BIN, -5, false, 0);
	orc = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	assert_int_eq(orc, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "world");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_upper(&ops, BIN, AS_STRING_FLAG_DEFAULT);
	orc = aerospike_key_operate(as, &err, NULL, &key, &ops, &out);
	assert_int_eq(orc, AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "HELLO WORLD");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_lower(&ops, BIN, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello world");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_insert(&ops, BIN, 5, " beautiful",
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello beautiful world");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_snip(&ops, BIN, 5, 15, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello world");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_overwrite(&ops, BIN, 6, "earth",
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello earth");
	as_record_destroy(out);

	as_key_destroy(&key);
}

TEST(string_replace_trim_pad_repeat, "replace, replace_all, trim, pad, repeat")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "trim_pad_test");

	(void)aerospike_key_remove(as, &err, NULL, &key);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hello world world");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	as_operations ops;

	as_operations_inita(&ops, 1);
	as_operations_add_string_replace(&ops, BIN, "world", "earth",
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello earth world");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "aabaa");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_replace_all(&ops, BIN, "a", "x",
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "xxbxx");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "  hello world  ");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_trim(&ops, BIN, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello world");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "  hello world");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_trim_start(&ops, BIN, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello world");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hello world  ");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_trim_end(&ops, BIN, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello world");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hello");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_pad_start(&ops, BIN, 10, "*",
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "*****hello");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hello");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_pad_end(&ops, BIN, 10, ".",
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello.....");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hi");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_pad_start(&ops, BIN, 8, "ab",
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "abababhi");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "ab");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_repeat(&ops, BIN, 3, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "ababab");
	as_record_destroy(out);

	as_key_destroy(&key);
}

TEST(string_regex_fold_normalize, "regex replace/compare, case_fold, normalize")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "regex_fold_test");

	(void)aerospike_key_remove(as, &err, NULL, &key);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "abc123def456");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	as_operations ops;

	as_operations_inita(&ops, 1);
	as_operations_add_string_regex_replace(&ops, BIN, "[0-9]+", "NUM", 0,
			AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "abcNUMdef456");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "abc123def456");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_regex_replace(&ops, BIN, "[0-9]+", "NUM",
			AS_STRING_REGEX_GLOBAL, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "abcNUMdefNUM");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "HELLO World");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_case_fold(&ops, BIN, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello world");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "hello");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_normalize_nfc(&ops, BIN, AS_STRING_FLAG_DEFAULT);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, BIN);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, BIN), "hello");
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "Hello123World");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_regex_compare(&ops, BIN, "[0-9]+", false, 0);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_true(as_record_get_bool(out, BIN));
	as_record_destroy(out);

	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, "HELLO");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_string_regex_compare(&ops, BIN, "[0-9]+", false, 0);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_false(as_record_get_bool(out, BIN));
	as_record_destroy(out);

	as_key_destroy(&key);
}

TEST(string_mixed_ops_batch,
		"mixed ops: string reads (strlen, find, split, predicates, casts); "
		"concat (modify); to_blob, b64_decode; to_string")
{
	as_error err;
	as_key key;
	as_key_init(&key, NAMESPACE, SET, "read_ops_test");

	(void)aerospike_key_remove(as, &err, NULL, &key);

	as_record rec;
	as_record_inita(&rec, 8);
	as_record_set_str(&rec, "text", "  hello world  ");
	as_record_set_str(&rec, "number_str", "12345");
	as_record_set_str(&rec, "float_str", "3.14");
	as_record_set_str(&rec, "mixed", "Hello123World");
	as_record_set_str(&rec, "upper_str", "HELLO");
	as_record_set_str(&rec, "lower_str", "hello");
	as_record_set_str(&rec, "b64_str", "aGVsbG8=");
	as_record_set_str(&rec, "csv", "one,two,three");
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_record* out = NULL;
	as_operations ops;

	as_operations_inita(&ops, 1);
	as_operations_add_string_strlen(&ops, "text");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_int_eq((int)as_record_get_int64(out, "text", INT64_MIN), 15);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_byte_length(&ops, "text");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_int_eq((int)as_record_get_int64(out, "text", INT64_MIN), 15);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_contains(&ops, "text", "hello");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_true(as_record_get_bool(out, "text"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_contains(&ops, "text", "xyz");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_false(as_record_get_bool(out, "text"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_find(&ops, "text", "world", false, 0);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_int_eq((int)as_record_get_int64(out, "text", INT64_MIN), 8);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_find(&ops, "text", "xyz", false, 0);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_int_eq((int)as_record_get_int64(out, "text", INT64_MIN), -1);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_char_at(&ops, "mixed", 0);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, "mixed"), "H");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_char_at(&ops, "mixed", 5);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, "mixed"), "1");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_starts_with(&ops, "mixed", "Hello");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_true(as_record_get_bool(out, "mixed"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_starts_with(&ops, "mixed", "World");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_false(as_record_get_bool(out, "mixed"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_ends_with(&ops, "mixed", "World");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_true(as_record_get_bool(out, "mixed"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_ends_with(&ops, "mixed", "Hello");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_false(as_record_get_bool(out, "mixed"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_split(&ops, "csv", ",", true);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	{
		as_list* L = as_record_get_list(out, "csv");
		assert_not_null(L);
		assert_int_eq((int)as_list_size(L), 3);
		assert_string_eq(as_list_get_str(L, 0), "one");
	}
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_split(&ops, "mixed", "|", true);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	{
		as_list* L = as_record_get_list(out, "mixed");
		assert_not_null(L);
		assert_int_eq((int)as_list_size(L), 1);
	}
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_is_upper(&ops, "upper_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_true(as_record_get_bool(out, "upper_str"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_is_upper(&ops, "lower_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_false(as_record_get_bool(out, "lower_str"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_is_lower(&ops, "lower_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_true(as_record_get_bool(out, "lower_str"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_is_lower(&ops, "upper_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_false(as_record_get_bool(out, "upper_str"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_is_numeric(&ops, "number_str", false,
			AS_STRING_NUMERIC_ANY);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_true(as_record_get_bool(out, "number_str"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_is_numeric(&ops, "mixed", false,
			AS_STRING_NUMERIC_ANY);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_false(as_record_get_bool(out, "mixed"));
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_to_integer(&ops, "number_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_int_eq((int)as_record_get_int64(out, "number_str", INT64_MIN), 12345);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_to_double(&ops, "float_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	double d = as_record_get_double(out, "float_str", NAN);
	assert_true(fabs(d - 3.14) < 0.001);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	const char* excl = "!";
	as_operations_add_string_concat(&ops, "text", AS_STRING_FLAG_DEFAULT, 1,
			&excl);
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_read(&ops, "text");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, "text"), "  hello world  !");
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_to_blob(&ops, "lower_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	{
		as_bytes* b = as_record_get_bytes(out, "lower_str");
		assert_not_null(b);
		assert_int_eq((int)b->type, (int)AS_BYTES_BLOB);
	}
	as_record_destroy(out);

	as_operations_inita(&ops, 1);
	as_operations_add_string_b64_decode(&ops, "b64_str");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	{
		as_bytes* b = as_record_get_bytes(out, "b64_str");
		assert_not_null(b);
		assert_int_eq((int)b->type, (int)AS_BYTES_BLOB);
	}
	as_record_destroy(out);

	(void)aerospike_key_remove(as, &err, NULL, &key);
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, "ival", 42);
	assert_int_eq(aerospike_key_put(as, &err, NULL, &key, &rec), AEROSPIKE_OK);
	as_record_destroy(&rec);

	as_operations_inita(&ops, 1);
	as_operations_add_to_string(&ops, "ival");
	assert_int_eq(aerospike_key_operate(as, &err, NULL, &key, &ops, &out),
			AEROSPIKE_OK);
	as_operations_destroy(&ops);
	assert_string_eq(as_record_get_str(out, "ival"), "42");
	as_record_destroy(out);

	as_key_destroy(&key);
}

/******************************************************************************
 * SUITE
 *****************************************************************************/

SUITE(string_basics, "string bin operations (SERVER-97)")
{
	suite_add(string_strlen_basic);
	suite_add(string_substr_and_modify);
	suite_add(string_replace_trim_pad_repeat);
	suite_add(string_regex_fold_normalize);
	suite_add(string_mixed_ops_batch);
}
