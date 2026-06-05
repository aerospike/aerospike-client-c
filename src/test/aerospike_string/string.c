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
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_double.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_record.h>
#include <aerospike/as_string.h>
#include <aerospike/as_string_operations.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_version.h>

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
#define BIN_NAME "strbin"
#define LIST_BIN "listbin"
#define OTHER_BIN "other"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool
before(atf_suite* suite)
{
	(void)suite;

	as_node* node = as_node_get_random(as->cluster);

	if (! node) {
		return false;
	}

	bool supported = as_version_compare(&node->version, &as_server_version_8_1_3) >= 0;
	as_node_release(node);
	return supported;
}

static as_status
put_string_key(int64_t id, const char* value)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, id);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		return status;
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN_NAME, value);
	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	as_record_destroy(&rec);
	return status;
}

static as_status
put_int_key(int64_t id, int64_t value)
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, id);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);

	if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		return status;
	}

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_int64(&rec, BIN_NAME, value);
	status = aerospike_key_put(as, &err, NULL, &key, &rec);
	as_record_destroy(&rec);
	return status;
}

/******************************************************************************
 * TEST CASES
 *****************************************************************************/

TEST(string_api_validation, "string operation api validation")
{
	as_operations ops;
	as_operations_inita(&ops, 1);

	assert_false(as_operations_string_find(&ops, BIN_NAME, NULL, NULL));
	assert_false(as_operations_string_contains(&ops, BIN_NAME, NULL, NULL));
	assert_false(as_operations_string_split_separator(&ops, BIN_NAME, NULL, NULL));
	assert_false(as_operations_string_regex_compare(&ops, BIN_NAME, NULL, NULL));
	assert_false(as_operations_string_insert(&ops, BIN_NAME, NULL, NULL, 0, NULL));
	assert_false(as_operations_string_concat(&ops, BIN_NAME, NULL, NULL, NULL));
	assert_false(as_operations_string_concat_list(&ops, BIN_NAME, NULL, NULL, NULL));
	assert_false(as_operations_string_replace(&ops, BIN_NAME, NULL, NULL, NULL, "x"));
	assert_false(as_operations_string_replace_all(&ops, BIN_NAME, NULL, NULL, "x", NULL));
	assert_false(as_operations_string_pad_start(&ops, BIN_NAME, NULL, NULL, 3, NULL));
	assert_false(as_operations_string_regex_replace(&ops, BIN_NAME, NULL, NULL, "x", AS_STRING_REGEX_FLAGS_NONE));
	assert_int_eq(ops.binops.size, 0);

	as_operations_destroy(&ops);
}

TEST(string_read_ops, "string read operations")
{
	assert_int_eq(put_string_key(100, "Hello123World"), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 100);

	as_operations ops;
	as_operations_inita(&ops, 8);
	as_operations_string_strlen(&ops, BIN_NAME, NULL);
	as_operations_string_substr_range(&ops, BIN_NAME, NULL, 0, 5);
	as_operations_string_char_at(&ops, BIN_NAME, NULL, -5);
	as_operations_string_find_occurrence(&ops, BIN_NAME, NULL, "l", 3);
	as_operations_string_contains(&ops, BIN_NAME, NULL, "123");
	as_operations_string_is_numeric(&ops, BIN_NAME, NULL);
	as_operations_string_regex_compare(&ops, BIN_NAME, NULL, "[0-9]+");
	as_operations_string_split_separator(&ops, BIN_NAME, NULL, "123");

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_int_eq(as_integer_get((as_integer*)results[0].valuep), 13);
	assert_string_eq(as_string_get((as_string*)results[1].valuep), "Hello");
	assert_string_eq(as_string_get((as_string*)results[2].valuep), "W");
	assert_int_eq(as_integer_get((as_integer*)results[3].valuep), 11);
	assert_true(as_boolean_get((as_boolean*)results[4].valuep));
	assert_false(as_boolean_get((as_boolean*)results[5].valuep));
	assert_true(as_boolean_get((as_boolean*)results[6].valuep));

	as_list* split = (as_list*)results[7].valuep;
	assert_int_eq(as_list_size(split), 2);
	assert_string_eq(as_string_get((as_string*)as_list_get(split, 0)), "Hello");
	assert_string_eq(as_string_get((as_string*)as_list_get(split, 1)), "World");

	as_record_destroy(rec);
}

TEST(string_read_more_ops, "additional string read operations")
{
	assert_int_eq(put_string_key(105, "12345"), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 105);

	as_operations ops;
	as_operations_inita(&ops, 11);
	as_operations_string_starts_with(&ops, BIN_NAME, NULL, "123");
	as_operations_string_ends_with(&ops, BIN_NAME, NULL, "45");
	as_operations_string_find(&ops, BIN_NAME, NULL, "x");
	as_operations_string_substr(&ops, BIN_NAME, NULL, 2);
	as_operations_string_to_integer(&ops, BIN_NAME, NULL);
	as_operations_string_to_double(&ops, BIN_NAME, NULL);
	as_operations_string_is_numeric_type(&ops, BIN_NAME, NULL, AS_STRING_NUMERIC_ANY);
	as_operations_string_is_numeric_type(&ops, BIN_NAME, NULL, AS_STRING_NUMERIC_INT);
	//as_operations_string_is_numeric_type(&ops, BIN_NAME, NULL, AS_STRING_NUMERIC_FLOAT); There is a bug in the server, this should return false
	as_operations_string_regex_compare_flags(&ops, BIN_NAME, NULL, "^123", AS_STRING_REGEX_FLAGS_NONE);
	as_operations_string_split(&ops, BIN_NAME, NULL);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_true(as_boolean_get((as_boolean*)results[0].valuep));
	assert_true(as_boolean_get((as_boolean*)results[1].valuep));
	assert_int_eq(as_integer_get((as_integer*)results[2].valuep), -1);
	assert_string_eq(as_string_get((as_string*)results[3].valuep), "345");
	assert_int_eq(as_integer_get((as_integer*)results[4].valuep), 12345);
	assert_double_eq(as_double_get((as_double*)results[5].valuep), 12345.0);
	assert_true(as_boolean_get((as_boolean*)results[6].valuep));
	assert_true(as_boolean_get((as_boolean*)results[7].valuep));
	//assert_false(as_boolean_get((as_boolean*)results[8].valuep));
	assert_true(as_boolean_get((as_boolean*)results[8].valuep));

	as_list* split = (as_list*)results[9].valuep;
	assert_int_eq(as_list_size(split), 5);
	assert_string_eq(as_string_get((as_string*)as_list_get(split, 0)), "1");
	assert_string_eq(as_string_get((as_string*)as_list_get(split, 4)), "5");

	as_record_destroy(rec);
}

TEST(string_case_predicate_ops, "string case predicate operations")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 106);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_record put;
	as_record_inita(&put, 2);
	as_record_set_str(&put, "upper", "HELLO");
	as_record_set_str(&put, "lower", "hello");
	status = aerospike_key_put(as, &err, NULL, &key, &put);
	as_record_destroy(&put);
	assert_int_eq(status, AEROSPIKE_OK);

	as_operations ops;
	as_operations_inita(&ops, 4);
	as_operations_string_is_upper(&ops, "upper", NULL);
	as_operations_string_is_lower(&ops, "upper", NULL);
	as_operations_string_is_upper(&ops, "lower", NULL);
	as_operations_string_is_lower(&ops, "lower", NULL);

	as_record* rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_true(as_boolean_get((as_boolean*)results[0].valuep));
	assert_false(as_boolean_get((as_boolean*)results[1].valuep));
	assert_false(as_boolean_get((as_boolean*)results[2].valuep));
	assert_true(as_boolean_get((as_boolean*)results[3].valuep));

	as_record_destroy(rec);
}

TEST(string_modify_ops, "string modify operations")
{
	assert_int_eq(put_string_key(101, "hello world"), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 101);

	as_string_policy policy;
	as_string_policy_init(&policy);
	as_string_policy_set(&policy, AS_STRING_WRITE_FLAGS_NO_FAIL);

	as_arraylist concat;
	as_arraylist_inita(&concat, 2);
	as_arraylist_append_str(&concat, "!");
	as_arraylist_append_str(&concat, "?");

	as_operations ops;
	as_operations_inita(&ops, 8);
	as_operations_string_insert(&ops, BIN_NAME, NULL, &policy, 5, " beautiful");
	as_operations_string_overwrite(&ops, BIN_NAME, NULL, NULL, 16, "earth");
	as_operations_string_concat_list(&ops, BIN_NAME, NULL, NULL, (as_list*)&concat);
	as_operations_string_replace(&ops, BIN_NAME, NULL, NULL, "beautiful", "wide");
	as_operations_string_replace_all(&ops, BIN_NAME, NULL, NULL, "!", ".");
	as_operations_string_regex_replace(&ops, BIN_NAME, NULL, "[?]", ".", AS_STRING_REGEX_FLAGS_GLOBAL);
	as_operations_string_upper(&ops, BIN_NAME, NULL, NULL);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	as_arraylist_destroy(&concat);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_string_eq(as_string_get((as_string*)results[0].valuep), "HELLO WIDE EARTH..");
	as_record_destroy(rec);
}

TEST(string_policy_ops, "string policy operations")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 113);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_record seed;
	as_record_inita(&seed, 1);
	as_record_set_str(&seed, OTHER_BIN, "untouched");
	status = aerospike_key_put(as, &err, NULL, &key, &seed);
	as_record_destroy(&seed);
	assert_int_eq(status, AEROSPIKE_OK);

	as_string_policy policy;
	as_string_policy_init(&policy);
	as_string_policy_set(&policy, AS_STRING_WRITE_FLAGS_NO_FAIL);

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_string_upper(&ops, BIN_NAME, NULL, &policy);

	status = aerospike_key_operate(as, &err, NULL, &key, &ops, NULL);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_record* rec = NULL;
	status = aerospike_key_get(as, &err, NULL, &key, &rec);
	assert_int_eq(status, AEROSPIKE_OK);
	assert_null(as_record_get(rec, BIN_NAME));
	assert_string_eq(as_record_get_str(rec, OTHER_BIN), "untouched");
	as_record_destroy(rec);

	as_operations_inita(&ops, 1);
	as_operations_string_upper(&ops, BIN_NAME, NULL, NULL);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_ERR_BIN_NOT_FOUND);
	assert_null(rec);

	assert_int_eq(put_int_key(113, 7), AEROSPIKE_OK);

	as_operations_inita(&ops, 1);
	as_operations_string_upper(&ops, BIN_NAME, NULL, &policy);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE);
	assert_null(rec);
}

TEST(string_modify_more_ops, "additional string modify operations")
{
	assert_int_eq(put_string_key(107, " hello "), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 107);

	as_operations ops;
	as_operations_inita(&ops, 6);
	as_operations_string_trim_start(&ops, BIN_NAME, NULL, NULL);
	as_operations_string_trim_end(&ops, BIN_NAME, NULL, NULL);
	as_operations_string_pad_start(&ops, BIN_NAME, NULL, NULL, 7, "*");
	as_operations_string_pad_end(&ops, BIN_NAME, NULL, NULL, 9, ".");
	as_operations_string_repeat(&ops, BIN_NAME, NULL, NULL, 2);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_string_eq(as_string_get((as_string*)results[0].valuep), "**hello..**hello..");
	as_record_destroy(rec);

	assert_int_eq(put_string_key(112, " hello "), AEROSPIKE_OK);
	as_key_init_int64(&key, NAMESPACE, SET, 112);

	as_operations_inita(&ops, 2);
	as_operations_string_trim(&ops, BIN_NAME, NULL, NULL);
	as_operations_add_read(&ops, BIN_NAME);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_string_eq(as_string_get((as_string*)results[0].valuep), "hello");
	as_record_destroy(rec);
}

TEST(string_modify_case_normalize_ops, "string case and normalize modify operations")
{
	assert_int_eq(put_string_key(108, "HELLO World"), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 108);

	as_operations ops;
	as_operations_inita(&ops, 3);
	as_operations_string_lower(&ops, BIN_NAME, NULL, NULL);
	as_operations_string_case_fold(&ops, BIN_NAME, NULL, NULL);
	as_operations_add_read(&ops, BIN_NAME);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_string_eq(as_string_get((as_string*)results[0].valuep), "hello world");
	as_record_destroy(rec);

	assert_int_eq(put_string_key(109, "e\xCC\x81"), AEROSPIKE_OK);
	as_key_init_int64(&key, NAMESPACE, SET, 109);

	as_operations_inita(&ops, 2);
	as_operations_string_normalize_nfc(&ops, BIN_NAME, NULL, NULL);
	as_operations_add_read(&ops, BIN_NAME);

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_string_eq(as_string_get((as_string*)results[0].valuep), "\xC3\xA9");
	as_record_destroy(rec);
}

TEST(string_modify_snip_concat_ops, "string snip and concat operations")
{
	assert_int_eq(put_string_key(110, "hello beautiful world"), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 110);

	as_operations ops;
	as_operations_inita(&ops, 4);
	as_operations_string_snip_range(&ops, BIN_NAME, NULL, NULL, 5, 15);
	as_operations_string_snip_range(&ops, BIN_NAME, NULL, NULL, 5, 11);
	as_operations_string_concat(&ops, BIN_NAME, NULL, NULL, "!");
	as_operations_add_read(&ops, BIN_NAME);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_string_eq(as_string_get((as_string*)results[0].valuep), "hello!");
	as_record_destroy(rec);
}

TEST(string_expression_ops, "string expression operations")
{
	assert_int_eq(put_string_key(111, "Hello123World"), AEROSPIKE_OK);

	as_exp_build(len_exp,
		as_exp_string_strlen(as_exp_bin_str(BIN_NAME)));
	assert_not_null(len_exp);

	as_exp_build(upper_exp,
		as_exp_string_upper(NULL, as_exp_bin_str(BIN_NAME)));
	assert_not_null(upper_exp);

	as_exp_build(replace_exp,
		as_exp_string_replace(NULL, "123", "-", as_exp_bin_str(BIN_NAME)));
	assert_not_null(replace_exp);

	as_exp_build(regex_replace_exp,
		as_exp_string_regex_replace(
			"[0-9]+", "NUM", AS_STRING_REGEX_FLAGS_GLOBAL, as_exp_bin_str(BIN_NAME)));
	assert_not_null(regex_replace_exp);

	as_exp_build(to_string_exp,
		as_exp_to_string(as_exp_int(42)));
	assert_not_null(to_string_exp);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 111);

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_exp_read(&ops, "len", len_exp, AS_EXP_READ_DEFAULT);
	as_operations_exp_read(&ops, "upper", upper_exp, AS_EXP_READ_DEFAULT);
	as_operations_exp_read(&ops, "replace", replace_exp, AS_EXP_READ_DEFAULT);
	as_operations_exp_read(&ops, "regex_replace", regex_replace_exp, AS_EXP_READ_DEFAULT);
	as_operations_exp_read(&ops, "to_string", to_string_exp, AS_EXP_READ_DEFAULT);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	as_exp_destroy(len_exp);
	as_exp_destroy(upper_exp);
	as_exp_destroy(replace_exp);
	as_exp_destroy(regex_replace_exp);
	as_exp_destroy(to_string_exp);
	assert_int_eq(status, AEROSPIKE_OK);

	assert_int_eq(as_record_get_int64(rec, "len", 0), 13);
	assert_string_eq(as_record_get_str(rec, "upper"), "HELLO123WORLD");
	assert_string_eq(as_record_get_str(rec, "replace"), "Hello-World");
	assert_string_eq(as_record_get_str(rec, "regex_replace"), "HelloNUMWorld");
	assert_string_eq(as_record_get_str(rec, "to_string"), "42");

	as_record_destroy(rec);
}

TEST(string_conversion_unicode_ops, "string conversion and unicode operations")
{
	assert_int_eq(put_string_key(102, "\xC3\xA9" "42"), AEROSPIKE_OK);

	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 102);

	as_operations ops;
	as_operations_inita(&ops, 5);
	as_operations_string_strlen(&ops, BIN_NAME, NULL);
	as_operations_string_byte_length(&ops, BIN_NAME, NULL);
	as_operations_string_substr_range(&ops, BIN_NAME, NULL, 0, 1);
	as_operations_string_char_at(&ops, BIN_NAME, NULL, 0);
	as_operations_string_to_blob(&ops, BIN_NAME, NULL);

	as_record* rec = NULL;
	as_error err;
	as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_int_eq(as_integer_get((as_integer*)results[0].valuep), 3);
	assert_int_eq(as_integer_get((as_integer*)results[1].valuep), 4);
	assert_string_eq(as_string_get((as_string*)results[2].valuep), "\xC3\xA9");
	assert_string_eq(as_string_get((as_string*)results[3].valuep), "\xC3\xA9");

	as_bytes* utf8 = (as_bytes*)results[4].valuep;
	uint8_t utf8_expected[] = {0xC3, 0xA9, '4', '2'};
	assert_bytes_eq(utf8->value, utf8->size, utf8_expected, sizeof(utf8_expected));
	as_record_destroy(rec);

	as_key key2;
	as_key_init_int64(&key2, NAMESPACE, SET, 103);

	as_record put;
	as_record_inita(&put, 2);
	as_record_set_int64(&put, "num", 42);
	as_record_set_str(&put, BIN_NAME, "aGVsbG8=");
	status = aerospike_key_put(as, &err, NULL, &key2, &put);
	as_record_destroy(&put);
	assert_int_eq(status, AEROSPIKE_OK);

	as_operations ops2;
	as_operations_inita(&ops2, 4);
	as_operations_string_strlen(&ops2, BIN_NAME, NULL);
	as_operations_string_byte_length(&ops2, BIN_NAME, NULL);
	as_operations_string_b64_decode(&ops2, BIN_NAME, NULL);
	as_operations_to_string(&ops2, "num");

	rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key2, &ops2, &rec);
	as_operations_destroy(&ops2);
	assert_int_eq(status, AEROSPIKE_OK);

	results = rec->bins.entries;
	assert_int_eq(as_integer_get((as_integer*)results[0].valuep), 8);
	assert_int_eq(as_integer_get((as_integer*)results[1].valuep), 8);

	as_bytes* bytes = (as_bytes*)results[2].valuep;
	uint8_t expected[] = {'h', 'e', 'l', 'l', 'o'};
	assert_bytes_eq(bytes->value, bytes->size, expected, sizeof(expected));
	assert_string_eq(as_string_get((as_string*)results[3].valuep), "42");
	as_record_destroy(rec);
}

TEST(string_ctx_ops, "string context operations")
{
	as_key key;
	as_key_init_int64(&key, NAMESPACE, SET, 104);

	as_error err;
	as_status status = aerospike_key_remove(as, &err, NULL, &key);
	assert_true(status == AEROSPIKE_OK || status == AEROSPIKE_ERR_RECORD_NOT_FOUND);

	as_arraylist list;
	as_arraylist_inita(&list, 2);
	as_arraylist_append_str(&list, "alpha");
	as_arraylist_append_str(&list, "beta");

	as_record put;
	as_record_inita(&put, 1);
	as_record_set_list(&put, LIST_BIN, (as_list*)&list);
	status = aerospike_key_put(as, &err, NULL, &key, &put);
	as_record_destroy(&put);
	assert_int_eq(status, AEROSPIKE_OK);

	as_cdt_ctx ctx;
	as_cdt_ctx_init(&ctx, 1);
	as_cdt_ctx_add_list_index(&ctx, 1);

	as_operations ops;
	as_operations_inita(&ops, 3);
	as_operations_string_strlen(&ops, LIST_BIN, &ctx);
	as_operations_string_concat(&ops, LIST_BIN, &ctx, NULL, "-gamma");
	as_operations_add_read(&ops, LIST_BIN);

	as_record* rec = NULL;
	status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	as_operations_destroy(&ops);
	as_cdt_ctx_destroy(&ctx);
	assert_int_eq(status, AEROSPIKE_OK);

	as_bin* results = rec->bins.entries;
	assert_int_eq(as_integer_get((as_integer*)results[0].valuep), 4);

	as_list* out = (as_list*)results[1].valuep;
	assert_string_eq(as_string_get((as_string*)as_list_get(out, 0)), "alpha");
	assert_string_eq(as_string_get((as_string*)as_list_get(out, 1)), "beta-gamma");
	as_record_destroy(rec);
}

SUITE(string, "aerospike string operation tests")
{
	suite_before(before);

	suite_add(string_api_validation);
	suite_add(string_read_ops);
	suite_add(string_read_more_ops);
	suite_add(string_case_predicate_ops);
	suite_add(string_modify_ops);
	suite_add(string_policy_ops);
	suite_add(string_modify_more_ops);
	suite_add(string_modify_case_normalize_ops);
	suite_add(string_modify_snip_concat_ops);
	suite_add(string_expression_ops);
	suite_add(string_conversion_unicode_ops);
	suite_add(string_ctx_ops);
}
