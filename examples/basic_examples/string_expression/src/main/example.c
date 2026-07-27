/*******************************************************************************
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
 ******************************************************************************/


//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_string_operations.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// STRING EXPRESSION Example
//

static const char* BIN = "text";
static const char* VAR = "result";
static const char* NUM_BIN = "n";

static void run_read_exps(aerospike* as);
static void run_modify_exps(aerospike* as);
static void run_to_string(aerospike* as);
static void put(aerospike* as, as_key* key, const char* value);
static as_record* eval_exp(aerospike* as, as_key* key, as_exp* exp);
static void log_bytes_array(const char* label, as_bytes* bytes);
static void log_bytes_string(const char* label, as_bytes* bytes);
static void log_list(const char* label, as_list* list);

int
main(int argc, char* argv[])
{
	// Parse command line arguments.
	if (! example_get_opts(argc, argv, EXAMPLE_BASIC_OPTS)) {
		exit(-1);
	}

	// Connect to the aerospike database cluster.
	aerospike as;
	example_connect_to_aerospike(&as);

	run_read_exps(&as);
	run_modify_exps(&as);
	run_to_string(&as);

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("string expression example successfully completed");

	return 0;
}

// Read-only string expressions.
static void
run_read_exps(aerospike* as)
{
#define EVAL(...) \
	do { \
		as_exp_build(exp, __VA_ARGS__); \
		rec = eval_exp(as, &key, exp); \
		as_exp_destroy(exp); \
	} while (false)

	as_key key;
	as_key_init_str(&key, g_namespace, g_set, "stringexp_read");
	as_record* rec;

	// strlen - codepoint count
	put(as, &key, "hello world");
	EVAL(as_exp_string_strlen(as_exp_bin_str(BIN)));
	LOG("strlen(\"hello world\") = %lld", (long long)as_record_get_int64(rec, VAR, 0));
	as_record_destroy(rec);

	// substr(start) - codepoint slice to end of string
	EVAL(as_exp_string_substr(6, as_exp_bin_str(BIN)));
	LOG("substr(6) = \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// substr(start, end) - half-open codepoint range
	EVAL(as_exp_string_substr_range(0, 5, as_exp_bin_str(BIN)));
	LOG("substr(0, 5) = \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// charAt - single-codepoint slice
	EVAL(as_exp_string_char_at(6, as_exp_bin_str(BIN)));
	LOG("charAt(6) = \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// find(needle) - index of first match, -1 if absent
	EVAL(as_exp_string_find("world", as_exp_bin_str(BIN)));
	LOG("find(\"world\") = %lld", (long long)as_record_get_int64(rec, VAR, 0));
	as_record_destroy(rec);

	// find(needle, occurrence) - index of nth match
	put(as, &key, "ababab");
	EVAL(as_exp_string_find_occurrence("ab", 2, as_exp_bin_str(BIN)));
	LOG("find(\"ab\", occurrence=2) on \"ababab\" = %lld",
			(long long)as_record_get_int64(rec, VAR, 0));
	as_record_destroy(rec);

	// contains
	put(as, &key, "hello world");
	EVAL(as_exp_string_contains("hello", as_exp_bin_str(BIN)));
	LOG("contains(\"hello\") = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// startsWith
	EVAL(as_exp_string_starts_with("hello", as_exp_bin_str(BIN)));
	LOG("startsWith(\"hello\") = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// endsWith
	EVAL(as_exp_string_ends_with("world", as_exp_bin_str(BIN)));
	LOG("endsWith(\"world\") = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// toInteger - parse string as int64
	put(as, &key, "12345");
	EVAL(as_exp_string_to_integer(as_exp_bin_str(BIN)));
	LOG("toInteger(\"12345\") = %lld", (long long)as_record_get_int64(rec, VAR, 0));
	as_record_destroy(rec);

	// toDouble - parse string as float64
	put(as, &key, "3.14");
	EVAL(as_exp_string_to_double(as_exp_bin_str(BIN)));
	LOG("toDouble(\"3.14\") = %f", as_record_get_double(rec, VAR, 0.0));
	as_record_destroy(rec);

	// byteLength - UTF-8 byte count (differs from strlen for non-ASCII)
	put(as, &key, "héllo");
	EVAL(as_exp_string_byte_length(as_exp_bin_str(BIN)));
	LOG("byteLength(\"héllo\") = %lld (5 codepoints, 6 UTF-8 bytes)",
			(long long)as_record_get_int64(rec, VAR, 0));
	as_record_destroy(rec);

	// isNumeric - accepts integer or float
	put(as, &key, "12345");
	EVAL(as_exp_string_is_numeric(as_exp_bin_str(BIN)));
	LOG("isNumeric(\"12345\") = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// isNumeric(numericType) - restrict by AS_STRING_NUMERIC_INT
	put(as, &key, "3.14");
	EVAL(as_exp_string_is_numeric_type(AS_STRING_NUMERIC_INT, as_exp_bin_str(BIN)));
	LOG("isNumeric(\"3.14\", INT) = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// isUpper
	put(as, &key, "HELLO");
	EVAL(as_exp_string_is_upper(as_exp_bin_str(BIN)));
	LOG("isUpper(\"HELLO\") = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// isLower
	put(as, &key, "hello");
	EVAL(as_exp_string_is_lower(as_exp_bin_str(BIN)));
	LOG("isLower(\"hello\") = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// toBlob - UTF-8 bytes as blob
	EVAL(as_exp_string_to_blob(as_exp_bin_str(BIN)));
	log_bytes_array("toBlob(\"hello\")", as_record_get_bytes(rec, VAR));
	as_record_destroy(rec);

	// split - one element per codepoint
	put(as, &key, "abc");
	EVAL(as_exp_string_split(as_exp_bin_str(BIN)));
	log_list("split()", as_record_get_list(rec, VAR));
	as_record_destroy(rec);

	// split(separator)
	put(as, &key, "one,two,three");
	EVAL(as_exp_string_split_separator(",", as_exp_bin_str(BIN)));
	log_list("split(\",\")", as_record_get_list(rec, VAR));
	as_record_destroy(rec);

	// b64Decode - decode base64 text to blob
	put(as, &key, "aGVsbG8=");
	EVAL(as_exp_string_b64_decode(as_exp_bin_str(BIN)));
	log_bytes_string("b64Decode(\"aGVsbG8=\")", as_record_get_bytes(rec, VAR));
	as_record_destroy(rec);

	// regexCompare - ICU regex pattern match
	put(as, &key, "Hello123World");
	EVAL(as_exp_string_regex_compare("[0-9]+", as_exp_bin_str(BIN)));
	LOG("regexCompare(\"[0-9]+\") = %s", as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

	// regexCompare(flags) - case-insensitive match
	put(as, &key, "HELLO");
	EVAL(as_exp_string_regex_compare_flags(
			"hello", AS_STRING_REGEX_FLAGS_CASE_INSENSITIVE, as_exp_bin_str(BIN)));
	LOG("regexCompare(\"hello\", CASE_INSENSITIVE) = %s",
			as_record_get_bool(rec, VAR) ? "true" : "false");
	as_record_destroy(rec);

#undef EVAL
}

// Modify-style expressions: each returns the transformed string value.
// The underlying bin is not mutated.
static void
run_modify_exps(aerospike* as)
{
#define EVAL(...) \
	do { \
		as_exp_build(exp, __VA_ARGS__); \
		rec = eval_exp(as, &key, exp); \
		as_exp_destroy(exp); \
	} while (false)

	as_key key;
	as_key_init_str(&key, g_namespace, g_set, "stringexp_modify");
	as_record* rec;

	as_string_policy policy;
	as_string_policy_init(&policy);

	// insert - splice value at codepoint index
	put(as, &key, "hello world");
	EVAL(as_exp_string_insert(&policy, 5, " beautiful", as_exp_bin_str(BIN)));
	LOG("insert(5, \" beautiful\") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// overwrite - replace codepoints starting at index
	EVAL(as_exp_string_overwrite(&policy, 6, "earth", as_exp_bin_str(BIN)));
	LOG("overwrite(6, \"earth\") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// concat - append a list of strings in order
	put(as, &key, "hello");
	as_arraylist concat;
	as_arraylist_inita(&concat, 3);
	as_arraylist_append_str(&concat, " ");
	as_arraylist_append_str(&concat, "big");
	as_arraylist_append_str(&concat, " world");
	EVAL(as_exp_string_concat_list(&policy, as_exp_val((as_val*)&concat), as_exp_bin_str(BIN)));
	LOG("concat([\" \", \"big\", \" world\"]) -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);
	as_arraylist_destroy(&concat);

	// append - Unicode-aware end-append
	EVAL(as_exp_string_append(&policy, "!", as_exp_bin_str(BIN)));
	LOG("append(\"!\") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// prepend - Unicode-aware front-insert
	put(as, &key, "world");
	EVAL(as_exp_string_prepend(&policy, "hello ", as_exp_bin_str(BIN)));
	LOG("prepend(\"hello \") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// snip - remove half-open codepoint range
	put(as, &key, "hello beautiful world");
	EVAL(as_exp_string_snip(&policy, 5, 15, as_exp_bin_str(BIN)));
	LOG("snip(5, 15) -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// replace - first occurrence only
	put(as, &key, "hello world world");
	EVAL(as_exp_string_replace(&policy, "world", "earth", as_exp_bin_str(BIN)));
	LOG("replace(\"world\", \"earth\") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// replaceAll - every occurrence
	put(as, &key, "aabaa");
	EVAL(as_exp_string_replace_all(&policy, "a", "x", as_exp_bin_str(BIN)));
	LOG("replaceAll(\"a\", \"x\") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// upper
	put(as, &key, "hello world");
	EVAL(as_exp_string_upper(&policy, as_exp_bin_str(BIN)));
	LOG("upper() -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// lower
	put(as, &key, "HELLO WORLD");
	EVAL(as_exp_string_lower(&policy, as_exp_bin_str(BIN)));
	LOG("lower() -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// caseFold - locale-independent fold for comparison keys
	put(as, &key, "HELLO World");
	EVAL(as_exp_string_case_fold(&policy, as_exp_bin_str(BIN)));
	LOG("caseFold() -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// normalizeNFC - Unicode NFC normalization
	put(as, &key, "café");
	EVAL(as_exp_string_normalize_nfc(&policy, as_exp_bin_str(BIN)));
	LOG("normalizeNFC() -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// trimStart - drop leading whitespace
	put(as, &key, "  hello  ");
	EVAL(as_exp_string_trim_start(&policy, as_exp_bin_str(BIN)));
	LOG("trimStart() -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// trimEnd - drop trailing whitespace
	EVAL(as_exp_string_trim_end(&policy, as_exp_bin_str(BIN)));
	LOG("trimEnd() -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// trim - drop both ends
	put(as, &key, "  hello world  ");
	EVAL(as_exp_string_trim(&policy, as_exp_bin_str(BIN)));
	LOG("trim() -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// padStart - left-pad up to target codepoint length
	put(as, &key, "hello");
	EVAL(as_exp_string_pad_start(&policy, 10, "*", as_exp_bin_str(BIN)));
	LOG("padStart(10, \"*\") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// padEnd - right-pad up to target codepoint length
	EVAL(as_exp_string_pad_end(&policy, 10, ".", as_exp_bin_str(BIN)));
	LOG("padEnd(10, \".\") -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// repeat - repeat string n times
	put(as, &key, "ab");
	EVAL(as_exp_string_repeat(&policy, 3, as_exp_bin_str(BIN)));
	LOG("repeat(3) -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);

	// regexReplace - pass GLOBAL to replace every match (default replaces first only)
	put(as, &key, "abc123def456");
	EVAL(as_exp_string_regex_replace(
			&policy, "[0-9]+", "NUM", AS_STRING_REGEX_FLAGS_GLOBAL, as_exp_bin_str(BIN)));
	LOG("regexReplace(\"[0-9]+\", \"NUM\", GLOBAL) -> \"%s\"",
			as_record_get_str(rec, VAR));
	as_record_destroy(rec);

#undef EVAL
}

// toString - stringify any int / float / string / blob source. Demonstrated
// on an integer bin.
static void
run_to_string(aerospike* as)
{
	as_key key;
	as_key_init_str(&key, g_namespace, g_set, "stringexp_tostring");

	as_error err;
	aerospike_key_remove(as, &err, NULL, &key);

	as_record put_rec;
	as_record_inita(&put_rec, 1);
	as_record_set_int64(&put_rec, NUM_BIN, 42);

	if (aerospike_key_put(as, &err, NULL, &key, &put_rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		as_record_destroy(&put_rec);
		example_cleanup(as);
		exit(-1);
	}

	as_record_destroy(&put_rec);

	as_exp_build(exp, as_exp_to_string(as_exp_bin_int(NUM_BIN)));
	as_record* rec = eval_exp(as, &key, exp);
	as_exp_destroy(exp);

	LOG("toString(intBin(\"n\") = 42) -> \"%s\"", as_record_get_str(rec, VAR));
	as_record_destroy(rec);
}

static void
put(aerospike* as, as_key* key, const char* value)
{
	as_error err;
	aerospike_key_remove(as, &err, NULL, key);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, BIN, value);

	if (aerospike_key_put(as, &err, NULL, key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_put() returned %d - %s", err.code, err.message);
		as_record_destroy(&rec);
		example_cleanup(as);
		exit(-1);
	}

	as_record_destroy(&rec);
}

static as_record*
eval_exp(aerospike* as, as_key* key, as_exp* exp)
{
	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, VAR, exp, AS_EXP_READ_DEFAULT);

	as_error err;
	as_record* rec = NULL;

	if (aerospike_key_operate(as, &err, NULL, key, &ops, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		as_operations_destroy(&ops);
		example_cleanup(as);
		exit(-1);
	}

	as_operations_destroy(&ops);
	return rec;
}

static void
log_bytes_array(const char* label, as_bytes* bytes)
{
	if (bytes == NULL) {
		LOG("%s = null", label);
		return;
	}

	printf("%s = [", label);

	for (uint32_t i = 0; i < bytes->size; i++) {
		printf("%s%u", i == 0 ? "" : ", ", bytes->value[i]);
	}

	printf("]\n");
	fflush(stdout);
}

static void
log_bytes_string(const char* label, as_bytes* bytes)
{
	if (bytes == NULL) {
		LOG("%s = null", label);
		return;
	}

	printf("%s = \"", label);

	for (uint32_t i = 0; i < bytes->size; i++) {
		printf("%c", bytes->value[i]);
	}

	printf("\"\n");
	fflush(stdout);
}

static void
log_list(const char* label, as_list* list)
{
	if (list == NULL) {
		LOG("%s = null", label);
		return;
	}

	uint32_t size = as_list_size(list);
	printf("%s = [", label);

	for (uint32_t i = 0; i < size; i++) {
		as_val* value = as_list_get(list, i);

		if (i != 0) {
			printf(", ");
		}

		if (value != NULL && as_val_type(value) == AS_STRING) {
			printf("%s", as_string_get((as_string*)value));
		}
		else {
			printf("<non-string>");
		}
	}

	printf("]\n");
	fflush(stdout);
}
