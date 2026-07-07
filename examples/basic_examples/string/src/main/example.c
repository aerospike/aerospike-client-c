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
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_string_operations.h>
#include <aerospike/as_val.h>

#include "example_utils.h"


//==========================================================
// STRING Example
//

static const char* BIN = "text";
static const char* NUM_BIN = "n";

static void run_read_ops(aerospike* as);
static void run_modify_ops(aerospike* as);
static void run_to_string(aerospike* as);
static void put(aerospike* as, as_key* key, const char* value);
static as_record* operate(aerospike* as, as_key* key, as_operations* ops);
static void modify_and_show(aerospike* as, as_key* key, const char* label, as_operations* ops);
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

	run_read_ops(&as);
	run_modify_ops(&as);
	run_to_string(&as);

	// Cleanup and disconnect from the database cluster.
	example_cleanup(&as);

	LOG("string example successfully completed");

	return 0;
}

// Read-only string operations: return information about the bin without
// mutating it.
static void
run_read_ops(aerospike* as)
{
	as_key key;
	as_key_init_str(&key, g_namespace, g_set, "opstr_read");
	as_operations ops;
	as_record* rec;

	// strlen - codepoint count
	put(as, &key, "hello world");
	as_operations_inita(&ops, 1);
	as_operations_string_strlen(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	LOG("strlen(\"hello world\") = %lld", (long long)as_record_get_int64(rec, BIN, 0));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// substr(start) - codepoint slice to end of string
	as_operations_inita(&ops, 1);
	as_operations_string_substr(&ops, BIN, NULL, 6);
	rec = operate(as, &key, &ops);
	LOG("substr(6) = \"%s\"", as_record_get_str(rec, BIN));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// substr(start, end) - half-open codepoint range
	as_operations_inita(&ops, 1);
	as_operations_string_substr_range(&ops, BIN, NULL, 0, 5);
	rec = operate(as, &key, &ops);
	LOG("substr(0, 5) = \"%s\"", as_record_get_str(rec, BIN));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// charAt - single-codepoint slice
	as_operations_inita(&ops, 1);
	as_operations_string_char_at(&ops, BIN, NULL, 6);
	rec = operate(as, &key, &ops);
	LOG("charAt(6) = \"%s\"", as_record_get_str(rec, BIN));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// find(needle) - index of first match, -1 if absent
	as_operations_inita(&ops, 1);
	as_operations_string_find(&ops, BIN, NULL, "world");
	rec = operate(as, &key, &ops);
	LOG("find(\"world\") = %lld", (long long)as_record_get_int64(rec, BIN, 0));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// find(needle, occurrence) - index of nth match
	put(as, &key, "ababab");
	as_operations_inita(&ops, 1);
	as_operations_string_find_occurrence(&ops, BIN, NULL, "ab", 2);
	rec = operate(as, &key, &ops);
	LOG("find(\"ab\", occurrence=2) on \"ababab\" = %lld",
			(long long)as_record_get_int64(rec, BIN, 0));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// contains
	put(as, &key, "hello world");
	as_operations_inita(&ops, 1);
	as_operations_string_contains(&ops, BIN, NULL, "hello");
	rec = operate(as, &key, &ops);
	LOG("contains(\"hello\") = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// startsWith
	as_operations_inita(&ops, 1);
	as_operations_string_starts_with(&ops, BIN, NULL, "hello");
	rec = operate(as, &key, &ops);
	LOG("startsWith(\"hello\") = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// endsWith
	as_operations_inita(&ops, 1);
	as_operations_string_ends_with(&ops, BIN, NULL, "world");
	rec = operate(as, &key, &ops);
	LOG("endsWith(\"world\") = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// toInteger - parse string as int64
	put(as, &key, "12345");
	as_operations_inita(&ops, 1);
	as_operations_string_to_integer(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	LOG("toInteger(\"12345\") = %lld", (long long)as_record_get_int64(rec, BIN, 0));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// toDouble - parse string as float64
	put(as, &key, "3.14");
	as_operations_inita(&ops, 1);
	as_operations_string_to_double(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	LOG("toDouble(\"3.14\") = %f", as_record_get_double(rec, BIN, 0.0));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// byteLength - UTF-8 byte count (differs from strlen for non-ASCII)
	put(as, &key, "héllo");
	as_operations_inita(&ops, 1);
	as_operations_string_byte_length(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	LOG("byteLength(\"héllo\") = %lld (5 codepoints, 6 UTF-8 bytes)",
			(long long)as_record_get_int64(rec, BIN, 0));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// isNumeric - accepts integer or float
	put(as, &key, "12345");
	as_operations_inita(&ops, 1);
	as_operations_string_is_numeric(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	LOG("isNumeric(\"12345\") = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// isNumeric(numericType) - restrict by AS_STRING_NUMERIC_INT
	put(as, &key, "3.14");
	as_operations_inita(&ops, 1);
	as_operations_string_is_numeric_type(&ops, BIN, NULL, AS_STRING_NUMERIC_INT);
	rec = operate(as, &key, &ops);
	LOG("isNumeric(\"3.14\", INT) = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// isUpper
	put(as, &key, "HELLO");
	as_operations_inita(&ops, 1);
	as_operations_string_is_upper(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	LOG("isUpper(\"HELLO\") = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// isLower
	put(as, &key, "hello");
	as_operations_inita(&ops, 1);
	as_operations_string_is_lower(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	LOG("isLower(\"hello\") = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// toBlob - UTF-8 bytes as blob
	as_operations_inita(&ops, 1);
	as_operations_string_to_blob(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	log_bytes_array("toBlob(\"hello\")", as_record_get_bytes(rec, BIN));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// split - one element per codepoint
	put(as, &key, "abc");
	as_operations_inita(&ops, 1);
	as_operations_string_split(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	log_list("split()", as_record_get_list(rec, BIN));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// split(separator)
	put(as, &key, "one,two,three");
	as_operations_inita(&ops, 1);
	as_operations_string_split_separator(&ops, BIN, NULL, ",");
	rec = operate(as, &key, &ops);
	log_list("split(\",\")", as_record_get_list(rec, BIN));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// b64Decode - decode base64 text to blob
	put(as, &key, "aGVsbG8=");
	as_operations_inita(&ops, 1);
	as_operations_string_b64_decode(&ops, BIN, NULL);
	rec = operate(as, &key, &ops);
	log_bytes_string("b64Decode(\"aGVsbG8=\")", as_record_get_bytes(rec, BIN));
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// regexCompare - ICU regex pattern match
	put(as, &key, "Hello123World");
	as_operations_inita(&ops, 1);
	as_operations_string_regex_compare(&ops, BIN, NULL, "[0-9]+");
	rec = operate(as, &key, &ops);
	LOG("regexCompare(\"[0-9]+\") = %s", as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);

	// regexCompare(flags) - case-insensitive match
	put(as, &key, "HELLO");
	as_operations_inita(&ops, 1);
	as_operations_string_regex_compare_flags(&ops, BIN, NULL, "hello",
			AS_STRING_REGEX_FLAGS_CASE_INSENSITIVE);
	rec = operate(as, &key, &ops);
	LOG("regexCompare(\"hello\", CASE_INSENSITIVE) = %s",
			as_record_get_bool(rec, BIN) ? "true" : "false");
	as_operations_destroy(&ops);
	as_record_destroy(rec);
}

// Modify operations: mutate the bin in place. Each call below performs the
// modify op then re-reads the bin to display the new value.
static void
run_modify_ops(aerospike* as)
{
	as_key key;
	as_key_init_str(&key, g_namespace, g_set, "opstr_modify");

	as_string_policy policy;
	as_string_policy_init(&policy);

	as_operations ops;

	// insert - splice value at codepoint index
	put(as, &key, "hello world");
	as_operations_inita(&ops, 1);
	as_operations_string_insert(&ops, BIN, NULL, &policy, 5, " beautiful");
	modify_and_show(as, &key, "insert(5, \" beautiful\")", &ops);

	// overwrite - replace codepoints starting at index
	put(as, &key, "hello world");
	as_operations_inita(&ops, 1);
	as_operations_string_overwrite(&ops, BIN, NULL, &policy, 6, "earth");
	modify_and_show(as, &key, "overwrite(6, \"earth\")", &ops);

	// concat(value) - append a single string
	put(as, &key, "hello");
	as_operations_inita(&ops, 1);
	as_operations_string_concat(&ops, BIN, NULL, &policy, "!");
	modify_and_show(as, &key, "concat(\"!\")", &ops);

	// concat(values) - append each list element in order
	put(as, &key, "hello");
	as_arraylist* concat = as_arraylist_new(3, 3);
	as_arraylist_append_str(concat, " ");
	as_arraylist_append_str(concat, "big");
	as_arraylist_append_str(concat, " world");
	as_operations_inita(&ops, 1);
	as_operations_string_concat_list(&ops, BIN, NULL, &policy, (as_list*)concat);
	modify_and_show(as, &key, "concat([\" \", \"big\", \" world\"])", &ops);

	// append - Unicode-aware end-append
	put(as, &key, "hello");
	as_operations_inita(&ops, 1);
	as_operations_string_append(&ops, BIN, NULL, &policy, "!");
	modify_and_show(as, &key, "append(\"!\")", &ops);

	// prepend - Unicode-aware front-insert
	put(as, &key, "world");
	as_operations_inita(&ops, 1);
	as_operations_string_prepend(&ops, BIN, NULL, &policy, "hello ");
	modify_and_show(as, &key, "prepend(\"hello \")", &ops);

	// snip - remove half-open codepoint range
	put(as, &key, "hello beautiful world");
	as_operations_inita(&ops, 1);
	as_operations_string_snip(&ops, BIN, NULL, &policy, 5, 15);
	modify_and_show(as, &key, "snip(5, 15)", &ops);

	// replace - first occurrence only
	put(as, &key, "hello world world");
	as_operations_inita(&ops, 1);
	as_operations_string_replace(&ops, BIN, NULL, &policy, "world", "earth");
	modify_and_show(as, &key, "replace(\"world\", \"earth\")", &ops);

	// replaceAll - every occurrence
	put(as, &key, "aabaa");
	as_operations_inita(&ops, 1);
	as_operations_string_replace_all(&ops, BIN, NULL, &policy, "a", "x");
	modify_and_show(as, &key, "replaceAll(\"a\", \"x\")", &ops);

	// upper
	put(as, &key, "hello world");
	as_operations_inita(&ops, 1);
	as_operations_string_upper(&ops, BIN, NULL, &policy);
	modify_and_show(as, &key, "upper()", &ops);

	// lower
	put(as, &key, "HELLO WORLD");
	as_operations_inita(&ops, 1);
	as_operations_string_lower(&ops, BIN, NULL, &policy);
	modify_and_show(as, &key, "lower()", &ops);

	// caseFold - locale-independent fold for comparison keys
	put(as, &key, "HELLO World");
	as_operations_inita(&ops, 1);
	as_operations_string_case_fold(&ops, BIN, NULL, &policy);
	modify_and_show(as, &key, "caseFold()", &ops);

	// normalizeNFC - Unicode NFC normalization
	put(as, &key, "café");
	as_operations_inita(&ops, 1);
	as_operations_string_normalize_nfc(&ops, BIN, NULL, &policy);
	modify_and_show(as, &key, "normalizeNFC()", &ops);

	// trimStart - drop leading whitespace
	put(as, &key, "  hello  ");
	as_operations_inita(&ops, 1);
	as_operations_string_trim_start(&ops, BIN, NULL, &policy);
	modify_and_show(as, &key, "trimStart()", &ops);

	// trimEnd - drop trailing whitespace
	put(as, &key, "  hello  ");
	as_operations_inita(&ops, 1);
	as_operations_string_trim_end(&ops, BIN, NULL, &policy);
	modify_and_show(as, &key, "trimEnd()", &ops);

	// trim - drop both ends
	put(as, &key, "  hello world  ");
	as_operations_inita(&ops, 1);
	as_operations_string_trim(&ops, BIN, NULL, &policy);
	modify_and_show(as, &key, "trim()", &ops);

	// padStart - left-pad up to target codepoint length
	put(as, &key, "hello");
	as_operations_inita(&ops, 1);
	as_operations_string_pad_start(&ops, BIN, NULL, &policy, 10, "*");
	modify_and_show(as, &key, "padStart(10, \"*\")", &ops);

	// padEnd - right-pad up to target codepoint length
	put(as, &key, "hello");
	as_operations_inita(&ops, 1);
	as_operations_string_pad_end(&ops, BIN, NULL, &policy, 10, ".");
	modify_and_show(as, &key, "padEnd(10, \".\")", &ops);

	// repeat - repeat string n times
	put(as, &key, "ab");
	as_operations_inita(&ops, 1);
	as_operations_string_repeat(&ops, BIN, NULL, &policy, 3);
	modify_and_show(as, &key, "repeat(3)", &ops);

	// regexReplace - pass GLOBAL to replace every match (default replaces first only)
	put(as, &key, "abc123def456");
	as_operations_inita(&ops, 1);
	as_operations_string_regex_replace(&ops, BIN, NULL, &policy, "[0-9]+", "NUM",
			AS_STRING_REGEX_FLAGS_GLOBAL);
	modify_and_show(as, &key, "regexReplace(\"[0-9]+\", \"NUM\", GLOBAL)", &ops);
}

// toString - convert any int / float / string / blob bin to its string
// representation. Unlike the other ops, this does not accept a CTX.
static void
run_to_string(aerospike* as)
{
	as_key key;
	as_key_init_str(&key, g_namespace, g_set, "opstr_tostring");

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

	as_operations ops;
	as_operations_inita(&ops, 1);
	as_operations_to_string(&ops, NUM_BIN);

	as_record* rec = operate(as, &key, &ops);
	LOG("toString(int 42) = \"%s\"", as_record_get_str(rec, NUM_BIN));
	as_operations_destroy(&ops);
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
operate(aerospike* as, as_key* key, as_operations* ops)
{
	as_error err;
	as_record* rec = NULL;

	if (aerospike_key_operate(as, &err, NULL, key, ops, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		example_cleanup(as);
		exit(-1);
	}

	return rec;
}

static void
modify_and_show(aerospike* as, as_key* key, const char* label, as_operations* ops)
{
	as_error err;

	if (aerospike_key_operate(as, &err, NULL, key, ops, NULL) != AEROSPIKE_OK) {
		LOG("aerospike_key_operate() returned %d - %s", err.code, err.message);
		as_operations_destroy(ops);
		example_cleanup(as);
		exit(-1);
	}

	as_operations_destroy(ops);

	as_record* rec = NULL;

	if (aerospike_key_get(as, &err, NULL, key, &rec) != AEROSPIKE_OK) {
		LOG("aerospike_key_get() returned %d - %s", err.code, err.message);
		example_cleanup(as);
		exit(-1);
	}

	LOG("%s -> \"%s\"", label, as_record_get_str(rec, BIN));
	as_record_destroy(rec);
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
