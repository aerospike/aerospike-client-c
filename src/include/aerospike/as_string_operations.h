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
#pragma once

/**
 * @defgroup string_operations String Operations
 * @ingroup client_operations
 *
 * String operations create operations for aerospike_key_operate() to inspect
 * and modify string bins.
 *
 * Index orientation is left-to-right with Unicode codepoint addressing.
 * Negative indexes count from the end of the string (-1 is the last
 * codepoint). Out-of-bounds indexes are clamped by the server.
 *
 * String operations require server version 8.1.3 or later. When ctx is not
 * NULL and not empty, the operation targets a string nested inside a list or
 * map. The ctx-navigated leaf must already be an Aerospike string; operations
 * on non-string leaves return AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE.
 *
 * as_operations_to_string() is a top-level conversion operation and does not
 * accept ctx because it is sent as its own wire operation instead of a string
 * sub-operation with a msgpack payload.
 *
 * All string arguments (needle, value, separator, pattern, etc.) are passed as
 * NULL-terminated C strings, so their length is determined with strlen() and they
 * cannot contain embedded NULL bytes.
 *
 * @code
 * // Read: bin "text" = "hello world".
 * as_operations ops;
 * as_operations_inita(&ops, 1);
 * as_operations_string_strlen(&ops, "text", NULL);
 *
 * as_record* rec = NULL;
 * as_error err;
 * aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
 * int64_t len = as_record_get_int64(rec, "text", 0); // 11
 * as_operations_destroy(&ops);
 * as_record_destroy(rec);
 *
 * // Modify: uppercase a string nested in list bin "items" at index 0.
 * as_cdt_ctx ctx;
 * as_cdt_ctx_init(&ctx, 1);
 * as_cdt_ctx_add_list_index(&ctx, 0);
 *
 * as_operations_inita(&ops, 1);
 * as_operations_string_upper(&ops, "items", &ctx, NULL);
 * aerospike_key_operate(as, &err, NULL, &key, &ops, NULL);
 * as_operations_destroy(&ops);
 * as_cdt_ctx_destroy(&ctx);
 * @endcode
 */

#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * String operation policy write bit flags. Use bitwise OR to combine flags.
 */
typedef enum as_string_write_flags_e {
	/** Default. Allow create or update. */
	AS_STRING_WRITE_FLAGS_DEFAULT = 0,

	/**
	 * Do not raise an error if a modify operation cannot be applied because
	 * the target bin does not exist. The record is left unchanged.
	 */
	AS_STRING_WRITE_FLAGS_NO_FAIL = 4
} as_string_write_flags;

/**
 * String operation policy.
 */
typedef struct as_string_policy_s {
	as_string_write_flags flags;
} as_string_policy;

/**
 * Numeric type filter for as_operations_string_is_numeric_type().
 */
typedef enum as_string_numeric_type_e {
	/** Match either an integer or a floating-point number. */
	AS_STRING_NUMERIC_ANY = 0,

	/** Match only integers. */
	AS_STRING_NUMERIC_INT = 1,

	/** Match only floating-point numbers. */
	AS_STRING_NUMERIC_FLOAT = 2
} as_string_numeric_type;

/**
 * Regex flags for string regex operations. Use bitwise OR to combine flags.
 */
typedef enum as_string_regex_flags_e {
	/** Default. No flags set. */
	AS_STRING_REGEX_FLAGS_NONE = 0,

	/** Case insensitive matching. */
	AS_STRING_REGEX_FLAGS_CASE_INSENSITIVE = 1 << 0,

	/**
	 * Treat input as a multi-line string. The ^ and $ metacharacters match the
	 * start and end of any line, not just the start and end of the input.
	 */
	AS_STRING_REGEX_FLAGS_MULTILINE = 1 << 1,

	/** The dot metacharacter matches line terminators. */
	AS_STRING_REGEX_FLAGS_DOTALL = 1 << 2,

	/** Treat only \n as a line terminator. */
	AS_STRING_REGEX_FLAGS_UNIX_LINES = 1 << 3,

	/** Replace all matches. Only applicable to regex_replace. */
	AS_STRING_REGEX_FLAGS_GLOBAL = 1 << 4
} as_string_regex_flags;

/**
 * @private
 * String operation codes.
 */
typedef enum as_string_op_e {
	AS_STRING_OP_STRLEN = 0,
	AS_STRING_OP_SUBSTR = 1,
	AS_STRING_OP_CHAR_AT = 2,
	AS_STRING_OP_FIND = 3,
	AS_STRING_OP_CONTAINS = 4,
	AS_STRING_OP_STARTS_WITH = 5,
	AS_STRING_OP_ENDS_WITH = 6,
	AS_STRING_OP_TO_INTEGER = 7,
	AS_STRING_OP_TO_DOUBLE = 8,
	AS_STRING_OP_BYTE_LENGTH = 9,
	AS_STRING_OP_IS_NUMERIC = 10,
	AS_STRING_OP_IS_UPPER = 11,
	AS_STRING_OP_IS_LOWER = 12,
	AS_STRING_OP_TO_BLOB = 13,
	AS_STRING_OP_SPLIT = 14,
	AS_STRING_OP_B64_DECODE = 15,
	AS_STRING_OP_REGEX_COMPARE = 16,
	AS_STRING_OP_INSERT = 50,
	AS_STRING_OP_OVERWRITE = 51,
	AS_STRING_OP_CONCAT = 52,
	AS_STRING_OP_SNIP = 53,
	AS_STRING_OP_REPLACE = 54,
	AS_STRING_OP_REPLACE_ALL = 55,
	AS_STRING_OP_UPPER = 56,
	AS_STRING_OP_LOWER = 57,
	AS_STRING_OP_CASE_FOLD = 58,
	AS_STRING_OP_NORMALIZE_NFC = 59,
	AS_STRING_OP_TRIM_START = 60,
	AS_STRING_OP_TRIM_END = 61,
	AS_STRING_OP_TRIM = 62,
	AS_STRING_OP_PAD_START = 63,
	AS_STRING_OP_PAD_END = 64,
	AS_STRING_OP_REPEAT = 65,
	AS_STRING_OP_REGEX_REPLACE = 66
} as_string_op;

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize string policy to default write semantics.
 */
AS_EXTERN void
as_string_policy_init(as_string_policy* policy);

/**
 * Set string policy write flags.
 */
AS_EXTERN void
as_string_policy_set(as_string_policy* policy, as_string_write_flags flags);

//---------------------------------
// String Read Operations
//---------------------------------

/**
 * Create string strlen operation. The server returns the number of Unicode
 * codepoints in the string bin as an int64. This is not the number of UTF-8
 * bytes and it is not a grapheme cluster count.
 *
 * Examples: precomposed "e with acute" counts as 1 codepoint, while "e" plus
 * a combining acute accent counts as 2 codepoints. Use
 * as_operations_string_byte_length() for UTF-8 byte length.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_strlen(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string substr operation from start to the end of the string. Negative
 * start indexes count from the end of the string.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param start Starting codepoint index.
 */
AS_EXTERN bool
as_operations_string_substr(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t start);

/**
 * Create string substr operation that returns length codepoints starting at
 * start. Negative start indexes count from the end of the string.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param start Starting codepoint index.
 * @param length Number of codepoints to return.
 */
AS_EXTERN bool
as_operations_string_substr_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t start, uint64_t length
	);

/**
 * Create string char_at operation that returns the codepoint at index as a
 * one-codepoint string. Negative indexes count from the end.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param index Index of the codepoint to return.
 */
AS_EXTERN bool
as_operations_string_char_at(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index);

/**
 * Create string find operation that returns the codepoint index of the first
 * occurrence of needle, or -1 if not found.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param needle The string to search for.
 */
AS_EXTERN bool
as_operations_string_find(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* needle);

/**
 * Create string find operation for a specific occurrence of needle. Occurrence
 * is 1-based. Negative occurrences count backward from the last match.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param needle The string to search for.
 * @param occurrence The occurrence of the string to search for.
 */
AS_EXTERN bool
as_operations_string_find_occurrence(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* needle, int64_t occurrence
	);

/**
 * Create string contains operation that returns true if the bin contains needle.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param needle The string to search for.
 */
AS_EXTERN bool
as_operations_string_contains(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* needle);

/**
 * Create string starts_with operation that returns true if the bin begins with
 * prefix.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param prefix The string to search for.
 */
AS_EXTERN bool
as_operations_string_starts_with(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* prefix
	);

/**
 * Create string ends_with operation that returns true if the bin ends with
 * suffix.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param suffix The string to search for.
 */
AS_EXTERN bool
as_operations_string_ends_with(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* suffix
	);

/**
 * Create string to_integer operation that parses the string as an int64.
 * Returns AEROSPIKE_ERR_PARAMETER if the bin cannot be parsed as an integer.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_to_integer(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string to_double operation that parses the string as a 64-bit float.
 * Returns AEROSPIKE_ERR_PARAMETER if the bin cannot be parsed as a double.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_to_double(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string byte_length operation that returns the number of UTF-8 bytes in
 * the string as an int64. This differs from strlen for non-ASCII text.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_byte_length(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string is_numeric operation that returns true if the bin contains a
 * valid integer or floating-point number.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_is_numeric(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string is_numeric operation with a numeric type filter.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param numeric_type The numeric type to filter for.
 */
AS_EXTERN bool
as_operations_string_is_numeric_type(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_numeric_type numeric_type
	);

/**
 * Create string is_upper operation that returns true if every cased codepoint in
 * the bin is uppercase.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_is_upper(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string is_lower operation that returns true if every cased codepoint in
 * the bin is lowercase.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_is_lower(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string to_blob operation that returns the UTF-8 bytes of the string as
 * a blob.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_to_blob(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string split operation that splits by Unicode codepoint. Each codepoint
 * becomes one string element in the returned list.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_split(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string split operation that splits by separator. If separator is not
 * found, the server returns a singleton list containing the whole string.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param separator The separator to split by.
 */
AS_EXTERN bool
as_operations_string_split_separator(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* separator
	);

/**
 * Create string b64_decode operation that treats the bin as base64 text and
 * returns the decoded bytes as a blob.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 */
AS_EXTERN bool
as_operations_string_b64_decode(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create string regex_compare operation that matches an ICU regex pattern
 * against the bin and returns true on match.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param pattern The regex pattern to match against.
 */
AS_EXTERN bool
as_operations_string_regex_compare(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* pattern
	);

/**
 * Create string regex_compare operation with regex flags.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param pattern The regex pattern to match against.
 * @param flags The regex flags to use.
 */
AS_EXTERN bool
as_operations_string_regex_compare_flags(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* pattern, as_string_regex_flags flags
	);

//---------------------------------
// String Modify Operations
//---------------------------------

/**
 * Create string insert operation that splices value into the bin at codepoint
 * index. Negative indexes count from the end of the string.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param index Index of the codepoint to insert at.
 * @param value The value to insert.
 */
AS_EXTERN bool
as_operations_string_insert(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	int64_t index, const char* value
	);

/**
 * Create string overwrite operation that overwrites codepoints starting at index
 * with value. The result may grow beyond the original length when value extends
 * past the end.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param index Index of the codepoint to overwrite at.
 * @param value The value to overwrite.
 */
AS_EXTERN bool
as_operations_string_overwrite(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	int64_t index, const char* value
	);

/**
 * Create string concat operation that appends value to the bin.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param value The value to append.
 */
AS_EXTERN bool
as_operations_string_concat(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, const char* value
	);

/**
 * Create string concat operation that appends each string element in values to
 * the bin in order.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param values The list of values to append.
 */
AS_EXTERN bool
as_operations_string_concat_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, as_list* values
	);

/**
 * Create string snip operation that removes codepoints from start through the
 * end of the string.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param start The index of the codepoint to remove from.
 */
AS_EXTERN bool
as_operations_string_snip(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, int64_t start
	);

/**
 * Create string snip operation that removes the half-open codepoint range
 * [start, end).
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param start The index of the codepoint to remove from.
 * @param end The index of the codepoint to remove to.
 */
AS_EXTERN bool
as_operations_string_snip_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	int64_t start, int64_t end
	);

/**
 * Create string replace operation that replaces the first occurrence of needle
 * with replacement.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param needle The string to replace.
 * @param replacement The string to replace with.
 */
AS_EXTERN bool
as_operations_string_replace(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	const char* needle, const char* replacement
	);

/**
 * Create string replace_all operation that replaces every occurrence of needle
 * with replacement.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param needle The string to replace.
 * @param replacement The string to replace with.
 */
AS_EXTERN bool
as_operations_string_replace_all(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	const char* needle, const char* replacement
	);

/**
 * Create string upper operation that uppercases the bin in place.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 */
AS_EXTERN bool
as_operations_string_upper(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy
	);

/**
 * Create string lower operation that lowercases the bin in place.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 */
AS_EXTERN bool
as_operations_string_lower(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy
	);

/**
 * Create string case_fold operation that applies locale-independent case folding
 * (lowercase) to the bin. This is useful for normalized comparison keys.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 */
AS_EXTERN bool
as_operations_string_case_fold(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy
	);

/**
 * Create string normalize_nfc operation that normalizes the bin to Unicode NFC.
 * Already-normalized strings are unchanged.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 */
AS_EXTERN bool
as_operations_string_normalize_nfc(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy
	);

/**
 * Create string trim_start operation that removes whitespace from the start of
 * the bin.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 */
AS_EXTERN bool
as_operations_string_trim_start(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy
	);

/**
 * Create string trim_end operation that removes whitespace from the end of the
 * bin.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 */
AS_EXTERN bool
as_operations_string_trim_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy
	);

/**
 * Create string trim operation that removes whitespace from both ends of the bin.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 */
AS_EXTERN bool
as_operations_string_trim(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy
	);

/**
 * Create string pad_start operation that prepends pad_string repeatedly until
 * the bin reaches target_length codepoints. No-op when the bin is already at or
 * above the target length.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param target_length The target length of the string.
 * @param pad_string The string to pad with.
 */
AS_EXTERN bool
as_operations_string_pad_start(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	uint64_t target_length, const char* pad_string
	);

/**
 * Create string pad_end operation that appends pad_string repeatedly until the
 * bin reaches target_length codepoints. No-op when the bin is already at or
 * above the target length.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param target_length The target length of the string.
 * @param pad_string The string to pad with.
 */
AS_EXTERN bool
as_operations_string_pad_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	uint64_t target_length, const char* pad_string
	);

/**
 * Create string repeat operation that repeats the bin contents count times.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param policy String policy.
 * @param count The number of times to repeat the string. Must be non-negative.
 */
AS_EXTERN bool
as_operations_string_repeat(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, uint64_t count
	);

/**
 * Create string regex_replace operation that replaces the first match of pattern
 * with replacement. Pass AS_STRING_REGEX_FLAGS_GLOBAL to replace every match.
 * This server operation accepts regex flags but not string policy flags.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 * @param ctx Optional path into a string nested inside a list or map.
 * @param pattern The regex pattern to match against.
 * @param replacement The string to replace with.
 * @param flags The regex flags to use.
 */
AS_EXTERN bool
as_operations_string_regex_replace(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* pattern,
	const char* replacement, as_string_regex_flags flags
	);

/**
 * Create to_string operation that converts an integer, double, string, or blob
 * bin to its string representation. Returns AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE for
 * any other bin type. This top-level operation does not accept ctx and does not
 * send a msgpack payload.
 *
 * @param ops Operations array.
 * @param name Name of string bin.
 */
AS_EXTERN bool
as_operations_to_string(as_operations* ops, const char* name);

#ifdef __cplusplus
} // end extern "C"
#endif
