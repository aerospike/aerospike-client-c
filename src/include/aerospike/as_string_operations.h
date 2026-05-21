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
#pragma once

/**
 * @defgroup string_operations String bin operations
 * @ingroup client_operations
 *
 * Operations on Aerospike STRING particle bins (SERVER-97 wire protocol).
 * Payload is a msgpack list: [ sub_op_code, ...args ] matching server
 * particle_string.c / proto.h.
 */

#include <aerospike/as_operations.h>

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types (match server proto.h)
//---------------------------------

typedef enum as_string_op_e {
	AS_STRING_OP_STRLEN        = 0,
	AS_STRING_OP_SUBSTR        = 1,
	AS_STRING_OP_CHAR_AT       = 2,
	AS_STRING_OP_FIND          = 3,
	AS_STRING_OP_CONTAINS      = 4,
	AS_STRING_OP_STARTS_WITH   = 5,
	AS_STRING_OP_ENDS_WITH     = 6,
	AS_STRING_OP_TO_INTEGER    = 7,
	AS_STRING_OP_TO_DOUBLE     = 8,
	AS_STRING_OP_BYTE_LENGTH   = 9,
	AS_STRING_OP_IS_NUMERIC    = 10,
	AS_STRING_OP_IS_UPPER      = 11,
	AS_STRING_OP_IS_LOWER      = 12,
	AS_STRING_OP_TO_BLOB       = 13,
	AS_STRING_OP_SPLIT         = 14,
	AS_STRING_OP_B64_DECODE    = 15,
	AS_STRING_OP_REGEX_COMPARE = 16,

	AS_STRING_OP_INSERT        = 50,
	AS_STRING_OP_OVERWRITE     = 51,
	AS_STRING_OP_CONCAT        = 52,
	AS_STRING_OP_SNIP          = 53,
	AS_STRING_OP_REPLACE       = 54,
	AS_STRING_OP_REPLACE_ALL   = 55,
	AS_STRING_OP_UPPER         = 56,
	AS_STRING_OP_LOWER         = 57,
	AS_STRING_OP_CASE_FOLD     = 58,
	AS_STRING_OP_NORMALIZE_NFC = 59,
	AS_STRING_OP_TRIM_START    = 60,
	AS_STRING_OP_TRIM_END      = 61,
	AS_STRING_OP_TRIM          = 62,
	AS_STRING_OP_PAD_START     = 63,
	AS_STRING_OP_PAD_END       = 64,
	AS_STRING_OP_REPEAT        = 65,
	AS_STRING_OP_REGEX_REPLACE = 66
} as_string_op;

typedef enum as_string_flags_e {
	AS_STRING_FLAG_DEFAULT  = 0,
	AS_STRING_FLAG_NO_FAIL  = 1 << 2
} as_string_flags;

typedef enum as_string_numeric_type_e {
	AS_STRING_NUMERIC_ANY   = 0,
	AS_STRING_NUMERIC_INT   = 1,
	AS_STRING_NUMERIC_FLOAT = 2
} as_string_numeric_type;

typedef enum as_string_regex_flags_e {
	AS_STRING_REGEX_CASE_INSENSITIVE = 1 << 0,
	AS_STRING_REGEX_MULTILINE        = 1 << 1,
	AS_STRING_REGEX_DOTALL           = 1 << 2,
	AS_STRING_REGEX_UNIX_LINES_ONLY  = 1 << 3,
	AS_STRING_REGEX_GLOBAL           = 1 << 4
} as_string_regex_flags;

//---------------------------------
// Top-level read: stringify any bin (AS_MSG_OP_TO_STRING)
//---------------------------------

bool
as_operations_add_to_string(as_operations* ops, const char* name);

//---------------------------------
// Read sub-operations
//---------------------------------

bool
as_operations_add_string_strlen(as_operations* ops, const char* name);

bool
as_operations_add_string_substr(
	as_operations* ops, const char* name, int64_t start, bool has_end,
	int64_t end
	);

bool
as_operations_add_string_char_at(
	as_operations* ops, const char* name, int64_t offset
	);

bool
as_operations_add_string_find(
	as_operations* ops, const char* name, const char* needle,
	bool has_occurrence, int64_t occurrence
	);

bool
as_operations_add_string_contains(
	as_operations* ops, const char* name, const char* needle
	);

bool
as_operations_add_string_starts_with(
	as_operations* ops, const char* name, const char* needle
	);

bool
as_operations_add_string_ends_with(
	as_operations* ops, const char* name, const char* needle
	);

bool
as_operations_add_string_to_integer(as_operations* ops, const char* name);

bool
as_operations_add_string_to_double(as_operations* ops, const char* name);

bool
as_operations_add_string_byte_length(as_operations* ops, const char* name);

bool
as_operations_add_string_is_numeric(
	as_operations* ops, const char* name, bool has_type,
	as_string_numeric_type numeric_type
	);

bool
as_operations_add_string_is_upper(as_operations* ops, const char* name);

bool
as_operations_add_string_is_lower(as_operations* ops, const char* name);

bool
as_operations_add_string_to_blob(as_operations* ops, const char* name);

bool
as_operations_add_string_split(
	as_operations* ops, const char* name, const char* separator,
	bool has_separator
	);

bool
as_operations_add_string_b64_decode(as_operations* ops, const char* name);

bool
as_operations_add_string_regex_compare(
	as_operations* ops, const char* name, const char* pattern,
	bool has_regex_flags, uint32_t regex_flags
	);

//---------------------------------
// Modify sub-operations
//---------------------------------

bool
as_operations_add_string_insert(
	as_operations* ops, const char* name, int64_t offset,
	const char* insert_str, as_string_flags flags
	);

bool
as_operations_add_string_overwrite(
	as_operations* ops, const char* name, int64_t offset,
	const char* new_str, as_string_flags flags
	);

bool
as_operations_add_string_concat(
	as_operations* ops, const char* name, as_string_flags flags,
	uint32_t n_str, const char* const* strs
	);

bool
as_operations_add_string_snip(
	as_operations* ops, const char* name, int64_t start, int64_t end,
	as_string_flags flags
	);

bool
as_operations_add_string_replace(
	as_operations* ops, const char* name, const char* old_str,
	const char* new_str, as_string_flags flags
	);

bool
as_operations_add_string_replace_all(
	as_operations* ops, const char* name, const char* old_str,
	const char* new_str, as_string_flags flags
	);

bool
as_operations_add_string_upper(
	as_operations* ops, const char* name, as_string_flags flags
	);

bool
as_operations_add_string_lower(
	as_operations* ops, const char* name, as_string_flags flags
	);

bool
as_operations_add_string_case_fold(
	as_operations* ops, const char* name, as_string_flags flags
	);

bool
as_operations_add_string_normalize_nfc(
	as_operations* ops, const char* name, as_string_flags flags
	);

bool
as_operations_add_string_trim_start(
	as_operations* ops, const char* name, as_string_flags flags
	);

bool
as_operations_add_string_trim_end(
	as_operations* ops, const char* name, as_string_flags flags
	);

bool
as_operations_add_string_trim(
	as_operations* ops, const char* name, as_string_flags flags
	);

bool
as_operations_add_string_pad_start(
	as_operations* ops, const char* name, int64_t target_length,
	const char* pad_str, as_string_flags flags
	);

bool
as_operations_add_string_pad_end(
	as_operations* ops, const char* name, int64_t target_length,
	const char* pad_str, as_string_flags flags
	);

bool
as_operations_add_string_repeat(
	as_operations* ops, const char* name, int64_t count,
	as_string_flags flags
	);

bool
as_operations_add_string_regex_replace(
	as_operations* ops, const char* name, const char* pattern,
	const char* replacement, uint32_t regex_flags, as_string_flags flags
	);

#ifdef __cplusplus
} // end extern "C"
#endif
