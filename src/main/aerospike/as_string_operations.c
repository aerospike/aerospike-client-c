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
#include <aerospike/as_bytes.h>
#include <aerospike/as_string_operations.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <citrusleaf/alloc.h>

#include "_bin.h"

extern as_binop*
as_binop_forappend(as_operations* ops, as_operator operator, const char* name);

static inline void
pack_modify_flags(as_packer* pk, as_string_flags flags)
{
	if (flags != AS_STRING_FLAG_DEFAULT) {
		as_pack_uint64(pk, (uint64_t)flags);
	}
}

// Server string_parse_buf / replace / concat expect msgpack string payload
// [AS_BYTES_STRING][utf8 bytes] (same as as_val string packing).
static inline void
pack_string_op_arg(as_packer* pk, const char* s)
{
	uint32_t len = (uint32_t)strlen(s);

	(void)as_pack_str_with_type(pk, AS_BYTES_STRING, (const uint8_t*)s, len);
}

static inline void
pack_two_string_list(as_packer* p, const char* a, const char* b)
{
	as_pack_list_header(p, 2);
	pack_string_op_arg(p, a);
	pack_string_op_arg(p, b);
}

bool
as_operations_add_to_string(as_operations* ops, const char* name)
{
	as_binop* binop = as_binop_forappend(ops, AS_OPERATOR_TO_STRING, name);

	if (! binop) {
		return false;
	}
	as_bin_init_nil(&binop->bin, name);
	return true;
}

bool
as_operations_add_string_strlen(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_STRLEN);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_substr(
	as_operations* ops, const char* name, int64_t start, bool has_end,
	int64_t end
	)
{
	uint32_t n = 2 + (has_end ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_SUBSTR);
	as_pack_int64(&pk, start);

	if (has_end) {
		as_pack_int64(&pk, end);
	}
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_char_at(
	as_operations* ops, const char* name, int64_t offset
	)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 2);
	as_pack_uint64(&pk, AS_STRING_OP_CHAR_AT);
	as_pack_int64(&pk, offset);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_find(
	as_operations* ops, const char* name, const char* needle,
	bool has_occurrence, int64_t occurrence
	)
{
	uint32_t n = 2 + (has_occurrence ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_FIND);
	pack_string_op_arg(&pk, needle);

	if (has_occurrence) {
		as_pack_int64(&pk, occurrence);
	}
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_contains(
	as_operations* ops, const char* name, const char* needle
	)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 2);
	as_pack_uint64(&pk, AS_STRING_OP_CONTAINS);
	pack_string_op_arg(&pk, needle);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_starts_with(
	as_operations* ops, const char* name, const char* needle
	)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 2);
	as_pack_uint64(&pk, AS_STRING_OP_STARTS_WITH);
	pack_string_op_arg(&pk, needle);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_ends_with(
	as_operations* ops, const char* name, const char* needle
	)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 2);
	as_pack_uint64(&pk, AS_STRING_OP_ENDS_WITH);
	pack_string_op_arg(&pk, needle);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_to_integer(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_TO_INTEGER);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_to_double(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_TO_DOUBLE);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_byte_length(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_BYTE_LENGTH);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_is_numeric(
	as_operations* ops, const char* name, bool has_type,
	as_string_numeric_type numeric_type
	)
{
	uint32_t n = 1 + (has_type ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_IS_NUMERIC);

	if (has_type) {
		as_pack_int64(&pk, (int64_t)numeric_type);
	}
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_is_upper(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_IS_UPPER);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_is_lower(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_IS_LOWER);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_to_blob(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_TO_BLOB);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_split(
	as_operations* ops, const char* name, const char* separator,
	bool has_separator
	)
{
	uint32_t n = 1 + (has_separator ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_SPLIT);

	if (has_separator) {
		pack_string_op_arg(&pk, separator);
	}
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_b64_decode(as_operations* ops, const char* name)
{
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, 1);
	as_pack_uint64(&pk, AS_STRING_OP_B64_DECODE);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_regex_compare(
	as_operations* ops, const char* name, const char* pattern,
	bool has_regex_flags, uint32_t regex_flags
	)
{
	uint32_t n = 2 + (has_regex_flags ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_REGEX_COMPARE);
	pack_string_op_arg(&pk, pattern);

	if (has_regex_flags) {
		as_pack_int64(&pk, (int64_t)regex_flags);
	}
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_add_string_insert(
	as_operations* ops, const char* name, int64_t offset,
	const char* insert_str, as_string_flags flags
	)
{
	uint32_t n = 3 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_INSERT);
	as_pack_int64(&pk, offset);
	pack_string_op_arg(&pk, insert_str);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_overwrite(
	as_operations* ops, const char* name, int64_t offset,
	const char* new_str, as_string_flags flags
	)
{
	uint32_t n = 3 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_OVERWRITE);
	as_pack_int64(&pk, offset);
	pack_string_op_arg(&pk, new_str);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_concat(
	as_operations* ops, const char* name, as_string_flags flags,
	uint32_t n_str, const char* const* strs
	)
{
	uint32_t n = 2 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_CONCAT);
	as_pack_list_header(&pk, n_str);

	for (uint32_t i = 0; i < n_str; i++) {
		pack_string_op_arg(&pk, strs[i]);
	}
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_snip(
	as_operations* ops, const char* name, int64_t start, int64_t end,
	as_string_flags flags
	)
{
	uint32_t n = 3 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_SNIP);
	as_pack_int64(&pk, start);
	as_pack_int64(&pk, end);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_replace(
	as_operations* ops, const char* name, const char* old_str,
	const char* new_str, as_string_flags flags
	)
{
	uint32_t n = 2 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_REPLACE);
	pack_two_string_list(&pk, old_str, new_str);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_replace_all(
	as_operations* ops, const char* name, const char* old_str,
	const char* new_str, as_string_flags flags
	)
{
	uint32_t n = 2 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_REPLACE_ALL);
	pack_two_string_list(&pk, old_str, new_str);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_upper(
	as_operations* ops, const char* name, as_string_flags flags
	)
{
	uint32_t n = 1 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_UPPER);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_lower(
	as_operations* ops, const char* name, as_string_flags flags
	)
{
	uint32_t n = 1 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_LOWER);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_case_fold(
	as_operations* ops, const char* name, as_string_flags flags
	)
{
	uint32_t n = 1 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_CASE_FOLD);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_normalize_nfc(
	as_operations* ops, const char* name, as_string_flags flags
	)
{
	uint32_t n = 1 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_NORMALIZE_NFC);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_trim_start(
	as_operations* ops, const char* name, as_string_flags flags
	)
{
	uint32_t n = 1 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_TRIM_START);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_trim_end(
	as_operations* ops, const char* name, as_string_flags flags
	)
{
	uint32_t n = 1 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_TRIM_END);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_trim(
	as_operations* ops, const char* name, as_string_flags flags
	)
{
	uint32_t n = 1 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_TRIM);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_pad_start(
	as_operations* ops, const char* name, int64_t target_length,
	const char* pad_str, as_string_flags flags
	)
{
	uint32_t n = 3 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_PAD_START);
	as_pack_int64(&pk, target_length);
	pack_string_op_arg(&pk, pad_str);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_pad_end(
	as_operations* ops, const char* name, int64_t target_length,
	const char* pad_str, as_string_flags flags
	)
{
	uint32_t n = 3 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_PAD_END);
	as_pack_int64(&pk, target_length);
	pack_string_op_arg(&pk, pad_str);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_repeat(
	as_operations* ops, const char* name, int64_t count,
	as_string_flags flags
	)
{
	uint32_t n = 2 + (flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_REPEAT);
	as_pack_int64(&pk, count);
	pack_modify_flags(&pk, flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_add_string_regex_replace(
	as_operations* ops, const char* name, const char* pattern,
	const char* replacement, uint32_t regex_flags, as_string_flags flags
	)
{
	bool ext = (regex_flags != 0 || flags != AS_STRING_FLAG_DEFAULT);
	uint32_t n = 2 + (ext ? 1 : 0) + (ext && flags != AS_STRING_FLAG_DEFAULT ? 1 : 0);
	as_packer pk = as_cdt_begin();
	as_pack_list_header(&pk, n);
	as_pack_uint64(&pk, AS_STRING_OP_REGEX_REPLACE);
	pack_two_string_list(&pk, pattern, replacement);

	if (ext) {
		as_pack_int64(&pk, (int64_t)regex_flags);

		if (flags != AS_STRING_FLAG_DEFAULT) {
			as_pack_uint64(&pk, (uint64_t)flags);
		}
	}
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}
