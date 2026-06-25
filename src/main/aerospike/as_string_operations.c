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
#include <aerospike/as_string_operations.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_msgpack.h>
#include <citrusleaf/alloc.h>

#include "_bin.h"

as_binop*
as_binop_forappend(as_operations* ops, as_operator operator, const char* name);

//---------------------------------
// Static Functions
//---------------------------------

static inline uint64_t
as_string_policy_flags(const as_string_policy* policy)
{
	return policy ? policy->flags : AS_STRING_WRITE_FLAGS_DEFAULT;
}

static inline bool
as_string_has_ctx(const as_cdt_ctx* ctx)
{
	return ctx && ctx->list.size > 0;
}

static void
as_string_pack_header(as_packer* pk, as_cdt_ctx* ctx, uint16_t command, uint32_t count)
{
	bool has_ctx = as_string_has_ctx(ctx);

	as_pack_list_header(pk, count + 1 + (has_ctx ? 2 : 0));

	if (has_ctx) {
		as_pack_uint64(pk, 0xff);
		as_cdt_ctx_pack(ctx, pk);
	}

	as_pack_uint64(pk, command);
}

static void
as_string_pack_value(as_packer* pk, const char* value)
{
	as_pack_str_with_type(pk, AS_BYTES_STRING, (const uint8_t*)value, (uint32_t)strlen(value));
}

static inline bool
as_string_arg_not_null(const char* value)
{
	return value != NULL;
}

static inline bool
as_string_args_not_null2(const char* value1, const char* value2)
{
	return value1 != NULL && value2 != NULL;
}

static void
as_string_pack_pair(as_packer* pk, const char* first, const char* second)
{
	as_pack_list_header(pk, 2);
	as_string_pack_value(pk, first);
	as_string_pack_value(pk, second);
}

static void
as_string_pack_one_value_list(as_packer* pk, const char* value)
{
	as_pack_list_header(pk, 1);
	as_string_pack_value(pk, value);
}

static bool
as_string_read0(as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, command, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

static bool
as_string_modify0(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	uint16_t command
	)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, command, 1);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

//---------------------------------
// String Functions
//---------------------------------

void
as_string_policy_init(as_string_policy* policy)
{
	policy->flags = AS_STRING_WRITE_FLAGS_DEFAULT;
}

void
as_string_policy_set(as_string_policy* policy, as_string_write_flags flags)
{
	policy->flags = flags;
}

//---------------------------------
// String Read Operations
//---------------------------------

bool
as_operations_string_strlen(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_STRLEN);
}

bool
as_operations_string_substr(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t start)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_SUBSTR, 1);
	as_pack_int64(&pk, start);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_substr_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t start, int64_t end
	)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_SUBSTR, 2);
	as_pack_int64(&pk, start);
	as_pack_int64(&pk, end);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_char_at(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_CHAR_AT, 1);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_find(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* needle)
{
	if (! as_string_arg_not_null(needle)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_FIND, 1);
	as_string_pack_value(&pk, needle);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_find_occurrence(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* needle, int64_t occurrence
	)
{
	if (! as_string_arg_not_null(needle)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_FIND, 2);
	as_string_pack_value(&pk, needle);
	as_pack_int64(&pk, occurrence);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_contains(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* needle)
{
	if (! as_string_arg_not_null(needle)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_CONTAINS, 1);
	as_string_pack_value(&pk, needle);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_starts_with(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* prefix)
{
	if (! as_string_arg_not_null(prefix)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_STARTS_WITH, 1);
	as_string_pack_value(&pk, prefix);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_ends_with(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* suffix)
{
	if (! as_string_arg_not_null(suffix)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_ENDS_WITH, 1);
	as_string_pack_value(&pk, suffix);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_to_integer(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_TO_INTEGER);
}

bool
as_operations_string_to_double(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_TO_DOUBLE);
}

bool
as_operations_string_byte_length(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_BYTE_LENGTH);
}

bool
as_operations_string_is_numeric(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_IS_NUMERIC);
}

bool
as_operations_string_is_numeric_type(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_numeric_type numeric_type
	)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_IS_NUMERIC, 1);
	as_pack_uint64(&pk, (uint64_t)numeric_type);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_is_upper(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_IS_UPPER);
}

bool
as_operations_string_is_lower(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_IS_LOWER);
}

bool
as_operations_string_to_blob(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_TO_BLOB);
}

bool
as_operations_string_split(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_SPLIT);
}

bool
as_operations_string_split_separator(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* separator)
{
	if (! as_string_arg_not_null(separator)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_SPLIT, 1);
	as_string_pack_value(&pk, separator);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_b64_decode(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	return as_string_read0(ops, name, ctx, AS_STRING_OP_B64_DECODE);
}

bool
as_operations_string_regex_compare(as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* pattern)
{
	if (! as_string_arg_not_null(pattern)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_REGEX_COMPARE, 1);
	as_string_pack_value(&pk, pattern);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

bool
as_operations_string_regex_compare_flags(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, const char* pattern, as_string_regex_flags flags
	)
{
	if (! as_string_arg_not_null(pattern)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_REGEX_COMPARE, 2);
	as_string_pack_value(&pk, pattern);
	as_pack_uint64(&pk, (uint64_t)flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_READ);
}

//---------------------------------
// String Modify Operations
//---------------------------------

bool
as_operations_string_insert(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	int64_t index, const char* value
	)
{
	if (! as_string_arg_not_null(value)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_INSERT, 3);
	as_pack_int64(&pk, index);
	as_string_pack_value(&pk, value);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_overwrite(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	int64_t index, const char* value
	)
{
	if (! as_string_arg_not_null(value)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_OVERWRITE, 3);
	as_pack_int64(&pk, index);
	as_string_pack_value(&pk, value);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_concat(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, const char* value
	)
{
	if (! as_string_arg_not_null(value)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_CONCAT, 2);
	as_string_pack_one_value_list(&pk, value);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_concat_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, as_list* values
	)
{
	if (! values) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_CONCAT, 2);
	as_pack_val(&pk, (as_val*)values);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	as_list_destroy(values);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_append(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, const char* value
	)
{
	if (! as_string_arg_not_null(value)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_APPEND, 2);
	as_string_pack_value(&pk, value);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_prepend(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, const char* value
	)
{
	if (! as_string_arg_not_null(value)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_PREPEND, 2);
	as_string_pack_value(&pk, value);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_snip(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	int64_t start, int64_t end
	)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_SNIP, 3);
	as_pack_int64(&pk, start);
	as_pack_int64(&pk, end);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_replace(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	const char* needle, const char* replacement
	)
{
	if (! as_string_args_not_null2(needle, replacement)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_REPLACE, 2);
	as_string_pack_pair(&pk, needle, replacement);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_replace_all(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	const char* needle, const char* replacement
	)
{
	if (! as_string_args_not_null2(needle, replacement)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_REPLACE_ALL, 2);
	as_string_pack_pair(&pk, needle, replacement);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_upper(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy)
{
	return as_string_modify0(ops, name, ctx, policy, AS_STRING_OP_UPPER);
}

bool
as_operations_string_lower(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy)
{
	return as_string_modify0(ops, name, ctx, policy, AS_STRING_OP_LOWER);
}

bool
as_operations_string_case_fold(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy)
{
	return as_string_modify0(ops, name, ctx, policy, AS_STRING_OP_CASE_FOLD);
}

bool
as_operations_string_normalize_nfc(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy)
{
	return as_string_modify0(ops, name, ctx, policy, AS_STRING_OP_NORMALIZE_NFC);
}

bool
as_operations_string_trim_start(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy)
{
	return as_string_modify0(ops, name, ctx, policy, AS_STRING_OP_TRIM_START);
}

bool
as_operations_string_trim_end(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy)
{
	return as_string_modify0(ops, name, ctx, policy, AS_STRING_OP_TRIM_END);
}

bool
as_operations_string_trim(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy)
{
	return as_string_modify0(ops, name, ctx, policy, AS_STRING_OP_TRIM);
}

bool
as_operations_string_pad_start(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	uint64_t target_length, const char* pad_string
	)
{
	if (! as_string_arg_not_null(pad_string)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_PAD_START, 3);
	as_pack_uint64(&pk, target_length);
	as_string_pack_value(&pk, pad_string);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_pad_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy,
	uint64_t target_length, const char* pad_string
	)
{
	if (! as_string_arg_not_null(pad_string)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_PAD_END, 3);
	as_pack_uint64(&pk, target_length);
	as_string_pack_value(&pk, pad_string);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_repeat(as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, uint64_t count)
{
	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_REPEAT, 2);
	as_pack_uint64(&pk, count);
	as_pack_uint64(&pk, as_string_policy_flags(policy));
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

bool
as_operations_string_regex_replace(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_string_policy* policy, const char* pattern,
	const char* replacement, as_string_regex_flags flags
	)
{
	(void)policy;

	if (! as_string_args_not_null2(pattern, replacement)) {
		return false;
	}

	as_packer pk = as_cdt_begin();
	as_string_pack_header(&pk, ctx, AS_STRING_OP_REGEX_REPLACE, 2);
	as_string_pack_pair(&pk, pattern, replacement);
	as_pack_uint64(&pk, (uint64_t)flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_STRING_MODIFY);
}

//---------------------------------
// String Type Conversion
//---------------------------------

bool
as_operations_to_string(as_operations* ops, const char* name)
{
	as_binop* binop = as_binop_forappend(ops, AS_OPERATOR_TO_STRING, name);

	if (! binop) {
		return false;
	}

	as_bin_init_nil(&binop->bin, name);
	return true;
}
