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
#include <aerospike/as_bit_operations.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_msgpack.h>
#include <citrusleaf/alloc.h>

//---------------------------------
// Macros
//---------------------------------

#define INT_FLAGS_SIGNED 1

//---------------------------------
// Static Functions
//---------------------------------

static inline void
as_bit_pack_policy(as_packer* pk, as_bit_policy* policy)
{
	as_pack_int64(pk, policy ? policy->flags : 0);
}

//---------------------------------
// Functions
//---------------------------------

bool
as_bit_write(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int offset, uint32_t size
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 3);
	as_pack_int64(&pk, offset);
	as_pack_uint64(&pk, size);
	as_bit_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_MODIFY);
}

bool
as_bit_shift(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int bit_offset, uint32_t bit_size, uint32_t shift
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 4);
	as_pack_int64(&pk, bit_offset);
	as_pack_uint64(&pk, bit_size);
	as_pack_uint64(&pk, shift);
	as_bit_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_MODIFY);
}

bool
as_bit_math(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int bit_offset, uint32_t bit_size, uint64_t value, bool sign,
	as_bit_overflow_action action
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 5);
	as_pack_int64(&pk, bit_offset);
	as_pack_uint64(&pk, bit_size);
	as_pack_uint64(&pk, value);
	as_bit_pack_policy(&pk, policy);

	uint64_t flags = (uint64_t)action;

	if (sign) {
		flags |= INT_FLAGS_SIGNED;
	}
	as_pack_uint64(&pk, flags);

	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_MODIFY);
}

bool
as_bit_byte_math(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int bit_offset, uint32_t bit_size, uint32_t value_size, uint8_t* value
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 4);
	as_pack_int64(&pk, bit_offset);
	as_pack_uint64(&pk, bit_size);
	as_pack_bytes(&pk, value, value_size);
	as_bit_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_MODIFY);
}

bool
as_operations_bit_resize(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint32_t byte_size, as_bit_resize_flags flags
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_BIT_OP_RESIZE, 3);
	as_pack_uint64(&pk, byte_size);
	as_bit_pack_policy(&pk, policy);
	as_pack_uint64(&pk, (uint64_t)flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_MODIFY);
}

bool
as_operations_bit_insert(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int byte_offset, uint32_t value_byte_size, uint8_t* value
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_BIT_OP_INSERT, 3);
	as_pack_int64(&pk, byte_offset);
	as_pack_bytes(&pk, value, value_byte_size);
	as_bit_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_MODIFY);
}

bool
as_operations_bit_set_int(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, int64_t value
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_BIT_OP_SET_INT, 4);
	as_pack_int64(&pk, bit_offset);
	as_pack_uint64(&pk, bit_size);
	as_pack_int64(&pk, value);
	as_bit_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_MODIFY);
}

bool
as_bit_read(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command, int bit_offset,
	uint32_t bit_size
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 2);
	as_pack_int64(&pk, bit_offset);
	as_pack_uint64(&pk, bit_size);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_READ);
}

bool
as_bit_scan(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command, int bit_offset,
	uint32_t bit_size, bool value
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 3);
	as_pack_int64(&pk, bit_offset);
	as_pack_uint64(&pk, bit_size);
	as_pack_bool(&pk, value);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_READ);
}

bool
as_operations_bit_get_int(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int bit_offset, uint32_t bit_size,
	bool sign
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_BIT_OP_GET_INT, sign ? 3 : 2);
	as_pack_int64(&pk, bit_offset);
	as_pack_uint64(&pk, bit_size);

	if (sign) {
		as_pack_uint64(&pk, INT_FLAGS_SIGNED);
	}
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_BIT_READ);
}
