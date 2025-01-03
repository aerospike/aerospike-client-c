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
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_msgpack.h>
#include <citrusleaf/alloc.h>

//---------------------------------
// Static Functions
//---------------------------------

static inline void
as_hll_pack_policy(as_packer* pk, as_hll_policy* policy)
{
	as_pack_int64(pk, policy ? policy->flags : 0);
}

//---------------------------------
// Functions
//---------------------------------

bool
as_operations_hll_init_mh(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	int index_bit_count, int mh_bit_count
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_HLL_OP_INIT, 3);
	as_pack_int64(&pk, index_bit_count);
	as_pack_int64(&pk, mh_bit_count);
	as_hll_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_add_mh(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list, int index_bit_count, int mh_bit_count
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_HLL_OP_ADD, 4);
	as_pack_val(&pk, (as_val*)list);
	as_pack_int64(&pk, index_bit_count);
	as_pack_int64(&pk, mh_bit_count);
	as_hll_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_set_union(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_HLL_OP_UNION, 2);
	as_pack_val(&pk, (as_val*)list);
	as_hll_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_refresh_count(
	as_operations* ops, const char* name, as_cdt_ctx* ctx
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_HLL_OP_REFRESH_COUNT, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_fold(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int index_bit_count
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, AS_HLL_OP_FOLD, 1);
	as_pack_int64(&pk, index_bit_count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_read(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_READ);
}

bool
as_operations_hll_read_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command, as_list* list
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, 1);
	as_pack_val(&pk, (as_val*)list);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_READ);
}
