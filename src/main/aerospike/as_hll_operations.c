/*
 * Copyright 2008-2020 Aerospike, Inc.
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
#include <citrusleaf/alloc.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define INIT 0
#define ADD 1
#define SET_UNION 2
#define SET_COUNT 3
#define FOLD 4

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static inline void
as_hll_pack_header(as_packer* pk, as_cdt_ctx* ctx, uint16_t command, uint32_t count)
{
	if (ctx) {
		as_cdt_pack_ctx(pk, ctx);
	}

	as_pack_list_header(pk, ++count);
	as_pack_uint64(pk, command);
}


static inline void
as_hll_pack_policy(as_packer* pk, as_hll_policy* policy)
{
	as_pack_int64(pk, policy ? policy->flags : 0);
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

bool
as_operations_hll_init_mh(
	as_operations* ops, const as_bin_name name, as_cdt_ctx* ctx, as_hll_policy* policy,
	int index_bit_count, int mh_bit_count
	)
{
	as_packer pk = as_cdt_begin();
	as_hll_pack_header(&pk, ctx, INIT, 3);
	as_pack_int64(&pk, index_bit_count);
	as_pack_int64(&pk, mh_bit_count);
	as_hll_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_add_mh(
	as_operations* ops, const as_bin_name name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list, int index_bit_count, int mh_bit_count
	)
{
	as_packer pk = as_cdt_begin();
	as_hll_pack_header(&pk, ctx, ADD, 4);
	as_pack_val(&pk, (as_val*)list);
	as_pack_int64(&pk, index_bit_count);
	as_pack_int64(&pk, mh_bit_count);
	as_hll_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_set_union(
	as_operations* ops, const as_bin_name name, as_cdt_ctx* ctx, as_hll_policy* policy,
	as_list* list
	)
{
	as_packer pk = as_cdt_begin();
	as_hll_pack_header(&pk, ctx, SET_UNION, 2);
	as_pack_val(&pk, (as_val*)list);
	as_hll_pack_policy(&pk, policy);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_refresh_count(
	as_operations* ops, const as_bin_name name, as_cdt_ctx* ctx
	)
{
	as_packer pk = as_cdt_begin();
	as_hll_pack_header(&pk, ctx, SET_COUNT, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_fold(
	as_operations* ops, const as_bin_name name, as_cdt_ctx* ctx, int index_bit_count
	)
{
	as_packer pk = as_cdt_begin();
	as_hll_pack_header(&pk, ctx, FOLD, 1);
	as_pack_int64(&pk, index_bit_count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_MODIFY);
}

bool
as_operations_hll_read(
	as_operations* ops, const as_bin_name name, as_cdt_ctx* ctx, uint16_t command
	)
{
	as_packer pk = as_cdt_begin();
	as_hll_pack_header(&pk, ctx, command, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_READ);
}

bool
as_operations_hll_read_list(
	as_operations* ops, const as_bin_name name, as_cdt_ctx* ctx, uint16_t command, as_list* list
	)
{
	as_packer pk = as_cdt_begin();
	as_hll_pack_header(&pk, ctx, command, 1);
	as_pack_val(&pk, (as_val*)list);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_HLL_READ);
}
