/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_cdt_internal.h>
#include <citrusleaf/cf_byte_order.h>
#include "_bin.h"

as_binop*
as_binop_forappend(as_operations* ops, as_operator operator, const as_bin_name name);

void
as_cdt_pack_header(as_packer* pk, as_cdt_ctx* ctx, uint16_t command, uint32_t count)
{
	if (ctx) {
		as_cdt_pack_ctx(pk, ctx);
		as_pack_list_header(pk, count + 1);
		as_pack_uint64(pk, command);
	}
	else {
		if (pk->buffer) {
			*(uint16_t*)pk->buffer = cf_swap_to_be16(command);
		}
		pk->offset = 2;

		if (count > 0) {
			as_pack_list_header(pk, count);
		}
	}
}

void
as_cdt_pack_ctx(as_packer* pk, as_cdt_ctx* ctx)
{
	as_pack_list_header(pk, 3);
	as_pack_uint64(pk, 0xff);
	as_pack_list_header(pk, ctx->list.size * 2);

	for (uint32_t i = 0; i < ctx->list.size; i++) {
		as_cdt_ctx_item* item = as_vector_get(&ctx->list, i);
		as_pack_uint64(pk, item->type);

		if (item->type & AS_CDT_CTX_VALUE) {
			as_pack_val(pk, item->val.pval);
		}
		else {
			as_pack_int64(pk, item->val.ival);
		}
	}
}

bool
as_cdt_add_packed(as_packer* pk, as_operations* ops, const as_bin_name name, as_operator op_type)
{
	as_bytes* bytes = as_bytes_new_wrap(pk->buffer, pk->offset, true);
	as_binop* binop = as_binop_forappend(ops, op_type, name);
	if (! binop) {
		return false;
	}
	as_bin_init(&binop->bin, name, (as_bin_value*)bytes);
	return true;
}
