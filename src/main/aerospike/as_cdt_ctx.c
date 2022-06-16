/*
 * Copyright 2008-2022 Aerospike, Inc.
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
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_val.h>
#include <citrusleaf/cf_b64.h>

uint32_t
as_cdt_ctx_pack(as_cdt_ctx* ctx, as_packer* pk)
{
	uint32_t start = pk->offset;

	if (as_pack_list_header(pk, ctx->list.size * 2) != 0) {
		return 0;
	}

	for (uint32_t j = 0; j < ctx->list.size; j++) {
		as_cdt_ctx_item* item = as_vector_get(&ctx->list, j);

		if (as_pack_uint64(pk, item->type) != 0) {
			return 0;
		}

		if (item->type & AS_CDT_CTX_VALUE) {
			if (as_pack_val(pk, item->val.pval) != 0) {
				return 0;
			}
		}
		else if (as_pack_int64(pk, item->val.ival) != 0) {
			return 0;
		}
	}

	return pk->offset - start;
}

void
as_cdt_ctx_destroy(as_cdt_ctx* ctx)
{
	as_vector* list = &ctx->list;

	for (uint32_t i = 0; i < list->size; i++) {
		as_cdt_ctx_item* item = as_vector_get(list, i);

		// Destroy ctx entries that contain an as_val.
		if (item->type & AS_CDT_CTX_VALUE) {
			as_val_destroy(item->val.pval);
		}
	}
	as_vector_destroy(list);
}
