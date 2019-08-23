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
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_val.h>

void
as_cdt_ctx_destroy(as_cdt_ctx* ctx)
{
	as_vector* list = &ctx->list;

	for (uint32_t i = 0; i < list->size; i++) {
		as_cdt_ctx_item* item = (as_cdt_ctx_item*)as_vector_get(list, i);

		switch (item->type) {
			case AS_CDT_CTX_LIST_VALUE:
			case AS_CDT_CTX_MAP_KEY:
			case AS_CDT_CTX_MAP_VALUE:
				as_val_destroy(item->val.pval);
				break;

			default:
				break;
		}
	}
	as_vector_destroy(list);
}
