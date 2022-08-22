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
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_val.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_b64.h>

#define CTX_STACK_BUF_SIZE 1024
#define ctx_buff_init(_sz) (_sz > CTX_STACK_BUF_SIZE) ? (uint8_t*)cf_malloc(_sz) : (uint8_t*)alloca(_sz)
#define ctx_buff_free(_buf, _sz) if (_sz > CTX_STACK_BUF_SIZE) {cf_free(_buf);}

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

uint32_t
as_cdt_ctx_byte_capacity(const as_cdt_ctx* ctx)
{
	as_packer pk = {.buffer = NULL, .capacity = UINT32_MAX};

	if (as_cdt_ctx_pack(ctx, &pk) == 0) {
		return 0;
	}
	return pk.offset;
}

uint32_t
as_cdt_ctx_to_bytes(const as_cdt_ctx* ctx, uint8_t* bytes, uint32_t capacity)
{
	as_packer pk = {.buffer = bytes, .capacity = capacity};

	if (as_cdt_ctx_pack(ctx, &pk) == 0) {
		return 0;
	}
	return pk.offset;
}

bool
as_cdt_ctx_from_bytes(as_cdt_ctx* ctx, const uint8_t* bytes, uint32_t size)
{
	as_unpacker pk = {.buffer = bytes, .length = size};
	as_val* listval = NULL;
	int rv = as_unpack_val(&pk, &listval);

	if (rv != 0 || !listval) {
		return false;
	}

	if (listval->type != AS_LIST) {
		return false;
	}

	as_list* list = (as_list*)listval;
	uint32_t max = as_list_size(list);

	as_cdt_ctx_init(ctx, max / 2);

	as_cdt_ctx_item item;
	uint32_t i = 0;

	while (i < max) {
		as_val* vtype = as_list_get(list, i);

		if (vtype->type != AS_INTEGER) {
			goto HandleError;
		}

		item.type = (uint32_t)((as_integer*)vtype)->value;

		if (++i >= max) {
			goto HandleError;
		}

		as_val* v = as_list_get(list, i);

		if (item.type & AS_CDT_CTX_VALUE) {
			as_val_reserve(v);
			item.val.pval = v;
		}
		else {
			if (v->type != AS_INTEGER) {
				goto HandleError;
			}

			item.val.ival = ((as_integer*)v)->value;
		}

		as_vector_append(&ctx->list, &item);
		i++;
	}
	as_val_destroy(listval);
	return true;

HandleError:
	as_cdt_ctx_destroy(ctx);
	as_val_destroy(listval);
	return false;
}

uint32_t
as_cdt_ctx_base64_capacity(const as_cdt_ctx* ctx)
{
	as_packer pk = {.buffer = NULL, .capacity = UINT32_MAX};

	if (as_cdt_ctx_pack(ctx, &pk) == 0) {
		return 0;
	}
	return cf_b64_encoded_len(pk.offset) + 1;
}

bool
as_cdt_ctx_to_base64(const as_cdt_ctx* ctx, char* base64, uint32_t capacity)
{
	// base64 capacity will be larger than bytes capacity, so use base64 capacity for both.
	uint8_t* bytes = ctx_buff_init(capacity);
	as_packer pk = {.buffer = bytes, .capacity = capacity};

	if (as_cdt_ctx_pack(ctx, &pk) == 0) {
		ctx_buff_free(bytes, capacity);
		return false;
	}

	if (cf_b64_encoded_len(pk.offset) + 1 > capacity) {
		ctx_buff_free(bytes, capacity);
		return false;
	}

	cf_b64_encode(pk.buffer, pk.offset, base64);
	base64[capacity-1] = 0;
	ctx_buff_free(bytes, capacity);
	return true;
}

bool
as_cdt_ctx_from_base64(as_cdt_ctx* ctx, const char* base64)
{
	uint32_t len = (uint32_t)strlen(base64);
	uint32_t sz = cf_b64_decoded_buf_size(len);
	uint8_t* bytes = ctx_buff_init(sz);
	uint32_t bytes_size = 0;

	cf_b64_decode(base64, len, bytes, &bytes_size);
	bool rv = as_cdt_ctx_from_bytes(ctx, bytes, bytes_size);
	ctx_buff_free(bytes, sz);
	return rv;
}
