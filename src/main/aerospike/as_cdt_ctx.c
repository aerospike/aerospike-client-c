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
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_val.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_b64.h>
#include <aerospike/as_exp.h>

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
		if (AS_CDT_CTX_HAS_VAL(item->type)) {
			as_val_destroy(item->val.pval);
		}
		else if ((item->type & ~AS_CDT_CTX_AND) == AS_CDT_CTX_EXP) {
			cf_free(item->val.exp);
		}
	}
	as_vector_destroy(list);
}

void
as_cdt_ctx_add_all_children(as_cdt_ctx* ctx)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_EXP;
	as_exp* new_exp = cf_malloc(sizeof(as_exp) + 1);
	new_exp->packed_sz = 1;
	new_exp->packed[0] = 0xc3;
	item.val.exp = new_exp;
	as_vector_append(&ctx->list, &item);
}

void
as_cdt_ctx_add_all_children_with_filter(as_cdt_ctx* ctx, const as_exp* exp)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_EXP;
	as_exp* new_exp = cf_malloc(sizeof(as_exp) + exp->packed_sz);
	memcpy(new_exp, exp, sizeof(as_exp) + exp->packed_sz);
	item.val.exp = new_exp;
	as_vector_append(&ctx->list, &item);
}

void
as_cdt_ctx_add_and_filter(as_cdt_ctx* ctx, const as_exp* exp)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_AND | AS_CDT_CTX_EXP;
	as_exp* new_exp = cf_malloc(sizeof(as_exp) + exp->packed_sz);
	memcpy(new_exp, exp, sizeof(as_exp) + exp->packed_sz);
	item.val.exp = new_exp;
	as_vector_append(&ctx->list, &item);
}

static void
as_cdt_ctx_add_val(as_cdt_ctx* ctx, uint32_t type, as_val* operand)
{
	as_cdt_ctx_item item;
	item.type = type;
	item.val.pval = operand;
	as_vector_append(&ctx->list, &item);
}

static as_val*
as_cdt_ctx_int_pair(int64_t a, int64_t b)
{
	as_arraylist* list = as_arraylist_new(2, 0);
	as_arraylist_append_int64(list, a);
	as_arraylist_append_int64(list, b);
	return (as_val*)list;
}

static as_val*
as_cdt_ctx_val_pair(as_val* a, as_val* b)
{
	as_arraylist* list = as_arraylist_new(2, 0);
	as_arraylist_append(list, a);
	as_arraylist_append(list, b);
	return (as_val*)list;
}

void
as_cdt_ctx_add_list_index_range(as_cdt_ctx* ctx, int index, uint32_t count)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_LIST_INDEX_RANGE,
			as_cdt_ctx_int_pair(index, count));
}

void
as_cdt_ctx_add_list_rank_range(as_cdt_ctx* ctx, int rank, uint32_t count)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_LIST_RANK_RANGE,
			as_cdt_ctx_int_pair(rank, count));
}

void
as_cdt_ctx_add_list_value_list(as_cdt_ctx* ctx, as_list* values)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_LIST_VALUE_LIST, (as_val*)values);
}

void
as_cdt_ctx_add_list_value_interval(as_cdt_ctx* ctx, as_val* begin, as_val* end)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_LIST_VALUE_INTERVAL,
			as_cdt_ctx_val_pair(begin, end));
}

void
as_cdt_ctx_add_map_index_range(as_cdt_ctx* ctx, int index, uint32_t count)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_MAP_INDEX_RANGE,
			as_cdt_ctx_int_pair(index, count));
}

void
as_cdt_ctx_add_map_rank_range(as_cdt_ctx* ctx, int rank, uint32_t count)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_MAP_RANK_RANGE,
			as_cdt_ctx_int_pair(rank, count));
}

void
as_cdt_ctx_add_map_value_list(as_cdt_ctx* ctx, as_list* values)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_MAP_VALUE_LIST, (as_val*)values);
}

void
as_cdt_ctx_add_map_key_interval(as_cdt_ctx* ctx, as_val* begin, as_val* end)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_MAP_KEY_INTERVAL,
			as_cdt_ctx_val_pair(begin, end));
}

void
as_cdt_ctx_add_map_value_interval(as_cdt_ctx* ctx, as_val* begin, as_val* end)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_MAP_VALUE_INTERVAL,
			as_cdt_ctx_val_pair(begin, end));
}

static as_val*
as_cdt_ctx_rel_triple(as_val* pivot, int64_t start, int64_t count)
{
	as_arraylist* list = as_arraylist_new(3, 0);
	as_arraylist_append(list, pivot);
	as_arraylist_append_int64(list, start);
	as_arraylist_append_int64(list, count);
	return (as_val*)list;
}

void
as_cdt_ctx_add_list_value_rel_rank_range(as_cdt_ctx* ctx, as_val* value, int rank,
		uint32_t count)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_LIST_VALUE_REL_RANK_RANGE,
			as_cdt_ctx_rel_triple(value, rank, count));
}

void
as_cdt_ctx_add_map_key_rel_index_range(as_cdt_ctx* ctx, as_val* key, int index,
		uint32_t count)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_MAP_KEY_REL_INDEX_RANGE,
			as_cdt_ctx_rel_triple(key, index, count));
}

void
as_cdt_ctx_add_map_value_rel_rank_range(as_cdt_ctx* ctx, as_val* value, int rank,
		uint32_t count)
{
	as_cdt_ctx_add_val(ctx, AS_CDT_CTX_MAP_VALUE_REL_RANK_RANGE,
			as_cdt_ctx_rel_triple(value, rank, count));
}

void
as_cdt_ctx_invert_last(as_cdt_ctx* ctx)
{
	if (ctx->list.size > 0) {
		as_cdt_ctx_item* item = as_vector_get(&ctx->list, ctx->list.size - 1);
		item->type |= AS_CDT_CTX_INVERTED;
	}
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
	return as_cdt_ctx_from_unpacker(ctx, &pk);
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
