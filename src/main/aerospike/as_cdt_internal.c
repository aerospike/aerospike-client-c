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
#include <aerospike/as_cdt_internal.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include "_bin.h"

as_binop*
as_binop_forappend(as_operations* ops, as_operator operator, const char* name);

void
as_cdt_pack_header(as_packer* pk, as_cdt_ctx* ctx, uint16_t command, uint32_t count)
{
	if (ctx) {
		as_cdt_pack_ctx(pk, ctx);
	}

	as_pack_list_header(pk, ++count);
	as_pack_uint64(pk, command);
}

void
as_cdt_pack_header_flag(as_packer* pk, as_cdt_ctx* ctx, uint16_t command, uint32_t count, uint32_t flag)
{
	as_pack_list_header(pk, 3);
	as_pack_uint64(pk, 0xff);
	as_pack_list_header(pk, ctx->list.size * 2);

	as_cdt_ctx_item* item;
	uint32_t last = ctx->list.size - 1;

	for (uint32_t i = 0; i < last; i++) {
		item = as_vector_get(&ctx->list, i);
		as_pack_uint64(pk, item->type);

		if (item->type & AS_CDT_CTX_VALUE) {
			as_pack_val(pk, item->val.pval);
		}
		else {
			as_pack_int64(pk, item->val.ival);
		}
	}

	item = as_vector_get(&ctx->list, last);
	as_pack_uint64(pk, item->type | flag);

	if (item->type & AS_CDT_CTX_VALUE) {
		as_pack_val(pk, item->val.pval);
	}
	else {
		as_pack_int64(pk, item->val.ival);
	}
	as_pack_list_header(pk, ++count);
	as_pack_uint64(pk, command);
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

uint32_t
as_cdt_ctx_pack(const as_cdt_ctx* ctx, as_packer* pk)
{
	uint32_t start = pk->offset;

	if (as_pack_list_header(pk, ctx->list.size * 2) != 0) {
		return 0;
	}

	as_vector* list = (as_vector*)&ctx->list;

	for (uint32_t j = 0; j < ctx->list.size; j++) {
		as_cdt_ctx_item* item = as_vector_get(list, j);

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

bool
as_cdt_add_packed(as_packer* pk, as_operations* ops, const char* name, as_operator op_type)
{
	as_bytes* bytes = as_bytes_new_wrap(pk->buffer, pk->offset, true);
	as_binop* binop = as_binop_forappend(ops, op_type, name);
	if (! binop) {
		return false;
	}
	as_bin_init(&binop->bin, name, (as_bin_value*)bytes);
	return true;
}

bool
as_cdt_ctx_from_unpacker(as_cdt_ctx* ctx, as_unpacker* pk)
{
	as_val* listval = NULL;
	int rv = as_unpack_val(pk, &listval);

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

bool
as_unpack_str_init(as_unpacker* pk, char* str, uint32_t max)
{
	uint32_t size;
	const uint8_t* p = as_unpack_str(pk, &size);

	if (!p || size >= max) {
		return false;
	}

	memcpy(str, p, size);
	*(str + size) = 0;
	return true;
}

bool
as_unpack_str_new(as_unpacker* pk, char** str, uint32_t max)
{
	uint32_t size;
	const uint8_t* p = as_unpack_str(pk, &size);

	if (!p || size >= max) {
		return false;
	}

	char* s = cf_malloc(size + 1);
	memcpy(s, p, size);
	*(s + size) = 0;
	*str = s;
	return true;
}

bool
as_unpack_bytes_init(as_unpacker* pk, uint8_t* b, uint32_t max)
{
	uint32_t size;
	const uint8_t* p = as_unpack_str(pk, &size);

	if (!p || size > max) {
		return false;
	}

	memcpy(b, p, size);
	return true;
}

bool
as_unpack_bytes_new(as_unpacker* pk, uint8_t** bytes, uint32_t* bytes_size, uint32_t max)
{
	uint32_t size;
	const uint8_t* p = as_unpack_str(pk, &size);

	if (!p || size > max) {
		return false;
	}

	uint8_t* b = cf_malloc(size);
	memcpy(b, p, size);
	*bytes = b;
	*bytes_size = size;
	return true;
}

bool
as_val_compare(as_val* v1, as_val* v2)
{
	char* s1 = as_val_tostring(v1);
	char* s2 = as_val_tostring(v2);
	bool rv = strcmp(s1, s2);
	cf_free(s1);
	cf_free(s2);
	return rv == 0;
}
