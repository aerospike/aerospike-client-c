/*
 * Copyright 2021-2023 Aerospike, Inc.
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
#include <aerospike/as_exp.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_cdt_internal.h>
#include <aerospike/as_command.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_msgpack.h>
#include <citrusleaf/cf_b64.h>

typedef enum {
	CALL_CDT = 0,
	CALL_BITS = 1,
	CALL_HLL = 2
} call_system_type;

#define AS_CDT_OP_CONTEXT_EVAL 0xff

as_exp*
as_exp_compile(as_exp_entry* table, uint32_t n)
{
	uint32_t total_sz = 0;
	as_serializer s;
	int32_t prev_va_args = -1;

	as_msgpack_init(&s);

	for (uint32_t i = 0; i < n; i++) {
		as_exp_entry* entry = &table[i];
		uint32_t sz;

		if (prev_va_args != -1) {
			table[prev_va_args].count++;
		}

		if (entry->count != 0) {
			total_sz += as_pack_list_header_get_size(entry->count);

			if (prev_va_args != -1) {
				table[prev_va_args].count -= entry->count - 1;
			}
		}

		switch (entry->op) {
		case _AS_EXP_CODE_CDT_LIST_CRMOD:
			if (entry->v.list_pol != NULL) {
				if (prev_va_args != -1) {
					table[prev_va_args].count++;
				}

				total_sz += as_pack_int64_size((uint64_t)entry->v.list_pol->order);
				total_sz += as_pack_int64_size((uint64_t)entry->v.list_pol->flags);
			}
			else { // no-op case
				if (prev_va_args != -1) {
					table[prev_va_args].count--;
				}
			}

			break;
		case _AS_EXP_CODE_CDT_LIST_MOD:
			if (entry->v.list_pol != NULL) {
				total_sz += as_pack_int64_size((uint64_t)entry->v.list_pol->flags);
			}
			else { // no-op case
				if (prev_va_args != -1) {
					table[prev_va_args].count--;
				}
			}

			break;
		case _AS_EXP_CODE_CDT_MAP_CRMOD:
			if (entry->v.map_pol != NULL) {
				if (prev_va_args != -1) {
					table[prev_va_args].count++;
				}

				total_sz += as_pack_int64_size((uint64_t)entry->v.map_pol->attributes);
				total_sz += as_pack_int64_size((uint64_t)entry->v.map_pol->flags);
			}
			else { // no-op case
				if (prev_va_args != -1) {
					table[prev_va_args].count--;
				}
			}

			break;
		case _AS_EXP_CODE_CDT_MAP_CR:
			if (entry->v.map_pol != NULL) {
				total_sz += as_pack_int64_size((uint64_t)entry->v.map_pol->attributes);
			}
			else { // no-op case
				if (prev_va_args != -1) {
					table[prev_va_args].count--;
				}
			}

			break;
		case _AS_EXP_CODE_CDT_MAP_MOD:
			if (entry->v.map_pol != NULL) {
				total_sz += as_pack_int64_size((uint64_t)entry->v.map_pol->flags);
			}
			else { // no-op case
				if (prev_va_args != -1) {
					table[prev_va_args].count--;
				}
			}

			break;
		case _AS_EXP_CODE_AS_VAL:
			if (as_val_type(entry->v.val) == AS_LIST) {
				total_sz += as_pack_list_header_get_size(2);
				total_sz += as_pack_int64_size(_AS_EXP_CODE_QUOTE);
			}

			if ((sz = as_serializer_serialize_getsize(&s, entry->v.val)) == 0) {
				return NULL;
			}

			total_sz += sz;
			break;
		case _AS_EXP_CODE_VAL_GEO:
			if ((sz = as_serializer_serialize_getsize(&s, entry->v.val)) == 0) {
				return NULL;
			}

			total_sz += sz;
			break;
		case _AS_EXP_CODE_VAL_RTYPE:
			if (entry->v.int_val == AS_EXP_TYPE_ERROR) {
				return NULL;
			}
			// no break
		case _AS_EXP_CODE_VAL_INT:
			total_sz += as_pack_int64_size(entry->v.int_val);
			break;
		case _AS_EXP_CODE_VAL_UINT:
			total_sz += as_pack_uint64_size(entry->v.uint_val);
			break;
		case _AS_EXP_CODE_VAL_FLOAT:
			total_sz += as_pack_double_size();
			break;
		case _AS_EXP_CODE_VAL_BOOL:
			total_sz += as_pack_bool_size();
			break;
		case _AS_EXP_CODE_VAL_STR:
			entry->sz = (uint32_t)strlen(entry->v.str_val);
			total_sz += as_pack_str_size(entry->sz + 1); // +1 for AS_BYTES type
			break;
		case _AS_EXP_CODE_VAL_BYTES:
			total_sz += as_pack_str_size(entry->sz + 1); // +1 for AS_BYTES type
			break;
		case _AS_EXP_CODE_VAL_RAWSTR:
			entry->sz = (uint32_t)strlen(entry->v.str_val);
			total_sz += as_pack_str_size(entry->sz);
			break;
		case _AS_EXP_CODE_END_OF_VA_ARGS:
			if (prev_va_args == -1) {
				return NULL;
			}

			entry = &table[prev_va_args];
			total_sz += as_pack_list_header_get_size(entry->count);
			prev_va_args = entry->prev_va_args;

			break;
		case _AS_EXP_CODE_CALL_VOP_START:
			if (prev_va_args != -1) {
				table[prev_va_args].count--;
			}

			if (entry->v.ctx != NULL) {
				if (entry->v.ctx->list.size == 0) {
					return NULL;
				}

				if (i == 0 || table[i - 1].op != _AS_EXP_CODE_VAL_INT ||
						(table[i - 1].v.int_val &
								~_AS_EXP_SYS_FLAG_MODIFY_LOCAL) !=
										_AS_EXP_SYS_CALL_CDT) {
					return NULL;
				}

				total_sz += as_pack_list_header_get_size(3);
				total_sz += as_pack_int64_size(AS_CDT_OP_CONTEXT_EVAL);

				as_packer pk = {.buffer = NULL, .capacity = UINT32_MAX};

				sz = as_cdt_ctx_pack(entry->v.ctx, &pk);

				if (sz == 0) {
					return NULL;
				}

				total_sz += sz;
			}

			break;
		case _AS_EXP_CODE_MERGE:
			total_sz += entry->v.expr->packed_sz;
			break;
		case _AS_EXP_CODE_COND:
		case _AS_EXP_CODE_LET:
		case _AS_EXP_CODE_AND:
		case _AS_EXP_CODE_OR:
		case _AS_EXP_CODE_EXCLUSIVE:
		case _AS_EXP_CODE_ADD:
		case _AS_EXP_CODE_SUB:
		case _AS_EXP_CODE_MUL:
		case _AS_EXP_CODE_DIV:
		case _AS_EXP_CODE_INT_AND:
		case _AS_EXP_CODE_INT_OR:
		case _AS_EXP_CODE_INT_XOR:
		case _AS_EXP_CODE_MIN:
		case _AS_EXP_CODE_MAX:
			entry->count = 0;
			entry->prev_va_args = prev_va_args;
			prev_va_args = i;
			// no break
		default:
			total_sz += as_pack_int64_size((int64_t)entry->op);
			break;
		}
	}

	as_exp* p2 = cf_malloc(sizeof(as_exp) + total_sz);

	p2->packed_sz = total_sz;

	as_packer pk = {
			.buffer = p2->packed,
			.capacity = total_sz
	};

	for (uint32_t i = 0; i < n; i++) {
		as_exp_entry* entry = &table[i];

		if (entry->count != 0 && entry->op != _AS_EXP_CODE_CALL_VOP_START) {
			as_pack_list_header(&pk, entry->count);
		}

		switch (entry->op) {
		case _AS_EXP_CODE_CDT_LIST_CRMOD:
			if (entry->v.list_pol != NULL) {
				as_pack_uint64(&pk, (uint64_t)entry->v.list_pol->order);
				as_pack_uint64(&pk, (uint64_t)entry->v.list_pol->flags);
			}

			break;
		case _AS_EXP_CODE_CDT_LIST_MOD:
			if (entry->v.list_pol != NULL) {
				as_pack_uint64(&pk, (uint64_t)entry->v.list_pol->flags);
			}

			break;
		case _AS_EXP_CODE_CDT_MAP_CRMOD:
			if (entry->v.map_pol != NULL) {
				as_pack_uint64(&pk, (uint64_t)entry->v.map_pol->attributes);
				as_pack_uint64(&pk, (uint64_t)entry->v.map_pol->flags);
			}

			break;
		case _AS_EXP_CODE_CDT_MAP_CR:
			if (entry->v.map_pol != NULL) {
				as_pack_uint64(&pk, (uint64_t)entry->v.map_pol->attributes);
			}

			break;
		case _AS_EXP_CODE_CDT_MAP_MOD:
			if (entry->v.map_pol != NULL) {
				as_pack_uint64(&pk, (uint64_t)entry->v.map_pol->flags);
			}

			break;
		case _AS_EXP_CODE_AS_VAL:
			if (as_val_type(entry->v.val) == AS_LIST) {
				as_pack_list_header(&pk, 2);
				as_pack_int64(&pk, _AS_EXP_CODE_QUOTE);
			}

			as_pack_val(&pk, entry->v.val);
			// Do not destroy value because it was created externally.
			break;
		case _AS_EXP_CODE_VAL_GEO:
			as_pack_val(&pk, entry->v.val);
			// Destroy geo value because it was created internally.
			as_val_destroy(entry->v.val);
			break;
		case _AS_EXP_CODE_VAL_RTYPE:
		case _AS_EXP_CODE_VAL_INT:
			as_pack_int64(&pk, entry->v.int_val);
			break;
		case _AS_EXP_CODE_VAL_UINT:
			as_pack_uint64(&pk, entry->v.uint_val);
			break;
		case _AS_EXP_CODE_VAL_FLOAT:
			as_pack_double(&pk, entry->v.float_val);
			break;
		case _AS_EXP_CODE_VAL_BOOL:
			as_pack_bool(&pk, entry->v.bool_val);
			break;
		case _AS_EXP_CODE_VAL_STR: {
			as_string temp;
			as_string_init_wlen(&temp, (char*)entry->v.str_val, entry->sz, false);
			as_pack_val(&pk, (const as_val*)&temp);
			break;
		}
		case _AS_EXP_CODE_VAL_BYTES: {
			as_bytes temp;
			as_bytes_init_wrap(&temp, entry->v.bytes_val, entry->sz, false);
			as_pack_val(&pk, (const as_val*)&temp);
			break;
		}
		case _AS_EXP_CODE_VAL_RAWSTR:
			as_pack_str(&pk, (const uint8_t*)entry->v.str_val, entry->sz);
			break;
		case _AS_EXP_CODE_END_OF_VA_ARGS:
			break;
		case _AS_EXP_CODE_CALL_VOP_START:
			if (entry->v.ctx != NULL) {
				as_pack_list_header(&pk, 3);
				as_pack_int64(&pk, AS_CDT_OP_CONTEXT_EVAL);
				as_cdt_ctx_pack(entry->v.ctx, &pk);
			}

			as_pack_list_header(&pk, entry->count);
			break;
		case _AS_EXP_CODE_MERGE: {
			as_exp* e = entry->v.expr;
			as_pack_append(&pk, e->packed, e->packed_sz);
			break;
		}
		default:
			as_pack_int64(&pk, (int64_t)entry->op);
			break;
		}
	}

	return p2;
}

char*
as_exp_compile_b64(as_exp* exp)
{
	if (exp == NULL) {
		return NULL;
	}

	uint32_t sz = cf_b64_encoded_len(exp->packed_sz);
	char* b64 = cf_malloc(sz + 1);

	cf_b64_encode(exp->packed, exp->packed_sz, b64);
	b64[sz] = 0;
	return b64;
}

as_exp*
as_exp_from_base64(const char* base64)
{
	uint32_t base64_len = (uint32_t)strlen(base64);
	uint32_t sz = cf_b64_decoded_buf_size(base64_len);
	as_exp* exp = cf_malloc(sizeof(as_exp) + sz);

	cf_b64_decode(base64, base64_len, exp->packed, &exp->packed_sz);
	return exp;
}

void
as_exp_destroy(as_exp* exp)
{
	cf_free(exp);
}

void
as_exp_destroy_b64(char* b64)
{
	cf_free(b64);
}

uint8_t*
as_exp_write(as_exp* exp, uint8_t* ptr)
{
	ptr = as_command_write_field_header(ptr, AS_FIELD_FILTER, exp->packed_sz);
	memcpy(ptr, exp->packed, exp->packed_sz);
	return ptr += exp->packed_sz;
}

int64_t
as_exp_get_ctx_type(const as_cdt_ctx* ctx, as_exp_type default_type)
{
	if (ctx == NULL || ctx->list.size == 0) {
		return default_type;
	}

	as_cdt_ctx_item* item = as_vector_get((as_vector*)&ctx->list, 0);

	return ((item->type & 0x10) == 0) ?
			AS_EXP_TYPE_MAP : AS_EXP_TYPE_LIST;
}

int64_t
as_exp_get_list_type(as_exp_type type, as_list_return_type rtype, bool is_multi)
{
	as_exp_type expected_type = type;

	switch (rtype & ~AS_LIST_RETURN_INVERTED) {
	case AS_LIST_RETURN_INDEX:
	case AS_LIST_RETURN_REVERSE_INDEX:
	case AS_LIST_RETURN_RANK:
	case AS_LIST_RETURN_REVERSE_RANK:
		expected_type = (is_multi ? AS_EXP_TYPE_LIST : AS_EXP_TYPE_INT);
		break;
	case AS_LIST_RETURN_COUNT:
		expected_type = AS_EXP_TYPE_INT;
		break;
	case AS_LIST_RETURN_VALUE:
		if (is_multi) {
			expected_type = AS_EXP_TYPE_LIST;
		}
		break;
	case AS_LIST_RETURN_EXISTS:
		expected_type = AS_EXP_TYPE_BOOL;
		break;
	case AS_LIST_RETURN_NONE:
	default:
		return AS_EXP_TYPE_ERROR;
	}

	if (type == AS_EXP_TYPE_AUTO || type == expected_type) {
		return expected_type;
	}

	return AS_EXP_TYPE_ERROR;
}

int64_t
as_exp_get_map_type(as_exp_type type, as_map_return_type rtype, bool is_multi)
{
	as_exp_type expected_type = type;

	switch (rtype & ~AS_MAP_RETURN_INVERTED) {
	case AS_MAP_RETURN_INDEX:
	case AS_MAP_RETURN_REVERSE_INDEX:
	case AS_MAP_RETURN_RANK:
	case AS_MAP_RETURN_REVERSE_RANK:
		expected_type = (is_multi ? AS_EXP_TYPE_LIST : AS_EXP_TYPE_INT);
		break;
	case AS_MAP_RETURN_COUNT:
		expected_type = AS_EXP_TYPE_INT;
		break;
	case AS_MAP_RETURN_KEY:
	case AS_MAP_RETURN_VALUE:
		if (is_multi) {
			expected_type = AS_EXP_TYPE_LIST;
		}
		break;
	case AS_MAP_RETURN_KEY_VALUE:
	case AS_MAP_RETURN_UNORDERED_MAP:
	case AS_MAP_RETURN_ORDERED_MAP:
		expected_type = AS_EXP_TYPE_MAP;
		break;
	case AS_MAP_RETURN_EXISTS:
		expected_type = AS_EXP_TYPE_BOOL;
		break;
	case AS_MAP_RETURN_NONE:
	default:
		return AS_EXP_TYPE_ERROR;
	}

	if (type == AS_EXP_TYPE_AUTO || type == expected_type) {
		return expected_type;
	}

	return AS_EXP_TYPE_ERROR;
}
