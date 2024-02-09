/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_map_operations.h>
#include <aerospike/as_cdt_internal.h>
#include <citrusleaf/alloc.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define SET_TYPE 64
#define ADD 65
#define ADD_ITEMS 66
#define PUT 67
#define PUT_ITEMS 68
#define REPLACE 69
#define REPLACE_ITEMS 70
#define INCREMENT 73
#define DECREMENT 74
#define CLEAR 75
#define REMOVE_BY_KEY 76
#define REMOVE_BY_INDEX 77
#define REMOVE_BY_RANK 79
#define REMOVE_BY_KEY_LIST 81
#define REMOVE_ALL_BY_VALUE 82
#define REMOVE_BY_VALUE_LIST 83
#define REMOVE_BY_KEY_INTERVAL 84
#define REMOVE_BY_INDEX_RANGE 85
#define REMOVE_BY_VALUE_INTERVAL 86
#define REMOVE_BY_RANK_RANGE 87
#define REMOVE_BY_KEY_REL_INDEX_RANGE 88
#define REMOVE_BY_VALUE_REL_RANK_RANGE 89
#define SIZE 96
#define GET_BY_KEY 97
#define GET_BY_INDEX 98
#define GET_BY_RANK 100
#define GET_ALL_BY_VALUE 102
#define GET_BY_KEY_INTERVAL 103
#define GET_BY_INDEX_RANGE 104
#define GET_BY_VALUE_INTERVAL 105
#define GET_BY_RANK_RANGE 106
#define GET_BY_KEY_LIST 107
#define GET_BY_VALUE_LIST 108
#define GET_BY_KEY_REL_INDEX_RANGE 109
#define GET_BY_VALUE_REL_RANK_RANGE 110

/******************************************************************************
 * MAP FUNCTIONS
 *****************************************************************************/

static bool
as_map_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_operator op_type,
	uint16_t command, as_val* begin, as_val* end, int64_t return_type
	)
{
	if (! begin) {
		begin = (as_val*)&as_nil;
	}

	int count = end ? 3 : 2;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, count);
	as_pack_int64(&pk, return_type);
	as_pack_val(&pk, begin);

	if (end) {
		as_pack_val(&pk, end);
	}
	as_cdt_end(&pk);
	as_val_destroy(begin);
	as_val_destroy(end);
	return as_cdt_add_packed(&pk, ops, name, op_type);
}

void
as_map_policy_init(as_map_policy* policy)
{
	policy->attributes = AS_MAP_UNORDERED;
	policy->flags = AS_MAP_WRITE_DEFAULT;
	policy->item_command = PUT;
	policy->items_command = PUT_ITEMS;
}

void
as_map_policy_set(as_map_policy* policy, as_map_order order, as_map_write_mode mode)
{
	policy->attributes = order;
	policy->flags = AS_MAP_WRITE_DEFAULT;

	switch (mode) {
		default:
		case AS_MAP_UPDATE:
			policy->item_command = PUT;
			policy->items_command = PUT_ITEMS;
			break;

		case AS_MAP_UPDATE_ONLY:
			policy->item_command = REPLACE;
			policy->items_command = REPLACE_ITEMS;
			break;

		case AS_MAP_CREATE_ONLY:
			policy->item_command = ADD;
			policy->items_command = ADD_ITEMS;
			break;
	}
}

void
as_map_policy_set_flags(as_map_policy* policy, as_map_order order, uint32_t flags)
{
	policy->attributes = order;
	policy->flags = flags;
	policy->item_command = PUT;
	policy->items_command = PUT_ITEMS;
}

void
as_map_policy_set_all(as_map_policy* policy, as_map_order order, uint32_t flags, bool persist_index)
{
	policy->attributes = order;

	if (persist_index) {
		policy->attributes |= 0x10;
	}
	policy->flags = flags;
	policy->item_command = PUT;
	policy->items_command = PUT_ITEMS;
}

bool
as_operations_map_create(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_order order
	)
{
	// If context not defined, the set order for top-level bin map.
	if (! ctx) {
		as_map_policy policy;
		as_map_policy_set(&policy, order, AS_MAP_UPDATE);
		return as_operations_map_set_policy(ops, name, NULL, &policy);
	}

	uint32_t flag = as_map_order_to_flag(order);

	as_packer pk = as_cdt_begin();
	as_cdt_pack_header_flag(&pk, ctx, SET_TYPE, 1, flag);
	as_pack_uint64(&pk, (uint64_t)order);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_create_all(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_order order, bool persist_index
	)
{
	// If context not defined, the set order for top-level bin map.
	if (! ctx) {
		as_map_policy policy;
		as_map_policy_set_all(&policy, order, AS_MAP_UPDATE, persist_index);
		return as_operations_map_set_policy(ops, name, NULL, &policy);
	}

	uint32_t flag = as_map_order_to_flag(order);

	// Create nested map. persist_index does not apply here, so ignore it.
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header_flag(&pk, ctx, SET_TYPE, 1, flag);
	as_pack_uint64(&pk, (uint64_t)order);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_set_policy(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy
	)
{
	uint64_t attr = policy->attributes;

	// Remove persist_index flag for nested maps.
	if (ctx && (attr & 0x10) != 0) {
		attr &= ~0x10;
	}

	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, SET_TYPE, 1);
	as_pack_uint64(&pk, attr);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_put(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy, as_val* key,
	as_val* value
	)
{
	as_packer pk = as_cdt_begin();

	if (! policy) {
		as_cdt_pack_header(&pk, ctx, PUT, 3);
		as_pack_val(&pk, key);
		as_pack_val(&pk, value);
		as_pack_uint64(&pk, 0);
	}
	else if (policy->flags != AS_MAP_WRITE_DEFAULT) {
		as_cdt_pack_header(&pk, ctx, PUT, 4);
		as_pack_val(&pk, key);
		as_pack_val(&pk, value);
		as_pack_uint64(&pk, policy->attributes);
		as_pack_uint64(&pk, policy->flags);
	}
	else if (policy->item_command == REPLACE) {
		as_cdt_pack_header(&pk, ctx, REPLACE, 2);
		as_pack_val(&pk, key);
		as_pack_val(&pk, value);
	}
	else {
		as_cdt_pack_header(&pk, ctx, policy->item_command, 3);
		as_pack_val(&pk, key);
		as_pack_val(&pk, value);
		as_pack_uint64(&pk, policy->attributes);
	}
	as_cdt_end(&pk);
	as_val_destroy(key);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_put_items(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy,
	as_map* items
	)
{
	as_packer pk = as_cdt_begin();

	if (! policy) {
		as_cdt_pack_header(&pk, ctx, PUT_ITEMS, 2);
		as_pack_val(&pk, (as_val*)items);
		as_pack_uint64(&pk, 0);
	}
	else if (policy->flags != AS_MAP_WRITE_DEFAULT) {
		as_cdt_pack_header(&pk, ctx, PUT_ITEMS, 3);
		as_pack_val(&pk, (as_val*)items);
		as_pack_uint64(&pk, policy->attributes);
		as_pack_uint64(&pk, policy->flags);
	}
	else if (policy && policy->items_command == REPLACE_ITEMS) {
		// Replace doesn't allow map attributes because it does not create on non-existing key.
		as_cdt_pack_header(&pk, ctx, REPLACE_ITEMS, 1);
		as_pack_val(&pk, (as_val*)items);
	}
	else {
		as_cdt_pack_header(&pk, ctx, policy->items_command, 2);
		as_pack_val(&pk, (as_val*)items);
		as_pack_uint64(&pk, policy->attributes);
	}

	as_cdt_end(&pk);
	as_map_destroy(items);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_increment(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy, as_val* key,
	as_val* value
	)
{
	as_val* val = value ? value : (as_val*)&as_nil;

	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, INCREMENT, 3);
	as_pack_val(&pk, key);
	as_pack_val(&pk, val);
	as_pack_uint64(&pk, policy->attributes);
	as_cdt_end(&pk);
	as_val_destroy(key);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_decrement(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy, as_val* key,
	as_val* value
	)
{
	as_val* val = value ? value : (as_val*)&as_nil;

	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, DECREMENT, 3);
	as_pack_val(&pk, key);
	as_pack_val(&pk, val);
	as_pack_uint64(&pk, policy->attributes);
	as_cdt_end(&pk);
	as_val_destroy(key);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_clear(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, CLEAR, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_key(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_KEY, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, key);
	as_cdt_end(&pk);
	as_val_destroy(key);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_key_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* keys,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_KEY_LIST, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, (as_val*)keys);
	as_cdt_end(&pk);
	as_list_destroy(keys);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_key_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_map_range(ops, name, ctx, AS_OPERATOR_MAP_MODIFY, REMOVE_BY_KEY_INTERVAL, begin, end,
						return_type);
}

bool
as_operations_map_remove_by_key_rel_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_KEY_REL_INDEX_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, key);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	as_val_destroy(key);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_key_rel_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	uint64_t count, as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_KEY_REL_INDEX_RANGE, 4);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, key);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	as_val_destroy(key);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_ALL_BY_VALUE, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, value);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_VALUE_LIST, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, (as_val*)values);
	as_cdt_end(&pk);
	as_list_destroy(values);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_map_range(ops, name, ctx, AS_OPERATOR_MAP_MODIFY, REMOVE_BY_VALUE_INTERVAL, begin,
						end, return_type);
}

bool
as_operations_map_remove_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, 4);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_INDEX, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_INDEX_RANGE, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_INDEX_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_RANK, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_RANK_RANGE, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_remove_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_RANK_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_MODIFY);
}

bool
as_operations_map_size(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, SIZE, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_key(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_KEY, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, key);
	as_cdt_end(&pk);
	as_val_destroy(key);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_key_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_map_range(ops, name, ctx, AS_OPERATOR_MAP_READ, GET_BY_KEY_INTERVAL, begin, end,
						return_type);
}

bool
as_operations_map_get_by_key_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* keys,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_KEY_LIST, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, (as_val*)keys);
	as_cdt_end(&pk);
	as_list_destroy(keys);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_key_rel_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_KEY_REL_INDEX_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, key);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	as_val_destroy(key);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_key_rel_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	uint64_t count, as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_KEY_REL_INDEX_RANGE, 4);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, key);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	as_val_destroy(key);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_ALL_BY_VALUE, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, value);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_map_range(ops, name, ctx, AS_OPERATOR_MAP_READ, GET_BY_VALUE_INTERVAL, begin, end,
						return_type);
}

bool
as_operations_map_get_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_VALUE_LIST, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, (as_val*)values);
	as_cdt_end(&pk);
	as_list_destroy(values);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_VALUE_REL_RANK_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_VALUE_REL_RANK_RANGE, 4);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_INDEX, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_INDEX_RANGE, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_INDEX_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_RANK, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_RANK_RANGE, 2);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}

bool
as_operations_map_get_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_map_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_RANK_RANGE, 3);
	as_pack_int64(&pk, (int64_t)return_type);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_MAP_READ);
}
