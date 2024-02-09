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
#include <aerospike/as_list_operations.h>
#include <aerospike/as_cdt_internal.h>
#include <citrusleaf/alloc.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define SET_TYPE 0
#define APPEND 1
#define APPEND_ITEMS 2
#define INSERT 3
#define INSERT_ITEMS 4
#define POP 5
#define POP_RANGE 6
#define REMOVE 7
#define REMOVE_RANGE 8
#define SET 9
#define TRIM 10
#define CLEAR 11
#define INCREMENT 12
#define SORT 13
#define SIZE 16
#define GET 17
#define GET_RANGE 18
#define GET_BY_INDEX 19
#define GET_BY_RANK 21
#define GET_ALL_BY_VALUE 22
#define GET_BY_VALUE_LIST 23
#define GET_BY_INDEX_RANGE 24
#define GET_BY_VALUE_INTERVAL 25
#define GET_BY_RANK_RANGE 26
#define GET_BY_VALUE_REL_RANK_RANGE 27
#define REMOVE_BY_INDEX 32
#define REMOVE_BY_RANK 34
#define REMOVE_ALL_BY_VALUE 35
#define REMOVE_BY_VALUE_LIST 36
#define REMOVE_BY_INDEX_RANGE 37
#define REMOVE_BY_VALUE_INTERVAL 38
#define REMOVE_BY_RANK_RANGE 39
#define REMOVE_BY_VALUE_REL_RANK_RANGE 40

/******************************************************************************
 * LIST FUNCTIONS
 *****************************************************************************/

static bool
as_list_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_operator op_type,
	uint16_t command, as_val* begin, as_val* end, uint64_t return_type
	)
{
	if (! begin) {
		begin = (as_val*)&as_nil;
	}

	int count = end ? 3 : 2;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, command, count);
	as_pack_uint64(&pk, return_type);
	as_pack_val(&pk, begin);

	if (end) {
		as_pack_val(&pk, end);
	}
	as_cdt_end(&pk);
	as_val_destroy(begin);
	as_val_destroy(end);
	return as_cdt_add_packed(&pk, ops, name, op_type);
}

bool
as_operations_list_create(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_order order, bool pad
	)
{
	// If context not defined, the set order for top-level bin list.
	if (! ctx) {
		return as_operations_list_set_order(ops, name, NULL, order);
	}

	uint32_t flag = as_list_order_to_flag(order, pad);

	as_packer pk = as_cdt_begin();
	as_cdt_pack_header_flag(&pk, ctx, SET_TYPE, 1, flag);
	as_pack_uint64(&pk, (uint64_t)order);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_create_all(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_order order, bool pad, bool persist_index
	)
{
	// If context not defined, the set order for top-level bin list.
	if (! ctx) {
		uint64_t flag = (uint64_t)order;
		
		if (persist_index) {
			flag |= 0x10;
		}

		as_packer pk = as_cdt_begin();
		as_cdt_pack_header(&pk, ctx, SET_TYPE, 1);
		as_pack_uint64(&pk, flag);
		as_cdt_end(&pk);
		return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
	}

	uint32_t flag = as_list_order_to_flag(order, pad);
	
	// Create nested list. persist_index does not apply here, so ignore it.
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header_flag(&pk, ctx, SET_TYPE, 1, flag);
	as_pack_uint64(&pk, (uint64_t)order);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_set_order(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_order order
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, SET_TYPE, 1);
	as_pack_uint64(&pk, (uint64_t)order);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_sort(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_sort_flags flags
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, SORT, 1);
	as_pack_uint64(&pk, (uint64_t)flags);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_append(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	as_val* val
	)
{
	int count = policy ? 3 : 1;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, APPEND, count);
	as_pack_val(&pk, val);

	if (policy) {
		as_pack_uint64(&pk, (uint64_t)policy->order);
		as_pack_uint64(&pk, (uint64_t)policy->flags);
	}
	as_cdt_end(&pk);
	as_val_destroy(val);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_append_items(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	as_list* list
	)
{
	int count = policy ? 3 : 1;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, APPEND_ITEMS, count);
	as_pack_val(&pk, (as_val*)list);

	if (policy) {
		as_pack_uint64(&pk, (uint64_t)policy->order);
		as_pack_uint64(&pk, (uint64_t)policy->flags);
	}
	as_cdt_end(&pk);
	as_list_destroy(list);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_insert(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_val* val
	)
{
	int count = policy ? 3 : 2;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, INSERT, count);
	as_pack_int64(&pk, index);
	as_pack_val(&pk, val);

	if (policy) {
		// as_list_policy.order is not sent because inserts are not allowed on sorted lists.
		as_pack_uint64(&pk, (uint64_t)policy->flags);
	}
	as_cdt_end(&pk);
	as_val_destroy(val);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_insert_items(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_list* list
	)
{
	int count = policy ? 3 : 2;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, INSERT_ITEMS, count);
	as_pack_int64(&pk, index);
	as_pack_val(&pk, (as_val*)list);

	if (policy) {
		// as_list_policy.order is not sent because inserts are not allowed on sorted lists.
		as_pack_uint64(&pk, (uint64_t)policy->flags);
	}
	as_cdt_end(&pk);
	as_list_destroy(list);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_increment(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_val* incr
	)
{
	int count = policy ? 4 : 2;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, INCREMENT, count);
	as_pack_int64(&pk, index);

	if (incr) {
		as_pack_val(&pk, incr);
	}
	else {
		as_integer inc;
		as_integer_init(&inc, 1);
		as_pack_val(&pk, (as_val*)&inc);
	}

	if (policy) {
		as_pack_uint64(&pk, (uint64_t)policy->order);
		as_pack_uint64(&pk, (uint64_t)policy->flags);
	}
	as_cdt_end(&pk);
	as_val_destroy(incr);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_set(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_val* val
	)
{
	int count = policy ? 3 : 2;
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, SET, count);
	as_pack_int64(&pk, index);
	as_pack_val(&pk, val);

	if (policy) {
		as_pack_uint64(&pk, (uint64_t)policy->flags);
	}
	as_cdt_end(&pk);
	as_val_destroy(val);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_pop(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, POP, 1);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_pop_range(as_operations* ops, const char* name, as_cdt_ctx* ctx,
	int64_t index, uint64_t count
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, POP_RANGE, 2);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_pop_range_from(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, POP_RANGE, 1);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE, 1);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_RANGE, 2);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_range_from(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_RANGE, 1);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_ALL_BY_VALUE, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, value);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_VALUE_LIST, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, (as_val*)values);
	as_cdt_end(&pk);
	as_list_destroy(values);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_list_return_type return_type
	)
{
	return as_list_range(ops, name, ctx, AS_OPERATOR_CDT_MODIFY,
						 REMOVE_BY_VALUE_INTERVAL, begin, end, return_type);
}

bool
as_operations_list_remove_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, 3);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_VALUE_REL_RANK_RANGE, 4);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_INDEX, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_INDEX_RANGE, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_INDEX_RANGE, 3);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_RANK, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_RANK_RANGE, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_remove_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, REMOVE_BY_RANK_RANGE, 3);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_trim(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, TRIM, 2);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_clear(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, CLEAR, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_MODIFY);
}

bool
as_operations_list_size(as_operations* ops, const char* name, as_cdt_ctx* ctx)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, SIZE, 0);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET, 1);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_RANGE, 2);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_range_from(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_RANGE, 1);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_ALL_BY_VALUE, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, value);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_list_return_type return_type
	)
{
	return as_list_range(ops, name, ctx, AS_OPERATOR_CDT_READ,
						 GET_BY_VALUE_INTERVAL, begin, end, return_type);
}

bool
as_operations_list_get_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_VALUE_LIST, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, (as_val*)values);
	as_cdt_end(&pk);
	as_list_destroy(values);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_VALUE_REL_RANK_RANGE, 3);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_VALUE_REL_RANK_RANGE, 4);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_val(&pk, value);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	as_val_destroy(value);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_INDEX, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_INDEX_RANGE, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, index);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_INDEX_RANGE, 3);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, index);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_RANK, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_RANK_RANGE, 2);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, rank);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}

bool
as_operations_list_get_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_list_return_type return_type
	)
{
	as_packer pk = as_cdt_begin();
	as_cdt_pack_header(&pk, ctx, GET_BY_RANK_RANGE, 3);
	as_pack_uint64(&pk, (uint64_t)return_type);
	as_pack_int64(&pk, rank);
	as_pack_uint64(&pk, count);
	as_cdt_end(&pk);
	return as_cdt_add_packed(&pk, ops, name, AS_OPERATOR_CDT_READ);
}
