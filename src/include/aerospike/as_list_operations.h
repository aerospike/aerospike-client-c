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
#pragma once

/**
 * @defgroup list_operations List Operations
 * @ingroup client_operations
 *
 * List bin operations. Create list operations used by client operate command.
 *
 * List operations support negative indexing.  If the index is negative, the
 * resolved index starts backwards from end of list. If an index is out of bounds,
 * a parameter error will be returned. If a range is partially out of bounds, the
 * valid part of the range will be returned. Index/Range examples:
 * <ul>
 * <li>Index 0: First item in list.</li>
 * <li>Index 4: Fifth item in list.</li>
 * <li>Index -1: Last item in list.</li>
 * <li>Index -3: Third to last item in list.</li>
 * <li>Index 1 Count 2: Second and third items in list.</li>
 * <li>Index -3 Count 3: Last three items in list.</li>
 * <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
 * </ul>
 *
 * Example 1:
 *
 * ~~~~~~~~~~{.c}
 * // list bin = [7,9,5]
 * // Append 11 to list bin.
 * as_operations ops;
 * as_operations_inita(&ops, 1);
 *
 * as_integer val;
 * as_integer_init(&val, 11);
 * as_operations_list_append(&ops, "bin", NULL, NULL, &val);
 * 
 * as_record* rec = 0;
 * as_error err;
 * aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec);
 * // bin result = [7,9,5,11]
 * as_operations_destroy(&ops);
 * as_record_destroy(rec);
 * ~~~~~~~~~~
 *
 * Nested CDT operations are supported by optional context (as_cdt_ctx).  Example:
 *
 * ~~~~~~~~~~{.c}
 * // bin = [[7,9,5],[1,2,3],[6,5,4,1]]
 * // Append 11 to last list.
 * as_cdt_ctx ctx;
 * as_cdt_ctx_inita(&ctx, 1);
 * as_cdt_ctx_add_list_index(&ctx, -1);
 *
 * as_operations ops;
 * as_operations_inita(&ops, 1);
 *
 * as_integer val;
 * as_integer_init(&val, 11);
 * as_operations_list_append(&ops, "bin", &ctx, NULL, &val);
 * 
 * as_record* rec = 0;
 * as_error err;
 * aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec);
 * // bin result = [[7,9,5],[1,2,3],[6,5,4,1,11]]
 * as_operations_destroy(&ops);
 * as_record_destroy(rec);
 * ~~~~~~~~~~
 */

#include <aerospike/as_operations.h>
#include <aerospike/as_cdt_ctx.h>
#include <aerospike/as_cdt_order.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * List sort flags.
 *
 * @ingroup list_operations
 */
typedef enum as_list_sort_flags_e {
	/**
	 * Default.  Preserve duplicate values when sorting list.
	 */
	AS_LIST_SORT_DEFAULT = 0,

	/**
	 * Drop duplicate values when sorting list.
	 */
	AS_LIST_SORT_DROP_DUPLICATES = 2,
} as_list_sort_flags;

/**
 * List write bit flags.
 *
 * @ingroup list_operations
 */
typedef enum as_list_write_flags_e {
	/**
	 * Default.  Allow duplicate values and insertions at any index.
	 */
	AS_LIST_WRITE_DEFAULT = 0,

	/**
	 * Only add unique values.
	 */
	AS_LIST_WRITE_ADD_UNIQUE = 1,

	/**
	 * Enforce list boundaries when inserting.  Do not allow values to be inserted
	 * at index outside current list boundaries.
	 */
	AS_LIST_WRITE_INSERT_BOUNDED = 2,

	/**
	 * Do not raise error if a list item fails due to write flag constraints.
	 */
	AS_LIST_WRITE_NO_FAIL = 4,

	/**
	 * Allow other valid list items to be committed if a list item fails due to
	 * write flag constraints.
	 */
	AS_LIST_WRITE_PARTIAL = 8,
} as_list_write_flags;

/**
 * List policy directives when creating a list and writing list items.
 *
 * @ingroup list_operations
 */
typedef struct as_list_policy_s {
	as_list_order order;
	as_list_write_flags flags;
} as_list_policy;

/**
 * List return type. Type of data to return when selecting or removing items from the list.
 *
 * @ingroup list_operations
 */
typedef enum as_list_return_type_e {
	/**
	 * Do not return a result.
	 */
	AS_LIST_RETURN_NONE = 0,

	/**
	 * Return key index order.
	 */
	AS_LIST_RETURN_INDEX = 1,

	/**
	 * Return reverse key order.
	 */
	AS_LIST_RETURN_REVERSE_INDEX = 2,

	/**
	 * Return value order.
	 */
	AS_LIST_RETURN_RANK = 3,

	/**
	 * Return reverse value order.
	 */
	AS_LIST_RETURN_REVERSE_RANK = 4,

	/**
	 * Return count of items selected.
	 */
	AS_LIST_RETURN_COUNT = 5,

	/**
	 * Return value for single key read and value list for range read.
	 */
	AS_LIST_RETURN_VALUE = 7,

	/**
	 * Return true if count > 0.
	 */
	AS_LIST_RETURN_EXISTS = 13,

	/**
	 * Invert meaning of list command and return values.  For example:
	 *
	 * ~~~~~~~~~~{.c}
	 * as_operations ops;
	 * as_operations_inita(&ops, 1);
	 *
	 * as_operations_add_list_remove_by_index_range(&ops, BIN_NAME, index, count,
	 *                                           AS_LIST_RETURN_VALUE | AS_LIST_RETURN_INVERTED);
	 *
	 * as_record* rec = NULL;
	 * as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	 * as_operations_destroy(&ops);
	 * ~~~~~~~~~~
	 *
	 * With AS_LIST_RETURN_INVERTED enabled, the items outside of the specified index range will be
	 * removed and returned.
	 */
	AS_LIST_RETURN_INVERTED = 0x10000,
} as_list_return_type;

/**
 * @private
 * List operation codes.
 */
typedef enum {
	AS_CDT_OP_LIST_SET_TYPE = 0,
	AS_CDT_OP_LIST_APPEND = 1,
	AS_CDT_OP_LIST_APPEND_ITEMS = 2,
	AS_CDT_OP_LIST_INSERT = 3,
	AS_CDT_OP_LIST_INSERT_ITEMS = 4,
	AS_CDT_OP_LIST_POP = 5,
	AS_CDT_OP_LIST_POP_RANGE = 6,
	AS_CDT_OP_LIST_REMOVE = 7,
	AS_CDT_OP_LIST_REMOVE_RANGE = 8,
	AS_CDT_OP_LIST_SET = 9,
	AS_CDT_OP_LIST_TRIM = 10,
	AS_CDT_OP_LIST_CLEAR = 11,
	AS_CDT_OP_LIST_INCREMENT = 12,
	AS_CDT_OP_LIST_SORT = 13,
	AS_CDT_OP_LIST_SIZE = 16,
	AS_CDT_OP_LIST_GET = 17,
	AS_CDT_OP_LIST_GET_RANGE = 18,
	AS_CDT_OP_LIST_GET_BY_INDEX = 19,
	AS_CDT_OP_LIST_GET_BY_RANK = 21,
	AS_CDT_OP_LIST_GET_ALL_BY_VALUE = 22,
	AS_CDT_OP_LIST_GET_BY_VALUE_LIST = 23,
	AS_CDT_OP_LIST_GET_BY_INDEX_RANGE = 24,
	AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL = 25,
	AS_CDT_OP_LIST_GET_BY_RANK_RANGE = 26,
	AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE = 27,
	AS_CDT_OP_LIST_REMOVE_BY_INDEX = 32,
	AS_CDT_OP_LIST_REMOVE_BY_RANK = 34,
	AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE = 35,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_LIST = 36,
	AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE = 37,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL = 38,
	AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE = 39,
	AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE = 40
} as_cdt_op_list;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize list attributes to default unordered list with standard overwrite semantics.
 *
 * @ingroup list_operations
 */
static inline void
as_list_policy_init(as_list_policy* policy)
{
	policy->order = AS_LIST_UNORDERED;
	policy->flags = AS_LIST_WRITE_DEFAULT;
}

/**
 * Set list attributes to specified list order and write flag semantics.
 *
 * @ingroup list_operations
 */
static inline void
as_list_policy_set(as_list_policy* policy, as_list_order order, as_list_write_flags flags)
{
	policy->order = order;
	policy->flags = flags;
}

/**
 * Create list create operation.
 * Server creates list at given context level. The context is allowed to be beyond list
 * boundaries only if pad is set to true.  In that case, nil list entries will be inserted to
 * satisfy the context position.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_create(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_order order, bool pad
	);

/**
 * Create list create operation.
 * Server creates list  at given context level.
 *
 * @param ops			Target operations list.
 * @param name			Bin name.
 * @param ctx			Optional path to nested list. If not defined, the top-level list is used.
 * @param order			List  order.
 * @param pad			If true, the context is allowed to be beyond list boundaries. In that case, nil
 * 						list entries will be inserted to satisfy the context position.
 * @param persist_index	If true, persist list index. A list index improves lookup performance,
 * 						but requires more storage. A list index can be created for a top-level
 * 						ordered list only. Nested and unordered list indexes are not supported.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_list_create_all(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_order order, bool pad, bool persist_index
	);

/**
 * Create set list order operation.
 * Server sets list order.  Server returns null.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_set_order(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_order order
	);

/**
 * Create list sort operation.
 * Server sorts list according to flags.
 * Server does not return a result by default.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_sort(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_sort_flags flags
	);

/**
 * Create list append operation with policy.
 * Server appends value to list bin.
 * Server returns list size.
 *
 * This function takes ownership and frees heap memory associated with val parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_append(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	as_val* val
	);

/**
 * Create list append items operation with policy.
 * Server appends each input list item to end of list bin.
 * Server returns list size.
 *
 * This function takes ownership and frees heap memory associated with list parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_append_items(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	as_list* list
	);

/**
 * Create default list insert operation with policy.
 * Server inserts value to specified index of list bin.
 * Server returns list size.
 *
 * This function takes ownership and frees heap memory associated with val parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_insert(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_val* val
	);

/**
 * Create default list insert items operation with policy.
 * Server inserts each input list item starting at specified index of list bin.
 * Server returns list size.
 *
 * This function takes ownership and frees heap memory associated with list parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_insert_items(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_list* list
	);

/**
 * Create list increment operation with policy.
 * Server increments value at index by incr and returns final result.
 * Valid only for numbers.
 *
 * This function takes ownership and frees heap memory associated with incr parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_increment(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_val* incr
	);

/**
 * Create list set operation with policy.
 * Server sets item value at specified index in list bin.
 * Server does not return a result by default.
 *
 * This function takes ownership and frees heap memory associated with val parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_set(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list_policy* policy,
	int64_t index, as_val* val
	);

/**
 * Create list pop operation.
 * Server returns item at specified index and removes item from list bin.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_pop(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index);

/**
 * Create list pop range operation.
 * Server returns "count" items starting at specified index and removes items from list bin.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_pop_range(as_operations* ops, const char* name, as_cdt_ctx* ctx,
	int64_t index, uint64_t count
	);

/**
 * Create list pop range operation.
 * Server returns items starting at specified index to the end of list and removes those items
 * from list bin.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_pop_range_from(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	);

/**
 * Create list remove operation.
 * Server removes item at specified index from list bin.
 * Server returns number of items removed.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	);

/**
 * Create list remove range operation.
 * Server removes "count" items starting at specified index from list bin.
 * Server returns number of items removed.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count
	);

/**
 * Create list remove range operation.
 * Server removes items starting at specified index to the end of list.
 * Server returns number of items removed.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_range_from(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	);

/**
 * Create list remove operation.
 * Server removes list items identified by value and returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes list items identified by values and returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with values parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes list items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with begin/end parameters.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_list_return_type return_type
	);

/**
 * Create list remove by value relative to rank range operation.
 * Server removes list items nearest to value and greater by relative rank.
 * Server returns removed data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank) = [removed items]</li>
 * <li>(5,0) = [5,9,11,15]</li>
 * <li>(5,1) = [9,11,15]</li>
 * <li>(5,-1) = [4,5,9,11,15]</li>
 * <li>(3,0) = [4,5,9,11,15]</li>
 * <li>(3,3) = [11,15]</li>
 * <li>(3,-3) = [0,4,5,9,11,15]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_list_return_type return_type
	);

/**
 * Create list remove by value relative to rank range operation.
 * Server removes list items nearest to value and greater by relative rank with a count limit.
 * Server returns removed data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank,count) = [removed items]</li>
 * <li>(5,0,2) = [5,9]</li>
 * <li>(5,1,1) = [9]</li>
 * <li>(5,-1,2) = [4,5]</li>
 * <li>(3,0,1) = [4]</li>
 * <li>(3,3,7) = [11,15]</li>
 * <li>(3,-3,2) = []</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes list item identified by index and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes list items starting at specified index to the end of list and returns removed
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes `count` list items starting at specified index and returns removed data specified
 * by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes list item identified by rank and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes list items starting at specified rank to the last ranked item and returns removed
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	);

/**
 * Create list remove operation.
 * Server removes `count` list items starting at specified rank and returns removed data specified
 * by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_remove_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_list_return_type return_type
	);

/**
 * Create list trim operation.
 * Server removes items in list bin that do not fall into range specified by index
 * and count range.  If the range is out of bounds, then all items will be removed.
 * Server returns list size after trim.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_trim(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count
	);

/**
 * Create list clear operation.
 * Server removes all items in list bin.
 * Server does not return a result by default.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_clear(as_operations* ops, const char* name, as_cdt_ctx* ctx);
	
//-----------------------------------------------------------------------------
// Read operations

/**
 * Create list size operation.
 * Server returns size of list.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_size(as_operations* ops, const char* name, as_cdt_ctx* ctx);
	
/**
 * Create list get operation.
 * Server returns item at specified index in list bin.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get(as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index);

/**
 * Create list get range operation.
 * Server returns "count" items starting at specified index in list bin.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count
	);

/**
 * Create list get range operation.
 * Server returns items starting at index to the end of list.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_range_from(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index
	);

/**
 * Create list get by value operation.
 * Server selects list items identified by value and returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_list_return_type return_type
	);

/**
 * Create list get by value range operation.
 * Server selects list items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with begin/end parameters.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_list_return_type return_type
	);

/**
 * Create list get by value list operation.
 * Server selects list items identified by values and returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with values parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_list_return_type return_type
	);

/**
 * Create list get by value relative to rank range operation.
 * Server selects list items nearest to value and greater by relative rank.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank) = [selected items]</li>
 * <li>(5,0) = [5,9,11,15]</li>
 * <li>(5,1) = [9,11,15]</li>
 * <li>(5,-1) = [4,5,9,11,15]</li>
 * <li>(3,0) = [4,5,9,11,15]</li>
 * <li>(3,3) = [11,15]</li>
 * <li>(3,-3) = [0,4,5,9,11,15]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_list_return_type return_type
	);

/**
 * Create list get by value relative to rank range operation.
 * Server selects list items nearest to value and greater by relative rank with a count limit.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank,count) = [selected items]</li>
 * <li>(5,0,2) = [5,9]</li>
 * <li>(5,1,1) = [9]</li>
 * <li>(5,-1,2) = [4,5]</li>
 * <li>(3,0,1) = [4]</li>
 * <li>(3,3,7) = [11,15]</li>
 * <li>(3,-3,2) = []</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_list_return_type return_type
	);

/**
 * Create list get by index operation.
 * Server selects list item identified by index and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	);

/**
 * Create list get by index range operation.
 * Server selects list items starting at specified index to the end of list and returns selected
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_list_return_type return_type
	);

/**
 * Create list get by index range operation.
 * Server selects `count` list items starting at specified index and returns selected data specified
 * by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_list_return_type return_type
	);

/**
 * Create list get by rank operation.
 * Server selects list item identified by rank and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	);

/**
 * Create list get by rank range operation.
 * Server selects list items starting at specified rank to the last ranked item and returns selected
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_list_return_type return_type
	);

/**
 * Create list get by rank range operation.
 * Server selects `count` list items starting at specified rank and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_list_get_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_list_return_type return_type
	);

/******************************************************************************
 * LEGACY FUNCTIONS
 *****************************************************************************/

/**
 * Create set list order operation.
 * Server sets list order.  Server returns null.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_order(as_operations* ops, const char* name, as_list_order order)
{
	return as_operations_list_set_order(ops, name, NULL, order);
}

/**
 * Create list sort operation.
 * Server sorts list according to flags.
 * Server does not return a result by default.
 *
 * @return true on success. Otherwise an error occurred.
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_sort(as_operations* ops, const char* name, as_list_sort_flags flags)
{
	return as_operations_list_sort(ops, name, NULL, flags);
}

/**
 * Create list append operation.
 * Server appends value to list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append(as_operations* ops, const char* name, as_val* val)
{
	return as_operations_list_append(ops, name, NULL, NULL, val);
}

/**
 * Create list append operation with policy.
 * Server appends value to list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_with_policy(
	as_operations* ops, const char* name, as_list_policy* policy, as_val* val
	)
{
	return as_operations_list_append(ops, name, NULL, policy, val);
}

/**
 * Create list append operation.
 * Server appends integer to list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_int64(as_operations* ops, const char* name, int64_t value)
{
	as_integer v;
	as_integer_init(&v, value);
	return as_operations_list_append(ops, name, NULL, NULL, (as_val*)&v);
}

/**
 * Create list append operation.
 * Server appends double to list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_double(as_operations* ops, const char* name, double value)
{
	as_double v;
	as_double_init(&v, value);
	return as_operations_list_append(ops, name, NULL, NULL, (as_val*)&v);
}

/**
 * Create list append operation.
 * Server appends string to list bin.
 * Server returns list size.
 *
 * If free is true, the value will be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_strp(
	as_operations* ops, const char* name, const char* value, bool free
	)
{
	as_string v;
	as_string_init(&v, (char*)value, free);
	return as_operations_list_append(ops, name, NULL, NULL, (as_val*)&v);
}

/**
 * Create list append operation.
 * Server appends string to list bin.
 * Server returns list size.
 *
 * The value will not be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_str(as_operations* ops, const char* name, const char* value)
{
	return as_operations_add_list_append_strp(ops, name, value, false);
}

/**
 * Create list append operation.
 * Server appends blob (byte array) to list bin.
 * Server returns list size.
 *
 * If free is true, the value will be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_rawp(
	as_operations* ops, const char* name, const uint8_t* value, uint32_t size, bool free
	)
{
	as_bytes v;
	as_bytes_init_wrap(&v, (uint8_t*)value, size, free);
	return as_operations_list_append(ops, name, NULL, NULL, (as_val*)&v);
}

/**
 * Create list append operation.
 * Server appends blob (byte array) to list bin.
 * Server returns list size.
 *
 * The value will not be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_raw(
	as_operations* ops, const char* name, const uint8_t* value, uint32_t size
	)
{
	return as_operations_add_list_append_rawp(ops, name, value, size, false);
}

/**
 * Create list append items operation.
 * Server appends each input list item to end of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_items(as_operations* ops, const char* name, as_list* list)
{
	return as_operations_list_append_items(ops, name, NULL, NULL, list);
}

/**
 * Create list append items operation with policy.
 * Server appends each input list item to end of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_items_with_policy(
	as_operations* ops, const char* name, as_list_policy* policy, as_list* list
	)
{
	return as_operations_list_append_items(ops, name, NULL, policy, list);
}

/**
 * Create default list insert operation.
 * Server inserts value to specified index of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert(
	as_operations* ops, const char* name, int64_t index, as_val* val
	)
{
	return as_operations_list_insert(ops, name, NULL, NULL, index, val);
}

/**
 * Create default list insert operation with policy.
 * Server inserts value to specified index of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_with_policy(
	as_operations* ops, const char* name, as_list_policy* policy, int64_t index, as_val* val
	)
{
	return as_operations_list_insert(ops, name, NULL, policy, index, val);
}

/**
 * Create default list insert operation with policy.
 * Server inserts integer to specified index of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_int64(
	as_operations* ops, const char* name, int64_t index, int64_t value
	)
{
	as_integer v;
	as_integer_init(&v, value);
	return as_operations_list_insert(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create default list insert operation with policy.
 * Server inserts double to specified index of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_double(
	as_operations* ops, const char* name, int64_t index, double value
	)
{
	as_double v;
	as_double_init(&v, value);
	return as_operations_list_insert(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create default list insert operation with policy.
 * Server inserts string to specified index of list bin.
 * Server returns list size.
 *
 * If free is true, the value will be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_strp(
	as_operations* ops, const char* name, int64_t index, const char* value, bool free
	)
{
	as_string v;
	as_string_init(&v, (char *)value, free);
	return as_operations_list_insert(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create default list insert operation with policy.
 * Server inserts string to specified index of list bin.
 * Server returns list size.
 *
 * The value will not be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_str(
	as_operations* ops, const char* name, int64_t index, const char* value
	)
{
	return as_operations_add_list_insert_strp(ops, name, index, value, false);
}

/**
 * Create default list insert operation with policy.
 * Server inserts blob (byte array) to specified index of list bin.
 * Server returns list size.
 *
 * If free is true, the value will be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_rawp(
	as_operations* ops, const char* name, int64_t index, const uint8_t* value, uint32_t size,
	bool free
	)
{
	as_bytes v;
	as_bytes_init_wrap(&v, (uint8_t *)value, size, free);
	return as_operations_list_insert(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create default list insert operation with policy.
 * Server inserts blob (byte array) to specified index of list bin.
 * Server returns list size.
 *
 * The value will not be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_raw(
	as_operations* ops, const char* name, int64_t index, const uint8_t* value, uint32_t size
	)
{
	return as_operations_add_list_insert_rawp(ops, name, index, value, size, false);
}

/**
 * Create default list insert items operation.
 * Server inserts each input list item starting at specified index of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_items(
	as_operations* ops, const char* name, int64_t index, as_list* list
	)
{
	return as_operations_list_insert_items(ops, name, NULL, NULL, index, list);
}

/**
 * Create default list insert items operation with policy.
 * Server inserts each input list item starting at specified index of list bin.
 * Server returns list size.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_items_with_policy(
	as_operations* ops, const char* name, as_list_policy* policy, int64_t index,
	as_list* list
	)
{
	return as_operations_list_insert_items(ops, name, NULL, policy, index, list);
}

/**
 * Create list increment operation.
 * Server increments value at index by incr and returns final result.
 * Valid only for numbers.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_increment(
	as_operations* ops, const char* name, int64_t index, as_val* incr
	)
{
	return as_operations_list_increment(ops, name, NULL, NULL, index, incr);
}

/**
 * Create list increment operation with policy.
 * Server increments value at index by incr and returns final result.
 * Valid only for numbers.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_increment_with_policy(
	as_operations* ops, const char* name, as_list_policy* policy, int64_t index, as_val* incr
	)
{
	return as_operations_list_increment(ops, name, NULL, policy, index, incr);
}

/**
 * Create list set operation.
 * Server sets item value at specified index in list bin.
 * Server does not return a result by default.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set(as_operations* ops, const char* name, int64_t index, as_val* val)
{
	return as_operations_list_set(ops, name, NULL, NULL, index, val);
}
	
/**
 * Create list set operation with policy.
 * Server sets item value at specified index in list bin.
 * Server does not return a result by default.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_with_policy(
	as_operations* ops, const char* name, as_list_policy* policy, int64_t index, as_val* val
	)
{
	return as_operations_list_set(ops, name, NULL, policy, index, val);
}

/**
 * Create list set operation with policy.
 * Server sets integer at specified index in list bin.
 * Server does not return a result by default.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_int64(
	as_operations* ops, const char* name, int64_t index, int64_t value
	)
{
	as_integer v;
	as_integer_init(&v, value);
	return as_operations_list_set(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create list set operation with policy.
 * Server sets double at specified index in list bin.
 * Server does not return a result by default.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_double(
	as_operations* ops, const char* name, int64_t index, double value
	)
{
	as_double v;
	as_double_init(&v, value);
	return as_operations_list_set(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create list set operation with policy.
 * Server sets string at specified index in list bin.
 * Server does not return a result by default.
 *
 * If free is true, the value will be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_strp(
	as_operations* ops, const char* name, int64_t index, const char* value, bool free
	)
{
	as_string v;
	as_string_init(&v, (char *)value, free);
	return as_operations_list_set(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create list set operation with policy.
 * Server sets string at specified index in list bin.
 * Server does not return a result by default.
 *
 * The value will not be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_str(
	as_operations* ops, const char* name, int64_t index, const char* value
	)
{
	return as_operations_add_list_set_strp(ops, name, index, value, false);
}

/**
 * Create list set operation with policy.
 * Server sets blob (byte array) at specified index in list bin.
 * Server does not return a result by default.
 *
 * If free is true, the value will be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_rawp(
	as_operations* ops, const char* name, int64_t index, const uint8_t* value, uint32_t size,
	bool free
	)
{
	as_bytes v;
	as_bytes_init_wrap(&v, (uint8_t *)value, size, free);
	return as_operations_list_set(ops, name, NULL, NULL, index, (as_val*)&v);
}

/**
 * Create list set operation with policy.
 * Server sets blob (byte array) at specified index in list bin.
 * Server does not return a result by default.
 *
 * The value will not be freed when the operations are destroyed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_raw(
	as_operations* ops, const char* name, int64_t index, const uint8_t* value, uint32_t size
	)
{
	return as_operations_add_list_set_rawp(ops, name, index, value, size, false);
}

/**
 * Create list pop operation.
 * Server returns item at specified index and removes item from list bin.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_pop(as_operations* ops, const char* name, int64_t index)
{
	return as_operations_list_pop(ops, name, NULL, index);
}

/**
 * Create list pop range operation.
 * Server returns "count" items starting at specified index and removes items from list bin.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_pop_range(
	as_operations* ops, const char* name, int64_t index, uint64_t count
	)
{
	return as_operations_list_pop_range(ops, name, NULL, index, count);
}

/**
 * Create list pop range operation.
 * Server returns items starting at specified index to the end of list and removes those items
 * from list bin.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_pop_range_from(as_operations* ops, const char* name, int64_t index)
{
	return as_operations_list_pop_range_from(ops, name, NULL, index);
}

/**
 * Create list remove operation.
 * Server removes item at specified index from list bin.
 * Server returns number of items removed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove(as_operations* ops, const char* name, int64_t index)
{
	return as_operations_list_remove(ops, name, NULL, index);
}

/**
 * Create list remove range operation.
 * Server removes "count" items starting at specified index from list bin.
 * Server returns number of items removed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_range(
	as_operations* ops, const char* name, int64_t index, uint64_t count
	)
{
	return as_operations_list_remove_range(ops, name, NULL, index, count);
}

/**
 * Create list remove range operation.
 * Server removes items starting at specified index to the end of list.
 * Server returns number of items removed.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_range_from(as_operations* ops, const char* name, int64_t index)
{
	return as_operations_list_remove_range_from(ops, name, NULL, index);
}

/**
 * Create list remove operation.
 * Server removes list items identified by value and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_value(
	as_operations* ops, const char* name, as_val* value, as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_value(ops, name, NULL, value, return_type);
}

/**
 * Create list remove operation.
 * Server removes list items identified by values and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_value_list(
	as_operations* ops, const char* name, as_list* values, as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_value_list(ops, name, NULL, values, return_type);
}

/**
 * Create list remove operation.
 * Server removes list items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_value_range(
	as_operations* ops, const char* name, as_val* begin, as_val* end,
	as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_value_range(ops, name, NULL, begin, end, return_type);
}

/**
 * Create list remove by value relative to rank range operation.
 * Server removes list items nearest to value and greater by relative rank.
 * Server returns removed data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank) = [removed items]</li>
 * <li>(5,0) = [5,9,11,15]</li>
 * <li>(5,1) = [9,11,15]</li>
 * <li>(5,-1) = [4,5,9,11,15]</li>
 * <li>(3,0) = [4,5,9,11,15]</li>
 * <li>(3,3) = [11,15]</li>
 * <li>(3,-3) = [0,4,5,9,11,15]</li>
 * </ul>
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_val* value, int64_t rank,
	as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_value_rel_rank_range_to_end(ops, name, NULL, value, rank,
																	return_type);
}

/**
 * Create list remove by value relative to rank range operation.
 * Server removes list items nearest to value and greater by relative rank with a count limit.
 * Server returns removed data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank,count) = [removed items]</li>
 * <li>(5,0,2) = [5,9]</li>
 * <li>(5,1,1) = [9]</li>
 * <li>(5,-1,2) = [4,5]</li>
 * <li>(3,0,1) = [4]</li>
 * <li>(3,3,7) = [11,15]</li>
 * <li>(3,-3,2) = []</li>
 * </ul>
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_val* value, int64_t rank, uint64_t count,
	as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_value_rel_rank_range(ops, name, NULL, value, rank, count,
															 return_type);
}

/**
 * Create list remove operation.
 * Server removes list item identified by index and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_index(
	as_operations* ops, const char* name, int64_t index, as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_index(ops, name, NULL, index, return_type);
}

/**
 * Create list remove operation.
 * Server removes list items starting at specified index to the end of list and returns removed
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_index_range_to_end(
	as_operations* ops, const char* name, int64_t index, as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_index_range_to_end(ops, name, NULL, index, return_type);
}

/**
 * Create list remove operation.
 * Server removes `count` list items starting at specified index and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_index_range(
	as_operations* ops, const char* name, int64_t index, uint64_t count,
	as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_index_range(ops, name, NULL, index, count, return_type);
}

/**
 * Create list remove operation.
 * Server removes list item identified by rank and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_rank(
	as_operations* ops, const char* name, int64_t rank, as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_rank(ops, name, NULL, rank, return_type);
}

/**
 * Create list remove operation.
 * Server removes list items starting at specified rank to the last ranked item and returns removed
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_rank_range_to_end(
	as_operations* ops, const char* name, int64_t rank, as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_rank_range_to_end(ops, name, NULL, rank, return_type);
}

/**
 * Create list remove operation.
 * Server removes `count` list items starting at specified rank and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_remove_by_rank_range(
	as_operations* ops, const char* name, int64_t rank, uint64_t count,
	as_list_return_type return_type
	)
{
	return as_operations_list_remove_by_rank_range(ops, name, NULL, rank, count, return_type);
}

/**
 * Create list trim operation.
 * Server removes items in list bin that do not fall into range specified by index
 * and count range.  If the range is out of bounds, then all items will be removed.
 * Server returns list size after trim.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_trim(
	as_operations* ops, const char* name, int64_t index, uint64_t count
	)
{
	return as_operations_list_trim(ops, name, NULL, index, count);
}

/**
 * Create list clear operation.
 * Server removes all items in list bin.
 * Server does not return a result by default.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_clear(as_operations* ops, const char* name)
{
	return as_operations_list_clear(ops, name, NULL);
}

//-----------------------------------------------------------------------------
// Read operations

/**
 * Create list size operation.
 * Server returns size of list.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_size(as_operations* ops, const char* name)
{
	return as_operations_list_size(ops, name, NULL);
}

/**
 * Create list get operation.
 * Server returns item at specified index in list bin.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get(as_operations* ops, const char* name, int64_t index)
{
	return as_operations_list_get(ops, name, NULL, index);
}

/**
 * Create list get range operation.
 * Server returns "count" items starting at specified index in list bin.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_range(
	as_operations* ops, const char* name, int64_t index, uint64_t count
	)
{
	return as_operations_list_get_range(ops, name, NULL, index, count);
}

/**
 * Create list get range operation.
 * Server returns items starting at index to the end of list.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_range_from(as_operations* ops, const char* name, int64_t index)
{
	return as_operations_list_get_range_from(ops, name, NULL, index);
}

/**
 * Create list get by value operation.
 * Server selects list items identified by value and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_value(
	as_operations* ops, const char* name, as_val* value, as_list_return_type return_type
	)
{
	return as_operations_list_get_by_value(ops, name, NULL, value, return_type);
}

/**
 * Create list get by value range operation.
 * Server selects list items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_value_range(
	as_operations* ops, const char* name, as_val* begin, as_val* end,
	as_list_return_type return_type
	)
{
	return as_operations_list_get_by_value_range(ops, name, NULL, begin, end, return_type);
}

/**
 * Create list get by value list operation.
 * Server selects list items identified by values and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_value_list(
	as_operations* ops, const char* name, as_list* values, as_list_return_type return_type
	)
{
	return as_operations_list_get_by_value_list(ops, name, NULL, values, return_type);
}

/**
 * Create list get by value relative to rank range operation.
 * Server selects list items nearest to value and greater by relative rank.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank) = [selected items]</li>
 * <li>(5,0) = [5,9,11,15]</li>
 * <li>(5,1) = [9,11,15]</li>
 * <li>(5,-1) = [4,5,9,11,15]</li>
 * <li>(3,0) = [4,5,9,11,15]</li>
 * <li>(3,3) = [11,15]</li>
 * <li>(3,-3) = [0,4,5,9,11,15]</li>
 * </ul>
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_val* value, int64_t rank,
	as_list_return_type return_type
	)
{
	return as_operations_list_get_by_value_rel_rank_range_to_end(ops, name, NULL, value, rank,
																 return_type);
}

/**
 * Create list get by value relative to rank range operation.
 * Server selects list items nearest to value and greater by relative rank with a count limit.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered list [0,4,5,9,11,15]:
 * <ul>
 * <li>(value,rank,count) = [selected items]</li>
 * <li>(5,0,2) = [5,9]</li>
 * <li>(5,1,1) = [9]</li>
 * <li>(5,-1,2) = [4,5]</li>
 * <li>(3,0,1) = [4]</li>
 * <li>(3,3,7) = [11,15]</li>
 * <li>(3,-3,2) = []</li>
 * </ul>
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_val* value, int64_t rank, uint64_t count,
	as_list_return_type return_type
	)
{
	return as_operations_list_get_by_value_rel_rank_range(ops, name, NULL, value, rank, count,
														  return_type);
}

/**
 * Create list get by index operation.
 * Server selects list item identified by index and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_index(
	as_operations* ops, const char* name, int64_t index, as_list_return_type return_type
	)
{
	return as_operations_list_get_by_index(ops, name, NULL, index, return_type);
}

/**
 * Create list get by index range operation.
 * Server selects list items starting at specified index to the end of list and returns selected
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_index_range_to_end(
	as_operations* ops, const char* name, int64_t index, as_list_return_type return_type
	)
{
	return as_operations_list_get_by_index_range_to_end(ops, name, NULL, index, return_type);
}

/**
 * Create list get by index range operation.
 * Server selects `count` list items starting at specified index and returns selected data specified
 * by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_index_range(
	as_operations* ops, const char* name, int64_t index, uint64_t count,
	as_list_return_type return_type
	)
{
	return as_operations_list_get_by_index_range(ops, name, NULL, index, count, return_type);
}

/**
 * Create list get by rank operation.
 * Server selects list item identified by rank and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_rank(
	as_operations* ops, const char* name, int64_t rank, as_list_return_type return_type
	)
{
	return as_operations_list_get_by_rank(ops, name, NULL, rank, return_type);
}

/**
 * Create list get by rank range operation.
 * Server selects list items starting at specified rank to the last ranked item and returns selected
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_rank_range_to_end(
	as_operations* ops, const char* name, int64_t rank, as_list_return_type return_type
	)
{
	return as_operations_list_get_by_rank_range_to_end(ops, name, NULL, rank, return_type);
}

/**
 * Create list get by rank range operation.
 * Server selects `count` list items starting at specified rank and returns selected data specified
 * by return_type.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_get_by_rank_range(
	as_operations* ops, const char* name, int64_t rank, uint64_t count,
	as_list_return_type return_type
	)
{
	return as_operations_list_get_by_rank_range(ops, name, NULL, rank, count, return_type);
}

#ifdef __cplusplus
} // end extern "C"
#endif
