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
 * @defgroup map_operations Map Operations
 * @ingroup client_operations
 *
 * Unique key map bin operations. Create map operations used by the client operate command.
 * The default unique key map is unordered. Valid map key types are:
 * <ul>
 * <li>as_string</li>
 * <li>as_integer</li>
 * <li>as_bytes</li>
 * </ul>
 *
 * The server will validate map key types in an upcoming release.
 *
 * All maps maintain an index and a rank.  The index is the item offset from the start of the map,
 * for both unordered and ordered maps.  The rank is the sorted index of the value component.
 * Map supports negative indexing for index and rank.
 *
 * Index examples:
 * <ul>
 * <li>Index 0: First item in map.</li>
 * <li>Index 4: Fifth item in map.</li>
 * <li>Index -1: Last item in map.</li>
 * <li>Index -3: Third to last item in map.</li>
 * <li>Index 1 Count 2: Second and third items in map.</li>
 * <li>Index -3 Count 3: Last three items in map.</li>
 * <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
 * </ul>
 *
 * Rank examples:
 * <ul>
 * <li>Rank 0: Item with lowest value rank in map.</li>
 * <li>Rank 4: Fifth lowest ranked item in map.</li>
 * <li>Rank -1: Item with highest ranked value in map.</li>
 * <li>Rank -3: Item with third highest ranked value in map.</li>
 * <li>Rank 1 Count 2: Second and third lowest ranked items in map.</li>
 * <li>Rank -3 Count 3: Top three ranked items in map.</li>
 * </ul>
 *
 * Code examples:
 *
 * ~~~~~~~~~~{.c}
 * // bin = {key1=11, key2=22, key3=33}
 * // Set map value to 11 for map key "key2".
 * as_operations ops;
 * as_operations_inita(&ops, 1);
 *
 * as_string key;
 * as_string_init(&key, "key2", false);
 * as_integer val;
 * as_integer_init(&val, 11);
 * as_operations_map_put(&ops, "bin", NULL, NULL, (as_val*)&key, (as_val*)&val);
 * 
 * as_record* rec = NULL;
 * as_error err;
 * aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec);
 * // bin result = {key1=11, key2=11, key3=33}
 * as_operations_destroy(&ops);
 * as_record_destroy(rec);
 * ~~~~~~~~~~
 *
 * Nested CDT operations are supported by optional context (as_cdt_ctx):
 *
 * ~~~~~~~~~~{.c}
 * // bin = {key1={key11=9,key12=4}, key2={key21=3,key22=5}}
 * // Set map value to 11 for map key "key21" inside of map key "key2".
 * as_cdt_ctx ctx;
 * as_cdt_ctx_inita(&ctx, 1);
 * as_string str;
 * as_string_init(&str, "key2", false);
 * as_cdt_ctx_add_map_key(&ctx, (as_val*)&str);
 *
 * as_operations ops;
 * as_operations_inita(&ops, 1);
 *
 * as_string key;
 * as_string_init(&key, "key21", false);
 * as_integer val;
 * as_integer_init(&val, 11);
 * as_operations_map_put(&ops, "bin", &ctx, NULL, (as_val*)&key, (as_val*)&val);
 * 
 * as_record* rec = NULL;
 * as_error err;
 * aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec);
 * // bin result = {key1={key11=9,key12=4},key2={key21=11,key22=5}}
 * as_operations_destroy(&ops);
 * as_record_destroy(rec);
 * ~~~~~~~~~~
 */

#include <aerospike/as_cdt_order.h>
#include <aerospike/as_operations.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Map write mode.
 * This enum should only be used for server versions < 4.3.
 * as_map_write_flags is recommended for server versions >= 4.3.
 *
 * @ingroup map_operations
 */
typedef enum as_map_write_mode_e {
	/**
	 * If the key already exists, the item will be overwritten.
	 * If the key does not exist, a new item will be created.
	 */
	AS_MAP_UPDATE,
	
	/**
	 * If the key already exists, the item will be overwritten.
	 * If the key does not exist, the write will fail.
	 */
	AS_MAP_UPDATE_ONLY,
	
	/**
	 * If the key already exists, the write will fail.
	 * If the key does not exist, a new item will be created.
	 */
	AS_MAP_CREATE_ONLY
} as_map_write_mode;

/**
 * Map write bit flags.
 * Requires server versions >= 4.3.
 *
 * @ingroup map_operations
 */
typedef enum as_map_write_flags_e {
	/**
	 * Default.  Allow create or update.
	 */
	AS_MAP_WRITE_DEFAULT = 0,

	/**
	 * If the key already exists, the item will be denied.
	 * If the key does not exist, a new item will be created.
	 */
	AS_MAP_WRITE_CREATE_ONLY = 1,

	/**
	 * If the key already exists, the item will be overwritten.
	 * If the key does not exist, the item will be denied.
	 */
	AS_MAP_WRITE_UPDATE_ONLY = 2,

	/**
	 * Do not raise error if a map item is denied due to write flag constraints.
	 */
	AS_MAP_WRITE_NO_FAIL = 4,

	/**
	 * Allow other valid map items to be committed if a map item is denied due to
	 * write flag constraints.
	 */
	AS_MAP_WRITE_PARTIAL = 8
} as_map_write_flags;

/**
 * Map policy directives when creating a map and writing map items.
 *
 * @ingroup map_operations
 */
typedef struct as_map_policy_s {
	uint64_t attributes;
	uint64_t flags;
	int item_command;
	int items_command;
} as_map_policy;
	
/**
 * Map return type. Type of data to return when selecting or removing items from the map.
 *
 * @ingroup map_operations
 */
typedef enum as_map_return_type_e {
	/**
	 * Do not return a result.
	 */
	AS_MAP_RETURN_NONE = 0,
	
	/**
	 * Return key index order.
	 */
	AS_MAP_RETURN_INDEX = 1,
	
	/**
	 * Return reverse key order.
	 */
	AS_MAP_RETURN_REVERSE_INDEX = 2,
	
	/**
	 * Return value order.
	 */
	AS_MAP_RETURN_RANK = 3,
	
	/**
	 * Return reverse value order.
	 */
	AS_MAP_RETURN_REVERSE_RANK = 4,
	
	/**
	 * Return count of items selected.
	 */
	AS_MAP_RETURN_COUNT = 5,
	
	/**
	 * Return key for single key read and key list for range read.
	 */
	AS_MAP_RETURN_KEY = 6,
	
	/**
	 * Return value for single key read and value list for range read.
	 */
	AS_MAP_RETURN_VALUE	= 7,
	
	/**
	 * Return key/value items.
	 */
	AS_MAP_RETURN_KEY_VALUE	= 8,

	/**
	 * Return true if count > 0.
	 */
	AS_MAP_RETURN_EXISTS = 13,

	/**
	 * Return an unordered map.
	 */
	AS_MAP_RETURN_UNORDERED_MAP = 16,

	/**
	 * Return an ordered map.
	 */
	AS_MAP_RETURN_ORDERED_MAP = 17,

	/**
	 * Invert meaning of map command and return values.  For example:
	 *
	 * ~~~~~~~~~~{.c}
	 * as_operations ops;
	 * as_operations_inita(&ops, 1);
	 *
	 * as_operations_add_map_remove_by_key_range(&ops, BIN_NAME, (as_val*)&mkey1, (as_val*)&mkey2,
	 *                                           AS_MAP_RETURN_KEY | AS_MAP_RETURN_INVERTED);
	 *
	 * as_record* rec = NULL;
	 * as_status status = aerospike_key_operate(as, &err, NULL, &key, &ops, &rec);
	 * as_operations_destroy(&ops);
	 * ~~~~~~~~~~
	 *
	 * With AS_MAP_RETURN_INVERTED enabled, the keys outside of the specified key range will be
	 * removed and returned.
	 */
	AS_MAP_RETURN_INVERTED = 0x10000,
} as_map_return_type;
	
/**
 * @private
 * Map operation codes.
 */
typedef enum {
	AS_CDT_OP_MAP_SET_TYPE = 64,
	AS_CDT_OP_MAP_ADD = 65,
	AS_CDT_OP_MAP_ADD_ITEMS = 66,
	AS_CDT_OP_MAP_PUT = 67,
	AS_CDT_OP_MAP_PUT_ITEMS = 68,
	AS_CDT_OP_MAP_REPLACE = 69,
	AS_CDT_OP_MAP_REPLACE_ITEMS = 70,
	AS_CDT_OP_MAP_INCREMENT = 73,
	AS_CDT_OP_MAP_DECREMENT = 74,
	AS_CDT_OP_MAP_CLEAR = 75,
	AS_CDT_OP_MAP_REMOVE_BY_KEY = 76,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX = 77,
	AS_CDT_OP_MAP_REMOVE_BY_RANK = 79,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST = 81,
	AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE = 82,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST = 83,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL = 84,
	AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE = 85,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL = 86,
	AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE = 87,
	AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE = 88,
	AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE = 89,
	AS_CDT_OP_MAP_SIZE = 96,
	AS_CDT_OP_MAP_GET_BY_KEY = 97,
	AS_CDT_OP_MAP_GET_BY_INDEX = 98,
	AS_CDT_OP_MAP_GET_BY_RANK = 100,
	AS_CDT_OP_MAP_GET_ALL_BY_VALUE = 102,
	AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL = 103,
	AS_CDT_OP_MAP_GET_BY_INDEX_RANGE = 104,
	AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL = 105,
	AS_CDT_OP_MAP_GET_BY_RANK_RANGE = 106,
	AS_CDT_OP_MAP_GET_BY_KEY_LIST = 107,
	AS_CDT_OP_MAP_GET_BY_VALUE_LIST = 108,
	AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE = 109,
	AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE = 110
} as_cdt_op_map;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/
	
/**
 * Initialize map attributes to default unordered map with standard overwrite semantics.
 *
 * @ingroup map_operations
 */
AS_EXTERN void
as_map_policy_init(as_map_policy* policy);

/**
 * Set map attributes to specified map order and write mode semantics.
 *
 * This function should only be used for server versions < 4.3.
 * #as_map_policy_set_flags is recommended for server versions >= 4.3.
 *
 * @ingroup map_operations
 */
AS_EXTERN void
as_map_policy_set(as_map_policy* policy, as_map_order order, as_map_write_mode mode);

/**
 * Set map attributes to specified map order and write flags (See as_map_write_flags).
 *
 * @ingroup map_operations
 */
AS_EXTERN void
as_map_policy_set_flags(as_map_policy* policy, as_map_order order, uint32_t flags);

/**
 * Set map attributes to specified map order, write flags and whether to persist the map index.
 *
 * @param policy		Target map policy.
 * @param order			Map order.
 * @param flags			Map write flags. See as_map_write_flags.
 * @param persist_index	If true, persist map index. A map index improves lookup performance,
 * 						but requires more storage. A map index can be created for a top-level
 * 						ordered map only. Nested and unordered map indexes are not supported.
 *
 * @ingroup map_operations
 */
AS_EXTERN void
as_map_policy_set_all(as_map_policy* policy, as_map_order order, uint32_t flags, bool persist_index);

/**
 * Create map create operation.
 * Server creates map at given context level.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_create(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_order order
	);

/**
 * Create map create operation.
 * Server creates map at given context level.
 *
 * @param ops			Target operations list.
 * @param name			Bin name.
 * @param ctx			Optional path to nested map. If not defined, the top-level map is used.
 * @param order			Map order.
 * @param persist_index	If true, persist map index. A map index improves lookup performance,
 * 						but requires more storage. A map index can be created for a top-level
 * 						ordered map only. Nested and unordered map indexes are not supported.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_create_all(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_order order, bool persist_index
	);

/**
 * Create set map policy operation.
 * Server sets map policy attributes.  Server does not return a value.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_set_policy(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy
	);

/**
 * Create map put operation.
 * Server writes key/value item to map bin and returns map size.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * This function takes ownership and frees heap memory associated with key/value parameters.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_put(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy,
	as_val* key, as_val* value
	);

/**
 * Create map put items operation.
 * Server writes each map item to map bin and returns map size.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * This function takes ownership and frees heap memory associated with items parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_put_items(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy,
	as_map* items
	);

/**
 * Create map increment operation.
 * Server increments values by incr for all items identified by key and returns final result.
 * Valid only for numbers.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * This function takes ownership and frees heap memory associated with key/value parameters.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_increment(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy, as_val* key,
	as_val* value
	);

/**
 * Create map decrement operation.
 * Server decrement values by decr for all items identified by key and returns final result.
 * Valid only for numbers.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * This function takes ownership and frees heap memory associated with key/value parameters.
 *
 * @deprecated Use as_operations_map_increment() with negative value instead.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_decrement(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_map_policy* policy, as_val* key,
	as_val* value
	);

/**
 * Create map clear operation.
 * Server removes all items in map.  Server returns null.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_clear(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create map remove operation.
 * Server removes map item identified by key and returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with key parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_key(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map items identified by keys and returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with keys parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_key_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* keys,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map items identified by key range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with begin/end parameters.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_key_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	);

/**
 * Create map remove by key relative to index range operation.
 * Server removes map items nearest to key and greater by index.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index) = [removed items]</li>
 * <li>(5,0) = [{5=15},{9=10}]</li>
 * <li>(5,1) = [{9=10}]</li>
 * <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
 * <li>(3,2) = [{9=10}]</li>
 * <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with key parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_key_rel_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	as_map_return_type return_type
	);

/**
 * Create map remove by key relative to index range operation.
 * Server removes map items nearest to key and greater by index with a count limit.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index,count) = [removed items]</li>
 * <li>(5,0,1) = [{5=15}]</li>
 * <li>(5,1,2) = [{9=10}]</li>
 * <li>(5,-1,1) = [{4=2}]</li>
 * <li>(3,2,1) = [{9=10}]</li>
 * <li>(3,-2,2) = [{0=17}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with key parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_key_rel_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	uint64_t count, as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map items identified by value and returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map items identified by values and returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with values parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns removed data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with begin/end parameters.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	);

/**
 * Create map remove by value relative to rank range operation.
 * Server removes map items nearest to value and greater by relative rank.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank) = [removed items]</li>
 * <li>(11,1) = [{0=17}]</li>
 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_map_return_type return_type
	);

/**
 * Create map remove by value relative to rank range operation.
 * Server removes map items nearest to value and greater by relative rank with a count limit.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank,count) = [removed items]</li>
 * <li>(11,1,1) = [{0=17}]</li>
 * <li>(11,-1,1) = [{9=10}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map item identified by index and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map items starting at specified index to the end of map and returns removed
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes `count` map items starting at specified index and returns removed data specified
 * by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map item identified by rank and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes map items starting at specified rank to the last ranked item and returns removed
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	);

/**
 * Create map remove operation.
 * Server removes `count` map items starting at specified rank and returns removed data specified by
 * return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_remove_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_map_return_type return_type
	);

/**
 * Create map size operation.
 * Server returns size of map.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_size(as_operations* ops, const char* name, as_cdt_ctx* ctx);

/**
 * Create map get by key operation.
 * Server selects map item identified by key and returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with key parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_key(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key,
	as_map_return_type return_type
	);

/**
 * Create map get by key range operation.
 * Server selects map items identified by key range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with begin/end parameters.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_key_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	);

/**
 * Create map get by key list operation.
 * Server selects map items identified by keys and returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with keys parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_key_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* keys,
	as_map_return_type return_type
	);

/**
 * Create map get by key relative to index range operation.
 * Server selects map items nearest to key and greater by index.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index) = [selected items]</li>
 * <li>(5,0) = [{5=15},{9=10}]</li>
 * <li>(5,1) = [{9=10}]</li>
 * <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
 * <li>(3,2) = [{9=10}]</li>
 * <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with key parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_key_rel_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	as_map_return_type return_type
	);

/**
 * Create map get by key relative to index range operation.
 * Server selects map items nearest to key and greater by index with a count limit.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index,count) = [selected items]</li>
 * <li>(5,0,1) = [{5=15}]</li>
 * <li>(5,1,2) = [{9=10}]</li>
 * <li>(5,-1,1) = [{4=2}]</li>
 * <li>(3,2,1) = [{9=10}]</li>
 * <li>(3,-2,2) = [{0=17}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with key parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_key_rel_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* key, int64_t index,
	uint64_t count, as_map_return_type return_type
	);

/**
 * Create map get by value operation.
 * Server selects map items identified by value and returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_value(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value,
	as_map_return_type return_type
	);

/**
 * Create map get by value range operation.
 * Server selects map items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with begin/end parameters.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_value_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* begin, as_val* end,
	as_map_return_type return_type
	);

/**
 * Create map get by value list operation.
 * Server selects map items identified by values and returns selected data specified by return_type.
 *
 * This function takes ownership and frees heap memory associated with values parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_value_list(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_list* values,
	as_map_return_type return_type
	);

/**
 * Create map get by value relative to rank range operation.
 * Server selects map items nearest to value and greater by relative rank.
 * Server returns selected data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank) = [selected items]</li>
 * <li>(11,1) = [{0=17}]</li>
 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	as_map_return_type return_type
	);

/**
 * Create map get by value relative to rank range operation.
 * Server selects map items nearest to value and greater by relative rank with a count limit.
 * Server returns selected data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank,count) = [selected items]</li>
 * <li>(11,1,1) = [{0=17}]</li>
 * <li>(11,-1,1) = [{9=10}]</li>
 * </ul>
 *
 * This function takes ownership and frees heap memory associated with value parameter.
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_val* value, int64_t rank,
	uint64_t count, as_map_return_type return_type
	);

/**
 * Create map get by index operation.
 * Server selects map item identified by index and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_index(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	);

/**
 * Create map get by index range operation.
 * Server selects map items starting at specified index to the end of map and returns selected
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_index_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index,
	as_map_return_type return_type
	);

/**
 * Create map get by index range operation.
 * Server selects `count` map items starting at specified index and returns selected data specified
 * by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_index_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t index, uint64_t count,
	as_map_return_type return_type
	);

/**
 * Create map get by rank operation.
 * Server selects map item identified by rank and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_rank(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	);

/**
 * Create map get by rank range operation.
 * Server selects map items starting at specified rank to the last ranked item and returns selected
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_rank_range_to_end(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank,
	as_map_return_type return_type
	);

/**
 * Create map get by rank range operation.
 * Server selects `count` map items starting at specified rank and returns selected data specified
 * by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_map_get_by_rank_range(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int64_t rank, uint64_t count,
	as_map_return_type return_type
	);

/******************************************************************************
 * LEGACY FUNCTIONS
 *****************************************************************************/

/**
 * Create set map policy operation.
 * Server sets map policy attributes.  Server does not return a value.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_set_policy(as_operations* ops, const char* name, as_map_policy* policy)
{
	return as_operations_map_set_policy(ops, name, NULL, policy);
}

/**
 * Create map put operation.
 * Server writes key/value item to map bin and returns map size.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_put(
	as_operations* ops, const char* name, as_map_policy* policy, as_val* key, as_val* value
	)
{
	return as_operations_map_put(ops, name, NULL, policy, key, value);
}

/**
 * Create map put items operation.
 * Server writes each map item to map bin and returns map size.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_put_items(
	as_operations* ops, const char* name, as_map_policy* policy, as_map* items
	)
{
	return as_operations_map_put_items(ops, name, NULL, policy, items);
}

/**
 * Create map increment operation.
 * Server increments values by incr for all items identified by key and returns final result.
 * Valid only for numbers.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_increment(
	as_operations* ops, const char* name, as_map_policy* policy, as_val* key, as_val* value
	)
{
	return as_operations_map_increment(ops, name, NULL, policy, key, value);
}

/**
 * Create map decrement operation.
 * Server decrement values by decr for all items identified by key and returns final result.
 * Valid only for numbers.
 *
 * The required map policy dictates the type of map to create when it does not exist.
 * The map policy also specifies the mode used when writing items to the map.
 * See `as_map_policy` and `as_map_write_mode`.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_decrement(
	as_operations* ops, const char* name, as_map_policy* policy, as_val* key, as_val* value
	)
{
	return as_operations_map_decrement(ops, name, NULL, policy, key, value);
}

/**
 * Create map clear operation.
 * Server removes all items in map.  Server returns null.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_clear(as_operations* ops, const char* name)
{
	return as_operations_map_clear(ops, name, NULL);
}

/**
 * Create map remove operation.
 * Server removes map item identified by key and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_key(
	as_operations* ops, const char* name, as_val* key, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_key(ops, name, NULL, key, return_type);
}

/**
 * Create map remove operation.
 * Server removes map items identified by keys and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_key_list(
	as_operations* ops, const char* name, as_list* keys, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_key_list(ops, name, NULL, keys, return_type);
}

/**
 * Create map remove operation.
 * Server removes map items identified by key range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_key_range(
	as_operations* ops, const char* name, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_key_range(ops, name, NULL, begin, end, return_type);
}

/**
 * Create map remove by key relative to index range operation.
 * Server removes map items nearest to key and greater by index.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index) = [removed items]</li>
 * <li>(5,0) = [{5=15},{9=10}]</li>
 * <li>(5,1) = [{9=10}]</li>
 * <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
 * <li>(3,2) = [{9=10}]</li>
 * <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_key_rel_index_range_to_end(
	as_operations* ops, const char* name, as_val* key, int64_t index,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_key_rel_index_range_to_end(ops, name, NULL, key, index, return_type);
}

/**
 * Create map remove by key relative to index range operation.
 * Server removes map items nearest to key and greater by index with a count limit.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index,count) = [removed items]</li>
 * <li>(5,0,1) = [{5=15}]</li>
 * <li>(5,1,2) = [{9=10}]</li>
 * <li>(5,-1,1) = [{4=2}]</li>
 * <li>(3,2,1) = [{9=10}]</li>
 * <li>(3,-2,2) = [{0=17}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_key_rel_index_range(
	as_operations* ops, const char* name, as_val* key, int64_t index, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_key_rel_index_range(ops, name, NULL, key, index, count,
														   return_type);
}

/**
 * Create map remove operation.
 * Server removes map items identified by value and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_value(
	as_operations* ops, const char* name, as_val* value, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_value(ops, name, NULL, value, return_type);
}

/**
 * Create map remove operation.
 * Server removes map items identified by values and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_value_list(
	as_operations* ops, const char* name, as_list* values, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_value_list(ops, name, NULL, values, return_type);
}

/**
 * Create map remove operation.
 * Server removes map items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_value_range(
	as_operations* ops, const char* name, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_value_range(ops, name, NULL, begin, end, return_type);
}

/**
 * Create map remove by value relative to rank range operation.
 * Server removes map items nearest to value and greater by relative rank.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank) = [removed items]</li>
 * <li>(11,1) = [{0=17}]</li>
 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_val* value, int64_t rank,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_value_rel_rank_range_to_end(ops, name, NULL, value, rank,
																   return_type);
}

/**
 * Create map remove by value relative to rank range operation.
 * Server removes map items nearest to value and greater by relative rank with a count limit.
 * Server returns removed data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank,count) = [removed items]</li>
 * <li>(11,1,1) = [{0=17}]</li>
 * <li>(11,-1,1) = [{9=10}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_val* value, int64_t rank, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_value_rel_rank_range(ops, name, NULL, value, rank, count,
															return_type);
}

/**
 * Create map remove operation.
 * Server removes map item identified by index and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_index(
	as_operations* ops, const char* name, int64_t index, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_index(ops, name, NULL, index, return_type);
}

/**
 * Create map remove operation.
 * Server removes map items starting at specified index to the end of map and returns removed
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_index_range_to_end(
	as_operations* ops, const char* name, int64_t index, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_index_range_to_end(ops, name, NULL, index, return_type);
}

/**
 * Create map remove operation.
 * Server removes `count` map items starting at specified index and returns removed data specified
 * by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_index_range(
	as_operations* ops, const char* name, int64_t index, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_index_range(ops, name, NULL, index, count, return_type);
}

/**
 * Create map remove operation.
 * Server removes map item identified by rank and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_rank(
	as_operations* ops, const char* name, int64_t rank, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_rank(ops, name, NULL, rank, return_type);
}

/**
 * Create map remove operation.
 * Server removes map items starting at specified rank to the last ranked item and returns removed
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_rank_range_to_end(
	as_operations* ops, const char* name, int64_t rank, as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_rank_range_to_end(ops, name, NULL, rank, return_type);
}

/**
 * Create map remove operation.
 * Server removes `count` map items starting at specified rank and returns removed data specified by
 * return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_remove_by_rank_range(
	as_operations* ops, const char* name, int64_t rank, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_remove_by_rank_range(ops, name, NULL, rank, count, return_type);
}

/**
 * Create map size operation.
 * Server returns size of map.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_size(as_operations* ops, const char* name)
{
	return as_operations_map_size(ops, name, NULL);
}

/**
 * Create map get by key operation.
 * Server selects map item identified by key and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_key(
	as_operations* ops, const char* name, as_val* key, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_key(ops, name, NULL, key, return_type);
}

/**
 * Create map get by key range operation.
 * Server selects map items identified by key range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_key_range(
	as_operations* ops, const char* name, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_key_range(ops, name, NULL, begin, end, return_type);
}

/**
 * Create map get by key list operation.
 * Server selects map items identified by keys and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_key_list(
	as_operations* ops, const char* name, as_list* keys, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_key_list(ops, name, NULL, keys, return_type);
}

/**
 * Create map get by key relative to index range operation.
 * Server selects map items nearest to key and greater by index.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index) = [selected items]</li>
 * <li>(5,0) = [{5=15},{9=10}]</li>
 * <li>(5,1) = [{9=10}]</li>
 * <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
 * <li>(3,2) = [{9=10}]</li>
 * <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_key_rel_index_range_to_end(
	as_operations* ops, const char* name, as_val* key, int64_t index,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_key_rel_index_range_to_end(ops, name, NULL, key, index,
															   return_type);
}

/**
 * Create map get by key relative to index range operation.
 * Server selects map items nearest to key and greater by index with a count limit.
 * Server returns selected data specified by return_type.
 *
 * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
 * <ul>
 * <li>(value,index,count) = [selected items]</li>
 * <li>(5,0,1) = [{5=15}]</li>
 * <li>(5,1,2) = [{9=10}]</li>
 * <li>(5,-1,1) = [{4=2}]</li>
 * <li>(3,2,1) = [{9=10}]</li>
 * <li>(3,-2,2) = [{0=17}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_key_rel_index_range(
	as_operations* ops, const char* name, as_val* key, int64_t index, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_key_rel_index_range(ops, name, NULL, key, index, count,
														return_type);
}

/**
 * Create map get by value operation.
 * Server selects map items identified by value and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_value(
	as_operations* ops, const char* name, as_val* value, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_value(ops, name, NULL, value, return_type);
}

/**
 * Create map get by value range operation.
 * Server selects map items identified by value range (begin inclusive, end exclusive).
 * If begin is null, the range is less than end.
 * If end is null, the range is greater than equal to begin.
 *
 * Server returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_value_range(
	as_operations* ops, const char* name, as_val* begin, as_val* end,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_value_range(ops, name, NULL, begin, end, return_type);
}

/**
 * Create map get by value list operation.
 * Server selects map items identified by values and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_value_list(
	as_operations* ops, const char* name, as_list* values, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_value_list(ops, name, NULL, values, return_type);
}

/**
 * Create map get by value relative to rank range operation.
 * Server selects map items nearest to value and greater by relative rank.
 * Server returns selected data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank) = [selected items]</li>
 * <li>(11,1) = [{0=17}]</li>
 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_value_rel_rank_range_to_end(
	as_operations* ops, const char* name, as_val* value, int64_t rank,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_value_rel_rank_range_to_end(ops, name, NULL, value, rank,
																return_type);
}

/**
 * Create map get by value relative to rank range operation.
 * Server selects map items nearest to value and greater by relative rank with a count limit.
 * Server returns selected data specified by return_type.
 *
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank,count) = [selected items]</li>
 * <li>(11,1,1) = [{0=17}]</li>
 * <li>(11,-1,1) = [{9=10}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_value_rel_rank_range(
	as_operations* ops, const char* name, as_val* value, int64_t rank, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_value_rel_rank_range(ops, name, NULL, value, rank, count,
														 return_type);
}

/**
 * Create map get by index operation.
 * Server selects map item identified by index and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_index(
	as_operations* ops, const char* name, int64_t index, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_index(ops, name, NULL, index, return_type);
}

/**
 * Create map get by index range operation.
 * Server selects map items starting at specified index to the end of map and returns selected
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_index_range_to_end(
	as_operations* ops, const char* name, int64_t index, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_index_range_to_end(ops, name, NULL, index, return_type);
}

/**
 * Create map get by index range operation.
 * Server selects `count` map items starting at specified index and returns selected data specified
 * by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_index_range(
	as_operations* ops, const char* name, int64_t index, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_index_range(ops, name, NULL, index, count, return_type);
}

/**
 * Create map get by rank operation.
 * Server selects map item identified by rank and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_rank(
	as_operations* ops, const char* name, int64_t rank, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_rank(ops, name, NULL, rank, return_type);
}

/**
 * Create map get by rank range operation.
 * Server selects map items starting at specified rank to the last ranked item and returns selected
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_rank_range_to_end(
	as_operations* ops, const char* name, int64_t rank, as_map_return_type return_type
	)
{
	return as_operations_map_get_by_rank_range_to_end(ops, name, NULL, rank, return_type);
}

/**
 * Create map get by rank range operation.
 * Server selects `count` map items starting at specified rank and returns selected data specified
 * by return_type.
 *
 * @ingroup map_operations
 */
static inline bool
as_operations_add_map_get_by_rank_range(
	as_operations* ops, const char* name, int64_t rank, uint64_t count,
	as_map_return_type return_type
	)
{
	return as_operations_map_get_by_rank_range(ops, name, NULL, rank, count, return_type);
}

#ifdef __cplusplus
} // end extern "C"
#endif
