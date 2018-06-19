/*
 * Copyright 2008-2018 Aerospike, Inc.
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
 * Aerospike operations on a map bin.
 */

#include <aerospike/as_operations.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Map storage order.
 *
 * @ingroup map_operations
 */
typedef enum as_map_order_e {
	/**
	 * Map is not ordered.  This is the default.
	 */
	AS_MAP_UNORDERED = 0,
	
	/**
	 * Order map by key.
	 */
	AS_MAP_KEY_ORDERED = 1,
	
	/**
	 * Order map by key, then value.
	 */
	AS_MAP_KEY_VALUE_ORDERED = 3
} as_map_order;

/**
 * Map write mode.
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
 * Map policy directives when creating a map and writing map items.
 *
 * @ingroup map_operations
 */
typedef struct as_map_policy_s {
	uint64_t attributes;
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
	 * Return reserve value order.
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
 * @ingroup map_operations
 */
AS_EXTERN void
as_map_policy_set(as_map_policy* policy, as_map_order order, as_map_write_mode mode);

/**
 * Create set map policy operation.
 * Server sets map policy attributes.  Server does not return a value.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_set_policy(as_operations* ops, const as_bin_name name, as_map_policy* policy);

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
AS_EXTERN bool
as_operations_add_map_put(as_operations* ops, const as_bin_name name, as_map_policy* policy, as_val* key, as_val* value);

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
AS_EXTERN bool
as_operations_add_map_put_items(as_operations* ops, const as_bin_name name, as_map_policy* policy, as_map *items);

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
AS_EXTERN bool
as_operations_add_map_increment(as_operations* ops, const as_bin_name name, as_map_policy* policy, as_val* key, as_val* value);

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
AS_EXTERN bool
as_operations_add_map_decrement(as_operations* ops, const as_bin_name name, as_map_policy* policy, as_val* key, as_val* value);

/**
 * Create map clear operation.
 * Server removes all items in map.  Server returns null.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_clear(as_operations* ops, const as_bin_name name);

/**
 * Create map remove operation.
 * Server removes map item identified by key and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_key(as_operations* ops, const as_bin_name name, as_val* key, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes map items identified by keys and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_key_list(as_operations* ops, const as_bin_name name, as_list* keys, as_map_return_type return_type);

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
AS_EXTERN bool
as_operations_add_map_remove_by_key_range(as_operations* ops, const as_bin_name name, as_val* begin, as_val* end, as_map_return_type return_type);

/**
 * Create map remove by key relative to index range operation.
 * Server removes map items nearest to key and greater by index.
 * Server returns removed data specified by return_type.
 * <p>
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
AS_EXTERN bool
as_operations_add_map_remove_by_key_rel_index_range_to_end(as_operations* ops, const as_bin_name name, as_val* key, int64_t index, as_map_return_type return_type);

/**
 * Create map remove by key relative to index range operation.
 * Server removes map items nearest to key and greater by index with a count limit.
 * Server returns removed data specified by return_type.
 * <p>
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
AS_EXTERN bool
as_operations_add_map_remove_by_key_rel_index_range(as_operations* ops, const as_bin_name name, as_val* key, int64_t index, uint64_t count, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes map items identified by value and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_value(as_operations* ops, const as_bin_name name, as_val* value, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes map items identified by values and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_value_list(as_operations* ops, const as_bin_name name, as_list* values, as_map_return_type return_type);

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
AS_EXTERN bool
as_operations_add_map_remove_by_value_range(as_operations* ops, const as_bin_name name, as_val* begin, as_val* end, as_map_return_type return_type);

/**
 * Create map remove by value relative to rank range operation.
 * Server removes map items nearest to value and greater by relative rank.
 * Server returns removed data specified by return_type.
 * <p>
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank) = [removed items]</li>
 * <li>(11,1) = [{0=17}]</li>
 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_value_rel_rank_range_to_end(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, as_map_return_type return_type);

/**
 * Create map remove by value relative to rank range operation.
 * Server removes map items nearest to value and greater by relative rank with a count limit.
 * Server returns removed data specified by return_type.
 * <p>
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank,count) = [removed items]</li>
 * <li>(11,1,1) = [{0=17}]</li>
 * <li>(11,-1,1) = [{9=10}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_value_rel_rank_range(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, uint64_t count, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes map item identified by index and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_index(as_operations* ops, const as_bin_name name, int64_t index, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes map items starting at specified index to the end of map and returns removed
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_index_range_to_end(as_operations* ops, const as_bin_name name, int64_t index, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes `count` map items starting at specified index and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_index_range(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes map item identified by rank and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_rank(as_operations* ops, const as_bin_name name, int64_t rank, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes map items starting at specified rank to the last ranked item and returns removed
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_rank_range_to_end(as_operations* ops, const as_bin_name name, int64_t rank, as_map_return_type return_type);

/**
 * Create map remove operation.
 * Server removes `count` map items starting at specified rank and returns removed data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_remove_by_rank_range(as_operations* ops, const as_bin_name name, int64_t rank, uint64_t count, as_map_return_type return_type);

/**
 * Create map size operation.
 * Server returns size of map.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_size(as_operations* ops, const as_bin_name name);

/**
 * Create map get by key operation.
 * Server selects map item identified by key and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_key(as_operations* ops, const as_bin_name name, as_val* key, as_map_return_type return_type);

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
AS_EXTERN bool
as_operations_add_map_get_by_key_range(as_operations* ops, const as_bin_name name, as_val* begin, as_val* end, as_map_return_type return_type);

/**
 * Create map get by key list operation.
 * Server selects map items identified by keys and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_key_list(as_operations* ops, const as_bin_name name, as_list* keys, as_map_return_type return_type);

/**
 * Create map get by key relative to index range operation.
 * Server selects map items nearest to key and greater by index.
 * Server returns selected data specified by return_type.
 * <p>
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
AS_EXTERN bool
as_operations_add_map_get_by_key_rel_index_range_to_end(as_operations* ops, const as_bin_name name, as_val* key, int64_t index, as_map_return_type return_type);

/**
 * Create map get by key relative to index range operation.
 * Server selects map items nearest to key and greater by index with a count limit.
 * Server returns selected data specified by return_type.
 * <p>
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
AS_EXTERN bool
as_operations_add_map_get_by_key_rel_index_range(as_operations* ops, const as_bin_name name, as_val* key, int64_t index, uint64_t count, as_map_return_type return_type);

/**
 * Create map get by value operation.
 * Server selects map items identified by value and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_value(as_operations* ops, const as_bin_name name, as_val* value, as_map_return_type return_type);

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
AS_EXTERN bool
as_operations_add_map_get_by_value_range(as_operations* ops, const as_bin_name name, as_val* begin, as_val* end, as_map_return_type return_type);

/**
 * Create map get by value list operation.
 * Server selects map items identified by values and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_value_list(as_operations* ops, const as_bin_name name, as_list* values, as_map_return_type return_type);

/**
 * Create map get by value relative to rank range operation.
 * Server selects map items nearest to value and greater by relative rank.
 * Server returns selected data specified by return_type.
 * <p>
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank) = [selected items]</li>
 * <li>(11,1) = [{0=17}]</li>
 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_value_rel_rank_range_to_end(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, as_map_return_type return_type);

/**
 * Create map get by value relative to rank range operation.
 * Server selects map items nearest to value and greater by relative rank with a count limit.
 * Server returns selected data specified by return_type.
 * <p>
 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
 * <ul>
 * <li>(value,rank,count) = [selected items]</li>
 * <li>(11,1,1) = [{0=17}]</li>
 * <li>(11,-1,1) = [{9=10}]</li>
 * </ul>
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_value_rel_rank_range(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, uint64_t count, as_map_return_type return_type);

/**
 * Create map get by index operation.
 * Server selects map item identified by index and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_index(as_operations* ops, const as_bin_name name, int64_t index, as_map_return_type return_type);

/**
 * Create map get by index range operation.
 * Server selects map items starting at specified index to the end of map and returns selected
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_index_range_to_end(as_operations* ops, const as_bin_name name, int64_t index, as_map_return_type return_type);

/**
 * Create map get by index range operation.
 * Server selects `count` map items starting at specified index and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_index_range(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count, as_map_return_type return_type);

/**
 * Create map get by rank operation.
 * Server selects map item identified by rank and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_rank(as_operations* ops, const as_bin_name name, int64_t rank, as_map_return_type return_type);

/**
 * Create map get by rank range operation.
 * Server selects map items starting at specified rank to the last ranked item and returns selected
 * data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_rank_range_to_end(as_operations* ops, const as_bin_name name, int64_t rank, as_map_return_type return_type);

/**
 * Create map get by rank range operation.
 * Server selects `count` map items starting at specified rank and returns selected data specified by return_type.
 *
 * @ingroup map_operations
 */
AS_EXTERN bool
as_operations_add_map_get_by_rank_range(as_operations* ops, const as_bin_name name, int64_t rank, uint64_t count, as_map_return_type return_type);

#ifdef __cplusplus
} // end extern "C"
#endif
