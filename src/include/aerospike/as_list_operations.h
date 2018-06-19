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
 * @defgroup list_operations List Operations
 * @ingroup client_operations
 *
 * Aerospike operations on a list bin.
 */

#include <aerospike/as_operations.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * List storage order.
 *
 * @ingroup list_operations
 */
typedef enum as_list_order_e {
	/**
	 * List is not ordered.  This is the default.
	 */
	AS_LIST_UNORDERED = 0,

	/**
	 * List is ordered.
	 */
	AS_LIST_ORDERED = 1,
} as_list_order;

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
 * List write flags.
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
	 * Return reserve value order.
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
 * Create set list order operation.
 * Server sets list order.  Server returns null.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_set_order(as_operations* ops, const as_bin_name name, as_list_order order);

/**
 * Create list sort operation.
 * Server sorts list according to flags.
 * Server does not return a result by default.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_sort(as_operations* ops, const as_bin_name name, as_list_sort_flags flags);
	
/**
 * Add an as_val element to end of list.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param val		Value to append. Consumes a reference of this as_val.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append(as_operations* ops, const as_bin_name name, as_val* val);

/**
 * Add an as_val element to end of list with policy.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param policy 	The list write flags.
 * @param val		Value to append. Consumes a reference of this as_val.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append_with_policy(as_operations* ops, const as_bin_name name, as_list_policy* policy, as_val* val);

/**
 * Add an integer to end of list. Convenience function of as_operations_add_list_append()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param value		An integer value.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append_int64(as_operations* ops, const as_bin_name name, int64_t value);

/**
 * Add a double to end of list. Convenience function of as_operations_add_list_append()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param value		A double value.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append_double(as_operations* ops, const as_bin_name name, double value);

/**
 * Add a string to end of list. Convenience function of as_operations_add_list_append()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param value		A c-string.
 * @param free		If true, then the value will be freed when the operations is destroyed.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append_strp(as_operations* ops, const as_bin_name name, const char* value, bool free);

/**
 * Add a string to end of list. Convenience function of as_operations_add_list_append()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param value		A c-string.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_str(as_operations* ops, const as_bin_name name, const char* value)
{
	return as_operations_add_list_append_strp(ops, name, value, false);
}

/**
 * Add a blob to end of list. Convenience function of as_operations_add_list_append()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param value		A blob.
 * @param size 		Size of the blob.
 * @param free		If true, then the value will be freed when the operations is destroyed.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append_rawp(as_operations* ops, const as_bin_name name, const uint8_t* value, uint32_t size, bool free);

/**
 * Add a blob to end of list. Convenience function of as_operations_add_list_append()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param value		A blob.
 * @param size 		Size of the blob.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_append_raw(as_operations* ops, const as_bin_name name, const uint8_t* value, uint32_t size)
{
	return as_operations_add_list_append_rawp(ops, name, value, size, false);
}

/**
 * Add multiple values to end of list.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param list		List of values to append. Consumes a reference of this as_list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append_items(as_operations* ops, const as_bin_name name, as_list* list);

/**
 * Add multiple values to end of list with policy.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param policy 	The list write flags.
 * @param list		List of values to append. Consumes a reference of this as_list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_append_items_with_policy(as_operations* ops, const as_bin_name name, as_list_policy* policy, as_list* list);

/**
 * Insert an as_val element to list at index position.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the as_val will be inserted at. Negative index counts from end of list.
 * @param val		Value to insert. Consumes a reference of this as_list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert(as_operations* ops, const as_bin_name name, int64_t index, as_val* val);

/**
 * Insert an as_val element to list at index position with policy.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param policy 	The list write flags.
 * @param index 	Index position which the as_val will be inserted at. Negative index counts from end of list.
 * @param val		Value to insert. Consumes a reference of this as_list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert_with_policy(as_operations* ops, const as_bin_name name, as_list_policy* policy, int64_t index, as_val* val);

/**
 * Insert integer to list at index position. Convenience function of as_operations_add_list_insert()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the integer will be inserted at. Negative index counts from end of list.
 * @param value 	An integer value.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert_int64(as_operations* ops, const as_bin_name name, int64_t index, int64_t value);

/**
 * Insert double to list at index position. Convenience function of as_operations_add_list_insert()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the double will be inserted at. Negative index counts from end of list.
 * @param value 	A double value.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert_double(as_operations* ops, const as_bin_name name, int64_t index, double value);

/**
 * Insert string to list at index position. Convenience function of as_operations_add_list_insert()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the string will be inserted at. Negative index counts from end of list.
 * @param value 	A c-string.
 * @param free		If true, then the value will be freed when the operations is destroyed.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert_strp(as_operations* ops, const as_bin_name name, int64_t index, const char* value, bool free);

/**
 * Insert string to list at index position. Convenience function of as_operations_add_list_insert()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the string will be inserted at. Negative index counts from end of list.
 * @param value 	A c-string.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_str(as_operations* ops, const as_bin_name name, int64_t index, const char* value)
{
	return as_operations_add_list_insert_strp(ops, name, index, value, false);
}

/**
 * Insert blob to list at index position. Convenience function of as_operations_add_list_insert()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the blob will be inserted at. Negative index counts from end of list.
 * @param value 	A blob.
 * @param size 		Size of the blob.
 * @param free		If true, then the value will be freed when the operations is destroyed.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert_rawp(as_operations* ops, const as_bin_name name, int64_t index, const uint8_t* value, uint32_t size, bool free);

/**
 * Insert blob to list at index position. Convenience function of as_operations_add_list_insert()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the blob will be inserted at. Negative index counts from end of list.
 * @param value 	A blob.
 * @param size 		Size of the blob.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_insert_raw(as_operations* ops, const as_bin_name name, int64_t index, const uint8_t* value, uint32_t size)
{
	return as_operations_add_list_insert_rawp(ops, name, index, value, size, false);
}

/**
 * Insert multiple values to list at index position.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position which the blob will be inserted at. Negative index counts from end of list.
 * @param list 		List of values to insert. Consumes reference of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert_items(as_operations* ops, const as_bin_name name, int64_t index, as_list* list);

/**
 * Insert multiple values to list at index position.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param policy 	The list write flags.
 * @param index 	Index position which the blob will be inserted at. Negative index counts from end of list.
 * @param list 		List of values to insert. Consumes reference of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_insert_items_with_policy(as_operations* ops, const as_bin_name name, as_list_policy* policy, int64_t index, as_list* list);

/**
 * Create list increment operation.
 * Server increments value at index by incr and returns final result.
 * Valid only for numbers.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_increment(as_operations* ops, const as_bin_name name, int64_t index, as_val* incr);

/**
 * Create list increment operation with policy.
 * Server increments value at index by incr and returns final result.
 * Valid only for numbers.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_increment_with_policy(as_operations* ops, const as_bin_name name, as_list_policy* policy, int64_t index, as_val* incr);

//-----------------------------------------------------------------------------
// Other list modifies

/**
 * Set an as_val element of the list at the index position.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param val		Consumes a reference of this as_val.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_set(as_operations* ops, const as_bin_name name, int64_t index, as_val* val);
	
/**
 * Set an as_val element of the list at the index position with policy.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param policy 	The list write flags.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param val		Consumes a reference of this as_val.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_set_with_policy(as_operations* ops, const as_bin_name name, as_list_policy* policy, int64_t index, as_val* val);

/**
 * Set value at index as integer. Convenience function of as_operations_add_list_set()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param value		An integer value.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_set_int64(as_operations* ops, const as_bin_name name, int64_t index, int64_t value);

/**
 * Set value at index as double. Convenience function of as_operations_add_list_set()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param value		A double value.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_set_double(as_operations* ops, const as_bin_name name, int64_t index, double value);

/**
 * Set value at index as string. Convenience function of as_operations_add_list_set()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param value		A c-string.
 * @param free		If true, then the value will be freed when the operations is destroyed.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_set_strp(as_operations* ops, const as_bin_name name, int64_t index, const char* value, bool free);

/**
 * Set value at index as string. Convenience function of as_operations_add_list_set()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param value		A c-string.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_str(as_operations* ops, const as_bin_name name, int64_t index, const char* value)
{
	return as_operations_add_list_set_strp(ops, name, index, value, false);
}

/**
 * Set value at index as blob. Convenience function of as_operations_add_list_set()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param value		A blob.
 * @param size 		Size of the blob.
 * @param free		If true, then the value will be freed when the operations is destroyed.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_set_rawp(as_operations* ops, const as_bin_name name, int64_t index, const uint8_t* value, uint32_t size, bool free);

/**
 * Set value at index as blob. Convenience function of as_operations_add_list_set()
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to set. Negative index counts from end of list.
 * @param value		A blob.
 * @param size 		Size of the blob.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
static inline bool
as_operations_add_list_set_raw(as_operations* ops, const as_bin_name name, int64_t index, const uint8_t* value, uint32_t size)
{
	return as_operations_add_list_set_rawp(ops, name, index, value, size, false);
}

/**
 * Remove and return a value at index.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which the value will be removed and returned.  Negative index counts from end of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_pop(as_operations* ops, const as_bin_name name, int64_t index);

/**
 * Remove and return N values from index.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which to start the removal. Negative index counts from end of list.
 * @param count 	Number of values to remove. If not enough values in list, will remove to list end.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_pop_range(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count);

/**
 * Remove and return all values from index to the end of list.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which to start the removal. Negative index counts from end of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_pop_range_from(as_operations* ops, const as_bin_name name, int64_t index);

/**
 * Remove value at index.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which to start the removal. Negative index counts from end of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove(as_operations* ops, const as_bin_name name, int64_t index);

/**
 * Remove N values from index.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which to start the removal. Negative index counts from end of list.
 * @param count 	Number of values to remove. If not enough values in list, will remove to list end.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_range(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count);

/**
 * Remove all values from index until end of list.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which to start the removal. Negative index counts from end of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_range_from(as_operations* ops, const as_bin_name name, int64_t index);

/**
 * Create list remove operation.
 * Server removes list items identified by value and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_value(as_operations* ops, const as_bin_name name, as_val* value, as_list_return_type return_type);

/**
 * Create list remove operation.
 * Server removes list items identified by values and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_value_list(as_operations* ops, const as_bin_name name, as_list* values, as_list_return_type return_type);

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
AS_EXTERN bool
as_operations_add_list_remove_by_value_range(as_operations* ops, const as_bin_name name, as_val* begin, as_val* end, as_list_return_type return_type);

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
AS_EXTERN bool
as_operations_add_list_remove_by_value_rel_rank_range_to_end(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, as_list_return_type return_type);

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
AS_EXTERN bool
as_operations_add_list_remove_by_value_rel_rank_range(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, uint64_t count, as_list_return_type return_type);

/**
 * Create list remove operation.
 * Server removes list item identified by index and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_index(as_operations* ops, const as_bin_name name, int64_t index, as_list_return_type return_type);

/**
 * Create list remove operation.
 * Server removes list items starting at specified index to the end of list and returns removed
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_index_range_to_end(as_operations* ops, const as_bin_name name, int64_t index, as_list_return_type return_type);

/**
 * Create list remove operation.
 * Server removes `count` list items starting at specified index and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_index_range(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count, as_list_return_type return_type);

/**
 * Create list remove operation.
 * Server removes list item identified by rank and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_rank(as_operations* ops, const as_bin_name name, int64_t rank, as_list_return_type return_type);

/**
 * Create list remove operation.
 * Server removes list items starting at specified rank to the last ranked item and returns removed
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_rank_range_to_end(as_operations* ops, const as_bin_name name, int64_t rank, as_list_return_type return_type);

/**
 * Create list remove operation.
 * Server removes `count` list items starting at specified rank and returns removed data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_remove_by_rank_range(as_operations* ops, const as_bin_name name, int64_t rank, uint64_t count, as_list_return_type return_type);

/**
 * Remove values NOT within range(index, count).
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Values from 0-index position are removed. Negative index counts from end of list.
 * @param count 	Number of values to keep. All other values beyond count are removed.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_trim(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count);

/**
 * Remove all values. Will leave empty list in bin.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_clear(as_operations* ops, const as_bin_name name);
	
//-----------------------------------------------------------------------------
// Read operations

/**
 * Get number of values in list.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_size(as_operations* ops, const as_bin_name name);
	
/**
 * Get value of list at index.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position to get. Negative index counts from end of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get(as_operations* ops, const as_bin_name name, int64_t index);

/**
 * Get multiple values of list starting at index.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which to start. Negative index counts from end of list.
 * @param count 	Number of values to get. If not enough in list, will return all remaining.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_range(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count);

/**
 * Get multiple values of list starting at index until end of list.
 *
 * @param ops		The `as_operations` to append the operation to.
 * @param name 		The name of the bin to perform the operation on.
 * @param index 	Index position at which to start. Negative index counts from end of list.
 *
 * @return true on success. Otherwise an error occurred.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_range_from(as_operations* ops, const as_bin_name name, int64_t index);

/**
 * Create list get by value operation.
 * Server selects list items identified by value and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_value(as_operations* ops, const as_bin_name name, as_val* value, as_list_return_type return_type);

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
AS_EXTERN bool
as_operations_add_list_get_by_value_range(as_operations* ops, const as_bin_name name, as_val* begin, as_val* end, as_list_return_type return_type);

/**
 * Create list get by value list operation.
 * Server selects list items identified by values and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_value_list(as_operations* ops, const as_bin_name name, as_list* values, as_list_return_type return_type);

/**
 * Create list get by value relative to rank range operation.
 * Server selects list items nearest to value and greater by relative rank.
 * Server returns selected data specified by return_type.
 * <p>
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
bool
as_operations_add_list_get_by_value_rel_rank_range_to_end(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, as_list_return_type return_type);

/**
 * Create list get by value relative to rank range operation.
 * Server selects list items nearest to value and greater by relative rank with a count limit.
 * Server returns selected data specified by return_type.
 * <p>
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
bool
as_operations_add_list_get_by_value_rel_rank_range(as_operations* ops, const as_bin_name name, as_val* value, int64_t rank, uint64_t count, as_list_return_type return_type);

/**
 * Create list get by index operation.
 * Server selects list item identified by index and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_index(as_operations* ops, const as_bin_name name, int64_t index, as_list_return_type return_type);

/**
 * Create list get by index range operation.
 * Server selects list items starting at specified index to the end of list and returns selected
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_index_range_to_end(as_operations* ops, const as_bin_name name, int64_t index, as_list_return_type return_type);

/**
 * Create list get by index range operation.
 * Server selects `count` list items starting at specified index and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_index_range(as_operations* ops, const as_bin_name name, int64_t index, uint64_t count, as_list_return_type return_type);

/**
 * Create list get by rank operation.
 * Server selects list item identified by rank and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_rank(as_operations* ops, const as_bin_name name, int64_t rank, as_list_return_type return_type);

/**
 * Create list get by rank range operation.
 * Server selects list items starting at specified rank to the last ranked item and returns selected
 * data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_rank_range_to_end(as_operations* ops, const as_bin_name name, int64_t rank, as_list_return_type return_type);

/**
 * Create list get by rank range operation.
 * Server selects `count` list items starting at specified rank and returns selected data specified by return_type.
 *
 * @ingroup list_operations
 */
AS_EXTERN bool
as_operations_add_list_get_by_rank_range(as_operations* ops, const as_bin_name name, int64_t rank, uint64_t count, as_list_return_type return_type);

#ifdef __cplusplus
} // end extern "C"
#endif
