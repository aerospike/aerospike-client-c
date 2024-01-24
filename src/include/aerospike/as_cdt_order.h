/*
 * Copyright 2008-2020 Aerospike, Inc.
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

#include <aerospike/as_std.h>

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

	/**
	 * Persist index on server.
	 */
	AS_LIST_FLAG_PERSIST_INDEX = 0x10

} as_list_order;

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
	AS_MAP_KEY_VALUE_ORDERED = 3,

	/**
	 * Persist index on server.
	 */
	AS_MAP_FLAG_PERSIST_INDEX = 0x10
} as_map_order;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static inline uint32_t
as_list_order_to_flag(as_list_order order, bool pad)
{
	if ((order & AS_LIST_ORDERED) != 0) {
		return ((order & AS_LIST_FLAG_PERSIST_INDEX) != 0) ? 0x1c0 : 0xc0;
	}

	return (pad ? 0x80 : 0x40) |
			(((order & AS_LIST_FLAG_PERSIST_INDEX) != 0) ? 0x100 : 0x0);
}

static inline uint32_t
as_map_order_to_flag(as_map_order order)
{
	switch (order & 0x3) {
		default:
		case AS_MAP_UNORDERED:
			return 0x40 |
					(((order & AS_LIST_FLAG_PERSIST_INDEX) != 0) ? 0x100 : 0x0);

		case AS_MAP_KEY_ORDERED:
			return 0x80  |
					(((order & AS_LIST_FLAG_PERSIST_INDEX) != 0) ? 0x100 : 0x0);

		case AS_MAP_KEY_VALUE_ORDERED:
			return 0xc0 |
					(((order & AS_LIST_FLAG_PERSIST_INDEX) != 0) ? 0x100 : 0x0);
	}
}

#ifdef __cplusplus
} // end extern "C"
#endif
