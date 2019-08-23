/*
 * Copyright 2008-2019 Aerospike, Inc.
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

#include <aerospike/as_vector.h>
#include <aerospike/as_val.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Nested CDT context type.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
typedef enum {
	AS_CDT_CTX_LIST_INDEX = 0x10,
	AS_CDT_CTX_LIST_RANK = 0x11,
	AS_CDT_CTX_LIST_VALUE = 0x13,
	AS_CDT_CTX_MAP_INDEX = 0x20,
	AS_CDT_CTX_MAP_RANK = 0x21,
	AS_CDT_CTX_MAP_KEY = 0x22,
	AS_CDT_CTX_MAP_VALUE = 0x23
} as_cdt_ctx_type;

#define AS_CDT_CTX_VALUE 0x2

/**
 * Nested CDT context level.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
typedef struct as_cdt_ctx_item {
	as_cdt_ctx_type type;
	union
	{
		int64_t ival;
		as_val* pval;
	} val;
} as_cdt_ctx_item;

/**
 * List of CDT context level(s).
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
typedef struct as_cdt_ctx {
	as_vector list;
} as_cdt_ctx;

/******************************************************************************
 * MACROS
 *****************************************************************************/

/**
 * Initialize a stack allocated nested CDT context list.
 *
 * ~~~~~~~~~~{.c}
 * Lookup last list in list of lists.
 * as_cdt_ctx ctx;
 * as_cdt_ctx_inita(&ctx, 1);
 * as_cdt_ctx_add_list_index(&ctx, -1);
 * ~~~~~~~~~~
 *
 * Call as_cdt_ctx_destroy() when done with the context list if any context levels contain
 * a heap allocated as_val instance.  If in doubt, call as_cdt_ctx_destroy().
 *
 * @param __ctx		The nested context to initialize.
 * @param __cap		The max number of context levels allowed.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
#define as_cdt_ctx_inita(__ctx, __cap) as_vector_inita(&(__ctx)->list, sizeof(as_cdt_ctx_item), __cap)

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize a stack allocated nested CDT context list, with item storage on the heap.
 * Call as_cdt_ctx_destroy() when done with the context list.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_init(as_cdt_ctx* ctx, uint32_t capacity)
{
	as_vector_init(&ctx->list, sizeof(as_cdt_ctx_item), capacity);
}

/**
 * Initialize a heap allocated nested CDT context list, with item storage on the heap.
 * Call as_cdt_ctx_destroy() when done with the context list.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline as_cdt_ctx*
as_cdt_ctx_create(uint32_t capacity)
{
	return (as_cdt_ctx*)as_vector_create(sizeof(as_cdt_ctx_item), capacity);
}

/**
 * Destroy nested CDT context list and as_val based context items that were allocated on the heap.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
AS_EXTERN void
as_cdt_ctx_destroy(as_cdt_ctx* ctx);

/**
 * Lookup list by index offset.
 *
 * If the index is negative, the resolved index starts backwards from end of list.
 * If an index is out of bounds, a parameter error will be returned.  Examples:
 * <ul>
 * <li>0: First item.</li>
 * <li>4: Fifth item.</li>
 * <li>-1: Last item.</li>
 * <li>-3: Third to last item.</li>
 * </ul>
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_add_list_index(as_cdt_ctx* ctx, int index)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_LIST_INDEX;
	item.val.ival = index;
	as_vector_append(&ctx->list, &item);
}

/**
 * Lookup list by rank.
 * <ul>
 * <li>0 = smallest value</li>
 * <li>N = Nth smallest value</li>
 * <li>-1 = largest value</li>
 * </ul>
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_add_list_rank(as_cdt_ctx* ctx, int rank)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_LIST_RANK;
	item.val.ival = rank;
	as_vector_append(&ctx->list, &item);
}

/**
 * Lookup list by value.  The ctx list takes ownership of val.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_add_list_value(as_cdt_ctx* ctx, as_val* val)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_LIST_VALUE;
	item.val.pval = val;
	as_vector_append(&ctx->list, &item);
}

/**
 * Lookup map by index offset.
 * <p>
 * If the index is negative, the resolved index starts backwards from end of list.
 * If an index is out of bounds, a parameter error will be returned.  Examples:
 * <ul>
 * <li>0: First item.</li>
 * <li>4: Fifth item.</li>
 * <li>-1: Last item.</li>
 * <li>-3: Third to last item.</li>
 * </ul>
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_add_map_index(as_cdt_ctx* ctx, int index)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_MAP_INDEX;
	item.val.ival = index;
	as_vector_append(&ctx->list, &item);
}

/**
 * Lookup map by rank.
 * <ul>
 * <li>0 = smallest value</li>
 * <li>N = Nth smallest value</li>
 * <li>-1 = largest value</li>
 * </ul>
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_add_map_rank(as_cdt_ctx* ctx, int rank)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_MAP_RANK;
	item.val.ival = rank;
	as_vector_append(&ctx->list, &item);
}

/**
 * Lookup map by key.  The ctx list takes ownership of key.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_add_map_key(as_cdt_ctx* ctx, as_val* key)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_MAP_KEY;
	item.val.pval = key;
	as_vector_append(&ctx->list, &item);
}

/**
 * Lookup map by value.  The ctx list takes ownership of val.
 *
 * @relates as_operations
 * @ingroup as_operations_object
 */
static inline void
as_cdt_ctx_add_map_value(as_cdt_ctx* ctx, as_val* val)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_MAP_VALUE;
	item.val.pval = val;
	as_vector_append(&ctx->list, &item);
}

#ifdef __cplusplus
} // end extern "C"
#endif
