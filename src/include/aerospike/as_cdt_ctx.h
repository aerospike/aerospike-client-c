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

#include <aerospike/as_cdt_order.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_val.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * Nested CDT context type.
 *
 * @relates as_operations
 * @ingroup base_operations
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
 * @ingroup base_operations
 */
typedef struct as_cdt_ctx_item {
	uint32_t type;
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
 * @ingroup base_operations
 */
typedef struct as_cdt_ctx {
	as_vector list;
} as_cdt_ctx;

//---------------------------------
// Macros
//---------------------------------

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
 * @ingroup base_operations
 */
#define as_cdt_ctx_inita(__ctx, __cap) as_vector_inita(&(__ctx)->list, sizeof(as_cdt_ctx_item), __cap)

//---------------------------------
// Functions
//---------------------------------

/**
 * Initialize a stack allocated nested CDT context list, with item storage on the heap.
 * Call as_cdt_ctx_destroy() when done with the context list.
 *
 * @relates as_operations
 * @ingroup base_operations
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
 * @ingroup base_operations
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
 * @ingroup base_operations
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
 * @ingroup base_operations
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
 * Create list with given type at index offset.
 *
 * @relates as_operations
 * @ingroup base_operations
 */
static inline void
as_cdt_ctx_add_list_index_create(as_cdt_ctx* ctx, int index, as_list_order order, bool pad)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_LIST_INDEX | as_list_order_to_flag(order, pad);
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
 * @ingroup base_operations
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
 * @ingroup base_operations
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
 * @ingroup base_operations
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
 * @ingroup base_operations
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
 * @ingroup base_operations
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
 * Create map with given type at map key.
 * The ctx list takes ownership of key.
 *
 * @relates as_operations
 * @ingroup base_operations
 */
static inline void
as_cdt_ctx_add_map_key_create(as_cdt_ctx* ctx, as_val* key, as_map_order order)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_MAP_KEY | as_map_order_to_flag(order);
	item.val.pval = key;
	as_vector_append(&ctx->list, &item);
}

/**
 * Lookup map by value.  The ctx list takes ownership of val.
 *
 * @relates as_operations
 * @ingroup base_operations
 */
static inline void
as_cdt_ctx_add_map_value(as_cdt_ctx* ctx, as_val* val)
{
	as_cdt_ctx_item item;
	item.type = AS_CDT_CTX_MAP_VALUE;
	item.val.pval = val;
	as_vector_append(&ctx->list, &item);
}

/**
 * Return exact serialized size of ctx. Return zero on error.
 */
AS_EXTERN uint32_t
as_cdt_ctx_byte_capacity(const as_cdt_ctx* ctx);

/**
 * Serialize ctx to bytes. Use as_cdt_ctx_byte_capacity() to determine required bytes capacity.
 *
 * @param ctx		Source CDT context.
 * @param bytes		Target bytes array which must be allocated before calling this function.
 * @param capacity	Max size of bytes array.
 * @return			Length of serialized bytes on success or zero on error.
 */
AS_EXTERN uint32_t
as_cdt_ctx_to_bytes(const as_cdt_ctx* ctx, uint8_t* bytes, uint32_t capacity);

/**
 * Deserialize bytes to ctx.
 *
 * @param ctx		Target CDT context.
 * @param bytes		Source byte array.
 * @param size		Length of source byte array.
 * @return			true on success, false on error.
 */
AS_EXTERN bool
as_cdt_ctx_from_bytes(as_cdt_ctx* ctx, const uint8_t* bytes, uint32_t size);

/**
 * Return estimated base64 encoded size of ctx. Return zero on error.
 */
AS_EXTERN uint32_t
as_cdt_ctx_base64_capacity(const as_cdt_ctx* ctx);

/**
 * Serialize ctx to base64 encoded string. Use as_cdt_ctx_base64_capacity() to determine
 * required string capacity.
 *
 * @param ctx		Source CDT context.
 * @param base64	Target base64 encoded null terminated string which must be allocated before
 *					calling this function.
 * @param capacity	Max size of base64 encoded string.
 * @return			true on success, false on error.
 */
AS_EXTERN bool
as_cdt_ctx_to_base64(const as_cdt_ctx* ctx, char* base64, uint32_t capacity);

/**
 * Deserialize base64 encoded string to ctx.
 *
 * @param ctx		Target CDT context.
 * @param base64	Source base64 encoded string.
 * @return			true on success, false on error.
 */
AS_EXTERN bool
as_cdt_ctx_from_base64(as_cdt_ctx* ctx, const char* base64);

#ifdef __cplusplus
} // end extern "C"
#endif
