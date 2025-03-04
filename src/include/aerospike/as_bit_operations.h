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
 * @defgroup bit_operations Bit Operations
 * @ingroup client_operations
 *
 * Bit operations. Create bit operations used in aerospike_key_operate().
 * Offset orientation is left-to-right.  Negative offsets are supported.
 * If the offset is negative, the offset starts backwards from end of the bitmap.
 * If an offset is out of bounds, a parameter error will be returned.
 *
 * Code examples:
 *
 * ~~~~~~~~~~{.c}
 * // Set bitmap bin.
 * as_operations ops;
 * as_operations_inita(&ops, 1);
 * uint8_t val[] = {0x11, 0x22, 0x33, 0x44};
 * as_operations_bit_set(&ops, "bin", NULL, NULL, 0, 32, sizeof(val), val);
 * 
 * as_record* rec = NULL;
 * as_error err;
 * aerospike_key_operate(&as, &err, NULL, &key, &ops, &rec);
 * as_operations_destroy(&ops);
 * as_record_destroy(rec);
 * ~~~~~~~~~~
 *
 * Bit operations on bitmap items nested in lists/maps are not currently
 * supported by the server.  The as_cdt_ctx argument in bit operations must
 * be set to NULL.
 */

#include <aerospike/as_operations.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Bitmap write flags.
 *
 * @ingroup bit_operations
 */
typedef enum as_bit_write_flags_e {
	/**
	 * Default.  Allow create or update.
	 */
	AS_BIT_WRITE_DEFAULT = 0,

	/**
	 * If the bin already exists, the operation will be denied.
	 * If the bin does not exist, a new bin will be created.
	 */
	AS_BIT_WRITE_CREATE_ONLY = 1,

	/**
	 * If the bin already exists, the bin will be overwritten.
	 * If the bin does not exist, the operation will be denied.
	 */
	AS_BIT_WRITE_UPDATE_ONLY = 2,

	/**
	 * Do not raise error if operation is denied.
	 */
	AS_BIT_WRITE_NO_FAIL = 4,

	/**
	 * Don't fail if the bit operation would increase the blob size.
	 * Instead, apply the bit operation without increasing the blob size.
	 */
	AS_BIT_WRITE_PARTIAL = 8
} as_bit_write_flags;

/**
 * Bitmap resize flags.
 *
 * @ingroup bit_operations
 */
typedef enum as_bit_resize_flags_e {
	/**
	 * Default.
	 */
	AS_BIT_RESIZE_DEFAULT = 0,

	/**
	 * Add/remove bytes from the beginning instead of the end.
	 */
	AS_BIT_RESIZE_FROM_FRONT = 1,

	/**
	 * Only allow the bitmap size to increase.
	 */
	AS_BIT_RESIZE_GROW_ONLY = 2,

	/**
	 * Only allow the bitmap size to decrease.
	 */
	AS_BIT_RESIZE_SHRINK_ONLY = 4
} as_bit_resize_flags;

/**
 * Action to take when bitwise add/subtract results in overflow/underflow.
 *
 * @ingroup bit_operations
 */
typedef enum as_bit_overflow_action_e {
	/**
	 * Fail operation with error.
	 */
	AS_BIT_OVERFLOW_FAIL = 0,

	/**
	 * If add/subtract overflows/underflows, set to max/min value.
	 * Example: MAXINT + 1 = MAXINT
	 */
	AS_BIT_OVERFLOW_SATURATE = 2,

	/**
	 * If add/subtract overflows/underflows, wrap the value.
	 * Example: MAXINT + 1 = -1
	 */
	AS_BIT_OVERFLOW_WRAP = 4
} as_bit_overflow_action;

/**
 * Bit operation policy.
 *
 * @ingroup bit_operations
 */
typedef struct as_bit_policy_s {
	uint64_t flags;
} as_bit_policy;

/**
 * @private
 * Bit operation codes.
 */
typedef enum {
	AS_BIT_OP_RESIZE = 0,
	AS_BIT_OP_INSERT = 1,
	AS_BIT_OP_REMOVE = 2,
	AS_BIT_OP_SET = 3,
	AS_BIT_OP_OR = 4,
	AS_BIT_OP_XOR = 5,
	AS_BIT_OP_AND = 6,
	AS_BIT_OP_NOT = 7,
	AS_BIT_OP_LSHIFT = 8,
	AS_BIT_OP_RSHIFT = 9,
	AS_BIT_OP_ADD = 10,
	AS_BIT_OP_SUBTRACT = 11,
	AS_BIT_OP_SET_INT = 12,
	AS_BIT_OP_GET = 50,
	AS_BIT_OP_COUNT = 51,
	AS_BIT_OP_LSCAN = 52,
	AS_BIT_OP_RSCAN = 53,
	AS_BIT_OP_GET_INT = 54
} as_bit_op;

/******************************************************************************
 * PRIVATE FUNCTIONS
 *****************************************************************************/

AS_EXTERN bool
as_bit_write(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int offset, uint32_t size
	);

AS_EXTERN bool
as_bit_shift(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int bit_offset, uint32_t bit_size, uint32_t shift
	);

AS_EXTERN bool
as_bit_math(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int bit_offset, uint32_t bit_size, uint64_t value, bool sign,
	as_bit_overflow_action action
	);

AS_EXTERN bool
as_bit_byte_math(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint16_t command, int bit_offset, uint32_t bit_size, uint32_t value_size, uint8_t* value
	);

AS_EXTERN bool
as_bit_read(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command, int bit_offset,
	uint32_t bit_size
	);

AS_EXTERN bool
as_bit_scan(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, uint16_t command, int bit_offset,
	uint32_t bit_size, bool value
	);

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

/**
 * Initialize bit policy to default.
 *
 * @ingroup bit_operations
 */
static inline void
as_bit_policy_init(as_bit_policy* policy)
{
	policy->flags = AS_BIT_WRITE_DEFAULT;
}

/**
 * Set bit write flags in bit policy.
 *
 * @ingroup bit_operations
 */
static inline void
as_bit_policy_set_write_flags(as_bit_policy* policy, as_bit_write_flags flags)
{
	policy->flags = flags;
}

/**
 * Create byte "resize" operation.
 * Server resizes bitmap to byte_size according to flags.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010]</li>
 * <li>byte_size = 4</li>
 * <li>flags = 0</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b00000000, 0b00000000]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
AS_EXTERN bool
as_operations_bit_resize(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	uint32_t byte_size, as_bit_resize_flags flags
	);

/**
 * Create byte "insert" operation.
 * Server inserts value bytes into bitmap at byte_offset.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>byte_offset = 1</li>
 * <li>value_byte_size = 2</li>
 * <li>value = [0b11111111, 0b11000111]</li>
 * <li>bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
AS_EXTERN bool
as_operations_bit_insert(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int byte_offset, uint32_t value_byte_size, uint8_t* value
	);

/**
 * Create byte "remove" operation.
 * Server removes bytes from bitmap at byte_offset for byte_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>byte_offset = 2</li>
 * <li>byte_size = 3</li>
 * <li>bin result = [0b00000001, 0b01000010]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_remove(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int byte_offset, uint32_t byte_size
	)
{
	return as_bit_write(ops, name, ctx, policy, AS_BIT_OP_REMOVE, byte_offset, byte_size);
}

/**
 * Create bit "set" operation.
 * Server sets value on bitmap at bit_offset for bit_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 13</li>
 * <li>bit_size = 3</li>
 * <li>value_byte_size = 1</li>
 * <li>value = [0b11100000]</li>
 * <li>bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_set(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint32_t value_byte_size, uint8_t* value
	)
{
	return as_bit_byte_math(ops, name, ctx, policy, AS_BIT_OP_SET, bit_offset, bit_size, value_byte_size, value);
}

/**
 * Create bit "or" operation.
 * Server performs bitwise "or" on value and bitmap at bit_offset for bit_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 17</li>
 * <li>bit_size = 6</li>
 * <li>value_byte_size = 1</li>
 * <li>value = [0b10101000]</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_or(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint32_t value_byte_size, uint8_t* value
	)
{
	return as_bit_byte_math(ops, name, ctx, policy, AS_BIT_OP_OR, bit_offset, bit_size, value_byte_size, value);
}

/**
 * Create bit "exclusive or" operation.
 * Server performs bitwise "xor" on value and bitmap at bit_offset for bit_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 17</li>
 * <li>bit_size = 6</li>
 * <li>value_byte_size = 1</li>
 * <li>value = [0b10101100]</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_xor(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint32_t value_byte_size, uint8_t* value
	)
{
	return as_bit_byte_math(ops, name, ctx, policy, AS_BIT_OP_XOR, bit_offset, bit_size, value_byte_size, value);
}

/**
 * Create bit "and" operation.
 * Server performs bitwise "and" on value and bitmap at bit_offset for bit_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 23</li>
 * <li>bit_size = 9</li>
 * <li>value_byte_size = 2</li>
 * <li>value = [0b00111100, 0b10000000]</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_and(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint32_t value_byte_size, uint8_t* value
	)
{
	return as_bit_byte_math(ops, name, ctx, policy, AS_BIT_OP_AND, bit_offset, bit_size, value_byte_size, value);
}

/**
 * Create bit "not" operation.
 * Server negates bitmap starting at bit_offset for bit_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 25</li>
 * <li>bit_size = 6</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_not(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size
	)
{
	return as_bit_write(ops, name, ctx, policy, AS_BIT_OP_NOT, bit_offset, bit_size);
}

/**
 * Create bit "left shift" operation.
 * Server shifts left bitmap starting at bit_offset for bit_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 32</li>
 * <li>bit_size = 8</li>
 * <li>shift = 3</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_lshift(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint32_t shift
	)
{
	return as_bit_shift(ops, name, ctx, policy, AS_BIT_OP_LSHIFT, bit_offset, bit_size, shift);
}

/**
 * Create bit "right shift" operation.
 * Server shifts right bitmap starting at bit_offset for bit_size.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 0</li>
 * <li>bit_size = 9</li>
 * <li>shift = 1</li>
 * <li>bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_rshift(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint32_t shift
	)
{
	return as_bit_shift(ops, name, ctx, policy, AS_BIT_OP_RSHIFT, bit_offset, bit_size, shift);
}

/**
 * Create bit "add" operation.
 * Server adds value to bitmap starting at bit_offset for bit_size. bit_size must be <= 64.
 * Sign indicates if bits should be treated as a signed number.
 * If add overflows/underflows, as_bit_overflow_action is used.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 24</li>
 * <li>bit_size = 16</li>
 * <li>value = 128</li>
 * <li>sign = false</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_add(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint64_t value, bool sign, as_bit_overflow_action action
	)
{
	return as_bit_math(ops, name, ctx, policy, AS_BIT_OP_ADD, bit_offset, bit_size, value, sign, action);
}

/**
 * Create bit "subtract" operation.
 * Server subtracts value from bitmap starting at bit_offset for bit_size. bit_size must be <= 64.
 * Sign indicates if bits should be treated as a signed number.
 * If add overflows/underflows, as_bit_overflow_action is used.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 24</li>
 * <li>bit_size = 16</li>
 * <li>value = 128</li>
 * <li>sign = false</li>
 * <li>bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_subtract(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, uint64_t value, bool sign, as_bit_overflow_action action
	)
{
	return as_bit_math(ops, name, ctx, policy, AS_BIT_OP_SUBTRACT, bit_offset, bit_size, value, sign, action);
}

/**
 * Create bit "set integer" operation.
 * Server sets value to bitmap starting at bit_offset for bit_size. Size must be <= 64.
 * Server does not return a value.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 1</li>
 * <li>bit_size = 8</li>
 * <li>value = 127</li>
 * <li>bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
AS_EXTERN bool
as_operations_bit_set_int(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, as_bit_policy* policy,
	int bit_offset, uint32_t bit_size, int64_t value
	);

/**
 * Create bit "get" operation.
 * Server returns bits from bitmap starting at bit_offset for bit_size.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 9</li>
 * <li>bit_size = 5</li>
 * <li>returns [0b10000000]</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_get(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int bit_offset, uint32_t bit_size
	)
{
	return as_bit_read(ops, name, ctx, AS_BIT_OP_GET, bit_offset, bit_size);
}

/**
 * Create bit "count" operation.
 * Server returns integer count of set bits from bitmap starting at bit_offset for bit_size.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 20</li>
 * <li>bit_size = 4</li>
 * <li>returns 2</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_count(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int bit_offset, uint32_t bit_size
	)
{
	return as_bit_read(ops, name, ctx, AS_BIT_OP_COUNT, bit_offset, bit_size);
}

/**
 * Create bit "left scan" operation.
 * Server returns integer bit offset of the first specified value bit in bitmap
 * starting at bit_offset for bit_size.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 24</li>
 * <li>bit_size = 8</li>
 * <li>value = true</li>
 * <li>returns 5</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_lscan(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int bit_offset, uint32_t bit_size,
	bool value
	)
{
	return as_bit_scan(ops, name, ctx, AS_BIT_OP_LSCAN, bit_offset, bit_size, value);
}

/**
 * Create bit "right scan" operation.
 * Server returns integer bit offset of the last specified value bit in bitmap
 * starting at bit_offset for bit_size.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 32</li>
 * <li>bit_size = 8</li>
 * <li>value = true</li>
 * <li>returns 7</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
static inline bool
as_operations_bit_rscan(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int bit_offset, uint32_t bit_size,
	bool value
	)
{
	return as_bit_scan(ops, name, ctx, AS_BIT_OP_RSCAN, bit_offset, bit_size, value);
}

/**
 * Create bit "get integer" operation.
 * Server returns integer from bitmap starting at bit_offset for bit_size.
 * Sign indicates if bits should be treated as a signed number.
 * Example:
 * <ul>
 * <li>bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]</li>
 * <li>bit_offset = 8</li>
 * <li>bit_size = 16</li>
 * <li>sign = false</li>
 * <li>returns 16899</li>
 * </ul>
 *
 * @ingroup bit_operations
 */
AS_EXTERN bool
as_operations_bit_get_int(
	as_operations* ops, const char* name, as_cdt_ctx* ctx, int bit_offset, uint32_t bit_size,
	bool sign
	);

#ifdef __cplusplus
} // end extern "C"
#endif
