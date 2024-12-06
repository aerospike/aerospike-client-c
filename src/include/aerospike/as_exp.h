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
 * @defgroup expression Expression Filters
 *
 * Expression filters are applied on each applicable server record.
 * Expression filters require server version >= 5.2.0.4.
 *
 * If the filter exists and evaluates to false in a single record command,
 * the command is ignored and AEROSPIKE_FILTERED_OUT is returned as an error code.
 *
 * If the filter exists and evaluates to false in a batch record row, AEROSPIKE_FILTERED_OUT
 * is returned as a status for that record row in the batch.
 *
 * If the filter exists and evaluates to false on a scan/query record, that record is not
 * returned.
 *
 * Expression filters can now be defined on all commands through the command policy
 * (as_policy_base contained in as_policy_read, as_policy_write, ...).
 *
 * Example:
 * ~~~~~~~~~~{.c}
 * as_exp_build(filter,
 *   as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(10)));
 *
 * as_policy_read p;
 * as_policy_read_init(&p);
 * p.filter_exp = filter;
 * ...
 * as_exp_destroy(filter);
 * ~~~~~~~~~~
 */

#include <aerospike/as_bit_operations.h>
#include <aerospike/as_hll_operations.h>
#include <aerospike/as_list_operations.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_msgpack_ext.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_std.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef enum {
	_AS_EXP_CODE_UNKNOWN = 0,

	_AS_EXP_CODE_CMP_EQ = 1,
	_AS_EXP_CODE_CMP_NE = 2,
	_AS_EXP_CODE_CMP_GT = 3,
	_AS_EXP_CODE_CMP_GE = 4,
	_AS_EXP_CODE_CMP_LT = 5,
	_AS_EXP_CODE_CMP_LE = 6,

	_AS_EXP_CODE_CMP_REGEX = 7,
	_AS_EXP_CODE_CMP_GEO = 8,

	_AS_EXP_CODE_AND = 16,
	_AS_EXP_CODE_OR = 17,
	_AS_EXP_CODE_NOT = 18,
	_AS_EXP_CODE_EXCLUSIVE = 19,

	_AS_EXP_CODE_ADD = 20,
	_AS_EXP_CODE_SUB = 21,
	_AS_EXP_CODE_MUL = 22,
	_AS_EXP_CODE_DIV = 23,
	_AS_EXP_CODE_POW = 24,
	_AS_EXP_CODE_LOG = 25,
	_AS_EXP_CODE_MOD = 26,
	_AS_EXP_CODE_ABS = 27,
	_AS_EXP_CODE_FLOOR = 28,
	_AS_EXP_CODE_CEIL = 29,

	_AS_EXP_CODE_TO_INT = 30,
	_AS_EXP_CODE_TO_FLOAT = 31,

	_AS_EXP_CODE_INT_AND = 32,
	_AS_EXP_CODE_INT_OR = 33,
	_AS_EXP_CODE_INT_XOR = 34,
	_AS_EXP_CODE_INT_NOT = 35,
	_AS_EXP_CODE_INT_LSHIFT = 36,
	_AS_EXP_CODE_INT_RSHIFT = 37,
	_AS_EXP_CODE_INT_ARSHIFT = 38,
	_AS_EXP_CODE_INT_COUNT = 39,
	_AS_EXP_CODE_INT_LSCAN = 40,
	_AS_EXP_CODE_INT_RSCAN = 41,

	_AS_EXP_CODE_MIN = 50,
	_AS_EXP_CODE_MAX = 51,

	_AS_EXP_CODE_DIGEST_MODULO = 64,
	_AS_EXP_CODE_DEVICE_SIZE = 65,
	_AS_EXP_CODE_LAST_UPDATE = 66,
	_AS_EXP_CODE_SINCE_UPDATE = 67,
	_AS_EXP_CODE_VOID_TIME = 68,
	_AS_EXP_CODE_TTL = 69,
	_AS_EXP_CODE_SET_NAME = 70,
	_AS_EXP_CODE_KEY_EXIST = 71,
	_AS_EXP_CODE_IS_TOMBSTONE = 72,
	_AS_EXP_CODE_MEMORY_SIZE = 73,
	_AS_EXP_CODE_RECORD_SIZE = 74,

	_AS_EXP_CODE_KEY = 80,
	_AS_EXP_CODE_BIN = 81,
	_AS_EXP_CODE_BIN_TYPE = 82,

	_AS_EXP_CODE_COND = 123,
	_AS_EXP_CODE_VAR = 124,
	_AS_EXP_CODE_LET = 125,
	_AS_EXP_CODE_QUOTE = 126,
	_AS_EXP_CODE_CALL = 127,

	// Begin virtual ops, these do not go on the wire.
	_AS_EXP_CODE_AS_VAL,
	_AS_EXP_CODE_VAL_GEO,
	_AS_EXP_CODE_VAL_PK,
	_AS_EXP_CODE_VAL_INT,
	_AS_EXP_CODE_VAL_UINT,
	_AS_EXP_CODE_VAL_FLOAT,
	_AS_EXP_CODE_VAL_BOOL,
	_AS_EXP_CODE_VAL_STR,
	_AS_EXP_CODE_VAL_BYTES,
	_AS_EXP_CODE_VAL_RAWSTR,
	_AS_EXP_CODE_VAL_RTYPE,

	_AS_EXP_CODE_CALL_VOP_START,
	_AS_EXP_CODE_CDT_LIST_CRMOD,
	_AS_EXP_CODE_CDT_LIST_MOD,
	_AS_EXP_CODE_CDT_MAP_CRMOD,
	_AS_EXP_CODE_CDT_MAP_CR,
	_AS_EXP_CODE_CDT_MAP_MOD,
	_AS_EXP_CODE_MERGE,

	_AS_EXP_CODE_END_OF_VA_ARGS
} as_exp_ops;

typedef enum {
	_AS_EXP_SYS_CALL_CDT = 0,
	_AS_EXP_SYS_CALL_BITS = 1,
	_AS_EXP_SYS_CALL_HLL = 2,

	_AS_EXP_SYS_FLAG_MODIFY_LOCAL = 0x40
} as_exp_call_system_type;

typedef enum {
	AS_EXP_TYPE_NIL = 0,
	AS_EXP_TYPE_BOOL = 1,
	AS_EXP_TYPE_INT = 2,
	AS_EXP_TYPE_STR = 3,
	AS_EXP_TYPE_LIST = 4,
	AS_EXP_TYPE_MAP = 5,
	AS_EXP_TYPE_BLOB = 6,
	AS_EXP_TYPE_FLOAT = 7,
	AS_EXP_TYPE_GEOJSON = 8,
	AS_EXP_TYPE_HLL = 9,

	AS_EXP_TYPE_AUTO,
	AS_EXP_TYPE_ERROR
} as_exp_type;

typedef struct as_exp {
	uint32_t packed_sz;
	uint8_t packed[];
} as_exp;

typedef struct {
	as_exp_ops op;
	uint32_t count;
	uint32_t sz;
	int32_t prev_va_args;

	union {
		as_val* val;
		const char* str_val;
		uint8_t* bytes_val;
		int64_t int_val;
		uint64_t uint_val;
		double float_val;
		bool bool_val;

		as_cdt_ctx* ctx; // for CALL
		as_list_policy* list_pol; // for LIST_POL_*
		as_map_policy* map_pol;
		as_exp* expr;
	} v;
} as_exp_entry;

/*********************************************************************************
 * PRIVATE FUNCTIONS
 *********************************************************************************/

AS_EXTERN as_exp* as_exp_compile(as_exp_entry* table, uint32_t n);
AS_EXTERN char* as_exp_compile_b64(as_exp* exp);
AS_EXTERN void as_exp_destroy_b64(char* b64);
AS_EXTERN uint8_t* as_exp_write(as_exp* exp, uint8_t* ptr);
AS_EXTERN int64_t as_exp_get_ctx_type(const as_cdt_ctx* ctx, as_exp_type default_type);
AS_EXTERN int64_t as_exp_get_list_type(as_exp_type default_type, as_list_return_type rtype, bool is_multi);
AS_EXTERN int64_t as_exp_get_map_type(as_exp_type type, as_map_return_type rtype, bool is_multi);

/*********************************************************************************
 * PUBLIC FUNCTIONS
 *********************************************************************************/

/**
 * Encode expression to null-terminated base64 string.
 * Call as_exp_destroy_base64() when done with base64 string.
 *
 * @ingroup expression
 */
static inline char*
as_exp_to_base64(as_exp* exp)
{
	return as_exp_compile_b64(exp);
}

/**
 * Decode null-terminated base64 string to expression.
 * Call as_exp_destroy() when done with expression.
 *
 * @ingroup expression
 */
AS_EXTERN as_exp* as_exp_from_base64(const char* base64);

/**
 * Free expression bytes.
 *
 * @ingroup expression
 */
AS_EXTERN void as_exp_destroy(as_exp* exp);

/**
 * Free base64 string.
 *
 * @ingroup expression
 */
static inline void
as_exp_destroy_base64(char* base64)
{
	as_exp_destroy_b64(base64);
}

/*********************************************************************************
 * VALUE EXPRESSIONS
 *********************************************************************************/

/**
 * Create an 'unknown' value. Used to intentionally fail an expression.
 * The failure can be ignored with AS_EXP_WRITE_EVAL_NO_FAIL or
 * AS_EXP_READ_NO_FAIL.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // If var("v") (bin("balance") - 100.0) is greater that or equal to 0.0 then
 * // return var("v") else fail operation.
 * as_exp_build(expression,
 *     as_exp_let(
 *         as_exp_def("v", as_exp_sub(
 *             as_exp_bin_float("balance"), as_exp_float(100.0))),
 *         as_exp_cond(
 *             as_exp_ge(as_exp_var("v"), as_exp_float(0)), as_exp_var("v"),
 *             as_exp_unknown())));
 * ~~~~~~~~~~
 *
 * @return (unknown value)
 * @ingroup expression
 */
#define as_exp_unknown() {.op=_AS_EXP_CODE_UNKNOWN, .count=1}

/**
 * Create boolean value.
 *
 * @param __val			boolean value.
 * @ingroup expression
 */
#define as_exp_bool(__val) {.op=_AS_EXP_CODE_VAL_BOOL, .v.bool_val=__val}

/**
 * Create 64 bit signed integer value.
 *
 * @param __val			integer value.
 * @ingroup expression
 */
#define as_exp_int(__val) {.op=_AS_EXP_CODE_VAL_INT, .v.int_val=__val}

/**
 * Create 64 bit unsigned integer value.
 *
 * @param __val			unsigned integer value.
 * @ingroup expression
 */
#define as_exp_uint(__val) {.op=_AS_EXP_CODE_VAL_UINT, .v.uint_val=__val}

/**
 * Create 64 bit floating point value.
 *
 * @param __val			floating point value.
 * @ingroup expression
 */
#define as_exp_float(__val) {.op=_AS_EXP_CODE_VAL_FLOAT, .v.float_val=__val}

/**
 * Create string value.
 *
 * @param __val			string value.
 * @ingroup expression
 */
#define as_exp_str(__val) {.op=_AS_EXP_CODE_VAL_STR, .v.str_val=__val}

/**
 * Create byte array value.
 *
 * @param __val			byte array value.
 * @param __size		number of bytes.
 * @ingroup expression
 */
#define as_exp_bytes(__val, __size) {.op=_AS_EXP_CODE_VAL_BYTES, .sz=__size, .v.bytes_val=__val}

/**
 * Create geojson value.
 *
 * @param __val			geojson value.
 * @ingroup expression
 */
#define as_exp_geo(__val) {.op=_AS_EXP_CODE_VAL_GEO, .v.val=(as_val*)as_geojson_new(__val, false)}

/**
 * Create value from an as_val.
 *
 * @param __val			as_val value.
 * @ingroup expression
 */
#define as_exp_val(__val) {.op=_AS_EXP_CODE_AS_VAL, .v.val=(as_val*)(__val)}

/**
 * Create 'nil' value.
 * @ingroup expression
 */
#define as_exp_nil() as_exp_val(&as_nil)

/**
 * Create infinity value.
 * @ingroup expression
 */
#define as_exp_inf() as_exp_val(&as_cmp_inf)

/**
 * Create wildcard value.
 * @ingroup expression
 */
#define as_exp_wildcard() as_exp_val(&as_cmp_wildcard)

/*********************************************************************************
 * KEY EXPRESSIONS
 *********************************************************************************/

/**
 * Create expression that returns the key as an integer. Returns 'unknown' if
 * the key is not an integer.
 *
 * ~~~~~~~~~~{.c}
 * // Integer record key >= 10000
 * as_exp_build(expression,
 *     as_exp_cmp_ge(as_exp_key_int(), as_exp_int(10000)));
 * ~~~~~~~~~~
 *
 * @return (integer value) Integer value of the key if the key is an integer.
 * @ingroup expression
 */
#define as_exp_key_int() {.op=_AS_EXP_CODE_KEY, .count=2}, \
		as_exp_int(AS_EXP_TYPE_INT)

/**
 * Create expression that returns the key as an string. Returns 'unknown' if
 * the key is not a string.
 *
 * ~~~~~~~~~~{.c}
 * // String record key == "aaa"
 * as_exp_build(expression,
 *     as_exp_cmp_eq(as_exp_key_str(), as_exp_str("aaa")));
 * ~~~~~~~~~~
 *
 * @return (string value) String value of the key if the key is a string.
 * @ingroup expression
 */
#define as_exp_key_str() {.op=_AS_EXP_CODE_KEY, .count=2}, \
		as_exp_int(AS_EXP_TYPE_STR)

/**
 * Create expression that returns the key as an blob. Returns 'unknown' if
 * the key is not an blob.
 *
 * ~~~~~~~~~~{.c}
 * // Blob record key <= { 0x65, 0x65 }
 * uint8_t val[] = {0x65, 0x65}
 * as_exp_build(expression,
 *     as_exp_cmp_le(as_exp_key_blob(), as_exp_bytes(val, sizeof(val))));
 * ~~~~~~~~~~
 *
 * @return (blob value) Blob value of the key if the key is a blob.
 * @ingroup expression
 */
#define as_exp_key_blob() {.op=_AS_EXP_CODE_KEY, .count=2}, \
		as_exp_int(AS_EXP_TYPE_BLOB)

/**
 * Create expression that returns if the primary key is stored in the record meta
 * data as a boolean expression. This would occur when "as_policy_write.key" is
 * AS_POLICY_KEY_SEND on record write.
 *
 * ~~~~~~~~~~{.c}
 * // Key exists in record meta data
 * as_exp_build(expression, as_exp_key_exists());
 * ~~~~~~~~~~
 *
 * @return (boolean value) True if the record has a stored key, false otherwise.
 * @ingroup expression
 */
#define as_exp_key_exist() {.op=_AS_EXP_CODE_KEY_EXIST, .count=1}

/*********************************************************************************
 * BIN EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_VAL_RAWSTR(__val) {.op=_AS_EXP_CODE_VAL_RAWSTR, .v.str_val=__val}

/**
 * Create expression that returns a bin as a boolean value. Returns 'unknown'
 * if the bin is not a boolean.
 *
 * ~~~~~~~~~~{.c}
 * // Check if the value in bin "a" is true.
 * as_exp_build(expression, as_exp_bin_bool("a"));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (boolean bin)
 * @ingroup expression
 */
#define as_exp_bin_bool(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_BOOL), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a signed integer. Returns 'unknown'
 * if the bin is not an integer.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" == 200
 * as_exp_build(expression,
 *     as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(200)));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (integer bin)
 * @ingroup expression
 */
#define as_exp_bin_int(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_INT), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a float. Returns 'unknown' if the bin
 * is not an float.
 *
 * ~~~~~~~~~~{.c}
 * // Float bin "a" >= 2.71
 * as_exp_build(expression,
 *     as_exp_cmp_ge(as_exp_bin_int("a"), as_exp_float(2.71)));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (float bin)
 * @ingroup expression
 */
#define as_exp_bin_float(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_FLOAT), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a string. Returns 'unknown' if the
 * bin is not an string.
 *
 * ~~~~~~~~~~{.c}
 * // String bin "a" == "b"
 * as_exp_build(expression,
 *     as_exp_cmp_eq(as_exp_bin_str("a"), as_exp_str("b")));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (string bin)
 * @ingroup expression
 */
#define as_exp_bin_str(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_STR), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a blob. Returns 'unknown' if the bin
 * is not an blob.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" == { 0x65, 0x65 }
 * uint8_t val[] = {0x65, 0x65}
 * as_exp_build(expression,
 *     as_exp_cmp_eq(as_exp_bin_blob("a"), as_exp_bytes(val, sizeof(val))));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (blob bin)
 * @ingroup expression
 */
#define as_exp_bin_blob(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_BLOB), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a geojson. Returns 'unknown' if the
 * bin is not geojson.
 *
 * ~~~~~~~~~~{.c}
 * // GeoJSON bin "a" contained by GeoJSON bin "b"
 * as_exp_build(expression,
 *     as_exp_cmp_geo(as_exp_bin_geo("a"), as_exp_bin_geo("b")));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (geojson bin)
 * @ingroup expression
 */
#define as_exp_bin_geo(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_GEOJSON), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a list. Returns 'unknown' if the bin
 * is not an list.
 *
 * ~~~~~~~~~~{.c}
 * // List bin "a" contains at least one item == "abc"
 * as_exp_build(filter,
 *     as_exp_cmp_gt(
 *         as_exp_list_get_by_value(NULL, AS_LIST_RETURN_COUNT,
 *             as_exp_str("abc"), as_exp_bin_list("a")),
 *         as_exp_int(0)));
  * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (list bin)
 * @ingroup expression
 */
#define as_exp_bin_list(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_LIST), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a map. Returns 'unknown' if the bin
 * is not an map.
 *
 * ~~~~~~~~~~{.c}
 * // Map bin "a" size > 7.
 * as_exp_build(expression,
 *     as_exp_cmp_gt(as_exp_map_size(NULL, as_exp_bin_map("a")),
 *     as_exp_int(7)));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (map bin)
 * @ingroup expression
 */
#define as_exp_bin_map(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_MAP), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns a bin as a HyperLogLog (hll). Returns
 * 'unknown' if the bin is not a HyperLogLog (hll).
 *
 * ~~~~~~~~~~{.c}
 * // HLL bin "a" have an hll_count > 1000000
 * as_exp_build(expression,
 *     as_exp_cmp_gt(as_exp_hll_get_count(AS_BIN_HLL("a")),
 *          as_exp_int(1000000)));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (hll bin)
 * @ingroup expression
 */
#define as_exp_bin_hll(__bin_name) \
		{.op=_AS_EXP_CODE_BIN, .count=3}, \
		as_exp_int(AS_EXP_TYPE_HLL), \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/**
 * Create expression that returns if bin of specified name exists.
 *
 * ~~~~~~~~~~{.c}
 * // Bin "a" exists in record.
 * as_exp_build(expression, as_exp_bin_exists("a"));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (boolean value) True if the bin exists, false otherwise.
 * @ingroup expression
 */
#define as_exp_bin_exists(__bin_name) \
		as_exp_cmp_ne(as_exp_bin_type(__bin_name), as_exp_int(AS_BYTES_UNDEF))

/**
 * Create expression that returns the type of a bin as a integer.
 *
 * ~~~~~~~~~~{.c}
 * // bin "a" == type.string
 * as_exp_build(expression,
 *     as_exp_cmp_eq(as_exp_bin_type("a"), as_exp_int(AS_BYTES_STRING)));
 * ~~~~~~~~~~
 *
 * @param __bin_name			Bin name.
 * @return (integer value) returns the bin_type as an as_bytes_type.
 * @ingroup expression
 */
#define as_exp_bin_type(__bin_name) \
		{.op=_AS_EXP_CODE_BIN_TYPE, .count=2}, \
		_AS_EXP_VAL_RAWSTR(__bin_name)

/*********************************************************************************
 * METADATA EXPRESSIONS
 *********************************************************************************/

/**
 * Create expression that returns record set name string. This expression usually
 * evaluates quickly because record meta data is cached in memory.
 *
 * ~~~~~~~~~~{.c}
 * // Record set name == "myset"
 * as_exp_build(expression,
 *     as_exp_cmp_eq(as_exp_set_name(), as_exp_str("myset")));
 * ~~~~~~~~~~
 *
 * @return (string value) Name of the set this record belongs to.
 * @ingroup expression
 */
#define as_exp_set_name() {.op=_AS_EXP_CODE_SET_NAME, .count=1}

/**
 * Create expression that returns the record size. This expression usually evaluates
 * quickly because record meta data is cached in memory.
 *
 * Requires server version 7.0+. This expression replaces as_exp_device_size() and
 * as_exp_memory_size().
 *
 * ~~~~~~~~~~{.c}
 * // Record size >= 100 KB
 * as_exp_build(expression,
 * 		as_exp_cmp_ge(as_exp_record_size(), as_exp_int(100 * 1024)));
 * ~~~~~~~~~~
 *
 * @return (integer value) Uncompressed size of the record.
 * @ingroup expression
 */
#define as_exp_record_size() {.op=_AS_EXP_CODE_RECORD_SIZE, .count=1}

/**
 * Create expression that returns record size on disk. If server storage-engine is
 * memory, then zero is returned. This expression usually evaluates quickly
 * because record meta data is cached in memory.
 *
 * This expression should only be used for server versions less than 7.0. Use
 * as_exp_record_size() for server version 7.0+.
 *
 * ~~~~~~~~~~{.c}
 * // Record device size >= 100 KB
 * as_exp_build(expression,
 * 		as_exp_cmp_ge(as_exp_device_size(), as_exp_int(100 * 1024)));
 * ~~~~~~~~~~
 *
 * @return (integer value) Uncompressed storage size of the record.
 * @ingroup expression
 */
#define as_exp_device_size() {.op=_AS_EXP_CODE_DEVICE_SIZE, .count=1}

/**
 * Create expression that returns record last update time expressed as 64 bit
 * integer nanoseconds since 1970-01-01 epoch.
 *
 * ~~~~~~~~~~{.c}
 * // Record last update time >= 2020-01-15
 * as_exp_build(expression,
 * 		as_exp_cmp_ge(as_exp_last_update(), as_exp_uint(1577836800)));
 * ~~~~~~~~~~
 *
 * @return (integer value) When the record was last updated.
 * @ingroup expression
 */
#define as_exp_last_update() {.op=_AS_EXP_CODE_LAST_UPDATE, .count=1}

/**
 * Create expression that returns milliseconds since the record was last updated.
 * This expression usually evaluates quickly because record meta data is cached
 * in memory.
 *
 * ~~~~~~~~~~{.c}
 * // Record last updated more than 2 hours ago
 * as_exp_build(expression,
 *     as_exp_cmp_gt(as_exp_since_update(),
 *         as_exp_int(2 * 60 * 60 * 1000)))
 * ~~~~~~~~~~
 *
 * @return (integer value) Number of milliseconds since last updated.
 * @ingroup expression
 */
#define as_exp_since_update() {.op=_AS_EXP_CODE_SINCE_UPDATE, .count=1}

/**
 * Create expression that returns record expiration time expressed as 64 bit
 * integer nanoseconds since 1970-01-01 epoch.
 *
 * ~~~~~~~~~~{.c}
 * // Record expires on 2021-01-01
 * as_exp_build(expression,
 *     as_exp_and(
 *         as_exp_cmp_ge(as_exp_void_time(), as_exp_int(1609459200)),
 *         as_exp_cmp_lt(as_exp_void_time(), as_exp_int(1609545600))));
 * ~~~~~~~~~~
 *
 * @return (integer value) Expiration time in nanoseconds since 1970-01-01.
 * @ingroup expression
 */
#define as_exp_void_time() {.op=_AS_EXP_CODE_VOID_TIME, .count=1}

/**
 * Create expression that returns record expiration time (time to live) in integer
 * seconds.
 *
 * ~~~~~~~~~~{.c}
 * // Record expires in less than 1 hour
 * as_exp_build(expression,
 *     as_exp_cmp_lt(as_exp_ttl(), as_exp_int(60 * 60)));
 * ~~~~~~~~~~
 *
 * @return (integer value) Number of seconds till the record will expire,
 *                         returns -1 if the record never expires.
 * @ingroup expression
 */
#define as_exp_ttl() {.op=_AS_EXP_CODE_TTL, .count=1}

/**
 * Create expression that returns if record has been deleted and is still in
 * tombstone state. This expression usually evaluates quickly because record
 * meta data is cached in memory.
 *
 * This expression works for XDR filters and when used from a write request within
 * as_operations_exp_write() or as_operations_exp_read(). This expression does not
 * work with normal filtering of records because the tombstone record will be filtered
 * out before this expression is evaluated.
 *
 * ~~~~~~~~~~{.c}
 * // Deleted records that are in tombstone state.
 * as_exp_build(expression, as_exp_is_tombstone());
 * ~~~~~~~~~~
 *
 * @return (boolean value) True if the record is a tombstone, false otherwise.
 * @ingroup expression
 */
#define as_exp_is_tombstone() {.op=_AS_EXP_CODE_IS_TOMBSTONE, .count=1}

/**
 * Create expression that returns record size in memory when either the
 * storage-engine is memory or data-in-memory is true, otherwise returns 0.
 * This expression usually evaluates quickly because record meta data is cached
 * in memory.
 *
 * Requires server version between 5.3 inclusive and 7.0 exclusive.
 * Use as_exp_record_size() for server version 7.0+.
 *
 * ~~~~~~~~~~{.c}
 * // Record memory size >= 100 KB
 * as_exp_build(expression,
 * 		as_exp_cmp_ge(as_exp_memory_size(), as_exp_int(100 * 1024)));
 * ~~~~~~~~~~
 *
 * @return (integer value) memory size of the record.
 * @ingroup expression
 */
#define as_exp_memory_size() {.op=_AS_EXP_CODE_MEMORY_SIZE, .count=1}

/**
 * Create expression that returns record digest modulo as integer.
 *
 * ~~~~~~~~~~{.c}
 * // Records that have digest(key) % 3 == 1
 * as_exp_build(expression,
 * 		as_exp_cmp_eq(as_exp_digest_modulo(3), as_exp_int(1)));
 * ~~~~~~~~~~
 *
 * @param __mod			Divisor used to divide the digest to get a remainder.
 * @return (integer value) Value in range 0 and mod (exclusive)..
 * @ingroup expression
 */
#define as_exp_digest_modulo(__mod) \
		{.op=_AS_EXP_CODE_DIGEST_MODULO, .count=2}, \
		as_exp_int(__mod)

/*********************************************************************************
 * COMPARISON EXPRESSIONS
 *********************************************************************************/

/**
 * Create equals (==) expression.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" == 11.
 * as_exp_build(expression, as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(11)));
 * ~~~~~~~~~~
 *
 * @param __left			left expression in comparison.
 * @param __right			right expression in comparison.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_eq(__left, __right) \
		{.op=_AS_EXP_CODE_CMP_EQ, .count=3}, __left, __right

/**
 * Create not equal (!=) expression.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" != 13.
 * as_exp_build(expression, as_exp_cmp_ne(as_exp_bin_int("a"), as_exp_int(13)));

 * ~~~~~~~~~~
 *
 * @param __left			left expression in comparison.
 * @param __right			right expression in comparison.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_ne(__left, __right) \
		{.op=_AS_EXP_CODE_CMP_NE, .count=3}, __left, __right

/**
 * Create a greater than (>) expression.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" > 8.
 * as_exp_build(expression, as_exp_cmp_gt(as_exp_bin_int("a"), as_exp_int(8)));
 * ~~~~~~~~~~
 *
 * @param __left			left expression in comparison.
 * @param __right			right expression in comparison.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_gt(__left, __right) \
		{.op=_AS_EXP_CODE_CMP_GT, .count=3}, __left, __right

/**
 * Create a greater than or equals (>=) expression.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" >= 88.
 * as_exp_build(expression, as_exp_cmp_ge(as_exp_bin_int("a"), as_exp_int(88)));
 * ~~~~~~~~~~
 *
 * @param __left			left expression in comparison.
 * @param __right			right expression in comparison.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_ge(__left, __right) \
		{.op=_AS_EXP_CODE_CMP_GE, .count=3}, __left, __right

/**
 * Create a less than (<) expression.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" < 1000.
 * as_exp_build(expression, as_exp_cmp_lt(as_exp_bin_int("a"), as_exp_int(1000)));
 * ~~~~~~~~~~
 *
 * @param __left			left expression in comparison.
 * @param __right			right expression in comparison.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_lt(__left, __right) \
		{.op=_AS_EXP_CODE_CMP_LT, .count=3}, __left, __right

/**
 * Create a less than or equals (<=) expression.
 *
 * ~~~~~~~~~~{.c}
 * // Integer bin "a" <= 1.
 * as_exp_build(expression, as_exp_cmp_le(as_exp_bin_int("a"), as_exp_int(1)));
 * ~~~~~~~~~~
 *
 * @param __left			left expression in comparison.
 * @param __right			right expression in comparison.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_le(__left, __right) \
		{.op=_AS_EXP_CODE_CMP_LE, .count=3}, __left, __right

/**
 * Create expression that performs a regex match on a string bin or value
 * expression.
 *
 * ~~~~~~~~~~{.c}
 * // Select string bin "a" that starts with "prefix" and ends with "suffix".
 * // Ignore case and do not match newline.
 * as_exp_build(expression,
 *     as_exp_cmp_regex(REG_ICASE | REG_NEWLINE, as_exp_str("prefix.*suffix"), as_exp_bin_str("a")));
 * ~~~~~~~~~~
 *
 * @param __options			POSIX regex flags defined in regex.h.
 * @param __regex_str		POSIX regex string.
 * @param __cmp_str			String expression to compare against.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_regex(__options, __regex_str, __cmp_str) \
		{.op=_AS_EXP_CODE_CMP_REGEX, .count=4}, \
		as_exp_int(__options), _AS_EXP_VAL_RAWSTR(__regex_str), \
		__cmp_str

/**
 * Create a point within region or region contains point expression.
 *
 * ~~~~~~~~~~{.c}
 * // Geo bin "point" is within geo bin "region".
 * as_exp_build(expression,
 *     as_exp_cmp_geo(as_exp_bin_geo("point"), as_exp_bin_geo("region")));
 * ~~~~~~~~~~
 *
 * @param __left			left expression in comparison.
 * @param __right			right expression in comparison.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_cmp_geo(__left, __right) \
		{.op=_AS_EXP_CODE_CMP_GEO, .count=3}, __left, __right

/*********************************************************************************
 * LOGICAL EXPRESSIONS
 *********************************************************************************/

/**
 * Create "not" (!) operator expression.
 *
 * ~~~~~~~~~~{.c}
 * // ! (a == 0 || a == 10)
 * as_exp_build(expression,
 *     as_exp_not(as_exp_or(
 *         as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(0)),
 *         as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(10)))));
  * ~~~~~~~~~~
 *
 * @param __expr			Boolean expression to negate.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_not(__expr) {.op=_AS_EXP_CODE_NOT, .count=2}, __expr

/**
 * Create "and" (&&) operator that applies to a variable number of expressions.
 *
 * ~~~~~~~~~~{.c}
 * // (a > 5 || a == 0) && b < 3
 * as_exp_build(expression,
 *     as_exp_and(
 *         as_exp_or(
 *             as_exp_cmp_gt(as_exp_bin_int("a"), as_exp_int(5)),
 *             as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(0)))
 *         as_exp_cmp_lt(as_exp_bin_int("b"), as_exp_int(3))));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of boolean expressions.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_and(...) {.op=_AS_EXP_CODE_AND}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create "or" (||) operator that applies to a variable number of expressions.
 *
 * ~~~~~~~~~~{.c}
 * // a == 0 || b == 0
 * as_exp_build(expression,
 *     as_exp_or(
 *         as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(0)),
 *         as_exp_cmp_eq(as_exp_bin_int("b"), as_exp_int(0))));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of boolean expressions.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_or(...) {.op=_AS_EXP_CODE_OR}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create expression that returns true if only one of the expressions are true.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // exclusive(a == 0, b == 0)
 * as_exp_build(expression,
 *     as_exp_exclusive(
 *         as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(0)),
 *         as_exp_cmp_eq(as_exp_bin_int("b"), as_exp_int(0))));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of boolean expressions.
 * @return (boolean value)
 * @ingroup expression
 */
#define as_exp_exclusive(...) {.op=_AS_EXP_CODE_EXCLUSIVE}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/*********************************************************************************
 * ARITHMETIC EXPRESSIONS
 *********************************************************************************/

/**
 * Create "add" (+) operator that applies to a variable number of expressions.
 * Return the sum of all arguments.
 * All arguments must be the same type (integer or float).
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a + b + c == 10
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_add(as_exp_bin_int("a"), as_exp_bin_int("b"), as_exp_bin_int("c")),
 *         as_exp_int(10)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer or float expressions.
 * @return (integer or float value)
 * @ingroup expression
 */
#define as_exp_add(...) {.op=_AS_EXP_CODE_ADD}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create "subtract" (-) operator that applies to a variable number of expressions.
 * If only one argument is provided, return the negation of that argument.
 * Otherwise, return the sum of the 2nd to Nth argument subtracted from the 1st
 * argument. All arguments must resolve to the same type (integer or float).
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a - b - c > 10
 * as_exp_build(expression,
 *     as_exp_cmp_gt(
 *         as_exp_sub(as_exp_bin_int("a"), as_exp_bin_int("b"), as_exp_bin_int("c")),
 *         as_exp_int(10)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer or float expressions.
 * @return (integer or float value)
 * @ingroup expression
 */
#define as_exp_sub(...) {.op=_AS_EXP_CODE_SUB}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create "multiply" (*) operator that applies to a variable number of expressions.
 * Return the product of all arguments. If only one argument is supplied, return
 * that argument. All arguments must resolve to the same type (integer or float).
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a * b * c < 100
 * as_exp_build(expression,
 *     as_exp_cmp_lt(
 *         as_exp_mul(as_exp_bin_int("a"), as_exp_bin_int("b"), as_exp_bin_int("c")),
 *         as_exp_int(100)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer or float expressions.
 * @return (integer or float value)
 * @ingroup expression
 */
#define as_exp_mul(...) {.op=_AS_EXP_CODE_MUL}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create "divide" (/) operator that applies to a variable number of expressions.
 * If there is only one argument, returns the reciprocal for that argument.
 * Otherwise, return the first argument divided by the product of the rest.
 * All arguments must resolve to the same type (integer or float).
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a / b / c == 1
 * as_exp_build(expression,
 *     as_exp_cmp_gt(
 *         as_exp_div(as_exp_bin_int("a"), as_exp_bin_int("b"), as_exp_bin_int("c")),
 *         as_exp_int(1)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer or float expressions.
 * @return (integer or float value)
 * @ingroup expression
 */
#define as_exp_div(...) {.op=_AS_EXP_CODE_DIV}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create "pow" operator that raises a "base" to the "exponent" power.
 * All arguments must resolve to floats.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // pow(a, 2.0) == 4.0
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_pow(as_exp_bin_float("a"), as_exp_float(2.0)),
 *         as_exp_float(4.0)));
 * ~~~~~~~~~~
 *
 * @param __base		Base value.
 * @param __exponent	Exponent value.
 * @return (float value)
 * @ingroup expression
 */
#define as_exp_pow(__base, __exponent) \
		{.op=_AS_EXP_CODE_POW, .count=3}, __base, __exponent

/**
 * Create "log" operator for logarithm of "num" with base "base".
 * All arguments must resolve to floats.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // log(a, 2) == 4.0
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_log(as_exp_bin_float("a"), as_exp_float(2.0)),
 *         as_exp_float(4.0)));
 * ~~~~~~~~~~
 *
 * @param __num			Number.
 * @param __base		Base value.
 * @return (float value)
 * @ingroup expression
 */
#define as_exp_log(__num, __base) \
		{.op=_AS_EXP_CODE_LOG, .count=3}, __num, __base

/**
 * Create "modulo" (%) operator that determines the remainder of "numerator"
 * divided by "denominator". All arguments must resolve to integers.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a % 10 == 0
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_mod(as_exp_bin_int("a"), as_exp_int(10)),
 *         as_exp_int(0)));
 * ~~~~~~~~~~
 *
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_mod(__numerator, __denominator) \
		{.op=_AS_EXP_CODE_MOD, .count=3}, __numerator, __denominator

/**
 * Create operator that returns absolute value of a number.
 * All arguments must resolve to integer or float.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // abs(a) == 1
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_abs(as_exp_bin_int("a")),
 *         as_exp_int(1)));
 * ~~~~~~~~~~
 *
 * @return (number value)
 * @ingroup expression
 */
#define as_exp_abs(__value) \
		{.op=_AS_EXP_CODE_ABS, .count=2}, __value

/**
 * Create expression that rounds a floating point number down to the closest integer value.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // floor(2.95) == 2.0
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_floor(as_exp_float(2.95)),
 *         as_exp_float(2.0)));
 * ~~~~~~~~~~
 *
 * @param __num			Floating point value to round down.
 * @return (float-value)
 * @ingroup expression
 */
#define as_exp_floor(__num) \
		{.op=_AS_EXP_CODE_FLOOR, .count=2}, __num

/**
 * Create expression that rounds a floating point number up to the closest integer value.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // ceil(2.15) == 3.0
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_ceil(as_exp_float(2.15)),
 *         as_exp_float(3.0)));
 * ~~~~~~~~~~
 *
 * @param __num			Floating point value to round up.
 * @return (integer-value)
 * @ingroup expression
 */
#define as_exp_ceil(__num) \
	{.op=_AS_EXP_CODE_CEIL, .count=2}, __num

/**
 * Create expression that converts a float to an integer.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // int(2.5) == 2
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_to_int(as_exp_float(2.5)),
 *         as_exp_int(2)));
 * ~~~~~~~~~~
 *
 * @param __num			Integer to convert to a float
 * @return (float-value)
 * @ingroup expression
 */
#define as_exp_to_int(__num) \
	{.op=_AS_EXP_CODE_TO_INT, .count=2}, __num

/**
 * Create expression that converts an integer to a float.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // float(2) == 2.0
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_to_float(as_exp_int(2)),
 *         as_exp_int(2.0)));
 * ~~~~~~~~~~
 *
 * @param __num			Integer to convert to a float
 * @return (float-value)
 * @ingroup expression
 */
#define as_exp_to_float(__num) \
	{.op=_AS_EXP_CODE_TO_FLOAT, .count=2}, __num

/**
 * Create integer "and" (&) operator that is applied to two or more integers.
 * All arguments must resolve to integers.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a & 0xff == 0x11
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_int_and(as_exp_bin_int("a"), as_exp_int(0xff)),
 *         as_exp_int(0x11)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer expressions.
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_and(...) {.op=_AS_EXP_CODE_INT_AND}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create integer "or" (|) operator that is applied to two or more integers.
 * All arguments must resolve to integers.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a | 0x10 != 0
 * as_exp_build(expression,
 *     as_exp_cmp_ne(
 *         as_exp_int_or(as_exp_bin_int("a"), as_exp_int(0x10)),
 *         as_exp_int(0)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer expressions.
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_or(...) {.op=_AS_EXP_CODE_INT_OR}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create integer "xor" (^) operator that is applied to two or more integers.
 * All arguments must resolve to integers.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a ^ b == 16
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_int_xor(as_exp_bin_int("a"), as_exp_bin_int("b")),
 *         as_exp_int(16)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer expressions.
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_xor(...) {.op=_AS_EXP_CODE_INT_XOR}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create integer "not" (~) operator.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // ~a == 7
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_int_not(as_exp_bin_int("a")),
 *         as_exp_int(7)));
 * ~~~~~~~~~~
 *
 * @param __expr		Integer expression.
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_not(__expr) {.op=_AS_EXP_CODE_INT_NOT, .count=2}, \
		__expr

/**
 * Create integer "left shift" (<<) operator.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a << 8 > 0xff
 * as_exp_build(expression,
 *     as_exp_cmp_gt(
 *         as_exp_int_lshift(as_exp_bin_int("a"), as_exp_int(8)),
 *         as_exp_int(0xff)));
 * ~~~~~~~~~~
 *
 * @param __value		Integer expression.
 * @param __shift		Number of bits to shift by.
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_lshift(__value, __shift) \
		{.op=_AS_EXP_CODE_INT_LSHIFT, .count=3}, __value, __shift

/**
 * Create integer "logical right shift" (>>>) operator.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a >>> 8 > 0xff
 * as_exp_build(expression,
 *     as_exp_cmp_gt(
 *         as_exp_int_rshift(as_exp_bin_int("a"), as_exp_int(8)),
 *         as_exp_int(0xff)));
 * ~~~~~~~~~~
 *
 * @param __value		Integer expression.
 * @param __shift		Number of bits to shift by.
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_rshift(__value, __shift) \
		{.op=_AS_EXP_CODE_INT_RSHIFT, .count=3}, __value, __shift

/**
 * Create integer "arithmetic right shift" (>>) operator.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // a >> 8 > 0xff
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_int_arshift(as_exp_bin_int("a"), as_exp_int(8)),
 *         as_exp_int(0xff)));
 * ~~~~~~~~~~
 *
 * @param __value		Integer expression.
 * @param __shift		Number of bits to shift by.
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_arshift(__value, __shift) \
		{.op=_AS_EXP_CODE_INT_ARSHIFT, .count=3}, __value, __shift

/**
 * Create expression that returns count of integer bits that are set to 1.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // count(a) == 4
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_int_count(as_exp_bin_int("a")),
 *         as_exp_int(4)));
 * ~~~~~~~~~~
 *
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_count(__expr) \
		{.op=_AS_EXP_CODE_INT_COUNT, .count=2}, __expr

/**
 * Create expression that scans integer bits from left (most significant bit) to
 * right (least significant bit), looking for a search bit value. When the
 * search value is found, the index of that bit (where the most significant bit is
 * index 0) is returned. If "search" is true, the scan will search for the bit
 * value 1. If "search" is false it will search for bit value 0.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // lscan(a, true) == 4
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_int_lscan(as_exp_bin_int("a"), as_exp_bool(true)),
 *         as_exp_int(4)));
 * ~~~~~~~~~~
 *
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_lscan(__value, __search) \
		{.op=_AS_EXP_CODE_INT_LSCAN, .count=3}, __value, __search

/**
 * Create expression that scans integer bits from right (least significant bit) to
 * left (most significant bit), looking for a search bit value. When the
 * search value is found, the index of that bit (where the most significant bit is
 * index 0) is returned. If "search" is true, the scan will search for the bit
 * value 1. If "search" is false it will search for bit value 0.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // rscan(a, true) == 4
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_int_rscan(as_exp_bin_int("a"), as_exp_bool(true)),
 *         as_exp_int(4)));
 * ~~~~~~~~~~
 *
 * @return (integer value)
 * @ingroup expression
 */
#define as_exp_int_rscan(__value, __search) \
		{.op=_AS_EXP_CODE_INT_RSCAN, .count=3}, __value, __search

/**
 * Create expression that returns the minimum value in a variable number of expressions.
 * All arguments must be the same type (integer or float).
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // min(a, b, c) > 0
 * as_exp_build(expression,
 *     as_exp_cmp_gt(
 *         as_exp_min(as_exp_bin_int("a"), as_exp_bin_int("b"), as_exp_bin_int("c")),
 *         as_exp_int(0)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer or float expressions.
 * @return (integer or float value)
 * @ingroup expression
 */
#define as_exp_min(...) {.op=_AS_EXP_CODE_MIN}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Create expression that returns the maximum value in a variable number of expressions.
 * All arguments must be the same type (integer or float).
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // max(a, b, c) > 100
 * as_exp_build(expression,
 *     as_exp_cmp_eq(
 *         as_exp_max(as_exp_bin_int("a"), as_exp_bin_int("b"), as_exp_bin_int("c")),
 *         as_exp_int(100)));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of integer or float expressions.
 * @return (integer or float value)
 * @ingroup expression
 */
#define as_exp_max(...) {.op=_AS_EXP_CODE_MAX}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/*********************************************************************************
 * FLOW CONTROL AND VARIABLE EXPRESSIONS
 *********************************************************************************/

/**
 * Conditionally select an action expression from a variable number of expression pairs
 * followed by a default action expression. Every action expression must return the same type.
 * The only exception is as_exp_unknown() which can be mixed with other types.
 *
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * Args Format: bool exp1, action exp1, bool exp2, action exp2, ..., action-default
 *
 * // Apply operator based on type and test if greater than 100
 * as_exp_build(expression,
 *     as_exp_cmp_gt(
 *         as_exp_cond(
 *             as_exp_eq(as_exp_bin_int("type"), as_exp_int(0)), 
 *                 as_exp_add(as_exp_bin_int("val1"), as_exp_bin_int("val2")),
 *             as_exp_eq(as_exp_bin_int("type"), as_exp_int(1)),
 *                 as_exp_sub(as_exp_bin_int("val1"), as_exp_bin_int("val2")),
 *             as_exp_eq(as_exp_bin_int("type"), as_exp_int(2)),
 *                 as_exp_mul(as_exp_bin_int("val1"), as_exp_bin_int("val2")),
 *             as_exp_int(-1)),
 *         as_exp_int(100)));
 * ~~~~~~~~~~
 *
 * @return first action expression where bool expression is true or action-default.
 * @ingroup expression
 */
#define as_exp_cond(...) {.op=_AS_EXP_CODE_COND}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Define variables and expressions in scope.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // 5 < a < 10
 * as_exp_build(expression,
 *     as_exp_let(as_exp_def("x", as_exp_bin_int("a")),
 *         as_exp_and(
 *             as_exp_lt(as_exp_int(5), as_exp_var("x")),
 *             as_exp_lt(as_exp_var("x"), as_exp_int(10)))));
 * ~~~~~~~~~~
 *
 * @param ...			Variable number of as_exp_def followed by a scoped
 *  expression.
 * @return result of scoped expression.
 * @ingroup expression
 */
#define as_exp_let(...) \
		{.op=_AS_EXP_CODE_LET}, __VA_ARGS__, \
		{.op=_AS_EXP_CODE_END_OF_VA_ARGS}

/**
 * Assign variable to an expression that can be accessed later.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // 5 < a < 10
 * as_exp_build(expression,
 *     as_exp_let(as_exp_def("x", as_exp_bin_int("a")),
 *         as_exp_and(
 *             as_exp_lt(as_exp_int(5), as_exp_var("x")),
 *             as_exp_lt(as_exp_var("x"), as_exp_int(10)))));
 * ~~~~~~~~~~
 *
 * @param __var_name		Variable name.
 * @param __expr			The variable is set to the result of __expr.
 * @return A variable name expression pair.
 * @ingroup expression
 */
#define as_exp_def(__var_name, __expr) \
		_AS_EXP_VAL_RAWSTR(__var_name), __expr

/**
 * Retrieve expression value from a variable.
 * Requires server version 5.6.0+.
 *
 * ~~~~~~~~~~{.c}
 * // 5 < a < 10
 * as_exp_build(expression,
 *     as_exp_let(as_exp_def("x", as_exp_bin_int("a")),
 *         as_exp_and(
 *             as_exp_lt(as_exp_int(5), as_exp_var("x")),
 *             as_exp_lt(as_exp_var("x"), as_exp_int(10)))));
 * ~~~~~~~~~~
 *
 * @param __var_name		Variable name.
 * @return value stored in variable.
 * @ingroup expression
 */
#define as_exp_var(__var_name) \
		{.op=_AS_EXP_CODE_VAR, .count=2}, _AS_EXP_VAL_RAWSTR(__var_name)

/*********************************************************************************
 * LIST MODIFY EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_VAL_RTYPE(__val) {.op=_AS_EXP_CODE_VAL_RTYPE, .v.int_val=__val}

#define _AS_EXP_LIST_MOD(__ctx, __pol, __op, __param, __extra_param) \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(as_exp_get_ctx_type(__ctx, AS_EXP_TYPE_LIST)), \
		as_exp_int(_AS_EXP_SYS_CALL_CDT | _AS_EXP_SYS_FLAG_MODIFY_LOCAL), \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + __param + ((uintptr_t)(__pol) == (uintptr_t)NULL ? 0 : __extra_param), .v.ctx=__ctx}, \
		as_exp_int(__op)

/**
 * Create expression that appends value to end of list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional list write policy (as_list_policy).
 * @param __val			Value expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_append(__ctx, __pol, __val, __bin) \
		_AS_EXP_LIST_MOD(__ctx, __pol, AS_CDT_OP_LIST_APPEND, 1, 2), \
		__val, \
		{.op=_AS_EXP_CODE_CDT_LIST_CRMOD, .v.list_pol = __pol}, \
		__bin

/**
 * Create expression that appends list items to end of list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional list write policy (as_list_policy).
 * @param __val			List items expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_append_items(__ctx, __pol, __val, __bin) \
		_AS_EXP_LIST_MOD(__ctx, __pol, AS_CDT_OP_LIST_APPEND_ITEMS, 1, 2), \
		__val, \
		{.op=_AS_EXP_CODE_CDT_LIST_CRMOD, .v.list_pol = __pol}, \
		__bin

/**
 * Create expression that inserts value to specified index of list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional list write policy (as_list_policy).
 * @param __idx			Index integer expression.
 * @param __val			Value expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_insert(__ctx, __pol, __idx, __val, __bin) \
		_AS_EXP_LIST_MOD(__ctx, __pol, AS_CDT_OP_LIST_INSERT, 2, 1), \
		__idx, __val, \
		{.op=_AS_EXP_CODE_CDT_LIST_MOD, .v.list_pol = __pol}, \
		__bin

/**
 * Create expression that inserts each input list item starting at specified index of list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional list write policy (as_list_policy).
 * @param __idx			Index integer expression.
 * @param __val			List items expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_insert_items(__ctx, __pol, __idx, __val, __bin) \
		_AS_EXP_LIST_MOD(__ctx, __pol, AS_CDT_OP_LIST_INSERT_ITEMS, 2, 1), \
		__idx, __val, \
		{.op=_AS_EXP_CODE_CDT_LIST_MOD, .v.list_pol = __pol}, \
		__bin

/**
 * Create expression that increments list[index] by value.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional list write policy (as_list_policy).
 * @param __idx			Index integer expression.
 * @param __val			Value expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_increment(__ctx, __pol, __idx, __val, __bin) \
		_AS_EXP_LIST_MOD(__ctx, __pol, AS_CDT_OP_LIST_INCREMENT, 2, 2), \
		__idx, __val, \
		{.op=_AS_EXP_CODE_CDT_LIST_CRMOD, .v.list_pol = __pol}, \
		__bin

/**
 * Create expression that sets item value at specified index in list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional list write policy (as_list_policy).
 * @param __idx			Index integer expression.
 * @param __val			Value expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_set(__ctx, __pol, __idx, __val, __bin) \
		_AS_EXP_LIST_MOD(__ctx, __pol, AS_CDT_OP_LIST_SET, 2, 1), \
		__idx, __val, \
		{.op=_AS_EXP_CODE_CDT_LIST_MOD, .v.list_pol = __pol}, \
		__bin

/**
 * Create expression that removes all items in list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_clear(__ctx, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_CLEAR, 0, 0), \
		__bin

/**
 * Create expression that sorts list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __order		Sort order (as_list_sort_flags).
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_sort(__ctx, __order, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_SORT, 1, 0), \
		as_exp_int(__order), \
		__bin

/**
 * Create expression that removes list items identified by value.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __val			Value expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_value(__ctx, __rtype, __val, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_ALL_BY_VALUE, 2, 0), \
		as_exp_int(__rtype), \
		__val, \
		__bin

/**
 * Create expression that removes list items identified by values.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __values		Values list expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_value_list(__ctx, __rtype, __values, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_VALUE_LIST, 2, 0), \
		as_exp_int(__rtype), \
		__values, \
		__bin

/**
 * Create expression that removes list items identified by value range
 * (begin inclusive, end exclusive). If begin is nil, the range is less than end.
 * If end is infinity, the range is greater than equal to begin.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __begin		Begin value expression.
 * @param __end			End value expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_value_range(__ctx, __rtype, __begin, __end, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_VALUE_INTERVAL, 3, 0), \
		as_exp_int(__rtype), \
		__begin, __end, \
		__bin

/**
 * Create expression that removes list items nearest to value and greater by relative rank.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __val			Value expression.
 * @param __rank		Rank integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_rel_rank_range_to_end(__ctx, __rtype, __val, __rank, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE, 3, 0), \
		as_exp_int(__rtype), \
		__val, __rank, \
		__bin

/**
 * Create expression that removes list items nearest to value and greater by relative rank with a
 * count limit.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __val			Value expression.
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_rel_rank_range(__ctx, __rtype, __val, __rank, __count, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_VALUE_REL_RANK_RANGE, 4, 0), \
		as_exp_int(__rtype), \
		__val, __rank, __count, \
		__bin

/**
 * Create expression that removes list item identified by index.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __idx			Index integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_index(__ctx, __idx, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_INDEX, 2, 0), \
		as_exp_int(AS_LIST_RETURN_NONE), \
		__idx, \
		__bin

/**
 * Create expression that removes list items starting at specified index to the end of list.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __idx			Index integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_index_range_to_end(__ctx, __rtype, __idx, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE, 2, 0), \
		as_exp_int(__rtype), \
		__idx, \
		__bin

/**
 * Create expression that removes "count" list items starting at specified index.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __idx			Index integer expression.
 * @param __count		Count integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_index_range(__ctx, __rtype, __idx, __count, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_INDEX_RANGE, 3, 0), \
		as_exp_int(__rtype), \
		__idx, __count, \
		__bin

/**
 * Create expression that removes list item identified by rank.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rank		Rank integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_rank(__ctx, __rank, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_RANK, 2, 0), \
		as_exp_int(AS_LIST_RETURN_NONE), \
		__rank, \
		__bin

/**
 * Create expression that removes list items starting at specified rank to the last ranked item.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __rank		Rank integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_rank_range_to_end(__ctx, __rtype, __rank, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE, 2, 0), \
		as_exp_int(__rtype), \
		__rank, \
		__bin

/**
 * Create expression that removes "count" list items starting at specified rank.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_LIST_RETURN_NONE or AS_LIST_RETURN_INVERTED.
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			List bin or list value expression.
 * @return (list expression)
 * @ingroup expression
 */
#define as_exp_list_remove_by_rank_range(__ctx, __rtype, __rank, __count, __bin) \
		_AS_EXP_LIST_MOD(__ctx, NULL, AS_CDT_OP_LIST_REMOVE_BY_RANK_RANGE, 3, 0), \
		as_exp_int(__rtype), \
		__rank, __count, \
		__bin

/*********************************************************************************
 * LIST READ EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_CDT_LIST_READ(__type, __rtype, __is_multi) \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(as_exp_get_list_type(__type, __rtype, __is_multi)), \
		as_exp_int(_AS_EXP_SYS_CALL_CDT)

#define _AS_EXP_LIST_START(__ctx, __op, __param) \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + __param, .v.ctx=__ctx}, \
		as_exp_int(__op)

/**
 * Create expression that returns list size.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __bin			List bin or list value expression.
 * @return (integer expression)
 * @ingroup expression
 */
#define as_exp_list_size(__ctx, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, AS_LIST_RETURN_COUNT, false), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_SIZE, 0), \
		__bin

/**
 * Create expression that selects list items identified by value and returns selected
 * data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __val			Value expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_value(__ctx, __rtype, __val, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_ALL_BY_VALUE, 2), \
		as_exp_int(__rtype), \
		__val, \
		__bin

/**
 * Create expression that selects list items identified by value range and returns selected
 * data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __begin		Begin value expression.
 * @param __end			End value expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_value_range(__ctx, __rtype, __begin, __end, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_VALUE_INTERVAL, 3), \
		as_exp_int(__rtype), \
		__begin, __end, \
		__bin

/**
 * Create expression that selects list items identified by values and returns selected
 * data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __val			Values list expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_value_list(__ctx, __rtype, __val, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_VALUE_LIST, 2), \
		as_exp_int(__rtype), \
		__val, \
		__bin

/**
 * Create expression that selects list items nearest to value and greater by relative rank
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __val			Values list expression.
 * @param __rank		Rank integer expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_rel_rank_range_to_end(__ctx, __rtype, __val, __rank, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE, 3), \
		as_exp_int(__rtype), \
		__val, __rank, \
		__bin

/**
 * Create expression that selects list items nearest to value and greater by relative rank with a
 * count limit and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __val			Values list expression.
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_rel_rank_range(__ctx, __rtype, __val, __rank, __count, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_VALUE_REL_RANK_RANGE, 4), \
		as_exp_int(__rtype), \
		__val, __rank, __count, \
		__bin

/**
 * Create expression that selects list item identified by index
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __vtype		Value type (as_exp_type).
 * @param __idx			Index integer expression.
 * @param __bin			List bin or list value expression.
 * @return (vtype expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_index(__ctx, __rtype, __vtype, __idx, __bin) \
		_AS_EXP_CDT_LIST_READ(__vtype, __rtype, false), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_INDEX, 2), \
		as_exp_int(__rtype), \
		__idx, \
		__bin

/**
 * Create expression that selects list items starting at specified index to the end of list
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __idx			Index integer expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_index_range_to_end(__ctx, __rtype, __idx, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_INDEX_RANGE, 2), \
		as_exp_int(__rtype), \
		__idx, \
		__bin

/**
 * Create expression that selects "count" list items starting at specified index
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __idx			Index integer expression.
 * @param __count		Count integer expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_index_range(__ctx, __rtype, __idx, __count, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_INDEX_RANGE, 3), \
		as_exp_int(__rtype), \
		__idx, __count, \
		__bin

/**
 * Create expression that selects list item identified by rank
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __vtype		Value type (as_exp_type).
 * @param __rank		Rank integer expression.
 * @param __bin			List bin or list value expression.
 * @return (vtype expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_rank(__ctx, __rtype, __vtype, __rank, __bin) \
		_AS_EXP_CDT_LIST_READ(__vtype, __rtype, false), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_RANK, 2), \
		as_exp_int(__rtype), \
		__rank, \
		__bin

/**
 * Create expression that selects list items starting at specified rank to the last ranked item
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __rank		Rank integer expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_rank_range_to_end(__ctx, __rtype, __rank, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_RANK_RANGE, 2), \
		as_exp_int(__rtype), \
		__rank, \
		__bin

/**
 * Create expression that selects "count" list items starting at specified rank
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_list_return_type).
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			List bin or list value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_list_get_by_rank_range(__ctx, __rtype, __rank, __count, __bin) \
		_AS_EXP_CDT_LIST_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_LIST_START(__ctx, AS_CDT_OP_LIST_GET_BY_RANK_RANGE, 3), \
		as_exp_int(__rtype), \
		__rank, __count, \
		__bin

/*********************************************************************************
 * MAP MODIFY EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_MAP_MOD(__ctx, __pol, __op, __param, __extra_param) \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(as_exp_get_ctx_type(__ctx, AS_EXP_TYPE_MAP)), \
		as_exp_int(_AS_EXP_SYS_CALL_CDT | _AS_EXP_SYS_FLAG_MODIFY_LOCAL), \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + __param + ((uintptr_t)(__pol) == (uintptr_t)NULL ? 0 : __extra_param), .v.ctx=__ctx}, \
		as_exp_int(__op)

/**
 * Create expression that writes key/val item to map bin.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional map write policy (as_map_policy).
 * @param __key			Key expression.
 * @param __val			Value expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_put(__ctx, __pol, __key, __val, __bin) \
		_AS_EXP_MAP_MOD(__ctx, __pol, AS_CDT_OP_MAP_PUT, 2, 2), \
		__key, __val, \
		{.op=_AS_EXP_CODE_CDT_MAP_CRMOD, .v.map_pol = __pol}, \
		__bin

/**
 * Create expression that writes each map item to map bin.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional map write policy (as_map_policy).
 * @param __map			Source map expression.
 * @param __bin			Target map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_put_items(__ctx, __pol, __map, __bin) \
		_AS_EXP_MAP_MOD(__ctx, __pol, AS_CDT_OP_MAP_PUT_ITEMS, 1, 2), \
		__map, \
		{.op=_AS_EXP_CODE_CDT_MAP_CRMOD, .v.map_pol = __pol}, \
		__bin

/**
 * Create expression that increments values by incr for all items identified by key.
 * Valid only for numbers.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __pol			Optional map write policy (as_map_policy).
 * @param __key			Key expression.
 * @param __val			Increment value number expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_increment(__ctx, __pol, __key, __val, __bin) \
		_AS_EXP_MAP_MOD(__ctx, __pol, AS_CDT_OP_MAP_INCREMENT, 2, 1), \
		__key, __val, \
		{.op=_AS_EXP_CODE_CDT_MAP_MOD, .v.map_pol = __pol}, \
		__bin

/**
 * Create expression that removes all items in map.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_clear(__ctx, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_CLEAR, 0, 0), \
		__bin

/**
 * Create expression that removes map item identified by key.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __key			Key expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_key(__ctx, __key, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_KEY, 2, 0), \
		as_exp_int(AS_MAP_RETURN_NONE), \
		__key, \
		__bin

/**
 * Create expression that removes map items identified by keys.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __keys		List expression of keys to remove.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_key_list(__ctx, __rtype, __keys, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_KEY_LIST, 2, 0), \
		as_exp_int(__rtype), \
		__keys, \
		__bin

/**
 * Create expression that removes map items identified by key range 
 * (begin inclusive, end exclusive). If begin is nil, the range is less than end.
 * If end is infinity, the range is greater than equal to begin.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __begin		Begin value expression.
 * @param __end			End value expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_key_range(__ctx, __rtype, __begin, __end, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_KEY_INTERVAL, 3, 0), \
		as_exp_int(__rtype), \
		__begin, __end, \
		__bin

/**
 * Create expression that removes map items nearest to key and greater by index.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __key			Key expression.
 * @param __idx			Index integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_key_rel_index_range_to_end(__ctx, __rtype, __key, __idx, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE, 3, 0), \
		as_exp_int(__rtype), \
		__key, __idx, \
		__bin

/**
 * Create expression that removes map items nearest to key and greater by index with a count limit.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __key			Key expression.
 * @param __idx			Index integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_key_rel_index_range(__ctx, __rtype, __key, __idx, __count, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_KEY_REL_INDEX_RANGE, 4, 0), \
		as_exp_int(__rtype), \
		__key, __idx, __count, \
		__bin

/**
 * Create expression that removes map items identified by value.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __val			Value expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_value(__ctx, __rtype, __val, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_ALL_BY_VALUE, 2, 0), \
		as_exp_int(__rtype), \
		__val, \
		__bin

/**
 * Create expression that removes map items identified by values.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __values		Values list expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_value_list(__ctx, __rtype, __values, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_VALUE_LIST, 2, 0), \
		as_exp_int(__rtype), \
		__values, \
		__bin

/**
 * Create expression that removes map items identified by value range
 * (begin inclusive, end exclusive). If begin is nil, the range is less than end.
 * If end is infinity, the range is greater than equal to begin.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __begin		Begin value expression.
 * @param __end			End value expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_value_range(__ctx, __rtype, __begin, __end, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_VALUE_INTERVAL, 3, 0), \
		as_exp_int(__rtype), \
		__begin, __end, \
		__bin

/**
 * Create expression that removes map items nearest to value and greater by relative rank.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __val			Value expression.
 * @param __rank		Rank integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_value_rel_rank_range_to_end(__ctx, __rtype, __val, __rank, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE, 3, 0), \
		as_exp_int(__rtype), \
		__val, __rank, \
		__bin

/**
 * Create expression that removes map items nearest to value and greater by relative rank with a
 * count limit.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __val			Value expression.
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_value_rel_rank_range(__ctx, __rtype, __val, __rank, __count, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_VALUE_REL_RANK_RANGE, 4, 0), \
		as_exp_int(__rtype), \
		__val, __rank, __count, \
		__bin

/**
 * Create expression that removes map item identified by index.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __idx			Index integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_index(__ctx, __idx, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_INDEX, 2, 0), \
		as_exp_int(AS_MAP_RETURN_NONE), \
		__idx, \
		__bin

/**
 * Create expression that removes map items starting at specified index to the end of map.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __idx			Index integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_index_range_to_end(__ctx, __rtype, __idx, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE, 2, 0), \
		as_exp_int(__rtype), \
		__idx, \
		__bin

/**
 * Create expression that removes "count" map items starting at specified index.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __idx			Index integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_index_range(__ctx, __rtype, __idx, __count, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_INDEX_RANGE, 3, 0), \
		as_exp_int(__rtype), \
		__idx, __count, \
		__bin

/**
 * Create expression that removes map item identified by rank.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rank		Rank integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_rank(__ctx, __rank, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_RANK, 2, 0), \
		as_exp_int(AS_MAP_RETURN_NONE), \
		__rank, \
		__bin

/**
 * Create expression that removes map items starting at specified rank to the last ranked item.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __rank		Rank integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_rank_range_to_end(__ctx, __rtype, __rank, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE, 2, 0), \
		as_exp_int(__rtype), \
		__rank, \
		__bin

/**
 * Create expression that removes "count" map items starting at specified rank.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type. Valid values are AS_MAP_RETURN_NONE or AS_MAP_RETURN_INVERTED.
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (map expression)
 * @ingroup expression
 */
#define as_exp_map_remove_by_rank_range(__ctx, __rtype, __rank, __count, __bin) \
		_AS_EXP_MAP_MOD(__ctx, NULL, AS_CDT_OP_MAP_REMOVE_BY_RANK_RANGE, 3, 0), \
		as_exp_int(__rtype), \
		__rank, __count, \
		__bin

/*********************************************************************************
 * MAP READ EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_MAP_READ(__type__, __rtype, __is_multi) \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(as_exp_get_map_type(__type__, __rtype, __is_multi)), \
		as_exp_int(_AS_EXP_SYS_CALL_CDT)

#define _AS_EXP_MAP_START(__ctx, __op, __param) \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + __param, .v.ctx=__ctx}, \
		as_exp_int(__op)

/**
 * Create expression that returns map size.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __bin			Map bin or map value expression.
 * @return (integer expression)
 * @ingroup expression
 */
#define as_exp_map_size(__ctx, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, AS_MAP_RETURN_COUNT, false), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_SIZE, 0), \
		__bin

/**
 * Create expression that selects map item identified by key
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __vtype		Value type (as_exp_type).
 * @param __key			Key expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_key(__ctx, __rtype, __vtype, __key, __bin) \
		_AS_EXP_MAP_READ(__vtype, __rtype, false), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_KEY, 2), \
		as_exp_int(__rtype), \
		__key, \
		__bin

/**
 * Create expression that selects map items identified by key range
 * (begin inclusive, end exclusive). If begin is nil, the range is less than end.
 * If end is infinity, the range is greater than equal to begin.
 * Expression returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __begin		Begin key expression.
 * @param __end			End key expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_key_range(__ctx, __rtype, __begin, __end, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_KEY_INTERVAL, 3), \
		as_exp_int(__rtype), \
		__begin, __end, \
		__bin

/**
 * Create expression that selects map items identified by keys
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __keys		Keys list expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_key_list(__ctx, __rtype, __keys, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_KEY_LIST, 2), \
		as_exp_int(__rtype), \
		__keys, \
		__bin

/**
 * Create expression that selects map items nearest to key and greater by index
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __key			Key expression.
 * @param __idx			Index integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_key_rel_index_range_to_end(__ctx, __rtype, __key, __idx, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE, 3), \
		as_exp_int(__rtype), \
		__key, __idx, \
		__bin

/**
 * Create expression that selects map items nearest to key and greater by index with a count limit.
 * Expression returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __key			Key expression.
 * @param __idx			Index integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_key_rel_index_range(__ctx, __rtype, __key, __idx, __count, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_KEY_REL_INDEX_RANGE, 4), \
		as_exp_int(__rtype), \
		__key, __idx, __count, \
		__bin

/**
 * Create expression that selects map items identified by value
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __val			Value expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_value(__ctx, __rtype, __val, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_ALL_BY_VALUE, 2), \
		as_exp_int(__rtype), \
		__val, \
		__bin

/**
 * Create expression that selects map items identified by value range
 * (begin inclusive, end exclusive). If begin is nil, the range is less than end.
 * If end is infinity, the range is greater than equal to begin.
 * Expression returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __begin		Begin value expression.
 * @param __end			End value expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_value_range(__ctx, __rtype, __begin, __end, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_VALUE_INTERVAL, 3), \
		as_exp_int(__rtype), \
		__begin, __end, \
		__bin

/**
 * Create expression that selects map items identified by values
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __values		Values list expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_value_list(__ctx, __rtype, __values, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_VALUE_LIST, 2), \
		as_exp_int(__rtype), \
		__values, \
		__bin

/**
 * Create expression that selects map items nearest to value and greater by relative rank.
 * Expression returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __val			Value expression.
 * @param __rank		Rank integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_value_rel_rank_range_to_end(__ctx, __rtype, __val, __rank, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE, 3), \
		as_exp_int(__rtype), \
		__val, __rank, \
		__bin

/**
 * Create expression that selects map items nearest to value and greater by relative rank with a
 * count limit. Expression returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __val			Value expression.
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_value_rel_rank_range(__ctx, __rtype, __val, __rank, __count, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_VALUE_REL_RANK_RANGE, 4), \
		as_exp_int(__rtype), \
		__val, __rank, __count, \
		__bin

/**
 * Create expression that selects map item identified by index
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __vtype		Value type (as_exp_type).
 * @param __idx			Index integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_index(__ctx, __rtype, __vtype, __idx, __bin) \
		_AS_EXP_MAP_READ(__vtype, __rtype, false), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_INDEX, 2), \
		as_exp_int(__rtype), \
		__idx, \
		__bin

/**
 * Create expression that selects map items starting at specified index to the end of map
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __idx			Index integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_index_range_to_end(__ctx, __rtype, __idx, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_INDEX_RANGE, 2), \
		as_exp_int(__rtype), \
		__idx, \
		__bin

/**
 * Create expression that selects "count" map items starting at specified index
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __idx			Index integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_index_range(__ctx, __rtype, __idx, __count, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_INDEX_RANGE, 3), \
		as_exp_int(__rtype), \
		__idx, __count, \
		__bin

/**
 * Create expression that selects map item identified by rank
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __vtype		Value type (as_exp_type).
 * @param __rank		Rank integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_rank(__ctx, __rtype, __vtype, __rank, __bin) \
		_AS_EXP_MAP_READ(__vtype, __rtype, false), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_RANK, 2), \
		as_exp_int(__rtype), \
		__rank, \
		__bin

/**
 * Create expression that selects map items starting at specified rank to the last ranked item
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __rank		Rank integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_rank_range_to_end(__ctx, __rtype, __rank, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_RANK_RANGE, 2), \
		as_exp_int(__rtype), \
		__rank, \
		__bin

/**
 * Create expression that selects "count" map items starting at specified rank
 * and returns selected data specified by rtype.
 *
 * @param __ctx			Optional context path for nested CDT (as_cdt_ctx).
 * @param __rtype		Return type (as_map_return_type).
 * @param __rank		Rank integer expression.
 * @param __count		Count integer expression.
 * @param __bin			Map bin or map value expression.
 * @return (expression)
 * @ingroup expression
 */
#define as_exp_map_get_by_rank_range(__ctx, __rtype, __rank, __count, __bin) \
		_AS_EXP_MAP_READ(AS_EXP_TYPE_AUTO, __rtype, true), \
		_AS_EXP_MAP_START(__ctx, AS_CDT_OP_MAP_GET_BY_RANK_RANGE, 3), \
		as_exp_int(__rtype), \
		__rank, __count, \
		__bin

/*********************************************************************************
 * BIT MODIFY EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_BIT_MOD() \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(AS_EXP_TYPE_BLOB), \
		as_exp_int(_AS_EXP_SYS_CALL_BITS | _AS_EXP_SYS_FLAG_MODIFY_LOCAL)

#define _AS_EXP_BIT_MOD_START(__op, __n_params) \
		_AS_EXP_BIT_MOD(), \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + (__n_params) }, \
		as_exp_int(__op)

/**
 * Create an expression that performs an as_operations_bit_resize operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __byte_size	Number of bytes the resulting blob should occupy.
 * @param __flags		as_bit_resize_flags value.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) __byte_size bytes.
 * @ingroup expression
 */
#define as_exp_bit_resize(__policy, __byte_size, __flags, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_RESIZE, 3), \
		__byte_size, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		as_exp_uint(__flags), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_insert operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __byte_offset	Byte index of where to insert the value.
 * @param __value		Blob expression containing the bytes to insert.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob containing the inserted bytes.
 * @ingroup expression
 */
#define as_exp_bit_insert(__policy, __byte_offset, __value, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_INSERT, 3), \
		__byte_offset, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_remove operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __byte_offset	Byte index of where to remove from.
 * @param __byte_size	Number of bytes to remove.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes removed.
 * @ingroup expression
 */
#define as_exp_bit_remove(__policy, __byte_offset, __byte_size, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_REMOVE, 3), \
		__byte_offset, \
		__byte_size, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_set operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start writing.
 * @param __bit_size	Number of bytes to overwrite.
 * @param __value		Blob expression containing bytes to write.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes overwritten.
 * @ingroup expression
 */
#define as_exp_bit_set(__policy, __bit_offset, __bit_size, __value, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_SET, 4), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_or operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bytes to be operated on.
 * @param __value		Blob expression containing bytes to write.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_or(__policy, __bit_offset, __bit_size, __value, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_OR, 4), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_xor operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __value		Blob expression containing bytes to write.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_xor(__policy, __bit_offset, __bit_size, __value, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_XOR, 4), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_and operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __value		Blob expression containing bytes to write.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_and(__policy, __bit_offset, __bit_size, __value, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_AND, 4), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_not operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_not(__policy, __bit_offset, __bit_size, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_NOT, 3), \
		__bit_offset, \
		__bit_size, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_lshift operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __shift		Number of bits to shift by.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_lshift(__policy, __bit_offset, __bit_size, __shift, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_LSHIFT, 4), \
		__bit_offset, \
		__bit_size, \
		__shift, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_rshift operation.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __shift		Number of bits to shift by.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_rshift(__policy, __bit_offset, __bit_size, __shift, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_RSHIFT, 4), \
		__bit_offset, \
		__bit_size, \
		__shift, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_add operation.
 * Note: integers are stored big-endian.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __value		Integer expression for value to add.
 * @param __action		as_bit_overflow_action value.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_add(__policy, __bit_offset, __bit_size, __value, __action, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_ADD, 5), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		as_exp_uint(__action), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_add operation.
 * Note: integers are stored big-endian.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __value		Integer expression for value to add.
 * @param __signed		Boolean indicating if bits should be treated as a signed number.
 * @param __action		as_bit_overflow_action value.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_add_signed(__policy, __bit_offset, __bit_size, __value, __signed, __action, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_ADD, 5), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		as_exp_uint(__signed ? __action | 0x01 : __action), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_subtract operation.
 * Note: integers are stored big-endian.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __value		Integer expression for value to subtract.
 * @param __action		as_bit_overflow_action value.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_subtract(__policy, __bit_offset, __bit_size, __value, __action, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_SUBTRACT, 5), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		as_exp_uint(__action), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_subtract operation.
 * Note: integers are stored big-endian.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __value		Integer expression for value to subtract.
 * @param __signed		Boolean indicating if bits should be treated as a signed number.
 * @param __action		as_bit_overflow_action value.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_subtract_signed(__policy, __bit_offset, __bit_size, __value, __signed, __action, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_SUBTRACT, 5), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		as_exp_uint(__signed ? __action | 0x01 : __action), \
		__bin

/**
 * Create an expression that performs an as_operations_bit_set_int operation.
 * Note: integers are stored big-endian.
 *
 * @param __policy		An as_bit_policy value.
 * @param __bit_offset	Bit index of where to start operation.
 * @param __bit_size	Number of bits to be operated on.
 * @param __value		Integer expression for value to set.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) resulting blob with the bytes operated on.
 * @ingroup expression
 */
#define as_exp_bit_set_int(__policy, __bit_offset, __bit_size, __value, __bin) \
		_AS_EXP_BIT_MOD_START(AS_BIT_OP_SET_INT, 4), \
		__bit_offset, \
		__bit_size, \
		__value, \
		as_exp_uint(__policy ? ((as_bit_policy*)(__policy))->flags : 0), \
		__bin

/*********************************************************************************
 * BIT READ EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_BIT_READ(__rtype) \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(__rtype), \
		as_exp_int(_AS_EXP_SYS_CALL_BITS)

#define _AS_EXP_BIT_READ_START(__rtype, __op, __n_params) \
		_AS_EXP_BIT_READ(__rtype), \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + (__n_params) }, \
		as_exp_int(__op)

/**
 * Create an expression that performs an as_operations_bit_get operation.
 *
 * @param __bit_offset	The bit index of where to start reading from.
 * @param __bit_size	Number of bits to read from the blob bin.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (blob bin) bit_size bits rounded up to the nearest byte size.
 * @ingroup expression
 */
#define as_exp_bit_get(__bit_offset, __bit_size, __bin) \
		_AS_EXP_BIT_READ_START(AS_EXP_TYPE_BLOB, AS_BIT_OP_GET, 2), \
		__bit_offset, \
		__bit_size, \
		__bin

/**
 * Create an expression that performs an as_operations_bit_count operation.
 *
 * @param __bit_offset	The bit index of where to start reading from.
 * @param __bit_size	Number of bits to read from the blob bin.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (integer value) number of bits set to 1 in the bit_size region.
 * @ingroup expression
 */
#define as_exp_bit_count(__bit_offset, __bit_size, __bin) \
		_AS_EXP_BIT_READ_START(AS_EXP_TYPE_INT, AS_BIT_OP_COUNT, 2), \
		__bit_offset, \
		__bit_size, \
		__bin

/**
 * Create an expression that performs an as_operations_bit_lscan operation.
 *
 * @param __bit_offset	The bit index of where to start reading from.
 * @param __bit_size	Number of bits to read from the blob bin.
 * @param __value		Boolean expression, true searches for 1, false for 0.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (integer value) Index of the left most bit starting from __offset set to __value.
 * @ingroup expression
 */
#define as_exp_bit_lscan(__bit_offset, __bit_size, __value, __bin) \
		_AS_EXP_BIT_READ_START(AS_EXP_TYPE_INT, AS_BIT_OP_LSCAN, 3), \
		__bit_offset, \
		__bit_size, \
		__value, \
		__bin

/**
 * Create an expression that performs an as_operations_bit_rscan operation.
 *
 * @param __bit_offset	The bit index of where to start reading from.
 * @param __bit_size	Number of bits to read from the blob bin.
 * @param __value		Boolean expression, true searches for 1, false for 0.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (integer value) Index of the right most bit starting from __offset set to __value.
 * @ingroup expression
 */
#define as_exp_bit_rscan(__bit_offset, __bit_size, __value, __bin) \
		_AS_EXP_BIT_READ_START(AS_EXP_TYPE_INT, AS_BIT_OP_RSCAN, 3), \
		__bit_offset, \
		__bit_size, \
		__value, \
		__bin

/**
 * Create an expression that performs an as_operations_bit_get_int operation.
 *
 * @param __bit_offset	The bit index of where to start reading from.
 * @param __bit_size	Number of bits to read from the blob bin.
 * @param __sign		Boolean value, true for signed, false for unsigned.
 * @param __bin			A blob bin expression to apply this function to.
 * @return (integer value) Index of the left most bit starting from __offset set to __value.
 * @ingroup expression
 */
#define as_exp_bit_get_int(__bit_offset, __bit_size, __sign, __bin) \
		_AS_EXP_BIT_READ_START(AS_EXP_TYPE_INT, AS_BIT_OP_GET_INT, 3), \
		__bit_offset, \
		__bit_size, \
		as_exp_int(__sign ? 1 : 0), \
		__bin

/*********************************************************************************
 * HLL MODIFY EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_HLL_MOD() \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(AS_EXP_TYPE_HLL), \
		as_exp_int(_AS_EXP_SYS_CALL_HLL | _AS_EXP_SYS_FLAG_MODIFY_LOCAL)

#define _AS_EXP_HLL_MOD_START(__op, __n_params) \
		_AS_EXP_HLL_MOD(), \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + (__n_params) }, \
		as_exp_int(__op)

/**
 * Create expression that creates a new HLL or resets an existing HLL with minhash bits.
 *
 * @param __policy			An as_hll_policy value.
 * @param __index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @param __mh_bit_count	Number of min hash bits. Must be between 4 and 51 inclusive.
 *							Also, __index_bit_count + __mh_bit_count must be <= 64.
 * @param __bin				A bin expression to apply this function to.
 * @return (hll bin) Returns the resulting hll bin.
 * @ingroup expression
 */
#define as_exp_hll_init_mh(__policy, __index_bit_count, __mh_bit_count, __bin) \
		_AS_EXP_HLL_MOD_START(AS_HLL_OP_INIT, 3), \
		as_exp_int(__index_bit_count), \
		as_exp_int(__mh_bit_count), \
		as_exp_int(__policy == NULL ? 0 : ((as_hll_policy*)__policy)->flags), \
		__bin

/**
 * Create expression that creates a new HLL or resets an existing HLL.
 *
 * @param __policy			An as_hll_policy value.
 * @param __index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @param __bin				A bin expression to apply this function to.
 * @return (hll bin) Returns the resulting hll bin.
 * @ingroup expression
 */
#define as_exp_hll_init(__policy, __index_bit_count, __bin) \
		_AS_EXP_HLL_MOD_START(AS_HLL_OP_INIT, 2), \
		as_exp_int(__index_bit_count), \
		as_exp_int(__policy == NULL ? 0 : ((as_hll_policy*)__policy)->flags), \
		__bin

/**
 * Create an expression that performs an as_operations_hll_add_mh.
 *
 * @param __policy			An as_hll_policy value.
 * @param __list			A list expression of elements to add to the HLL.
 * @param __index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @param __mh_bit_count	Number of min hash bits. Must be between 4 and 51 inclusive.
 *							Also, __index_bit_count + __mh_bit_count must be <= 64.
 * @param __bin				A bin expression to apply this function to.
 * @return (hll bin) Returns the resulting hll bin after adding elements from __list.
 * @ingroup expression
 */
#define as_exp_hll_add_mh(__policy, __list, __index_bit_count, __mh_bit_count, __bin) \
		_AS_EXP_HLL_MOD_START(AS_HLL_OP_ADD, 4), \
		__list, \
		as_exp_int(__index_bit_count), \
		as_exp_int(__mh_bit_count), \
		as_exp_int(__policy == NULL ? 0 : ((as_hll_policy*)__policy)->flags), \
		__bin

/**
 * Create an expression that performs an as_operations_hll_add.
 *
 * @param __policy			An as_hll_policy value.
 * @param __list			A list expression of elements to add to the HLL.
 * @param __index_bit_count	Number of index bits. Must be between 4 and 16 inclusive.
 * @param __bin				A bin expression to apply this function to.
 * @return (hll bin) Returns the resulting hll bin after adding elements from __list.
 * @ingroup expression
 */
#define as_exp_hll_add(__policy, __list, __index_bit_count, __bin) \
		_AS_EXP_HLL_MOD_START(AS_HLL_OP_ADD, 4), \
		__list, \
		as_exp_int(__index_bit_count), \
		as_exp_int(-1), \
		as_exp_int(__policy == NULL ? 0 : ((as_hll_policy*)__policy)->flags), \
		__bin

/**
 * Create an expression that performs an as_operations_hll_update.
 *
 * @param __policy		An as_hll_policy value.
 * @param __list		A list expression of elements to add to the HLL.
 * @param __bin			A bin expression to apply this function to.
 * @return (hll bin) Returns the resulting hll bin after adding elements from __list.
 * @ingroup expression
 */
#define as_exp_hll_update(__policy, __list, __bin) \
		_AS_EXP_HLL_MOD_START(AS_HLL_OP_ADD, 4), \
		__list, \
		as_exp_int(-1), \
		as_exp_int(-1), \
		as_exp_int(__policy == NULL ? 0 : ((as_hll_policy*)__policy)->flags), \
		__bin

/*********************************************************************************
 * HLL READ EXPRESSIONS
 *********************************************************************************/

#define _AS_EXP_HLL_READ(__rtype) \
		{.op=_AS_EXP_CODE_CALL, .count=5}, \
		_AS_EXP_VAL_RTYPE(__rtype), \
		as_exp_int(_AS_EXP_SYS_CALL_HLL)

#define _AS_EXP_HLL_READ_START(__rtype, __op, __n_params) \
		_AS_EXP_HLL_READ(__rtype), \
		{.op=_AS_EXP_CODE_CALL_VOP_START, .count=1 + (__n_params) }, \
		as_exp_int(__op)

/**
 * Create an expression that performs an as_operations_hll_get_count.
 *
 * @param __bin			A bin expression to apply this function to.
 * @return (integer bin) The estimated number of unique elements in an HLL.
 * @ingroup expression
 */
#define as_exp_hll_get_count(__bin) \
		_AS_EXP_HLL_READ_START(AS_EXP_TYPE_INT, AS_HLL_OP_COUNT, 0), \
		__bin

/**
 * Create an expression that performs an as_operations_hll_get_union.
 *
 * @param __list		A list expression of HLLs to union with.
 * @param __bin			A bin expression to apply this function to.
 * @return (hll bin) HLL bin representing the set union.
 * @ingroup expression
 */
#define as_exp_hll_get_union(__list, __bin) \
		_AS_EXP_HLL_READ_START(AS_EXP_TYPE_HLL, AS_HLL_OP_GET_UNION, 1), \
		__list, \
		__bin

/**
 * Create an expression that performs an as_operations_hll_get_union_count.
 *
 * @param __list		A list expression of HLLs to union with.
 * @param __bin			A bin expression to apply this function to.
 * @return (integer bin) Estimated number of elements in the set union.
 * @ingroup expression
 */
#define as_exp_hll_get_union_count(__list, __bin) \
		_AS_EXP_HLL_READ_START(AS_EXP_TYPE_INT, AS_HLL_OP_UNION_COUNT, 1), \
		__list, \
		__bin

/**
 * Create an expression that performs an as_operations_hll_get_inersect_count.
 *
 * @param __list		A list expression of HLLs to intersect with.
 * @param __bin			A bin expression to apply this function to.
 * @return (integer bin) Estimated number of elements in the set intersection.
 * @ingroup expression
 */
#define as_exp_hll_get_intersect_count(__list, __bin) \
		_AS_EXP_HLL_READ_START(AS_EXP_TYPE_INT, AS_HLL_OP_INTERSECT_COUNT, 1), \
		__list, \
		__bin

/**
 * Create an expression that performs an as_operations_hll_get_similarity.
 *
 * @param __list		A list expression of HLLs to calculate similarity with..
 * @param __bin			A bin expression to apply this function to.
 * @return (float bin) Estimated similarity between 0.0 and 1.0.
 * @ingroup expression
 */
#define as_exp_hll_get_similarity(__list, __bin) \
		_AS_EXP_HLL_READ_START(AS_EXP_TYPE_FLOAT, AS_HLL_OP_SIMILARITY, 1), \
		__list, \
		__bin

/**
 * Create an expression that performs an as_operations_hll_describe.
 *
 * @param __bin			A bin expression to apply this function to.
 * @return (list bin) A list containing the index_bit_count and minhash_bit_count.
 * @ingroup expression
 */
#define as_exp_hll_describe(__bin) \
		_AS_EXP_HLL_READ_START(AS_EXP_TYPE_LIST, AS_HLL_OP_DESCRIBE, 0), \
		__bin

/**
 * Create an expression that checks if the HLL __bin may contain all keys in
 *  __list..
 *
 * @param __list		A list expression of keys to check if the HLL may contain them.
 * @param __bin			A bin expression to apply this function to.
 * @return (integer bin) 1 __bin may contain all of __list, 0 otherwise.
 * @ingroup expression
 */
#define as_exp_hll_may_contain(__list, __bin) \
		_AS_EXP_HLL_READ_START(AS_EXP_TYPE_INT, AS_HLL_OP_MAY_CONTAIN, 1), \
		__list, \
		__bin

/*********************************************************************************
 * EXPRESSION MERGE
 *********************************************************************************/

/**
 * Merge precompiled expression into a new expression tree.
 * Useful for storing common precompiled expressions and then reusing
 * these expressions as part of a greater expression.
 *
 * ~~~~~~~~~~{.c}
 * // Merge precompiled expression into new expression.
 * as_exp_build(expr, as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(200)));
 * as_exp_build(merged,
 *		as_exp_and(
 *			as_exp_expr(expr), 
 *			as_exp_cmp_eq(as_exp_bin_int("b"), as_exp_int(100))));
 * ~~~~~~~~~~
 *
 * @param __e	Pre-compiled expression.
 * @ingroup expression
 */
#define as_exp_expr(__e) \
	{.op=_AS_EXP_CODE_MERGE, .v.expr=__e}

/*********************************************************************************
 * EXPRESSION BUILDERS
 *********************************************************************************/

/**
 * Declare and build an expression variable.
 *
 * ~~~~~~~~~~{.c}
 * // a == 10
 * as_exp_build(expression,
 *     as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(10)));
 * ...
 * as_exp_destroy(expression);
 * ~~~~~~~~~~
 *
 * @param __name			Name of the variable to hold the expression
 * @ingroup expression
 */
#define as_exp_build(__name, ...) \
		as_exp* __name; \
		do { \
			as_exp_entry __table__[] = { __VA_ARGS__ }; \
			__name = as_exp_compile(__table__, sizeof(__table__) / sizeof(as_exp_entry)); \
		} while (false)

/**
 * Declare and build an base64 packed expression variable.
 *
 * ~~~~~~~~~~{.c}
 * // a == 10
 * as_exp_build_b64(expression,
 *     as_exp_cmp_eq(as_exp_bin_int("a"), as_exp_int(10)));
 * ...
 * as_exp_destroy_b64(expression);
 * ~~~~~~~~~~
 *
 * @param __name			Name of the variable to hold the expression
 * @ingroup expression
 */
#define as_exp_build_b64(__name, ...) \
		char* __name; \
		do { \
			as_exp_entry __table__[] = { __VA_ARGS__ }; \
			as_exp* temp = as_exp_compile(__table__, sizeof(__table__) / sizeof(as_exp_entry)); \
			__name = as_exp_compile_b64(temp); \
			as_exp_destroy(temp); \
		} while (false)

#ifdef __cplusplus
} // end extern "C"
#endif
