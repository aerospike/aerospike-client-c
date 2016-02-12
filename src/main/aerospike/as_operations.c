/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <aerospike/as_arraylist.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_serializer.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "_bin.h"

/******************************************************************************
 *	TYPES
 *****************************************************************************/

typedef enum {
	CDT_RW_TYPE_READ = 0,
	CDT_RW_TYPE_MODIFY = 1,
} cdt_op_rw_type;

typedef struct {
	int count;
	cdt_op_rw_type rw_type;
	int opt_args;
	const as_cdt_paramtype *args;
} cdt_op_table_entry;

/******************************************************************************
 *	MACROS
 *****************************************************************************/

#define VA_FIRST(first, ...)	first
#define VA_REST(first, ...)		__VA_ARGS__

#if 1	// Only works for 9 or less args (but can be expanded).
#define VA_NARGS_SEQ	 9, 8, 7, 6, 5, 4, 3, 2, 1, 0
#define VA_NARGS_EXTRACT_N(_9, _8, _7, _6, _5, _4, _3, _2, _1, _0, N, ...) N
#define VA_NARGS_SEQ2N(...)  VA_NARGS_EXTRACT_N(__VA_ARGS__)
#define VA_NARGS(...)   VA_NARGS_SEQ2N(_, ##__VA_ARGS__, VA_NARGS_SEQ)
#else	// Only works for ints
#define VA_NARGS(...) (sizeof((int[]){__VA_ARGS__}) / sizeof(int))
#endif

#define CDT_OP_ENTRY(op, type, ...) [op].args = (const as_cdt_paramtype[]){VA_REST(__VA_ARGS__, 0)}, [op].count = VA_NARGS(__VA_ARGS__) - 1, [op].rw_type = type, [op].opt_args = VA_FIRST(__VA_ARGS__)

/**
 * Add a CDT operation to ops.
 * Get around needing to pass last named arg to va_start().
 */
#define AS_OPERATIONS_CDT_OP(ops, name, op, ...) as_operations_cdt_op(ops, name, op, VA_NARGS(__VA_ARGS__), ##__VA_ARGS__)

/******************************************************************************
 *	DATA
 *****************************************************************************/

const cdt_op_table_entry cdt_op_table[] = {
	//--------------------------------------------
	// Modify OPs

	// Add to list
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_APPEND_ITEMS,	CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_INSERT_ITEMS,	CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),

	// Remove from list
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_POP_RANGE,		CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_REMOVE_RANGE,	CDT_RW_TYPE_MODIFY, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),

	// Other list modifies
	CDT_OP_ENTRY(AS_CDT_OP_LIST_SET,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_PAYLOAD),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_TRIM,			CDT_RW_TYPE_MODIFY, 0, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_CLEAR,			CDT_RW_TYPE_MODIFY, 0),

	//--------------------------------------------
	// Read OPs

	// Read from list
	CDT_OP_ENTRY(AS_CDT_OP_LIST_SIZE,			CDT_RW_TYPE_READ, 0),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET,			CDT_RW_TYPE_READ, 0, AS_CDT_PARAM_INDEX),
	CDT_OP_ENTRY(AS_CDT_OP_LIST_GET_RANGE,		CDT_RW_TYPE_READ, 1, AS_CDT_PARAM_INDEX, AS_CDT_PARAM_COUNT),
};

const size_t cdt_op_table_size = sizeof(cdt_op_table) / sizeof(cdt_op_table_entry);

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

extern inline bool as_operations_add_write_str(as_operations * ops, const as_bin_name name, const char * value);
extern inline bool as_operations_add_write_geojson_str(as_operations * ops, const as_bin_name name, const char * value);
extern inline bool as_operations_add_write_raw(as_operations * ops, const as_bin_name name, const uint8_t * value, uint32_t size);
extern inline bool as_operations_add_prepend_str(as_operations * ops, const as_bin_name name, const char * value);
extern inline bool as_operations_add_prepend_raw(as_operations * ops, const as_bin_name name, const uint8_t * value, uint32_t size);
extern inline bool as_operations_add_append_str(as_operations * ops, const as_bin_name name, const char * value);
extern inline bool as_operations_add_append_raw(as_operations * ops, const as_bin_name name, const uint8_t * value, uint32_t size);

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_operations * as_operations_default(as_operations * ops, bool free, uint16_t nops)
{
	if ( !ops ) return ops;

	ops->_free = free;
	ops->gen = 0;
	ops->ttl = 0;

	as_binop * entries = NULL;
	if ( nops > 0 ) {
		entries = (as_binop *) cf_malloc(sizeof(as_binop) * nops);
	}

	if ( entries ) {
		ops->binops._free = true;
		ops->binops.capacity = nops;
		ops->binops.size = 0;
		ops->binops.entries = entries;
	}
	else {
		ops->binops._free = false;
		ops->binops.capacity = 0;
		ops->binops.size = 0;
		ops->binops.entries = NULL;
	}

	return ops;
}

/**
 *	Find the as_binop to update when appending.
 *	Returns an as_binop ready for bin initialization.
 *	If no more entries available or precondition failed, then returns NULL.
 */
static as_binop * as_binop_forappend(as_operations * ops, as_operator operator, const as_bin_name name)
{
	if ( ! (ops && ops->binops.size < ops->binops.capacity &&
			name && strlen(name) < AS_BIN_NAME_MAX_SIZE) ) {
		return NULL;
	}

	// Note - caller must successfully populate bin once we increment size.
	as_binop * binop = &ops->binops.entries[ops->binops.size++];
	binop->op = operator;

	return binop;
}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Intializes a stack allocated `as_operations`. 
 *
 *	~~~~~~~~~~{.c}
 *		as_operations ops;
 * 		as_operations_init(&ops, 2);
 *		as_operations_append_int64(&ops, AS_OPERATOR_INCR, "bin1", 123);
 *		as_operations_append_str(&ops, AS_OPERATOR_APPEND, "bin2", "abc");
 *	~~~~~~~~~~
 *
 *	Use `as_operations_destroy()` to free the resources allocated to the
 *	`as_operations`.
 *
 *	@param ops 		The `as_operations` to initialize.
 *	@param nops		The number of `as_operations.binops.entries` to allocate on the heap.
 *
 *	@return The initialized `as_operations` on success. Otherwise NULL.
 */
as_operations * as_operations_init(as_operations * ops, uint16_t nops)
{
	if ( !ops ) return ops;
	return as_operations_default(ops, false, nops);
}

/**
 *	Creates and initializes a heap allocated `as_operations`.
 *
 *	~~~~~~~~~~{.c}
 *		as_operations ops;
 * 		as_operations_init(&ops, 2);
 *		as_operations_append_int64(&ops, AS_OPERATOR_INCR, "bin1", 123);
 *		as_operations_append_str(&ops, AS_OPERATOR_APPEND, "bin2", "abc");
 *	~~~~~~~~~~
 *
 *	Use `as_operations_destroy()` to free the resources allocated to the
 *	`as_operations`.
 *
 *	@param ops 		The `as_operations` to initialize.
 *	@param nops		The number of `as_operations.binops.entries` to allocate on the heap.
 *
 *	@return The new `as_operations` on success. Otherwise NULL.
 */
as_operations * as_operations_new(uint16_t nops)
{
	as_operations *	ops = (as_operations *) cf_malloc(sizeof(as_operations));
	if ( !ops ) return ops;
	return as_operations_default(ops, true, nops);
}

/**
 *	Releases the `as_operations` and associated resources.
 *
 *	~~~~~~~~~~{.c}
 * 		as_operations_destroy(binops);
 *	~~~~~~~~~~
 *
 *	@param bins 	The `as_binops` to destroy.
 */
void as_operations_destroy(as_operations * ops)
{
	if ( !ops ) return;
	
	// destroy each bin in binops
	for(int i = 0; i < ops->binops.size; i++) {
		as_bin_destroy(&ops->binops.entries[i].bin);
	}

	// free binops
	if ( ops->binops._free ) {
		cf_free(ops->binops.entries);
	}

	// reset values 
	ops->binops._free = false;
	ops->binops.capacity = 0;
	ops->binops.size = 0;
	ops->binops.entries = NULL;

	if ( ops->_free ) {
		cf_free(ops);
	}
}

/**
 *	Add a AS_OPERATOR_WRITE bin operation.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write(as_operations * ops, const as_bin_name name, as_bin_value * value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init(&binop->bin, name, value);
	return true;
}

/**
 *	Add a AS_OPERATOR_WRITE bin operation with an int64_t value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_int64(as_operations * ops, const as_bin_name name, int64_t value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_int64(&binop->bin, name, value);
	return true;
}

/**
 *	Add a `AS_OPERATOR_WRITE` bin operation with a double value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_double(as_operations * ops, const as_bin_name name, double value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_double(&binop->bin, name, value);
	return true;
}

/**
 *	Add a AS_OPERATOR_WRITE bin operation with a NULL-terminated string value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *	@param free			If true, then the value will be freed when the operations is destroyed.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_strp(as_operations * ops, const as_bin_name name, const char * value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_str(&binop->bin, name, value, free);
	return true;
}

/**
 *	Add a AS_OPERATOR_WRITE bin operation with a NULL-terminated GeoJSON string value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name			The name of the bin to perform the operation on.
 *	@param value		The value to be used in the operation.
 *	@param free			If true, then the value will be freed when the operations is destroyed.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_geojson_strp(as_operations * ops, const as_bin_name name, const char * value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_geojson(&binop->bin, name, value, free);
	return true;
}

/**
 *	Add a AS_OPERATOR_WRITE bin operation with a raw bytes value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *	@param free			If true, then the value will be freed when the operations is destroyed.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_write_rawp(as_operations * ops, const as_bin_name name, const uint8_t * value, uint32_t size, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_raw(&binop->bin, name, value, size, free);
	return true;
}

/**
 *	Add a AS_OPERATOR_READ bin operation.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_read(as_operations * ops, const as_bin_name name)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_READ, name);
	if ( !binop ) return false;
	as_bin_init_nil(&binop->bin, name);
	return true;
}

/**
 *	Add a AS_OPERATOR_INCR bin operation with (required) int64_t value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_incr(as_operations * ops, const as_bin_name name, int64_t value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_INCR, name);
	if ( !binop ) return false;
	as_bin_init_int64(&binop->bin, name, value);
	return true;
}

/**
 *	Add a `AS_OPERATOR_INCR` bin operation with double value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_incr_double(as_operations * ops, const as_bin_name name, double value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_INCR, name);
	if ( !binop ) return false;
	as_bin_init_double(&binop->bin, name, value);
	return true;
}

/**
 *	Add a AS_OPERATOR_PREPEND bin operation with a NULL-terminated string value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *	@param free			If true, then the value will be freed when the operations is destroyed.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_prepend_strp(as_operations * ops, const as_bin_name name, const char * value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_PREPEND, name);
	if ( !binop ) return false;
	as_bin_init_str(&binop->bin, name, value, free);
	return true;
}

/**
 *	Add a AS_OPERATOR_PREPEND bin operation with a raw bytes value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *	@param free			If true, then the value will be freed when the operations is destroyed.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_prepend_rawp(as_operations * ops, const as_bin_name name, const uint8_t * value, uint32_t size, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_PREPEND, name);
	if ( !binop ) return false;
	as_bin_init_raw(&binop->bin, name, value, size, free);
	return true;
}

/**
 *	Add a AS_OPERATOR_APPEND bin operation with a NULL-terminated string value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *	@param free			If true, then the value will be freed when the operations is destroyed.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_append_strp(as_operations * ops, const as_bin_name name, const char * value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_APPEND, name);
	if ( !binop ) return false;
	as_bin_init_str(&binop->bin, name, value, free);
	return true;
}

/**
 *	Add a AS_OPERATOR_APPEND bin operation with a raw bytes value.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *	@param name 		The name of the bin to perform the operation on.
 *	@param value 		The value to be used in the operation.
 *	@param free			If true, then the value will be freed when the operations is destroyed.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_append_rawp(as_operations * ops, const as_bin_name name, const uint8_t * value, uint32_t size, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_APPEND, name);
	if ( !binop ) return false;
	as_bin_init_raw(&binop->bin, name, value, size, free);
	return true;
}

/**
 *	Add a AS_OPERATOR_TOUCH record operation.
 *
 *	@param ops			The `as_operations` to append the operation to.
 *
 *	@return true on success. Otherwise an error occurred.
 */
bool as_operations_add_touch(as_operations * ops)
{
	// TODO - what happens with null or empty bin name?
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_TOUCH, "");
	if ( !binop ) return false;
	as_bin_init_nil(&binop->bin, "");
	return true;
}

/******************************************************************************
 *	CDT FUNCTIONS
 *****************************************************************************/

static bool as_operations_add_cdt_modify(as_operations *ops, const as_bin_name name, as_bin_value *value)
{
	as_binop *binop = as_binop_forappend(ops, AS_OPERATOR_CDT_MODIFY, name);
	if (! binop) {
		return false;
	}
	as_bin_init(&binop->bin, name, value);
	return true;
}

static bool as_operations_add_cdt_read(as_operations *ops, const as_bin_name name, as_bin_value *value)
{
	as_binop *binop = as_binop_forappend(ops, AS_OPERATOR_CDT_READ, name);
	if (! binop) {
		return false;
	}
	as_bin_init(&binop->bin, name, value);
	return true;
}

/**
 * Call with AS_OPERATIONS_CDT_OP only.
 */
static bool as_operations_cdt_op(as_operations *ops, const as_bin_name name, as_cdt_optype op, size_t n, ...)
{
	if (op >= cdt_op_table_size) {
		return false;
	}

	const cdt_op_table_entry *entry = &cdt_op_table[op];
	if (n < entry->count - entry->opt_args || n > entry->count) {
		return false;
	}

	va_list vl;
	if (n > 0) {
		va_start(vl, n);
	}

	as_arraylist list;
	as_arraylist_inita(&list, (uint32_t)n + 1); // +1 to avoid alloca(0) undefined behavior

	for (size_t i = 0; i < n; i++) {
		as_cdt_paramtype type = entry->args[i];
		switch (type) {
		case AS_CDT_PARAM_PAYLOAD: {
			as_val *arg = va_arg(vl, as_val *);

			if (as_arraylist_append(&list, arg) != AS_ARRAYLIST_OK) {
				va_end(vl);
				as_arraylist_destroy(&list);
				return false;
			}
			break;
		}
		case AS_CDT_PARAM_COUNT: {
			uint64_t arg = va_arg(vl, uint64_t);

			if (as_arraylist_append(&list, (as_val *)as_integer_new(arg)) != AS_ARRAYLIST_OK) {
				va_end(vl);
				as_arraylist_destroy(&list);
				return false;
			}
			break;
		}
		case AS_CDT_PARAM_INDEX: {
			int64_t arg = va_arg(vl, int64_t);

			if (as_arraylist_append(&list, (as_val *)as_integer_new(arg)) != AS_ARRAYLIST_OK) {
				va_end(vl);
				as_arraylist_destroy(&list);
				return false;
			}
			break;
		}
		default:
			break;
		}
	}

	if (n > 0) {
		va_end(vl);
	}

	as_serializer ser;
	as_msgpack_init(&ser);

	uint32_t list_size = as_serializer_serialize_getsize(&ser, (as_val *) &list);
	as_bytes *bytes = as_bytes_new(sizeof(uint16_t) + list_size);
	uint8_t *list_write = as_bytes_get(bytes);
	uint16_t *list_write_op = (uint16_t *)list_write;

	*list_write_op = cf_swap_to_be16(op);
	list_write += sizeof(uint16_t);

	as_serializer_serialize_presized(&ser, (const as_val *) &list, list_write);
	as_serializer_destroy(&ser);
	as_arraylist_destroy(&list);
	bytes->size = bytes->capacity;
	// as_bytes->type default to AS_BYTES_BLOB

	if (entry->rw_type == CDT_RW_TYPE_MODIFY) {
		return as_operations_add_cdt_modify(ops, name, (as_bin_value *) bytes);
	}

	return as_operations_add_cdt_read(ops, name, (as_bin_value *) bytes);
}

bool as_operations_add_list_append(as_operations *ops, const as_bin_name name, as_val *val)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_APPEND, val);
}

bool as_operations_add_list_append_int64(as_operations *ops, const as_bin_name name, int64_t value)
{
	as_integer v;
	as_integer_init(&v, value);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_APPEND, &v);
}

bool as_operations_add_list_append_double(as_operations *ops, const as_bin_name name, double value)
{
	as_double v;
	as_double_init(&v, value);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_APPEND, &v);
}

bool as_operations_add_list_append_strp(as_operations *ops, const as_bin_name name, const char *value, bool free)
{
	as_string v;
	as_string_init(&v, (char *)value, free);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_APPEND, &v);
}

bool as_operations_add_list_append_rawp(as_operations *ops, const as_bin_name name, const uint8_t *value, uint32_t size, bool free)
{
	as_bytes v;
	as_bytes_init_wrap(&v, (uint8_t *)value, size, free);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_APPEND, &v);
}

bool as_operations_add_list_append_items(as_operations *ops, const as_bin_name name, as_list *list)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_APPEND_ITEMS, list);
}

bool as_operations_add_list_insert(as_operations *ops, const as_bin_name name, int64_t index, as_val *val)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_INSERT, index, val);
}

bool as_operations_add_list_insert_int64(as_operations *ops, const as_bin_name name, int64_t index, int64_t value)
{
	as_integer v;
	as_integer_init(&v, value);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_INSERT, index, &v);
}

bool as_operations_add_list_insert_double(as_operations *ops, const as_bin_name name, int64_t index, double value)
{
	as_double v;
	as_double_init(&v, value);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_INSERT, index, &v);
}

bool as_operations_add_list_insert_strp(as_operations *ops, const as_bin_name name, int64_t index, const char *value, bool free)
{
	as_string v;
	as_string_init(&v, (char *)value, free);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_INSERT, index, &v);
}

bool as_operations_add_list_insert_rawp(as_operations *ops, const as_bin_name name, int64_t index, const uint8_t *value, uint32_t size, bool free)
{
	as_bytes v;
	as_bytes_init_wrap(&v, (uint8_t *)value, size, free);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_INSERT, index, &v);
}

bool as_operations_add_list_insert_items(as_operations *ops, const as_bin_name name, int64_t index, as_list *list)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_INSERT_ITEMS, index, list);
}

bool as_operations_add_list_pop(as_operations *ops, const as_bin_name name, int64_t index)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_POP, index);
}

bool as_operations_add_list_pop_range(as_operations *ops, const as_bin_name name, int64_t index, uint64_t count)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_POP_RANGE, index, count);
}

bool as_operations_add_list_pop_range_from(as_operations *ops, const as_bin_name name, int64_t index)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_POP_RANGE, index);
}

bool as_operations_add_list_remove(as_operations *ops, const as_bin_name name, int64_t index)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_REMOVE, index);
}

bool as_operations_add_list_remove_range(as_operations *ops, const as_bin_name name, int64_t index, uint64_t count)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_REMOVE_RANGE, index, count);
}

bool as_operations_add_list_remove_range_from(as_operations *ops, const as_bin_name name, int64_t index)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_REMOVE_RANGE, index);
}

bool as_operations_add_list_clear(as_operations *ops, const as_bin_name name)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_CLEAR);
}

bool as_operations_add_list_set(as_operations *ops, const as_bin_name name, int64_t index, as_val *val)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_SET, index, val);
}

bool as_operations_add_list_set_int64(as_operations *ops, const as_bin_name name, int64_t index, int64_t value)
{
	as_integer v;
	as_integer_init(&v, value);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_SET, index, &v);
}

bool as_operations_add_list_set_double(as_operations *ops, const as_bin_name name, int64_t index, double value)
{
	as_double v;
	as_double_init(&v, value);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_SET, index, &v);
}

bool as_operations_add_list_set_strp(as_operations *ops, const as_bin_name name, int64_t index, const char *value, bool free)
{
	as_string v;
	as_string_init(&v, (char *)value, free);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_SET, index, &v);
}

bool as_operations_add_list_set_rawp(as_operations *ops, const as_bin_name name, int64_t index, const uint8_t *value, uint32_t size, bool free)
{
	as_bytes v;
	as_bytes_init_wrap(&v, (uint8_t *)value, size, free);
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_SET, index, &v);
}

bool as_operations_add_list_trim(as_operations *ops, const as_bin_name name, int64_t index, uint64_t count)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_TRIM, index, count);
}

bool as_operations_add_list_get(as_operations *ops, const as_bin_name name, int64_t index)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_GET, index);
}

bool as_operations_add_list_get_range(as_operations *ops, const as_bin_name name, int64_t index, uint64_t count)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_GET_RANGE, index, count);
}

bool as_operations_add_list_get_range_from(as_operations *ops, const as_bin_name name, int64_t index)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_GET_RANGE, index);
}

bool as_operations_add_list_size(as_operations *ops, const as_bin_name name)
{
	return AS_OPERATIONS_CDT_OP(ops, name, AS_CDT_OP_LIST_SIZE);
}
