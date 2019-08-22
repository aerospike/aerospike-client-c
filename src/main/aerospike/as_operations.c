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
#include <aerospike/as_operations.h>
#include <aerospike/as_bin.h>
#include <citrusleaf/alloc.h>

#include "_bin.h"

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_operations*
as_operations_default(as_operations* ops, bool free, uint16_t nops)
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
 * Find the as_binop to update when appending.
 * Returns an as_binop ready for bin initialization.
 * If no more entries available or precondition failed, then returns NULL.
 */
as_binop*
as_binop_forappend(as_operations* ops, as_operator operator, const as_bin_name name)
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
 * FUNCTIONS
 *****************************************************************************/

as_operations*
as_operations_init(as_operations* ops, uint16_t nops)
{
	if ( !ops ) return ops;
	return as_operations_default(ops, false, nops);
}

as_operations*
as_operations_new(uint16_t nops)
{
	as_operations* ops = (as_operations *) cf_malloc(sizeof(as_operations));
	if ( !ops ) return ops;
	return as_operations_default(ops, true, nops);
}

void
as_operations_destroy(as_operations* ops)
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

bool
as_operations_add_write(as_operations* ops, const as_bin_name name, as_bin_value* value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init(&binop->bin, name, value);
	return true;
}

bool
as_operations_add_write_int64(as_operations* ops, const as_bin_name name, int64_t value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_int64(&binop->bin, name, value);
	return true;
}

bool
as_operations_add_write_double(as_operations* ops, const as_bin_name name, double value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_double(&binop->bin, name, value);
	return true;
}

bool
as_operations_add_write_strp(as_operations* ops, const as_bin_name name, const char* value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_str(&binop->bin, name, value, free);
	return true;
}

bool
as_operations_add_write_geojson_strp(as_operations* ops, const as_bin_name name, const char* value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_geojson(&binop->bin, name, value, free);
	return true;
}

bool
as_operations_add_write_rawp(as_operations* ops, const as_bin_name name, const uint8_t* value, uint32_t size, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_WRITE, name);
	if ( !binop ) return false;
	as_bin_init_raw(&binop->bin, name, value, size, free);
	return true;
}

bool
as_operations_add_read(as_operations* ops, const as_bin_name name)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_READ, name);
	if ( !binop ) return false;
	as_bin_init_nil(&binop->bin, name);
	return true;
}

bool
as_operations_add_incr(as_operations* ops, const as_bin_name name, int64_t value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_INCR, name);
	if ( !binop ) return false;
	as_bin_init_int64(&binop->bin, name, value);
	return true;
}

bool
as_operations_add_incr_double(as_operations* ops, const as_bin_name name, double value)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_INCR, name);
	if ( !binop ) return false;
	as_bin_init_double(&binop->bin, name, value);
	return true;
}

bool
as_operations_add_prepend_strp(as_operations* ops, const as_bin_name name, const char* value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_PREPEND, name);
	if ( !binop ) return false;
	as_bin_init_str(&binop->bin, name, value, free);
	return true;
}

bool
as_operations_add_prepend_rawp(as_operations* ops, const as_bin_name name, const uint8_t* value, uint32_t size, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_PREPEND, name);
	if ( !binop ) return false;
	as_bin_init_raw(&binop->bin, name, value, size, free);
	return true;
}

bool
as_operations_add_append_strp(as_operations* ops, const as_bin_name name, const char* value, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_APPEND, name);
	if ( !binop ) return false;
	as_bin_init_str(&binop->bin, name, value, free);
	return true;
}

bool
as_operations_add_append_rawp(as_operations* ops, const as_bin_name name, const uint8_t* value, uint32_t size, bool free)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_APPEND, name);
	if ( !binop ) return false;
	as_bin_init_raw(&binop->bin, name, value, size, free);
	return true;
}

bool
as_operations_add_touch(as_operations* ops)
{
	// TODO - what happens with null or empty bin name?
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_TOUCH, "");
	if ( !binop ) return false;
	as_bin_init_nil(&binop->bin, "");
	return true;
}

bool
as_operations_add_delete(as_operations* ops)
{
	as_binop * binop = as_binop_forappend(ops, AS_OPERATOR_DELETE, "");
	if ( !binop ) return false;
	as_bin_init_nil(&binop->bin, "");
	return true;
}
