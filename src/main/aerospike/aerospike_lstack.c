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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_lstack.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_error.h>
#include <aerospike/as_log.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_types.h>

#include "_ldt.h"

// ++==============++
// || Fixed Values ||
// ++==============++
const char * DEFAULT_LSTACK_PACKAGE = "lstack";

// The names of the Lua Functions that implement Large Stack Ops
const char * LDT_STACK_OP_PUSH				= "push";
const char * LDT_STACK_OP_PUSHALL			= "push_all";
const char * LDT_STACK_OP_PEEK				= "peek";
// @TODO const char * LDT_STACK_OP_POP			= "pop";
// @TODO const char * LDT_STACK_OP_SCAN			= "scan";
const char * LDT_STACK_OP_FILTER  			= "filter";
const char * LDT_STACK_OP_DESTROY			= "destroy";
const char * LDT_STACK_OP_LDT_EXISTS		= "ldt_exists";
const char * LDT_STACK_OP_SIZE				= "size";
const char * LDT_STACK_OP_CAPACITY_SET		= "set_capacity";
const char * LDT_STACK_OP_CAPACITY_GET		= "get_capacity";
// We use these for performance measurements -- to get a baseline of a minimal
// UDF -- so that we can compare with KV and LDT and know where the costs are.
const char * LDT_STACK_OP_ONE         		= "one";
const char * LDT_STACK_OP_SAME         		= "same";


// =======================================================================
static as_status aerospike_lstack_push_internal(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val,
    const char *operation)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !val) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/n cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not stack type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, ldt->module[0] == 0 ? 2 : 3);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( val );
	as_arraylist_append(&arglist, (as_val *) val);

	if (ldt->module[0] != 0) {
		as_string ldt_module;
		as_string_init(&ldt_module, (char *)ldt->module, false);
		as_arraylist_append_string(&arglist, &ldt_module);
	}

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, operation,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	// return value is the input
	if (p_return_val) {
		as_val_destroy(p_return_val);
	}

	return err->code;
} // end aerospike_lstack_push_internal()

// =======================================================================
static as_status aerospike_lstack_peek_with_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t peek_count,
	const as_udf_function_name filter, const as_list *filter_args,
	as_list ** elements )
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (filter_args && !filter) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"filter arguments without filter name specification");
	}
	if (!as || !key || !ldt || !peek_count || !elements) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/peek_count/elements cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not stack type");
	}

	int list_argc = filter ? 5 : 2;
	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, list_argc);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, peek_count );

	if (filter){
   		as_string ldt_module;
		as_string_init(&ldt_module, (char *)ldt->module, false);
   		as_arraylist_append_string(&arglist, &ldt_module);

		as_string filter_name;
		as_string_init(&filter_name, (char *)filter, false);
		as_arraylist_append_string(&arglist, &filter_name );
		as_val_reserve( filter_args ); // bump the ref count
		as_arraylist_append(&arglist, (as_val *) filter_args );
	}

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE,
		filter ? LDT_STACK_OP_FILTER : LDT_STACK_OP_PEEK,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"no value returned from server");
	}

	*elements = (as_list *)p_return_val;

	return err->code;

} // aerospike_lstack_peek_with_filter()

// =======================================================================
as_status aerospike_lstack_push(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val)
{
	return aerospike_lstack_push_internal (as, err, policy, key, ldt,
            val, LDT_STACK_OP_PUSH);
}

// =======================================================================
as_status aerospike_lstack_push_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * val_listp)
{
	return aerospike_lstack_push_internal (as, err, policy, key, ldt,
			(as_val *) val_listp, LDT_STACK_OP_PUSHALL);
}


// =======================================================================
// Internal function to handle all of the functions that get an int back
// from a call.
// size()
// one()
// =======================================================================
as_status aerospike_lstack_ask_internal(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t *n, const char *operation )
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !n) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/n cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not stack type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

    // All we need to pass in is the LDT Bin Name
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, operation,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"no value returned from server");
	}
	int64_t ival = as_integer_getorelse(as_integer_fromval(p_return_val), -1);
	as_val_destroy(p_return_val);

	if (ival == -1) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"value returned from server not parse-able");
	}
	*n = (uint32_t)ival;

	return err->code;
} // end as_status aerospike_lstack_ask_internal()

// =======================================================================
as_status aerospike_lstack_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t *sizep)
{
	return aerospike_lstack_ask_internal (as, err, policy, key, ldt,
            sizep, LDT_STACK_OP_SIZE);
}


// =======================================================================
// Simple:  Just call the Lua UDF to return "1".
// There shouldn't be too much difference in performance between this and
// the size() call, but there will be some difference because size() has to
// unpack the entire LDT structure.
// =======================================================================
as_status aerospike_lstack_one(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t *sizep)
{
	return aerospike_lstack_ask_internal (as, err, policy, key, ldt,
            sizep, LDT_STACK_OP_ONE);
}


// =======================================================================
as_status aerospike_lstack_peek(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t peek_count,
	as_list ** elements )
{
	return aerospike_lstack_peek_with_filter(as, err, policy, key, ldt,
            peek_count, NULL, NULL, elements);
} // aerospike_lstack_peek()

// =======================================================================
as_status aerospike_lstack_filter(
		aerospike * as, as_error * err, const as_policy_apply * policy,
		const as_key * key, const as_ldt * ldt, uint32_t peek_count,
		const as_udf_function_name filter, const as_list *filter_args,
		as_list ** elements )
{
	if (!filter || ! filter_args) {
		return AEROSPIKE_ERR_PARAM;
	}
	return aerospike_lstack_peek_with_filter(as, err, policy, key, ldt, peek_count, filter, filter_args, elements);
}

// =======================================================================
as_status aerospike_lstack_set_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t elements_capacity
	)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !elements_capacity) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/capacity cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not stack type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, elements_capacity);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, LDT_STACK_OP_CAPACITY_SET,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	int64_t ival = as_integer_getorelse(as_integer_fromval(p_return_val), -1);
	as_val_destroy(p_return_val);

	if (ival == -1) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"value returned from server not parse-able");
	}
	if (ival !=0 ) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"capacity setting failed");
	}

	return err->code;
}

// =======================================================================
as_status aerospike_lstack_get_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t *elements_capacity
	)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !elements_capacity) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/capacity cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not stack type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, LDT_STACK_OP_CAPACITY_GET,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	int64_t ival = as_integer_getorelse(as_integer_fromval(p_return_val), -1);
	as_val_destroy(p_return_val);

	if (ival == -1) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"value returned from server not parse-able");
	}
	*elements_capacity = (uint32_t)ival;

	return err->code;
}

// =======================================================================
as_status aerospike_lstack_destroy(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt
	)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/capacity cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not stack type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, LDT_STACK_OP_DESTROY,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	int64_t ival = as_integer_getorelse(as_integer_fromval(p_return_val), -1);
	as_val_destroy(p_return_val);

	if (ival != 0) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"value returned from server not parse-able");
	}

	return err->code;
} // end as_status aerospike_lstack_destroy()

// =======================================================================
// LDT EXISTS
// =======================================================================
as_status aerospike_lstack_ldt_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, as_boolean *ldt_exists)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !ldt_exists) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/ldt_exists cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not LSTACK type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, LDT_STACK_OP_LDT_EXISTS,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"no value returned from server");
	}
	int64_t ival = as_integer_getorelse(as_integer_fromval(p_return_val), -1);
	as_val_destroy(p_return_val);

	if (ival == -1) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"value returned from server not parse-able");
	}
	as_boolean_init(ldt_exists, ival==1 ? true: false);

	return err->code;
} // end aerospike_lstack_ldt_exists()

// =======================================================================
// Pass in a value into the UDF -- and then get it back.  Simple.
// This is used to measure the performance of the end to end
// call infrastructure.
// Pass in LDT Bin and Value.
// Return Value.
// =======================================================================
as_status aerospike_lstack_same(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t  in_val,
    uint32_t * out_valp)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !out_valp) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/out_valp cannot be null");
	}
	if (ldt->type != AS_LDT_LSTACK) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not LSTACK type");
	}

	// stack allocate the arg list.
    // Pass in the LDT Bin and the IN VALUE.
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, in_val);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, LDT_STACK_OP_SAME,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	int64_t ival = as_integer_getorelse(as_integer_fromval(p_return_val), -1);
	as_val_destroy(p_return_val);

	if (ival == -1) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"value returned from server not parse-able");
	}
	if (ival !=0 ) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"same() Function Failed");
	}
    *out_valp = (uint32_t)ival;

	return err->code;
} // end as_status aerospike_lstack_same()

// =======================================================================
// =======================================================================
// =======================================================================
