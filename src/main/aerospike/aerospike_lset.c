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
#include <aerospike/aerospike_lset.h>
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
const char * DEFAULT_LSET_PACKAGE = "lset";

// The names of the Lua Functions that implement Large Set Ops
const char * LDT_SET_OP_ADD				= "add";
const char * LDT_SET_OP_ADDALL			= "add_all";
const char * LDT_SET_OP_GET				= "get";
const char * LDT_SET_OP_EXISTS			= "exists";
const char * LDT_SET_OP_SCAN			= "scan";
const char * LDT_SET_OP_FILTER			= "filter";
const char * LDT_SET_OP_REMOVE			= "remove";
const char * LDT_SET_OP_DESTROY			= "destroy";
const char * LDT_SET_OP_SIZE			= "size";
const char * LDT_SET_OP_SET_CAPACITY	= "set_capacity";
const char * LDT_SET_OP_GET_CAPACITY	= "get_capacity";
const char * LDT_SET_OP_LDT_EXISTS		= "ldt_exists";


static as_status aerospike_lset_add_internal(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val *val,  const char *operation)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/n cannot be null");
	}
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lset type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( val ); // bump the ref count so the arraylist_destroy will not reset the val
	as_arraylist_append(&arglist, (as_val *)val);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, operation,
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

	return err->code;
}

as_status aerospike_lset_add(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val) {
	return aerospike_lset_add_internal (as, err, policy, key, ldt, val, LDT_SET_OP_ADD);
}

as_status aerospike_lset_add_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * vals) {
	return aerospike_lset_add_internal (as, err, policy, key, ldt,
			(as_val *)vals, LDT_SET_OP_ADDALL);
}

as_status aerospike_lset_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t *n)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !n) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/n cannot be null");
	}
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lset type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_SIZE,
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
}

as_status aerospike_lset_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val,
    as_boolean *exists)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !exists) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/n cannot be null");
	}
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lset type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( val ); // bump the ref count
	as_arraylist_append(&arglist, (as_val *)val);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_EXISTS,
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

	as_boolean_init(exists, ival==1 ? true: false);

	return err->code;
} // aerospike_lset_exists()


as_status aerospike_lset_get(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val,
    as_val **pp_return_val)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !pp_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/return cannot be null");
	}
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lset type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( val ); // bump the ref count
	as_arraylist_append(&arglist, (as_val *)val);

	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_GET,
		(as_list *)&arglist, pp_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (! *pp_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"no value returned from server");
	}

	return err->code;
} // aerospike_lset_get()


as_status aerospike_lset_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
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
	if (!as || !key || !ldt || !elements) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/peek_count/elements cannot be null");
	}
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lset type");
	}

	int list_argc = filter ? 3 : 1;
	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, list_argc);
	as_arraylist_append_string(&arglist, &ldt_bin);

	if (filter){
		as_string filter_name;
		as_string_init(&filter_name, (char *)filter, false);
		as_arraylist_append_string(&arglist, &filter_name );
		as_val_reserve( filter_args ); // bump the ref count
		as_arraylist_append(&arglist, (as_val *) filter_args );
	}

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE,
		filter ? LDT_SET_OP_FILTER : LDT_SET_OP_SCAN,
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

} // aerospike_lset_filter()


as_status aerospike_lset_remove(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val *val
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
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lset type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( val );
	as_arraylist_append(&arglist, (as_val *) val);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_REMOVE,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	as_val_destroy(p_return_val);

	return err->code;
}


as_status aerospike_lset_destroy(
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
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lset type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_DESTROY,
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
}

// =======================================================================
// SET CAPACITY
// =======================================================================
as_status aerospike_lset_set_capacity(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t ldt_capacity
	)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !ldt_capacity) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/capacity cannot be null");
	}
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not stack type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, ldt_capacity);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_SET_CAPACITY,
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
} // aerospike_lset_set_capacity()

// =======================================================================
// GET_CAPACITY
// =======================================================================
as_status aerospike_lset_get_capacity(
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
	if (ldt->type != AS_LDT_LLIST) {
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
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_GET_CAPACITY,
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
} // end aerospike_lset_get_capacity()

// =======================================================================
// LDT EXISTS
// =======================================================================
as_status aerospike_lset_ldt_exists(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, as_boolean * ldt_exists)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !ldt_exists) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/ldt_exists cannot be null");
	}
	if (ldt->type != AS_LDT_LSET) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not LSET type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LSET_PACKAGE, LDT_SET_OP_LDT_EXISTS,
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
} // end aerospike_lset_exists()
