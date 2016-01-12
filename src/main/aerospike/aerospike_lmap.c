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
#include <aerospike/aerospike_lmap.h>
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
const char * DEFAULT_LMAP_PACKAGE = "lmap";

// The names of the Lua Functions that implement Large Map Ops
const char * LDT_MAP_OP_PUT				= "put";
const char * LDT_MAP_OP_PUTALL			= "put_all";
const char * LDT_MAP_OP_GET 			= "get";
const char * LDT_MAP_OP_SCAN   			= "scan";
const char * LDT_MAP_OP_FILTER			= "filter";
const char * LDT_MAP_OP_REMOVE 			= "remove";
const char * LDT_MAP_OP_DESTROY  		= "destroy";
const char * LDT_MAP_OP_SIZE   			= "size";
const char * LDT_MAP_OP_SET_CAPACITY	= "set_capacity";
const char * LDT_MAP_OP_GET_CAPACITY	= "get_capacity";
const char * LDT_MAP_OP_LDT_EXISTS		= "ldt_exists";

// =======================================================================
// PUT
// =======================================================================
as_status aerospike_lmap_put(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_val * mkey, const as_val * mval)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !mkey || !mval) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/mkey/mval cannot be null");
	}
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lmap type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 3);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( mkey ); // bump the ref count so the arraylist_destroy will not reset the val
	as_arraylist_append(&arglist, (as_val *)mkey);
	as_val_reserve( mval ); // bump the ref count so the arraylist_destroy will not reset the val
	as_arraylist_append(&arglist, (as_val *)mval);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_PUT,
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

// =======================================================================
// PUT ALL
// =======================================================================
as_status aerospike_lmap_put_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_map * vals)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !vals) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/vals cannot be null");
	}
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lmap type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( vals ); // bump the ref count so the arraylist_destroy will not reset the val
	as_arraylist_append(&arglist, (as_val *)vals);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_PUTALL,
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

// =======================================================================
// SIZE
// =======================================================================
as_status aerospike_lmap_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	uint32_t *n)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !n) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/n cannot be null");
	}
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lmap type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_SIZE,
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

// =======================================================================
// GET
// =======================================================================
as_status aerospike_lmap_get(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * mkey,
	as_val ** mval)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !mkey || !mval) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/n cannot be null");
	}
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lmap type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( mkey ); // bump the ref count so the arraylist_destroy will not reset the val

	as_arraylist_append(&arglist, (as_val *)mkey);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_GET,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"no value returned from server");
	}
    // This looks like left over code from a copy/paste.
    // We wouldn't be able to ASSUME that the map VALUE is an integer.
//	int64_t ival = as_integer_getorelse(as_integer_fromval(p_return_val), -1);
//	as_val_destroy(p_return_val);
//
//	if (ival == -1) {
//		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
//				"value returned from server not parse-able");
//	}
	*mval = p_return_val;

	return err->code;
} // aerospike_lmap_get()


// =======================================================================
// FILTER INTERNAL
// =======================================================================
as_status aerospike_lmap_filter_internal(
		aerospike * as, as_error * err, const as_policy_apply * policy,
		const as_key * key, const as_ldt * ldt,
		const as_udf_function_name filter, const as_list *filter_args,
		as_map ** elements)
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
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lmap type");
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
		as, err, policy, key, DEFAULT_LMAP_PACKAGE,
		filter ? LDT_MAP_OP_FILTER : LDT_MAP_OP_SCAN,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL,
				"no value returned from server");
	}

	*elements = (as_map *)p_return_val;

	return err->code;

} // aerospike_lmap_filter_internal()

// =======================================================================
// GET ALL
// =======================================================================
as_status aerospike_lmap_get_all(
		aerospike * as, as_error * err, const as_policy_apply * policy,
		const as_key * key, const as_ldt * ldt,
		as_map ** elements)
{
	return aerospike_lmap_filter_internal(as, err, policy,
		key, ldt, NULL, NULL, elements);

}

// =======================================================================
// FILTER
// =======================================================================
as_status aerospike_lmap_filter(
		aerospike * as, as_error * err, const as_policy_apply * policy,
		const as_key * key, const as_ldt * ldt,
		const as_udf_function_name filter, const as_list *filter_args,
		as_map ** elements)
{
	return aerospike_lmap_filter_internal(as, err, policy,
		key, ldt, filter, filter_args, elements);
}

// =======================================================================
// REMOVE
// =======================================================================
as_status aerospike_lmap_remove(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val *mkey)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/capacity cannot be null");
	}
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lmap type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( mkey );
	as_arraylist_append(&arglist, (as_val *) mkey);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_REMOVE,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}

	if (p_return_val != NULL) {
		as_val_destroy(p_return_val);
	}

	return err->code;
}

// =======================================================================
// DESTROY
// =======================================================================
as_status aerospike_lmap_destroy(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt)
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt cannot be null");
	}
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not lmap type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_DESTROY,
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
as_status aerospike_lmap_set_capacity(
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
	if (ldt->type != AS_LDT_LMAP) {
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
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_SET_CAPACITY,
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
} // aerospike_lmap_set_capacity()

// =======================================================================
// GET_CAPACITY
// =======================================================================
as_status aerospike_lmap_get_capacity(
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
	if (ldt->type != AS_LDT_LMAP) {
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
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_GET_CAPACITY,
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
} // end aerospike_lmap_get_capacity()

// =======================================================================
// LDT EXISTS
// =======================================================================
as_status aerospike_lmap_ldt_exists(
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
	if (ldt->type != AS_LDT_LMAP) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not LMAP type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LMAP_PACKAGE, LDT_MAP_OP_LDT_EXISTS,
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
} // end aerospike_lmap_ldt_exists()
