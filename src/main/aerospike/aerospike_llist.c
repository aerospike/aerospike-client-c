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
#include <aerospike/aerospike_llist.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_types.h>

#include "_ldt.h"

// ++==============++
// || Fixed Values ||
// ++==============++
const char * DEFAULT_LLIST_PACKAGE = "llist";

// The names of the Lua Functions that implement Large Set Ops
const char * LDT_LIST_OP_ADD			= "add";
const char * LDT_LIST_OP_UPDATE			= "update";
const char * LDT_LIST_OP_ADDALL			= "add_all";
const char * LDT_LIST_OP_UPDATEALL		= "update_all";
const char * LDT_LIST_OP_FIND		    = "find";
const char * LDT_LIST_OP_RANGE		    = "range";
const char * LDT_LIST_OP_FIND_RANGE_LIM	= "find_range_lim";
const char * LDT_LIST_OP_SCAN   		= "scan";
const char * LDT_LIST_OP_FILTER			= "filter";
const char * LDT_LIST_OP_REMOVE		 	= "remove";
const char * LDT_LIST_OP_DESTROY  		= "destroy";
const char * LDT_LIST_OP_SIZE		   	= "size";
const char * LDT_LIST_OP_SET_CAPACITY	= "set_capacity";
const char * LDT_LIST_OP_GET_CAPACITY	= "get_capacity";
const char * LDT_LIST_OP_LDT_EXISTS		= "ldt_exists";
const char * LDT_LIST_OP_SET_PAGE_SIZE	= "setPageSize";

// =======================================================================
// ADD INTERNAL
// =======================================================================
static as_status aerospike_llist_add_internal(
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
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
	}

	/* Stack allocate the argument list.  Note that we are IMPLICITLY sending in
	 * a NIL value for the Create Module -- which is the mechanism that we use
	 * to configure the LDT for custom use.  */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( val ); // bump the ref count so the arraylist_destroy will not reset the val
	as_arraylist_append(&arglist, (as_val *)val);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, operation,
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
} // end aerospike_llist_add_internal()

// =======================================================================
// ADD
// =======================================================================
as_status aerospike_llist_add(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val) {
	return aerospike_llist_add_internal (as, err, policy, key, ldt, val, LDT_LIST_OP_ADD);
}

// =======================================================================
// UPDATE
// =======================================================================
as_status aerospike_llist_update(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val) {
	return aerospike_llist_add_internal (as, err, policy, key, ldt, val, LDT_LIST_OP_UPDATE);
}

// =======================================================================
// ADD ALL
// =======================================================================
as_status aerospike_llist_add_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * val_list) {
	return aerospike_llist_add_internal (as, err, policy, key, ldt,
			(as_val *)val_list, LDT_LIST_OP_ADDALL);
}

// =======================================================================
// UPDATE ALL
// =======================================================================
as_status aerospike_llist_update_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * val_list) {
	return aerospike_llist_add_internal (as, err, policy, key, ldt,
			(as_val *)val_list, LDT_LIST_OP_UPDATEALL);
}

// =======================================================================
// SIZE
// =======================================================================
as_status aerospike_llist_size(
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
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_SIZE,
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
// FIND
// =======================================================================
as_status aerospike_llist_find(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * search_val,
	as_list ** elements )
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !search_val || !elements) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/search_val/elements cannot be null");
	}
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
	}

	int list_argc = 2;
	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, list_argc);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve( search_val ); // bump the ref count so the arraylist_destroy will not reset the search_val
	as_arraylist_append(&arglist, (as_val *) search_val);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_FIND,
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

} // aerospike_llist_find()

// =======================================================================
// FIND FIRST
// =======================================================================
as_status aerospike_llist_find_first(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count, as_list ** elements)
{
	as_error_reset(err);
	
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);
	
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, count);
	
	as_val* p_return_val = NULL;
	aerospike_key_apply(as, err, policy, key, DEFAULT_LLIST_PACKAGE, "find_first",
						(as_list *)&arglist, &p_return_val);
	
	as_arraylist_destroy(&arglist);
	
	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}
	
	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL, "no value returned from server");
	}
	
	*elements = (as_list *)p_return_val;
	return err->code;
}

as_status aerospike_llist_find_first_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count,
	const as_udf_function_name filter, const as_list *filter_args, as_list ** elements)
{
	as_error_reset(err);
	
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);
	
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 5);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, count);
	
	as_string module_name;
	as_string_init(&module_name, (char *)ldt->module, false);
	as_arraylist_append_string(&arglist, &module_name);
	as_string filter_name;
	as_string_init(&filter_name, (char *)filter, false);
	as_arraylist_append_string(&arglist, &filter_name );
	as_val_reserve( filter_args ); // bump the ref count
	as_arraylist_append(&arglist, (as_val *) filter_args );

	as_val* p_return_val = NULL;
	aerospike_key_apply(as, err, policy, key, DEFAULT_LLIST_PACKAGE, "find_first",
						(as_list *)&arglist, &p_return_val);
	
	as_arraylist_destroy(&arglist);
	
	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}
	
	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL, "no value returned from server");
	}
	
	*elements = (as_list *)p_return_val;
	return err->code;
}

// =======================================================================
// FIND LAST
// =======================================================================
as_status aerospike_llist_find_last(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count, as_list ** elements)
{
	as_error_reset(err);
	
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);
	
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, count);
	
	as_val* p_return_val = NULL;
	aerospike_key_apply(as, err, policy, key, DEFAULT_LLIST_PACKAGE, "find_last",
						(as_list *)&arglist, &p_return_val);
	
	as_arraylist_destroy(&arglist);
	
	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}
	
	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL, "no value returned from server");
	}
	
	*elements = (as_list *)p_return_val;
	return err->code;
}

as_status aerospike_llist_find_last_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t count,
	const as_udf_function_name filter, const as_list *filter_args, as_list ** elements)
{
	as_error_reset(err);
	
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);
	
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 5);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, count);
	
	as_string module_name;
	as_string_init(&module_name, (char *)ldt->module, false);
	as_arraylist_append_string(&arglist, &module_name);
	as_string filter_name;
	as_string_init(&filter_name, (char *)filter, false);
	as_arraylist_append_string(&arglist, &filter_name );
	as_val_reserve( filter_args ); // bump the ref count
	as_arraylist_append(&arglist, (as_val *) filter_args );
	
	as_val* p_return_val = NULL;
	aerospike_key_apply(as, err, policy, key, DEFAULT_LLIST_PACKAGE, "find_last",
						(as_list *)&arglist, &p_return_val);
	
	as_arraylist_destroy(&arglist);
	
	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}
	
	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL, "no value returned from server");
	}
	
	*elements = (as_list *)p_return_val;
	return err->code;
}

// =======================================================================
// FIND FROM
// =======================================================================
as_status aerospike_llist_find_from(
	aerospike * as, as_error * err, const as_policy_apply * policy, const as_key * key,
	const as_ldt * ldt, const as_val * from_val, uint32_t count, as_list ** elements)
{
	as_error_reset(err);
	
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);
	
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 3);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve(from_val); // bump the ref count so the arraylist_destroy will not reset
	as_arraylist_append(&arglist, (as_val *)from_val);
	as_arraylist_append_int64(&arglist, count);
	
	as_val* p_return_val = NULL;
	aerospike_key_apply(as, err, policy, key, DEFAULT_LLIST_PACKAGE, "find_from",
						(as_list *)&arglist, &p_return_val);
	
	as_arraylist_destroy(&arglist);
	
	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}
	
	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL, "no value returned from server");
	}
	
	*elements = (as_list *)p_return_val;
	return err->code;
}

as_status aerospike_llist_find_from_filter(
	aerospike * as, as_error * err, const as_policy_apply * policy, const as_key * key,
	const as_ldt * ldt, const as_val * from_val, uint32_t count,
	const as_udf_function_name filter, const as_list *filter_args, as_list ** elements)
{
	as_error_reset(err);
	
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);
	
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 6);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_val_reserve(from_val); // bump the ref count so the arraylist_destroy will not reset
	as_arraylist_append(&arglist, (as_val *)from_val);
	as_arraylist_append_int64(&arglist, count);
	
	as_string module_name;
	as_string_init(&module_name, (char *)ldt->module, false);
	as_arraylist_append_string(&arglist, &module_name);
	as_string filter_name;
	as_string_init(&filter_name, (char *)filter, false);
	as_arraylist_append_string(&arglist, &filter_name );
	as_val_reserve( filter_args ); // bump the ref count
	as_arraylist_append(&arglist, (as_val *) filter_args );

	as_val* p_return_val = NULL;
	aerospike_key_apply(as, err, policy, key, DEFAULT_LLIST_PACKAGE, "find_from",
						(as_list *)&arglist, &p_return_val);
	
	as_arraylist_destroy(&arglist);
	
	if (ldt_parse_error(err) != AEROSPIKE_OK) {
		return err->code;
	}
	
	if (!p_return_val) {
		return as_error_set(err, AEROSPIKE_ERR_LDT_INTERNAL, "no value returned from server");
	}
	
	*elements = (as_list *)p_return_val;
	return err->code;
}

// =======================================================================
// SCAN
// =======================================================================
as_status aerospike_llist_scan(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, as_list ** elements )
{
	if ( !err ) {
		return AEROSPIKE_ERR_PARAM;
	}
	as_error_reset(err);

	if (!as || !key || !ldt || !elements) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/elements cannot be null");
	}
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
	}

	int list_argc = 1;
	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, list_argc);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_SCAN,
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

} // aerospike_llist_scan()

// =======================================================================
// FILTER
// =======================================================================
as_status aerospike_llist_filter(
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
	if (filter && (!ldt->module || ldt->module[0] == '\0')) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"filter name without ldt udf module name specification");
	}
	if (!as || !key || !ldt || !elements) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/elements cannot be null");
	}
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
	}

	int list_argc = filter ? 5 : 1;
	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, list_argc);
	as_arraylist_append_string(&arglist, &ldt_bin);

	if (filter){
		as_arraylist_append(&arglist, (as_val *)&as_nil); // use a nil keyList to scan all elements
		as_string module_name;
		as_string_init(&module_name, (char *)ldt->module, false);
		as_arraylist_append_string(&arglist, &module_name);
		as_string filter_name;
		as_string_init(&filter_name, (char *)filter, false);
		as_arraylist_append_string(&arglist, &filter_name );
		as_val_reserve( filter_args ); // bump the ref count
		as_arraylist_append(&arglist, (as_val *) filter_args );
	}

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, filter ? LDT_LIST_OP_FILTER : LDT_LIST_OP_SCAN,
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

} // aerospike_llist_filter()


// =======================================================================
// RANGE
// =======================================================================
as_status aerospike_llist_range_limit(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt,
	const as_val * min_value, const as_val * max_value, uint32_t count,
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
	if (filter && (!ldt->module || ldt->module[0] == '\0')) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"filter name without ldt udf module name specification");
	}
	if (!as || !key || !ldt || !elements) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/elements cannot be null");
	}
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
	}

	// Determine the operation to use and the parameter count.
	int list_argc = filter ? 6 : 3;
	const char* operation;
	
	if (count > 0) {
		operation = LDT_LIST_OP_FIND_RANGE_LIM;
		list_argc++;
	}
	else {
		operation = LDT_LIST_OP_RANGE;
	}
	
	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, list_argc);
	as_arraylist_append_string(&arglist, &ldt_bin);

	// Push the two vars (Min and Max).
	// Note: We bump the ref count so the arraylist_destroy will not reset the
	// min and max values when the list gets destroyed.
	// TODO: We must properly handle NULL values being passed in (they are
	// valid).
	as_val_reserve( min_value );
	as_val_reserve( max_value );

	as_arraylist_append(&arglist, (as_val *) min_value);
	as_arraylist_append(&arglist, (as_val *) max_value);

	if (count > 0) {
		as_arraylist_append_int64(&arglist, count);
	}
	
	if (filter){
		as_string module_name;
		as_string_init(&module_name, (char *)ldt->module, false);
		as_arraylist_append_string(&arglist, &module_name);
		as_string filter_name;
		as_string_init(&filter_name, (char *)filter, false);
		as_arraylist_append_string(&arglist, &filter_name );
		as_val_reserve( filter_args ); // bump the ref count
		as_arraylist_append(&arglist, (as_val *) filter_args );
	}

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, operation,
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
}

// =======================================================================
// REMOVE
// =======================================================================
as_status aerospike_llist_remove(
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
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
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
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_REMOVE,
		(as_list *)&arglist, &p_return_val);

	as_arraylist_destroy(&arglist);

	if (ldt_parse_error(err) != AEROSPIKE_OK) {
			return err->code;
	}

	if (p_return_val != NULL) {
		as_val_destroy(p_return_val);
	}

	return err->code;
} // end aerospike_llist_remove()


// =======================================================================
// DESTROY
// =======================================================================
as_status aerospike_llist_destroy(
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
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_DESTROY,
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
} // end aerospike_llist_destroy()

// =======================================================================
// SET CAPACITY
// =======================================================================
as_status aerospike_llist_set_capacity(
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
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_SET_CAPACITY,
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
} // aerospike_llist_set_capacity()

// =======================================================================
// GET_CAPACITY
// =======================================================================
as_status aerospike_llist_get_capacity(
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
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_GET_CAPACITY,
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
} // end aerospike_llist_get_capacity()

// =======================================================================
// LDT EXISTS
// =======================================================================
as_status aerospike_llist_ldt_exists(
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
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not LLIST type");
	}

	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);

	as_arraylist arglist;
	as_arraylist_inita(&arglist, 1);
	as_arraylist_append_string(&arglist, &ldt_bin);

	as_val* p_return_val = NULL;
	aerospike_key_apply(
		as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_LDT_EXISTS,
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
} // end aerospike_llist_ldt_exists()

// =======================================================================
// LDT Set page size
// =======================================================================
as_status aerospike_llist_set_page_size(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t page_size)
{
	as_error_reset(err);
		
	/* stack allocate the arg list */
	as_string ldt_bin;
	as_string_init(&ldt_bin, (char *)ldt->name, false);
	
	as_arraylist arglist;
	as_arraylist_inita(&arglist, 2);
	as_arraylist_append_string(&arglist, &ldt_bin);
	as_arraylist_append_int64(&arglist, page_size);
	
	as_val* p_return_val = NULL;
	aerospike_key_apply(as, err, policy, key, DEFAULT_LLIST_PACKAGE, LDT_LIST_OP_SET_PAGE_SIZE,
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
							"set page size failed");
	}
	
	return err->code;
}
