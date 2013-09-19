/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_llist.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_list.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_types.h>

#include "_log.h"
#include "_ldt.h"

// ++==============++
// || Fixed Values ||
// ++==============++
// The names of the Lua Functions that implement Large Set Ops
static char * LDT_LIST_OP_ADD				= "add";
static char * LDT_LIST_OP_ADDALL			= "add_all";
// @TODO static char * LDT_LIST_OP_FIND     			= "find";
static char * LDT_LIST_OP_SCAN       		= "scan";
static char * LDT_LIST_OP_FILTER			= "filter";
static char * LDT_LIST_OP_REMOVE     		= "remove";
static char * LDT_LIST_OP_DESTROY  			= "destroy";
static char * LDT_LIST_OP_SIZE       		= "size";
// @TODO static char * LDT_LIST_OP_SET_CAPACITY      = "set_capacity";
// @TODO static char * LDT_LIST_OP_GET_CAPACITY      = "get_capacity";


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
		as, err, policy, key, ldt->module, operation,
		(as_list *)&arglist, &p_return_val);

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

as_status aerospike_llist_add(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val) {
	return aerospike_llist_add_internal (as, err, policy, key, ldt, val, LDT_LIST_OP_ADD);
}

as_status aerospike_llist_add_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * vals) {
	return aerospike_llist_add_internal (as, err, policy, key, ldt,
			(as_val *)vals, LDT_LIST_OP_ADDALL);
}

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
		as, err, policy, key, ldt->module, LDT_LIST_OP_SIZE,
		(as_list *)&arglist, &p_return_val);

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
    *n = ival;

    return err->code;
}


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
	if (!as || !key || !ldt || !elements) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"as/key/ldt/peek_count/elements cannot be null");
	}
	if (ldt->type != AS_LDT_LLIST) {
		return as_error_set(err, AEROSPIKE_ERR_PARAM, "invalid parameter. "
				"not llist type");
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
		as, err, policy, key, ldt->module, filter ? LDT_LIST_OP_FILTER : LDT_LIST_OP_SCAN,
		(as_list *)&arglist, &p_return_val);

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
		as, err, policy, key, ldt->module, LDT_LIST_OP_REMOVE,
		(as_list *)&arglist, &p_return_val);

    if (ldt_parse_error(err) != AEROSPIKE_OK) {
    	return err->code;
    }

    if (p_return_val != NULL) {
    	as_val_destroy(p_return_val);
    }

    return err->code;
}


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
		as, err, policy, key, ldt->module, LDT_LIST_OP_DESTROY,
		(as_list *)&arglist, &p_return_val);

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
