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
#include <aerospike/aerospike_lstack.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_ldt.h>
#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_status.h>
#include <aerospike/as_types.h>

#include "_log.h"
#include "_ldt.h"

// ++==============++
// || Fixed Values ||
// ++==============++
static char * DEFAULT_LSTACK_PACKAGE = "lstack";

// The names of the Lua Functions that implement Large Stack Ops
static char * LDT_STACK_OP_PUSH             = "push";
static char * LDT_STACK_OP_PUSHALL          = "push_all";
static char * LDT_STACK_OP_PEEK             = "peek";
// @TODO static char * LDT_STACK_OP_POP             	= "pop";
// @TODO static char * LDT_STACK_OP_SCAN  			= "scan";
static char * LDT_STACK_OP_FILTER  			= "filter";
static char * LDT_STACK_OP_DESTROY			= "destroy";
static char * LDT_STACK_OP_SIZE             = "size";
static char * LDT_STACK_OP_CAPACITY_SET		= "set_capacity";
static char * LDT_STACK_OP_CAPACITY_GET		= "get_capacity";


static as_status aerospike_lstack_push_internal(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val, const char *operation)
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
	as_arraylist_inita(&arglist, ldt->module[0] == '0' ? 2 : 3);
	as_arraylist_append_string(&arglist, &ldt_bin);
    as_val_reserve( val );
	as_arraylist_append(&arglist, (as_val *) val);
	if (ldt->module[0] == '0') {
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

as_status aerospike_lstack_push(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_val * val) {
	return aerospike_lstack_push_internal (as, err, policy, key, ldt, val, LDT_STACK_OP_PUSH);
}

as_status aerospike_lstack_push_all(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, const as_list * vals) {
	return aerospike_lstack_push_internal (as, err, policy, key, ldt,
			(as_val *)vals, LDT_STACK_OP_PUSHALL);
}

as_status aerospike_lstack_size(
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
		as, err, policy, key, DEFAULT_LSTACK_PACKAGE, LDT_STACK_OP_SIZE,
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
    *n = ival;

    return err->code;
}

as_status aerospike_lstack_peek(
	aerospike * as, as_error * err, const as_policy_apply * policy,
	const as_key * key, const as_ldt * ldt, uint32_t peek_count,
	as_list ** elements )
{
	return aerospike_lstack_peek_with_filter(as, err, policy, key, ldt, peek_count, NULL, NULL, elements);
} // aerospike_lstack_peek()

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
    *elements_capacity = ival;

    return err->code;
}

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
}
