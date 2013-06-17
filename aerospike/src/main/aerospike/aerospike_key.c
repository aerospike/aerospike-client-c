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
#include <aerospike/aerospike_key.h>

#include <aerospike/as_bin.h>
#include <aerospike/as_binop.h>
#include <aerospike/as_buffer.h>
#include <aerospike/as_digest.h>
#include <aerospike/as_error.h>
#include <aerospike/as_list.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include <aerospike/as_msgpack.h>
#include <aerospike/as_serializer.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_object.h>
#include <citrusleaf/cl_write.h>
#include <citrusleaf/cf_log_internal.h>

#include "log.h"
#include "shim.h"
#include "../internal.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Look up a record by key, then return all bins.
 *
 *      as_record * rec = NULL;
 *      if ( aerospike_key_get(&as, &err, NULL, "test", "demo", "foo", &rec) != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set-name of the record.p
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param rec       - the record to be populated with the data from request.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const char * set, const char * key, 
	as_record ** rec) 
{
	// if policy is NULL, then get default policy
	as_policy_read * p = (as_policy_read *) (policy ? policy : &as->config.policies.read);

	uint32_t    timeout = p->timeout;          
	uint32_t    gen = 0;
	int         nvalues = 0;
	cl_bin *    values = NULL;
	cl_object   okey;

	citrusleaf_object_init_str(&okey, key);

	cl_rv rc = citrusleaf_get_all(as->cluster, ns, set, &okey, &values, &nvalues, timeout, &gen);

	if ( rec != NULL ) {
		as_record * r = *rec;
		if ( r == NULL ) {
			r = as_record_new(0);
		}
		if ( r->bins.entries == NULL ) {
			r->bins.capacity = nvalues;
			r->bins.size = 0;
			r->bins.entries = malloc(sizeof(as_bin) * nvalues);
		}
		as_record_frombins(r, values, nvalues);
		*rec = r;
	}

	return as_error_fromrc(err,rc);
}

/**
 * Lookup a record by key, then return specified bins.
 *
 *      as_record * rec = NULL;
 *      char * select[] = {"bin1", "bin2", "bin3", NULL};
 *      if ( aerospike_key_select(&as, &err, NULL, "test", "demo", "foo", select, &rec) != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param bins      - the bins to select. A NULL terminated array of NULL terminated strings.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const char * set, const char * key, 
	const char * bins[], 
	as_record ** rec) 
{
	// if policy is NULL, then get default policy
	as_policy_read * p = (as_policy_read *) (policy ? policy : &as->config.policies.read);

	uint32_t    timeout = p->timeout;
	uint32_t    gen = 0;
	int         nvalues = 0;
	cl_bin *    values = NULL;
	cl_object   okey;

	for (nvalues = 0; bins[nvalues] != NULL; nvalues++);

	values = (cl_bin *) alloca(sizeof(cl_bin) * nvalues);
	for ( int i = 0; i < nvalues; i++ ) {
		strncpy(values[i].bin_name, bins[i], AS_BIN_NAME_LEN);
		values[i].bin_name[AS_BIN_NAME_LEN - 1] = '\0';
		citrusleaf_object_init(&values[i].object);
	}

	citrusleaf_object_init_str(&okey, key);

	cl_rv rc = citrusleaf_get(as->cluster, ns, set, &okey, values, nvalues, timeout, &gen);

	if ( rec != NULL ) {
		as_record * r = *rec;
		if ( r == NULL ) {
			r = as_record_new(0);
		}
		if ( r->bins.entries == NULL ) {
			r->bins.capacity = nvalues;
			r->bins.size = 0;
			r->bins.entries = malloc(sizeof(as_bin) * nvalues);
		}
		as_record_frombins(r, values, nvalues);
		*rec = r;
	}

	return as_error_fromrc(err,rc);
}

/**
 * Check if a record exists in the cluster via its key.
 *
 *      bool exists = true;
 *      if ( aerospike_key_exists(&as, &err, NULL, "test", "demo", "foo", &exists) != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *      else {
 *          fprintf(stdout, "Record %s", exists ? "exists." : "doesn't exist.");
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param exists    - will be `true` if the record exists, otherwise `false`.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred. Check the `err` for details on the error.
 */
as_status aerospike_key_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const char * set, const char * key,
	bool * exists) 
{
	// if policy is NULL, then get default policy
	as_policy_read * p = policy ? (as_policy_read *) policy : &(as->config.policies.read);

	uint32_t    timeout = p->timeout;
	uint32_t    gen = 0;
	int         nvalues = 0;
	cl_bin *    values = NULL;
	cl_object   okey;

	citrusleaf_object_init_str(&okey, key);

	cl_rv rc = citrusleaf_exists_key(as->cluster, ns, set, &okey, values, nvalues, timeout, &gen);
	
	switch(rc) {
		case CITRUSLEAF_OK:
			if ( exists ) {
				*exists = true;
			}
			return as_error_reset(err);
		case CITRUSLEAF_FAIL_NOTFOUND:
			if ( exists ) {
				*exists = false;
			}
			return as_error_reset(err);
		default:
			if ( exists ) {
				*exists = false;
			}
			return as_error_fromrc(err,rc);
	}
}

/**
 * Store a record in the cluster.
 *
 *      as_record * rec = as_record_new(2);
 *      as_record_set_string(rec, "bin1", "abc");
 *      as_record_set_integer(rec, "bin2", 123);
 *
 *      if ( aerospike_key_put(&as, &err, NULL, "test", "demo", "foo", rec) != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred. Check the `err` for details on the error.
 */
as_status aerospike_key_put(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const char * ns, const char * set, const char * key, 
	as_record * rec) 
{
	// if policy is NULL, then get default policy
	as_policy_write * p = policy ? (as_policy_write *) policy : &(as->config.policies.write);

	int         nvalues = rec->bins.size;
	cl_bin *    values = (cl_bin *) alloca(sizeof(cl_bin) * nvalues);

	cl_write_parameters wp;
	as_policy_write_towp(p, rec, &wp);

	cl_object okey;
	citrusleaf_object_init_str(&okey, key);

	as_record_tobins(rec, values, nvalues);

	cl_rv rc = citrusleaf_put(as->cluster, ns, set, &okey, values, nvalues, &wp);

	return as_error_fromrc(err,rc); 
}

/**
 * Remove a record from the cluster.
 *
 *      if ( aerospike_key_remove(&as, &err, NULL, "test", "demo", "foo") != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred. Check the `err` for details on the error.
 */
as_status aerospike_key_remove(
	aerospike * as, as_error * err, const as_policy_remove * policy, 
	const char * ns, const char * set, const char * key) 
{
	// if policy is NULL, then get default policy
	as_policy_remove * p = (as_policy_remove *) (policy ? policy : &as->config.policies.remove);

	cl_object okey;

	cl_write_parameters wp;
	as_policy_remove_towp(p, &wp);

	citrusleaf_object_init_str(&okey, key);

	cl_rv rc = citrusleaf_delete(as->cluster, ns, set, &okey, &wp);

	return as_error_fromrc(err,rc);
}

/**
 * Lookup a record by key, then apply the UDF.
 *
 *      as_list args;
 *      as_arraylist_init(&args, 2, 0);
 *      as_list_add_integer(&args, 1);
 *      as_list_add_integer(&args, 2);
 *
 *      as_val * res = NULL;
 *
 *      if ( aerospike_key_apply(&as, &err, NULL, "test", "demo", "foo", "math", "add", &args, &res) != AEROSPIKE_OK ) {
 *          fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *      }
 *
 * @param as        - the aerospike cluster to connect to
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param module    - the module containing the function to execute
 * @param function  - the function to execute
 * @param arglist   - arguments for the function
 * @param result    - the return value from the function
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred. Check the `err` for details on the error.
 */
as_status aerospike_key_apply(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const char * ns, const char * set, const char * key, 
	const char * module, const char * function, as_list * arglist, 
	as_val ** result) 
{
	// if policy is NULL, then get default policy
	as_policy_read * p = (as_policy_read *) (policy ? policy : &as->config.policies.read);

	cl_rv rv = CITRUSLEAF_OK;

	cl_object okey;
	citrusleaf_object_init_str(&okey, key);

	as_serializer ser;
	as_msgpack_init(&ser);

	as_string file;
	as_string_init(&file, (char *) module, true /*ismalloc*/);

	as_string func;
	as_string_init(&func, (char *) function, true /*ismalloc*/);
	
	as_buffer args;
	as_buffer_init(&args);

	as_serializer_serialize(&ser, (as_val *) arglist, &args);

	as_call call = {
		.file = &file,
		.func = &func,
		.args = &args
	};

	uint64_t trid = 0;

	cl_write_parameters wp;
	cl_write_parameters_set_default(&wp);
	wp.timeout_ms = p->timeout;

	cl_bin *bins = 0;
	int n_bins = 0;

	rv = do_the_full_monte( 
		as->cluster, 0, CL_MSG_INFO2_WRITE, 0, 
		ns, set, &okey, 0, &bins, CL_OP_WRITE, 0, &n_bins, 
		NULL, &wp, &trid, NULL, &call
	);

	as_buffer_destroy(&args);

	if (! (rv == CITRUSLEAF_OK || rv == CITRUSLEAF_FAIL_UDF_BAD_RESPONSE)) {
		as_error_update(err, AEROSPIKE_ERR, "Invalid Response (0)");
	} 
	else if ( n_bins == 1  ) {

		cl_bin * bin = &bins[0];
		as_val * val = as_val_frombin(&ser, bin);

		if ( val ) {
			if ( strcmp(bin->bin_name,"SUCCESS") == 0 ) {
				*result = val;
				val = NULL;
			}
			else if ( strcmp(bin->bin_name,"FAILURE") == 0 ) {
				if ( val->type == AS_STRING ) {
					as_string * s = as_string_fromval(val);
					as_error_update(err, AEROSPIKE_ERR, as_string_tostring(s));
				}
				else {
					as_error_update(err, AEROSPIKE_ERR, "Invalid Response (1)");
				}
				as_val_destroy(val);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR, "Invalid Response (2)");
				as_val_destroy(val);
			}
		}
		else {
			as_error_update(err, AEROSPIKE_ERR, "Invalid Response (3)");
		}
	}
	else {
		as_error_update(err, AEROSPIKE_ERR, "Invalid Response (4)");
	}

	if ( bins ) {
		citrusleaf_bins_free(bins, n_bins);
		free(bins);
	}

	as_serializer_destroy(&ser);
	
	return err->code;
}

/**
 * Lookup a record by key, then perform specified operations.
 * 
 * @param as        - the aerospike cluster to connect to.
 * @param err       - the error is populated if the return value is not AEROSPIKE_OK.
 * @param policy    - the policy to use for this operation. If NULL, then the default policy will be used.
 * @param ns        - the namespace of the record.
 * @param set       - the set of the record.
 * @param key       - the key of the record. Can be either as_integer or as_string.
 * @param ops       - an array of as_bin_operation, which specify the operation to perform on bins of the record.
 * @param nops      - the number of operations.
 *
 * @return AEROSPIKE_OK if successful. Otherwise an error occurred. Check the `err` for details on the error.
 */
as_status aerospike_key_operate(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const char * ns, const char * set, const char * key, 
	const as_binop * ops, uint32_t nops) 
{
	return AEROSPIKE_ERR;
	// if policy is NULL, then get default policy
	// // as_policy_write * p = policy ? (as_policy_write *) policy : &(as->config.policies.write);

	// // int         nvalues = rec->bins.size;
	// // cl_bin *    values = (cl_bin *) alloca(sizeof(cl_bin) * nvalues);

	// cl_write_parameters wp;
	// // as_policy_write_towp(p, rec, &wp);

	// cl_object okey;
	// citrusleaf_object_init_str(&okey, key);

	// cl_operation * operations = NULL;
	// int n_operations = 0;
	// int replace = 0;
	// uint32_t generation = 0;

	// cl_rv rc = citrusleaf_operate(as->cluster, ns, set, &okey, operations, n_operations, &wp, replace, &generation);

	// return rc ? AEROSPIKE_ERR : AEROSPIKE_OK;
}
