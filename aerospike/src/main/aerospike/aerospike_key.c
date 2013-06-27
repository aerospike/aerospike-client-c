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
#include <aerospike/as_buffer.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include <aerospike/as_msgpack.h>
#include <aerospike/as_serializer.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_object.h>
#include <citrusleaf/cl_write.h>
#include <citrusleaf/cf_log_internal.h>

#include "_log.h"
#include "_policy.h"
#include "_shim.h"

#include "../citrusleaf/internal.h"

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 *	Look up a record by key, then return all bins.
 *
 *	~~~~~~~~~~{.c}
 *		as_record * rec = NULL;
 *		if ( aerospike_key_get(&as, &err, NULL, "test", "demo", "foo", &rec) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_record_destroy(rec);
 *		}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param rec 			The record to be populated with the data from request.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, 
	as_record ** rec) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_read p;
	as_policy_read_resolve(&p, &as->config.policies, policy);

	uint32_t    timeout = p.timeout;          
	uint32_t    gen = 0;
	char *      set = NULL;
	int         nvalues = 0;
	cl_bin *    values = NULL;

	cl_rv rc = CITRUSLEAF_OK;

	switch ( p.key ) {
		case AS_POLICY_KEY_DIGEST: {
			as_digest * digest = as_key_digest((as_key *) key);
			rc = citrusleaf_get_all_digest_getsetname(as->cluster, key->namespace, (cf_digest *) digest->value, &values, &nvalues, timeout, &gen, &set);
			break;
		}
		case AS_POLICY_KEY_SEND: {
			cl_object okey;
			asval_to_clobject((as_val *) key->valuep, &okey);
			rc = citrusleaf_get_all(as->cluster, key->namespace, key->set, &okey, &values, &nvalues, timeout, &gen);
			break;
		}
		default: {
			// ERROR CASE
			break;
		}
	}
	
	if ( rc == CITRUSLEAF_OK && rec != NULL ) {
		as_record * r = *rec;
		if ( r == NULL ) {
			r = as_record_new(0);
		}
		if ( r->bins.entries == NULL ) {
			r->bins.capacity = nvalues;
			r->bins.size = 0;
			r->bins.entries = malloc(sizeof(as_bin) * nvalues);
		}
		clbins_to_asrecord(values, nvalues, r);
		r->gen = (uint16_t) gen;
		*rec = r;
	}

	return as_error_fromrc(err,rc);
}

/**
 *	Lookup a record by key, then return specified bins.
 *
 *	~~~~~~~~~~{.c}
 *		char * select[] = {"bin1", "bin2", "bin3", NULL};
 *		
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		as_record * rec = NULL;
 *		if ( aerospike_key_select(&as, &err, NULL, &key, select, &rec) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_record_destroy(rec);
 *		}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param bins			The bins to select. A NULL terminated array of NULL terminated strings.
 *	@param rec 			The record to be populated with the data from request.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, const char * bins[], 
	as_record ** rec) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_read p;
	as_policy_read_resolve(&p, &as->config.policies, policy);

	uint32_t    timeout = p.timeout;
	uint32_t    gen = 0;
	// char *      set = NULL;
	int         nvalues = 0;
	cl_bin *    values = NULL;

	for (nvalues = 0; bins[nvalues] != NULL; nvalues++)
		;

	values = (cl_bin *) alloca(sizeof(cl_bin) * nvalues);
	for ( int i = 0; i < nvalues; i++ ) {
		if ( strlen(bins[i]) > AS_BIN_NAME_MAX_LEN ) {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "bin name too long: %s", bins[i]);
		}

		strcpy(values[i].bin_name, bins[i]);
		citrusleaf_object_init(&values[i].object);
	}

	cl_rv rc = CITRUSLEAF_OK;

	switch ( p.key ) {
		case AS_POLICY_KEY_DIGEST: {
			as_digest * digest = as_key_digest((as_key *) key);
			rc = citrusleaf_get_digest(as->cluster, key->namespace, (cf_digest *) digest->value, values, nvalues, timeout, &gen);
			break;
		}
		case AS_POLICY_KEY_SEND: {
			cl_object okey;
			asval_to_clobject((as_val *) key->valuep, &okey);
			rc = citrusleaf_get(as->cluster, key->namespace, key->set, &okey, values, nvalues, timeout, &gen);
			break;
		}
		default: {
			// ERROR CASE
			break;
		}
	}

	if ( rc == CITRUSLEAF_OK && rec != NULL ) {
		as_record * r = *rec;
		if ( r == NULL ) {
			r = as_record_new(0);
		}
		if ( r->bins.entries == NULL ) {
			r->bins.capacity = nvalues;
			r->bins.size = 0;
			r->bins.entries = malloc(sizeof(as_bin) * nvalues);
		}
		clbins_to_asrecord(values, nvalues, r);
		r->gen = (uint16_t) gen;
		*rec = r;
	}

	return as_error_fromrc(err,rc);
}

/**
 *	Check if a record exists in the cluster via its key.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		bool exists = true;
 *		if ( aerospike_key_exists(&as, &err, NULL, &key, &exists) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			fprintf(stdout, "Record %s", exists ? "exists." : "doesn't exist.");
 *		}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param exists    	The variable to populate with `true` if the record exists, otherwise `false`.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, 
	bool * exists) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_read p;
	as_policy_read_resolve(&p, &as->config.policies, policy);

	uint32_t	timeout = p.timeout;
	uint32_t	gen = 0;
	int     	nvalues = 0;
	cl_bin *	values = NULL;
	
	cl_rv rc = CITRUSLEAF_OK;

	switch ( p.key ) {
		case AS_POLICY_KEY_DIGEST: {
			as_digest * digest = as_key_digest((as_key *) key);
			rc = citrusleaf_exists_digest(as->cluster, key->namespace, (cf_digest *) digest->value, values, nvalues, timeout, &gen);
			break;
		}
		case AS_POLICY_KEY_SEND: {
			cl_object okey;
			asval_to_clobject((as_val *) key->valuep, &okey);
			rc = citrusleaf_exists_key(as->cluster, key->namespace, key->set, &okey, values, nvalues, timeout, &gen);
			break;
		}
		default: {
			// ERROR CASE
			break;
		}
	}

	switch(rc) {
		case CITRUSLEAF_OK:
			if ( exists ) {
				*exists = true;
			}
			return AEROSPIKE_OK;
		case CITRUSLEAF_FAIL_NOTFOUND:
			if ( exists ) {
				*exists = false;
			}
			return AEROSPIKE_OK;
		default:
			if ( exists ) {
				*exists = false;
			}
			return as_error_fromrc(err,rc);
	}
}

/**
 *	Store a record in the cluster.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		as_record rec;
 *		as_record_init(&rec, 2);
 *		as_record_set_string(&rec, "bin1", "abc");
 *		as_record_set_integer(&rec, "bin2", 123);
 *		
 *		if ( aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		
 *		as_record_destroy(&rec);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param rec 			The record containing the data to be written.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_put(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const as_key * key, as_record * rec) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_write p;
	as_policy_write_resolve(&p, &as->config.policies, policy);

	cl_write_parameters wp;
	aspolicywrite_to_clwriteparameters(&p, rec, &wp);

	int			nvalues	= rec->bins.size;
	cl_bin *	values	= (cl_bin *) alloca(sizeof(cl_bin) * nvalues);

	asrecord_to_clbins(rec, values, nvalues);

	cl_rv rc = CITRUSLEAF_OK;

	switch ( p.key ) {
		case AS_POLICY_KEY_DIGEST: {
			as_digest * digest = as_key_digest((as_key *) key);
			rc = citrusleaf_put_digest_with_setname(as->cluster, key->namespace, key->set, (cf_digest *) digest->value, values, nvalues, &wp);
			break;
		}
		case AS_POLICY_KEY_SEND: {
			cl_object okey;
			asval_to_clobject((as_val *) key->valuep, &okey);
			rc = citrusleaf_put(as->cluster, key->namespace, key->set, &okey, values, nvalues, &wp);
			break;
		}
		default: {
			// ERROR CASE
			break;
		}
	}

	return as_error_fromrc(err,rc); 
}

/**
 *	Remove a record from the cluster.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		if ( aerospike_key_remove(&as, &err, NULL, &key) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_remove(
	aerospike * as, as_error * err, const as_policy_operate * policy, 
	const as_key * key) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_operate p;
	as_policy_operate_resolve(&p, &as->config.policies, policy);

	cl_write_parameters wp;
	aspolicyoperate_to_clwriteparameters(&p, &wp);

	cl_rv rc = CITRUSLEAF_OK;

	switch ( p.key ) {
		case AS_POLICY_KEY_DIGEST: {
			as_digest * digest = as_key_digest((as_key *) key);
			rc = citrusleaf_delete_digest(as->cluster, key->namespace, (cf_digest *) digest->value, &wp);
			break;
		}
		case AS_POLICY_KEY_SEND: {
			cl_object okey;
			asval_to_clobject((as_val *) key->valuep, &okey);
			rc = citrusleaf_delete(as->cluster, key->namespace, key->set, &okey, &wp);
			break;
		}
		default: {
			// ERROR CASE
			break;
		}
	}

	return as_error_fromrc(err,rc);
}

/**
 *	Lookup a record by key, then perform specified operations.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		as_operations ops;
 *		as_operations_inita(&ops,2);
 *		as_operations_append_int64(&ops, AS_OPERATOR_INCR, "bin1", 456);
 *		as_operations_append_str(&ops, AS_OPERATOR_APPEND, "bin1", "def");
 *
 *		if ( aerospike_key_remove(&as, &err, NULL, &key, &ops) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *	~~~~~~~~~~
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param ops			The operations to perform on the record.
 *	@param rec			The record to be populated with the data from AS_OPERATOR_READ operations.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_operate(
	aerospike * as, as_error * err, const as_policy_operate * policy, 
	const as_key * key, const as_operations * ops,
	as_record ** rec)
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_operate p;
	as_policy_operate_resolve(&p, &as->config.policies, policy);

	cl_write_parameters wp;
	aspolicyoperate_to_clwriteparameters(&p, &wp);

	int 			replace = 0;
	uint32_t 		gen = 0;
	int 			n_operations = ops->binops.size;
	cl_operation * 	operations = (cl_operation *) alloca(sizeof(cl_operation) * n_operations);
	as_bin_name *	read_op_bins = alloca(sizeof(as_bin_name) * n_operations);
	int				n_read_ops = 0;

	for(int i=0; i<n_operations; i++) {
		cl_operation * clop = &operations[i];
		as_binop * op = &ops->binops.entries[i];

		// Length check already done on as_bin name length. For performance we
		// we'll leave out this down-size check since this is a temporary shim
		// and we know the CL and AS limits are the same...
//		if ( strlen(op->bin.name) > CL_BINNAME_SIZE - 1 ) {
//			return as_error_update(err, AEROSPIKE_ERR_PARAM, "bin name too long: %s", op->bin.name);
//		}

		strcpy(clop->bin.bin_name, op->bin.name);
		clop->op = op->operator;

		// Collect bin names that are read.
		if (op->operator == AS_OPERATOR_READ) {
			strcpy(read_op_bins[n_read_ops++], op->bin.name);
		}

		asbinvalue_to_clobject(op->bin.valuep, &clop->bin.object);
	}

	cl_rv rc = CITRUSLEAF_OK;

	switch ( p.key ) {
		case AS_POLICY_KEY_DIGEST: {
			as_digest * digest = as_key_digest((as_key *) key);
			rc = citrusleaf_operate_digest(as->cluster, key->namespace, (cf_digest *) digest->value, operations, n_operations, &wp, replace, &gen);
			break;
		}
		case AS_POLICY_KEY_SEND: {
			cl_object okey;
			asval_to_clobject((as_val *) key->valuep, &okey);
			rc = citrusleaf_operate(as->cluster, key->namespace, key->set, &okey, operations, n_operations, &wp, replace, &gen);
			break;
		}
		default: {
			// ERROR CASE
			break;
		}
	}

	if ( n_read_ops != 0 && rc == CITRUSLEAF_OK && rec != NULL ) {
		as_record * r = *rec;
		if ( r == NULL ) {
			r = as_record_new(0);
		}
		if ( r->bins.entries == NULL ) {
			r->bins.capacity = n_read_ops;
			r->bins.size = 0;
			r->bins.entries = malloc(sizeof(as_bin) * n_read_ops);
		}
		r->gen = (uint16_t) gen;

		// This works around an existing client bug where the data returned for
		// a read operation is stored in the first bin struct with that bin
		// name, not necessarily the bin struct corresponding to the read.
		for (int i = 0; i < n_read_ops; i++) {
			for (int j = 0; j < n_operations; j++) {
				if (strcmp(read_op_bins[i], operations[j].bin.bin_name) == 0) {
					clbin_to_asrecord(&operations[j].bin, r);
					break;
				}
			}
		}

		*rec = r;
	}

	return as_error_fromrc(err,rc);
}

/**
 *	Lookup a record by key, then apply the UDF.
 *
 *	~~~~~~~~~~{.c}
 *		as_key key;
 *		as_key_init(&key, "ns", "set", "key");
 *
 *		as_list args;
 *		as_arraylist_init(&args, 2, 0);
 *		as_list_append_int64(&args, 1);
 *		as_list_append_int64(&args, 2);
 *		
 *		as_val * res = NULL;
 *		
 *		if ( aerospike_key_apply(&as, &err, NULL, &key, "math", "add", &args, &res) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_val_destroy(res);
 *		}
 *		
 *		as_list_destroy(&args);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param key			The key of the record.
 *	@param module		The module containing the function to execute.
 *	@param function 	The function to execute.
 *	@param arglist 		The arguments for the function.
 *	@param result 		The return value from the function.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_key_apply(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_key * key, 
	const char * module, const char * function, as_list * arglist, 
	as_val ** result) 
{
	// we want to reset the error so, we have a clean state
	as_error_reset(err);
	
	// resolve policies
	as_policy_read p;
	as_policy_read_resolve(&p, &as->config.policies, policy);

	cl_write_parameters wp;
	cl_write_parameters_set_default(&wp);
	wp.timeout_ms = p.timeout;

	cl_object okey;
	asval_to_clobject((as_val *) key->valuep, &okey);

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
	cl_bin * bins = 0;
	int n_bins = 0;

	cl_rv rc = CITRUSLEAF_OK;

	switch ( p.key ) {
		case AS_POLICY_KEY_DIGEST: {
			as_digest * digest = as_key_digest((as_key *) key);
			rc = do_the_full_monte( 
				as->cluster, 0, CL_MSG_INFO2_WRITE, 0, 
				key->namespace, key->set, 0, (cf_digest *) digest->value, &bins, CL_OP_WRITE, 0, &n_bins, 
				NULL, &wp, &trid, NULL, &call
			);
			break;
		}
		case AS_POLICY_KEY_SEND: {
			cl_object okey;
			asval_to_clobject((as_val *) key->valuep, &okey);
			rc = do_the_full_monte( 
				as->cluster, 0, CL_MSG_INFO2_WRITE, 0, 
				key->namespace, key->set, &okey, 0, &bins, CL_OP_WRITE, 0, &n_bins, 
				NULL, &wp, &trid, NULL, &call
			);
			break;
		}
		default: {
			// ERROR CASE
			break;
		}
	}

	as_buffer_destroy(&args);

	if (! (rc == CITRUSLEAF_OK || rc == CITRUSLEAF_FAIL_UDF_BAD_RESPONSE)) {
		as_error_update(err, AEROSPIKE_ERR, "Invalid Response (0)");
	} 
	else if ( n_bins == 1  ) {

		cl_bin * bin = &bins[0];
		as_val * val = NULL;
		clbin_to_asval(bin, &ser, &val);

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
