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
#include <aerospike/aerospike_digest.h>

#include <aerospike/as_bin.h>
#include <aerospike/as_digest.h>
#include <aerospike/as_error.h>
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
#include <citrusleaf/cf_digest.h>

#include "shim.h"
#include "../internal.h"


/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 *	Get a record using a digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		as_record * rec = NULL;
 *		if ( aerospike_key_get(&as, &err, NULL, "test", &digest, &rec) != AEROSPIKE_OK ) {
 *		    fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param digest		The digest of the record. 
 *	@param rec 			The record to be created and populated with the data.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_get(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_digest * digest, 
	as_record ** rec) 
{
	as_error_reset(err);

	// if policy is NULL, then get default policy
	as_policy_read * p = (as_policy_read *) (policy ? policy : &as->config.policies.read);

	uint32_t    timeout = p->timeout;          
	uint32_t    gen = 0;
	int         nvalues = 0;
	cl_bin *    values = NULL;
	char *      set = NULL;

	cl_rv rc = citrusleaf_get_all_digest_getsetname(as->cluster, digest->key.namespace, (cf_digest *) digest->value, &values, &nvalues, timeout, &gen, &set);
	
	as_record * r = *rec;
	if ( r->bins.entries == NULL ) {
		r->bins.capacity = nvalues;
		r->bins.size = 0;
		r->bins.entries = malloc(sizeof(as_bin) * nvalues);
	}
	as_record_frombins(r, values, nvalues);

	return as_error_fromrc(err,rc);   
}

/**
 *	Lookup a record by digest, then select specific bins.
 *
 *	The bins arguments must be a sequence of strings, terminated by a NULL:
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		char * select[] = {"bin1", "bin2", "bin3", NULL};
 *		
 *		as_record * rec = NULL;
 *		if ( aerospike_key_select(&as, &err, NULL, "test", &digest, select, &rec) != AEROSPIKE_OK ) {
 *		    fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *		    as_record_destroy(rec);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param digest		The digest of the record. 
 *	@param bins			The bins to select. 
 *	@param rec 			The record to be created and populated with the data.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_select(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_digest * digest, const char * bins[], 
	as_record ** rec) 
{
	as_error_reset(err);

	// if policy is NULL, then get default policy
	as_policy_read * p = (as_policy_read *) (policy ? policy : &as->config.policies.read);

	uint32_t    timeout = p->timeout;
	uint32_t    gen = 0;
	int         nvalues = 0;
	cl_bin *    values = NULL;
	
	for (nvalues = 0; bins[nvalues] != NULL; nvalues++);

	values = (cl_bin *) alloca(sizeof(cl_bin) * nvalues);
	for ( int i = 0; i < nvalues; i++ ) {
		strncpy(values[i].bin_name, bins[i], AS_BIN_NAME_MAX_LEN);
		values[i].bin_name[AS_BIN_NAME_MAX_LEN] = '\0';
		citrusleaf_object_init(&values[i].object);
	}

	cl_rv rc = citrusleaf_get_digest(as->cluster, digest->key.namespace, (cf_digest *) digest->value, values, nvalues, timeout, &gen);

	as_record * r = *rec;
	if ( r->bins.entries == NULL ) {
		r->bins.capacity = nvalues;
		r->bins.size = 0;
		r->bins.entries = malloc(sizeof(as_bin) * nvalues);
	}
	as_record_frombins(r, values, nvalues);

	return as_error_fromrc(err,rc);
}

/**
 *	Check if a record exists in the cluster using its digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		bool exists = true;
 *		if ( aerospike_digest_exists(&as, &err, NULL, "test", &digest, &exists) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			fprintf(stdout, "Record %s", exists ? "exists." : "doesn't exist.");
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param digest		The digest of the record. 
 *	@param exists    	The variable to populate with `true` if the record exists, otherwise `false`.
 *
 *	@return AEROSPIKE_OK if exists. AEROSPIKE_DOES_NOT_EXIST if doesn't exists. Otherwise an error.
 */
as_status aerospike_digest_exists(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_digest * digest, 
	bool * exists) 
{
	as_error_reset(err);

	// if policy is NULL, then get default policy
	as_policy_read * p = (as_policy_read *) (policy ? policy : &as->config.policies.read);

	uint32_t    timeout = p->timeout;
	uint32_t    gen = 0;
	int         nvalues = 0;
	cl_bin *    values = NULL;

	cl_rv rc = citrusleaf_exists_digest(as->cluster, digest->key.namespace, (cf_digest *) digest->value, values, nvalues, timeout, &gen);
	
	switch(rc) {
		case CITRUSLEAF_OK:
			*exists = true;
			return as_error_reset(err);
		case CITRUSLEAF_FAIL_NOTFOUND:
			*exists = false;
			return as_error_reset(err);
		default:
			return as_error_fromrc(err,rc);
	}
}

/**
 *	Put a record in the cluster using a digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		as_record rec;
 *		as_record_init(&rec, 2);
 *		as_record_set_string(&rec, "bin1", "abc");
 *		as_record_set_integer(&rec, "bin2", 123);
 *		
 *		if ( aerospike_key_put(&as, &err, NULL, "test", &digest, &rec) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_record_destroy(&rec);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param digest		The digest of the record. 
 *	@param rec 			The record containing the data to be written.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_put(
	aerospike * as, as_error * err, const as_policy_write * policy, 
	const as_digest * digest, as_record * rec) 
{
	as_error_reset(err);

	// if policy is NULL, then get default policy
	as_policy_write * p = (as_policy_write *) (policy ? policy : &as->config.policies.write);

	int         nvalues = rec->bins.size;
	cl_bin *    values = (cl_bin *) alloca(sizeof(cl_bin) * nvalues);

	cl_write_parameters wp;
	as_policy_write_towp(p, rec, &wp);

	as_record_tobins(rec, values, nvalues);

	cl_rv rc = citrusleaf_put_digest_with_setname(as->cluster, digest->key.namespace, digest->set, (cf_digest *) digest->value, values, nvalues, &wp);

	return as_error_fromrc(err,rc);  
}

/**
 *	Remove a record from the cluster using a digest.
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		if ( aerospike_key_remove(&as, &err, NULL, "test", &digest) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param digest		The digest of the record. 
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_remove(
	aerospike * as, as_error * err, const as_policy_operate * policy, 
	const as_digest * digest) 
{
	as_error_reset(err);

	// if policy is NULL, then get default policy
	as_policy_operate * p = (as_policy_operate *) (policy ? policy : &as->config.policies.operate);

	cl_write_parameters wp;
	as_policy_operate_towp(p, &wp);

	cl_rv rc = citrusleaf_delete_digest(as->cluster, digest->key.namespace, (cf_digest *) digest->value, &wp);

	return as_error_fromrc(err,rc);
}

/**
 *	Lookup a record by key, then perform specified operations.
 *	
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param digest		The digest of the record. 
 *	@param ops			The operations to perform on the record.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_operate(
	aerospike * as, as_error * err, const as_policy_operate * policy, 
	const as_digest * digest, const as_operations * ops) 
{
	return AEROSPIKE_OK;
}

/**
 *	Lookup a record by digest, then apply the UDF
 *
 *	~~~~~~~~~~{.c}
 *		as_digest digest;
 *		as_digest_init(&digest, "demo", "foo");
 *		
 *		as_list args;
 *		as_arraylist_init(&args, 2, 0);
 *		as_list_append_int64(&args, 1);
 *		as_list_append_int64(&args, 2);
 *		
 *		as_val * res = NULL;
 *		
 *		if ( aerospike_digest_apply(&as, &err, NULL, "test", &digest, "math", "add", &args, &res) != AEROSPIKE_OK ) {
 *			fprintf(stderr, "error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
 *		}
 *		else {
 *			as_val_destroy(res);
 *		}
 *		
 *		as_list_destroy(&args);
 *		as_digest_destroy(&digest);
 *	~~~~~~~~~~
 *
 *
 *	@param as			The aerospike instance to use for this operation.
 *	@param err			The as_error to be populated if an error occurs.
 *	@param policy		The policy to use for this operation. If NULL, then the default policy will be used.
 *	@param digest		The digest of the record. 
 *	@param module		The module containing the function to execute.
 *	@param function		The function to execute.
 *	@param arglist 		The arguments for the function.
 *	@param result 		The return value from the function.
 *
 *	@return AEROSPIKE_OK if successful. Otherwise an error.
 */
as_status aerospike_digest_apply(
	aerospike * as, as_error * err, const as_policy_read * policy, 
	const as_digest * digest, 
	const char * module, const char * function, const as_list * arglist, 
	as_val ** result) 
{
	as_error_reset(err);

	// if policy is NULL, then get default policy
	as_policy_read * p = (as_policy_read *) (policy ? policy : &as->config.policies.read);

	cl_rv rv = CITRUSLEAF_OK;

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
		digest->key.namespace, digest->key.set, 0, (cf_digest *) digest->value, &bins, CL_OP_WRITE, 0, &n_bins, 
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
