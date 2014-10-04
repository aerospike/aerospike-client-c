/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

#include <aerospike/as_msgpack.h>
#include <aerospike/as_serializer.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_types.h>
#include <citrusleaf/cf_log_internal.h>

#include <stdint.h>
#include <errno.h>

#include "_shim.h"

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

#define ERR_ASSIGN(__enum) strcpy(err->message, #__enum)

#define CASE_ASSIGN(__enum) \
	case __enum : \
		strcpy(err->message, #__enum); \
		break; \

as_status as_error_fromrc(as_error * err, cl_rv rc) 
{
	err->code = (as_status)rc;
	
	switch (err->code) {
		case AEROSPIKE_OK:
		case AEROSPIKE_QUERY_END:
			break;
			
		CASE_ASSIGN(AEROSPIKE_ERR_PARAM);
		CASE_ASSIGN(AEROSPIKE_ERR_CLIENT);
		CASE_ASSIGN(AEROSPIKE_ERR_SERVER);
		CASE_ASSIGN(AEROSPIKE_ERR_RECORD_NOT_FOUND);
		CASE_ASSIGN(AEROSPIKE_ERR_RECORD_GENERATION);
		CASE_ASSIGN(AEROSPIKE_ERR_REQUEST_INVALID);
		CASE_ASSIGN(AEROSPIKE_ERR_RECORD_EXISTS);
		CASE_ASSIGN(AEROSPIKE_ERR_BIN_EXISTS);
		CASE_ASSIGN(AEROSPIKE_ERR_CLUSTER_CHANGE);
		CASE_ASSIGN(AEROSPIKE_ERR_SERVER_FULL);
		CASE_ASSIGN(AEROSPIKE_ERR_TIMEOUT);
		CASE_ASSIGN(AEROSPIKE_ERR_NO_XDR);
		CASE_ASSIGN(AEROSPIKE_ERR_CLUSTER);
		CASE_ASSIGN(AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE);
		CASE_ASSIGN(AEROSPIKE_ERR_RECORD_TOO_BIG);
		CASE_ASSIGN(AEROSPIKE_ERR_RECORD_BUSY);
		CASE_ASSIGN(AEROSPIKE_ERR_SCAN_ABORTED);
		CASE_ASSIGN(AEROSPIKE_ERR_UNSUPPORTED_FEATURE);
		CASE_ASSIGN(AEROSPIKE_ERR_BIN_NOT_FOUND);
		CASE_ASSIGN(AEROSPIKE_ERR_DEVICE_OVERLOAD);
		CASE_ASSIGN(AEROSPIKE_ERR_RECORD_KEY_MISMATCH);
		CASE_ASSIGN(AEROSPIKE_ERR_NAMESPACE_NOT_FOUND);
		CASE_ASSIGN(AEROSPIKE_ERR_BIN_NAME);
		CASE_ASSIGN(AEROSPIKE_SECURITY_NOT_SUPPORTED);
		CASE_ASSIGN(AEROSPIKE_SECURITY_NOT_ENABLED);
		CASE_ASSIGN(AEROSPIKE_SECURITY_SCHEME_NOT_SUPPORTED);
		CASE_ASSIGN(AEROSPIKE_INVALID_COMMAND);
		CASE_ASSIGN(AEROSPIKE_INVALID_FIELD);
		CASE_ASSIGN(AEROSPIKE_ILLEGAL_STATE);
		CASE_ASSIGN(AEROSPIKE_INVALID_USER);
		CASE_ASSIGN(AEROSPIKE_USER_ALREADY_EXISTS);
		CASE_ASSIGN(AEROSPIKE_INVALID_PASSWORD);
		CASE_ASSIGN(AEROSPIKE_EXPIRED_PASSWORD);
		CASE_ASSIGN(AEROSPIKE_FORBIDDEN_PASSWORD);
		CASE_ASSIGN(AEROSPIKE_INVALID_CREDENTIAL);
		CASE_ASSIGN(AEROSPIKE_INVALID_ROLE);
		CASE_ASSIGN(AEROSPIKE_INVALID_PRIVILEGE);
		CASE_ASSIGN(AEROSPIKE_NOT_AUTHENTICATED);
		CASE_ASSIGN(AEROSPIKE_ROLE_VIOLATION);
		CASE_ASSIGN(AEROSPIKE_ERR_UDF);
		CASE_ASSIGN(AEROSPIKE_ERR_LARGE_ITEM_NOT_FOUND);
		CASE_ASSIGN(AEROSPIKE_ERR_INDEX_FOUND);
		CASE_ASSIGN(AEROSPIKE_ERR_INDEX_NOT_FOUND);
		CASE_ASSIGN(AEROSPIKE_ERR_INDEX_OOM);
		CASE_ASSIGN(AEROSPIKE_ERR_INDEX_NOT_READABLE);
		CASE_ASSIGN(AEROSPIKE_ERR_INDEX);
		CASE_ASSIGN(AEROSPIKE_ERR_INDEX_NAME_MAXLEN);
		CASE_ASSIGN(AEROSPIKE_ERR_INDEX_MAXCOUNT);
		CASE_ASSIGN(AEROSPIKE_ERR_QUERY_ABORTED);
		CASE_ASSIGN(AEROSPIKE_ERR_QUERY_QUEUE_FULL);
		CASE_ASSIGN(AEROSPIKE_ERR_QUERY_TIMEOUT);
		CASE_ASSIGN(AEROSPIKE_ERR_QUERY);
			
		default:
			if (err->code < 0) {
				ERR_ASSIGN(AEROSPIKE_ERR_CLIENT);
			}
			else {
				ERR_ASSIGN(AEROSPIKE_ERR_SERVER);
			}
			break;
	}
	return err->code;
}

void asval_to_clobject(as_val * val, cl_object * obj)
{
	switch(val->type) {
		case AS_NIL: {
			citrusleaf_object_init_null(obj);
			break;
		}
		case AS_INTEGER: {
			as_integer * v = as_integer_fromval(val);
			citrusleaf_object_init_int(obj, as_integer_toint(v));
			break;
		}
		case AS_STRING: {
			as_string * v = as_string_fromval(val);
			citrusleaf_object_init_str(obj, as_string_get(v));
			break;
		}
		case AS_BYTES: {
			as_bytes * v = as_bytes_fromval(val);
			citrusleaf_object_init_blob2(obj, v->value, v->size, (cl_type)v->type);
			break;
		}
		case AS_LIST:{
			as_buffer buffer;
			as_buffer_init(&buffer);

			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_serialize(&ser, val, &buffer);
			as_serializer_destroy(&ser);
			
			citrusleaf_object_init_blob_handoff(obj, buffer.data, buffer.size, CL_LIST);
			break;
		}
		case AS_MAP: {
			as_buffer buffer;
			as_buffer_init(&buffer);

			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_serialize(&ser, val, &buffer);
			as_serializer_destroy(&ser);

			citrusleaf_object_init_blob_handoff(obj, buffer.data, buffer.size, CL_MAP);
			break;
		}
		default: {
			// raise an error
			break;
		}
	}
}

void asbinvalue_to_clobject(as_bin_value * binval, cl_object * obj)
{
	asval_to_clobject((as_val *) binval, obj);
}

void asbin_to_clbin(as_bin * as, cl_bin * cl) 
{
	strncpy(cl->bin_name, as->name, CL_BINNAME_SIZE - 1);
	cl->bin_name[CL_BINNAME_SIZE - 1] = '\0';
	asbinvalue_to_clobject(as->valuep, &cl->object);
}

void asrecord_to_clbins(as_record * rec, cl_bin * bins, uint32_t nbins) 
{
	as_bin * rbin = rec->bins.entries;
	for ( int i = 0; i < nbins; i++ ) {
		asbin_to_clbin(&rbin[i], &bins[i]);
	}
}


void askey_from_clkey(as_key * key, const as_namespace ns, const as_set set, cl_object * clkey)
{
	if (! (key && clkey)) {
		return;
	}

	switch (clkey->type) {
		case CL_NULL:
			as_key_init_value(key, ns, set, NULL);
			break;
		case CL_INT:
			as_key_init_int64(key, ns, set, clkey->u.i64);
			break;
		case CL_STR: {
			// Must null-terminate here.
			char* s = (char*)malloc(clkey->sz + 1);
			memcpy(s, clkey->u.str, clkey->sz);
			s[clkey->sz] = 0;
			as_key_init_strp(key, ns, set, s, true);
			break;
		}
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_ERLANG_BLOB: {
			// obj value points into recv buf - don't free it.
			as_key_init_raw(key, ns, set, (const uint8_t*)clkey->u.blob, (uint32_t)clkey->sz);
			break;
		}
		// Unsupported as key types for now:
		case CL_LIST:
		case CL_MAP:
		default:
			as_key_init_value(key, ns, set, NULL);
			break;
	}
}


void clbin_to_asval(cl_bin * bin, as_serializer * ser, as_val ** val) 
{
	if ( val == NULL ) return;

	switch( bin->object.type ) {
		case CL_NULL :{
			*val = (as_val *) &as_nil;
			break;
		}
		case CL_INT : {
			*val = (as_val *) as_integer_new(bin->object.u.i64);
			break;
		}
		case CL_STR : {
			// steal the pointer from the object into the val
			*val = (as_val *) as_string_new(strdup(bin->object.u.str), true /*ismalloc*/);
			// TODO: re-evaluate the follow zero-copy for strings from cl_bins
			// *val = (as_val *) as_string_new(bin->object.u.str, true /*ismalloc*/);
			// bin->object.free = NULL;
			break;
		}
		case CL_LIST :
		case CL_MAP : {
			// use a temporary buffer, which doesn't need to be destroyed
			as_buffer buf = {
				.capacity = (uint32_t) bin->object.sz,
				.size = (uint32_t) bin->object.sz,
				.data = (uint8_t *) bin->object.u.blob
			};
			// print_buffer(&buf);
			as_serializer_deserialize(ser, &buf, val);
			break;
		}
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_ERLANG_BLOB:
		default : {
			*val = NULL;
			uint8_t * raw = malloc(sizeof(bin->object.sz));
			memcpy(raw, bin->object.u.blob, bin->object.sz);
			as_bytes * b = as_bytes_new_wrap(raw, (uint32_t)bin->object.sz, true /*ismalloc*/);
			b->type = (as_bytes_type)bin->object.type;
			*val = (as_val *) b;
			break;
		}
	}
}


void clbin_to_asrecord(cl_bin * bin, as_record * r)
{
	switch(bin->object.type) {
		case CL_NULL: {
			as_record_set_nil(r, bin->bin_name);
			break;
		}
		case CL_INT: {
			as_record_set_int64(r, bin->bin_name, bin->object.u.i64);
			break;
		}
		case CL_STR: {
			as_record_set_strp(r, bin->bin_name, bin->object.u.str, true);
			// the following completes the handoff of the value.
			bin->object.free = NULL;
			break;
		}
		case CL_LIST:
		case CL_MAP: {

			as_val * val = NULL;

			as_buffer buffer;
			buffer.data = (uint8_t *) bin->object.u.blob;
			buffer.size = (uint32_t)bin->object.sz;

			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_deserialize(&ser, &buffer, &val);
			as_serializer_destroy(&ser);

			as_record_set(r, bin->bin_name, (as_bin_value *) val);
			break;
		}
		default: {
			as_record_set_raw_typep(r, bin->bin_name, bin->object.u.blob, (uint32_t)bin->object.sz, (as_bytes_type)bin->object.type, true);
			// the following completes the handoff of the value.
			bin->object.free = NULL;
			break;
		}
	}
}


void clbins_to_asrecord(cl_bin * bins, uint32_t nbins, as_record * r) 
{
	uint32_t n = nbins < r->bins.capacity ? nbins : r->bins.capacity;
	for ( int i = 0; i < n; i++ ) {
		clbin_to_asrecord(&bins[i], r);
	}
}


void aspolicywrite_to_clwriteparameters(const as_policy_write * policy, const as_record * rec, cl_write_parameters * wp) 
{
	if ( !policy || !rec || !wp ) {
		return;
	}

	wp->unique = policy->exists == AS_POLICY_EXISTS_CREATE;
	wp->unique_bin = false;
	wp->update_only = policy->exists == AS_POLICY_EXISTS_UPDATE;
	wp->create_or_replace = policy->exists == AS_POLICY_EXISTS_CREATE_OR_REPLACE;
	wp->replace_only = policy->exists == AS_POLICY_EXISTS_REPLACE;
	wp->bin_replace_only = false;

	wp->use_generation = false;
	wp->use_generation_gt = false;
	wp->use_generation_dup = false;
	
	wp->timeout_ms = policy->timeout == UINT32_MAX ? 0 : policy->timeout;
	wp->record_ttl = rec->ttl;

	switch(policy->gen) {
		case AS_POLICY_GEN_EQ:
			wp->generation = rec->gen;
			wp->use_generation = true;
			break;
		case AS_POLICY_GEN_GT:
			wp->generation = rec->gen;
			wp->use_generation_gt = true;
			break;
		case AS_POLICY_GEN_DUP:
			wp->generation = rec->gen;
			wp->use_generation_dup = true;
			break;
		default:
			break;
	}

	switch(policy->retry) {
		case AS_POLICY_RETRY_ONCE:
			wp->w_pol = CL_WRITE_RETRY;
			break;
		case AS_POLICY_RETRY_NONE:
		default:
			// default is to no retry
			wp->w_pol = CL_WRITE_ONESHOT;
			break;
	}
}

void aspolicyoperate_to_clwriteparameters(const as_policy_operate * policy, const as_operations * ops, cl_write_parameters * wp) 
{
	if ( !policy || !wp ) {
		return;
	}
	
	wp->unique = false;
	wp->unique_bin = false;
	wp->update_only = false;
	wp->create_or_replace = false;
	wp->replace_only = false;
	wp->bin_replace_only = false;

	wp->use_generation = false;
	wp->use_generation_gt = false;
	wp->use_generation_dup = false;
	
	wp->timeout_ms = policy->timeout == UINT32_MAX ? 0 : policy->timeout;
	wp->record_ttl = ops->ttl;

	switch(policy->gen) {
		case AS_POLICY_GEN_EQ:
			wp->generation = ops->gen;
			wp->use_generation = true;
			break;
		case AS_POLICY_GEN_GT:
			wp->generation = ops->gen;
			wp->use_generation_gt = true;
			break;
		case AS_POLICY_GEN_DUP:
			wp->generation = ops->gen;
			wp->use_generation_dup = true;
			break;
		default:
			break;
	}

	switch(policy->retry) {
		case AS_POLICY_RETRY_ONCE:
			wp->w_pol = CL_WRITE_RETRY;
			break;
		case AS_POLICY_RETRY_NONE:
		default:
			// default is to no retry
			wp->w_pol = CL_WRITE_ONESHOT;
			break;
	}
}

void aspolicyremove_to_clwriteparameters(const as_policy_remove * policy, cl_write_parameters * wp) 
{
	if ( !policy || !wp ) {
		return;
	}
	
	wp->unique = false;
	wp->unique_bin = false;
	wp->update_only = false;
	wp->create_or_replace = false;
	wp->replace_only = false;
	wp->bin_replace_only = false;

	wp->use_generation = false;
	wp->use_generation_gt = false;
	wp->use_generation_dup = false;
	
	wp->timeout_ms = policy->timeout == UINT32_MAX ? 0 : policy->timeout;
	wp->record_ttl = 0;

	switch(policy->gen) {
		case AS_POLICY_GEN_EQ:
			wp->generation = policy->generation;
			wp->use_generation = true;
			break;
		case AS_POLICY_GEN_GT:
			wp->generation = policy->generation;
			wp->use_generation_gt = true;
			break;
		case AS_POLICY_GEN_DUP:
			wp->generation = policy->generation;
			wp->use_generation_dup = true;
			break;
		default:
			break;
	}

	switch(policy->retry) {
		case AS_POLICY_RETRY_ONCE:
			wp->w_pol = CL_WRITE_RETRY;
			break;
		case AS_POLICY_RETRY_NONE:
		default:
			// default is to no retry
			wp->w_pol = CL_WRITE_ONESHOT;
			break;
	}
}
