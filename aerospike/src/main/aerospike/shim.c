#include "shim.h"

#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
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

as_status as_error_fromrc(as_error * err, cl_rv rc) 
{
	err->code = rc ? AEROSPIKE_ERR : AEROSPIKE_OK;
	return err->code;
}

void as_record_tobins(as_record * rec, cl_bin * bins, uint32_t nbins) 
{
	as_bin * rbin = rec->bins.entries;
	for ( int i = 0; i < nbins; i++ ) {

		strncpy(bins[i].bin_name, rbin[i].name, AS_BIN_NAME_LEN);
		bins[i].bin_name[AS_BIN_NAME_LEN - 1] = '\0';

		as_val * val = (as_val *) rbin[i].value;
		switch(val->type) {
			case AS_NIL: {
				citrusleaf_object_init_null(&bins[i].object);
				break;
			}
			case AS_INTEGER: {
				as_integer * v = as_integer_fromval(val);
				citrusleaf_object_init_int(&bins[i].object, as_integer_toint(v));
				break;
			}
			case AS_STRING: {
				as_string * v = as_string_fromval(val);
				citrusleaf_object_init_str(&bins[i].object, as_string_tostring(v));
				break;
			}
			case AS_LIST:{
				as_buffer buffer;
				as_buffer_init(&buffer);

				as_serializer ser;
				as_msgpack_init(&ser);
				as_serializer_serialize(&ser, val, &buffer);
				as_serializer_destroy(&ser);
				
				citrusleaf_object_init_blob2(&bins[i].object, buffer.data, buffer.size, CL_LIST);
				break;
			}
			case AS_MAP: {
				as_buffer buffer;
				as_buffer_init(&buffer);

				as_serializer ser;
				as_msgpack_init(&ser);
				as_serializer_serialize(&ser, val, &buffer);
				as_serializer_destroy(&ser);

				citrusleaf_object_init_blob2(&bins[i].object, buffer.data, buffer.size, CL_MAP);
				break;
			}
			case AS_BYTES: {
				as_bytes * v = as_bytes_fromval(val);
				citrusleaf_object_init_blob2(&bins[i].object, v->value, v->len, v->type);
				break;
			}
			default: {
				// raise an error
				break;
			}
		}
	}
}


as_record * as_record_frombins(as_record * r, cl_bin * bins, uint32_t nbins) 
{
	uint32_t n = nbins < r->bins.capacity ? nbins : r->bins.capacity;
	for ( int i = 0; i < n; i++ ) {
		switch(bins[i].object.type) {
			case CL_NULL: {
				as_record_set_nil(r, bins[i].bin_name);
				break;
			}
			case CL_INT: {
				as_record_set_int64(r, bins[i].bin_name, bins[i].object.u.i64);
				break;
			}
			case CL_STR: {
				as_record_set_str(r, bins[i].bin_name, bins[i].object.u.str);
				break;
			}
			case CL_LIST:
			case CL_MAP: {

				as_val * val = NULL;

				as_buffer buffer;
				buffer.data = (uint8_t *) bins[i].object.u.blob;
				buffer.size = bins[i].object.sz;

				as_serializer ser;
				as_msgpack_init(&ser);
				as_serializer_deserialize(&ser, &buffer, &val);
				as_serializer_destroy(&ser);

				as_record_set(r, bins[i].bin_name, (as_bin_value *) val);
				break;
			}
			default: {
				as_bytes * b = as_bytes_empty_new(bins[i].object.sz);
				as_bytes_append(b, (uint8_t *) bins[i].object.u.blob, bins[i].object.sz);
				as_record_set_bytes(r, bins[i].bin_name, b);
				break;
			}
		}
	}

	return r;
}


as_val * as_val_frombin(as_serializer * ser, cl_bin * bin) 
{
	as_val * val;
	
	switch( bin->object.type ) {
		case CL_NULL :{
			val = NULL;
			break;
		}
		case CL_INT : {
			val = (as_val *) as_integer_new(bin->object.u.i64);
			break;
		}
		case CL_STR : {
			// steal the pointer from the object into the val
			val = (as_val *) as_string_new(strdup(bin->object.u.str), true /*ismalloc*/);
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
			as_serializer_deserialize(ser, &buf, &val);
			break;
		}
		case CL_BLOB:
		case CL_JAVA_BLOB:
		case CL_CSHARP_BLOB:
		case CL_PYTHON_BLOB:
		case CL_RUBY_BLOB:
		case CL_ERLANG_BLOB:
		default : {
			val = NULL;
			uint8_t * raw = malloc(sizeof(bin->object.sz));
			memcpy(raw, bin->object.u.blob, bin->object.sz);
			as_bytes * b = as_bytes_new(raw, bin->object.sz, true /*ismalloc*/);
			b->type = bin->object.type;
			val = (as_val *) b;
			break;
		}
	}

	return val;
}

void as_policy_write_towp(as_policy_write * policy, as_record * rec, cl_write_parameters * wp) 
{
	if ( !policy || !rec || !wp ) {
		return;
	}

	wp->unique = policy->digest == AS_POLICY_DIGEST_CREATE;
	wp->unique_bin = false;

	wp->use_generation = false;
	wp->use_generation_gt = false;
	wp->use_generation_dup = false;
	
	wp->timeout_ms = policy->timeout;
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

	switch(policy->mode) {
		case AS_POLICY_WRITEMODE_ASYNC:
			wp->w_pol = CL_WRITE_ASYNC;
			break;
		case AS_POLICY_WRITEMODE_ONESHOT:
			wp->w_pol = CL_WRITE_ONESHOT;
			break;
		default:
			wp->w_pol = CL_WRITE_RETRY;
			break;
	}
}

void as_policy_remove_towp(as_policy_remove * policy, cl_write_parameters * wp) 
{
	if ( !policy || !wp ) {
		return;
	}
	
	wp->unique = false;
	wp->unique_bin = false;

	wp->use_generation = false;
	wp->use_generation_gt = false;
	wp->use_generation_dup = false;
	
	wp->timeout_ms = policy->timeout;
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

	switch(policy->mode) {
		case AS_POLICY_WRITEMODE_ASYNC:
			wp->w_pol = CL_WRITE_ASYNC;
			break;
		case AS_POLICY_WRITEMODE_ONESHOT:
			wp->w_pol = CL_WRITE_ONESHOT;
			break;
		default:
			wp->w_pol = CL_WRITE_RETRY;
			break;
	}
}