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
#include <aerospike/as_key.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_command.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_string.h>
#include <aerospike/as_bytes.h>

#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_digest.h>

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 *	INLINE FUNCTIONS
 *****************************************************************************/

extern inline as_key * as_key_init_str(as_key * key, const as_namespace ns, const as_set set, const char * value);
extern inline as_key * as_key_init_raw(as_key * key, const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size);
extern inline as_key * as_key_new_str(const as_namespace ns, const as_set set, const char * value);
extern inline as_key * as_key_new_raw(const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size);

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static as_key * as_key_cons(as_key * key, bool free, const as_namespace ns, const char * set, const as_key_value * valuep, const as_digest_value digest)
{
	if ( ! set ) {
		set = "";
	}

	if ( ! (ns && *ns != '\0' && strlen(ns) < AS_NAMESPACE_MAX_SIZE && strlen(set) < AS_SET_MAX_SIZE) ) {
		return NULL;
	}

	key->_free = free;
	strcpy(key->ns, ns);
	strcpy(key->set, set);
	key->valuep = (as_key_value *) valuep;
	
	if ( digest == NULL ) {
		key->digest.init = false;
		memset(key->digest.value, 0, AS_DIGEST_VALUE_SIZE);	
	}
	else {
		key->digest.init = true;
		memcpy(key->digest.value, digest, AS_DIGEST_VALUE_SIZE);
	}
	return key;
}

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_init(as_key * key, const as_namespace ns, const as_set set, const char * value)
{
	return as_key_init_str(key, ns, set, value);
}

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 */
as_key * as_key_init_int64(as_key * key, const as_namespace ns, const as_set set, int64_t value)
{
	if ( !key ) return key;

	as_integer_init((as_integer *) &key->value, value);
	return as_key_cons(key, false, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_init_strp(as_key * key, const as_namespace ns, const as_set set, const char * value, bool free)
{
	if ( !key ) return key;
	
	as_string_init((as_string *) &key->value, (char *) value, free);
	return as_key_cons(key, false, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 */
as_key * as_key_init_rawp(as_key * key, const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size, bool free)
{
	if ( !key ) return key;
	
	as_bytes_init_wrap((as_bytes *) &key->value, (uint8_t *) value, size, free);
	return as_key_cons(key, false, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated as_key with a digest.
 */
as_key * as_key_init_digest(as_key * key, const as_namespace ns, const as_set set, const as_digest_value digest)
{
	if ( !key ) return key;

	return as_key_cons(key, false, ns, set, NULL, digest);
}

/**
 *	Initialize a stack allocated `as_key` with a digest value.
 */
as_key * as_key_init_value(as_key * key, const as_namespace ns, const as_set set, const as_key_value * value)
{
	if ( !key ) return key;

	return as_key_cons(key, false, ns, set, value, NULL);
}

/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_new(const as_namespace ns, const as_set set, const char * value)
{
	return as_key_new_str(ns, set, value);
}

/**
 *	Initialize a stack allocated `as_key` to a int64_t value.
 */
as_key * as_key_new_int64(const as_namespace ns, const as_set set, int64_t value)
{
	as_key * key = (as_key *) cf_malloc(sizeof(as_key));
	if ( !key ) return key;

	as_integer_init((as_integer *) &key->value, value);
	return as_key_cons(key, true, ns, set, &key->value, NULL);
}


/**
 *	Creates and initializes a heap allocated `as_key` to a NULL-terminated string value.
 */
as_key * as_key_new_strp(const as_namespace ns, const as_set set, const char * value, bool free)
{
	as_key * key = (as_key *) cf_malloc(sizeof(as_key));
	if ( !key ) return key;
	
	as_string_init((as_string *) &key->value, (char *) value, free);
	return as_key_cons(key, true, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated `as_key` to a raw bytes value.
 */
as_key * as_key_new_rawp(const as_namespace ns, const as_set set, const uint8_t * value, uint32_t size, bool free)
{
	as_key * key = (as_key *) cf_malloc(sizeof(as_key));
	if ( !key ) return key;
	
	as_bytes_init_wrap((as_bytes *) &key->value, (uint8_t *) value, size, free);
	return as_key_cons(key, true, ns, set, &key->value, NULL);
}

/**
 *	Initialize a stack allocated as_key with a digest value.
 */
as_key * as_key_new_digest(const as_namespace ns, const as_set set, const as_digest_value digest)
{
	as_key * key = (as_key *) cf_malloc(sizeof(as_key));
	if ( !key ) return key;

	return as_key_cons(key, true, ns, set, NULL, digest);
}

/**
 *	Initialize a stack allocated `as_key` to a an `as_key_value`.
 */
as_key * as_key_new_value(const as_namespace ns, const as_set set, const as_key_value * value)
{
	as_key * key = (as_key *) cf_malloc(sizeof(as_key));
	if ( !key ) return key;

	return as_key_cons(key, true, ns, set, value, NULL);
}

/**
 *	Destory the `as_key`, releasing resources.
 */
void as_key_destroy(as_key * key)
{
	if ( !key ) return;

	as_val_destroy((as_val *) key->valuep);

	if ( key->_free ) {
		cf_free(key);
	}
}

/**
 *	Compute the digest value.
 */
as_digest * as_key_digest(as_key * key)
{
	as_error err;
	as_status status = as_key_set_digest(&err, key);
	
	if (status == AEROSPIKE_OK) {
		return &key->digest;
	}
	else {
		as_log_error(err.message);
		return NULL;
	}
}

as_status
as_key_set_digest(as_error* err, as_key* key)
{
	if (key->digest.init) {
		return AEROSPIKE_OK;
	}
	
	size_t set_len = strlen(key->set);
	size_t size;
	
	as_val* val = (as_val*)key->valuep;
	uint8_t* buf;
	
	switch (val->type) {
		case AS_INTEGER: {
			as_integer* v = as_integer_fromval(val);
			size = 9;
			buf = alloca(size);
			buf[0] = AS_BYTES_INTEGER;
			*(uint64_t*)&buf[1] = cf_swap_to_be64(v->value);
			break;
		}
		case AS_DOUBLE: {
			as_double* v = as_double_fromval(val);
			size = 9;
			buf = alloca(size);
			buf[0] = AS_BYTES_DOUBLE;
			*(double*)&buf[1] = cf_swap_to_big_float64(v->value);
			break;
		}
		case AS_STRING: {
			as_string* v = as_string_fromval(val);
			size_t len = as_string_len(v);
			size = len + 1;
			buf = alloca(size);
			buf[0] = AS_BYTES_STRING;
			memcpy(&buf[1], v->value, len);
			break;
		}
		case AS_BYTES: {
			as_bytes* v = as_bytes_fromval(val);
			size = v->size + 1;
			buf = alloca(size);
			// Note: v->type must be a blob type (AS_BYTES_BLOB, AS_BYTES_JAVA, AS_BYTES_PYTHON ...).
			// Otherwise, the particle type will be reassigned to a non-blob which causes a
			// mismatch between type and value.
			buf[0] = v->type;
			memcpy(&buf[1], v->value, v->size);
			break;
		}
		default: {
			return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid key type: %d", val->type);
		}
	}
		
	cf_digest_compute2(key->set, set_len, buf, size, (cf_digest*)key->digest.value);
	key->digest.init = true;
	return AEROSPIKE_OK;
}
