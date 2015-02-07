/*
 * Copyright 2008-2015 Aerospike, Inc.
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
#include <aerospike/as_command.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/cf_clock.h>
#include <string.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static size_t
as_command_user_key_size(const as_key* key)
{
	size_t size = AS_FIELD_HEADER_SIZE + 1;  // Add 1 for key's value type.
	as_val* val = (as_val*)key->valuep;
	
	// Key must not be list or map.
	switch (val->type) {
		case AS_NIL: {
			break;
		}
		case AS_INTEGER: {
			size += 8;
			break;
		}
		case AS_STRING: {
			as_string* v = as_string_fromval(val);
			// v->len should have been already set when calculating the digest.
			size += v->len;
			break;
		}
		case AS_BYTES: {
			as_bytes* v = as_bytes_fromval(val);
			size += v->size;
			break;
		}
		default: {
			break;
		}
	}
	return size;
}

size_t
as_command_key_size(as_policy_key policy, const as_key* key, uint16_t* n_fields)
{
	*n_fields = 3;
	size_t size = strlen(key->ns) + strlen(key->set) + sizeof(cf_digest) + 45;
	
	if (policy == AS_POLICY_KEY_SEND) {
		size += as_command_user_key_size(key);
		(*n_fields)++;
	}
	return size;
}

size_t
as_command_value_size(as_val* val, as_buffer* buffer)
{
	switch (val->type) {
		case AS_NIL: {
			return 0;
		}
		case AS_INTEGER: {
			return 8;
		}
		case AS_STRING: {
			as_string* v = as_string_fromval(val);
			return as_string_len(v);
		}
		case AS_BYTES: {
			as_bytes* v = as_bytes_fromval(val);
			return v->size;
		}
		case AS_LIST:
		case AS_MAP: {
			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_serialize(&ser, val, buffer);
			as_serializer_destroy(&ser);
			return buffer->size;
		}
		default: {
			return 0;
		}
	}
}

uint8_t*
as_command_write_header(uint8_t* cmd, uint8_t read_attr, uint8_t write_attr,
	as_policy_commit_level commit_level, as_policy_consistency_level consistency,
	as_policy_exists exists, as_policy_gen gen_policy, uint32_t gen, uint32_t ttl,
	uint32_t timeout_ms, uint16_t n_fields, uint16_t n_bins)
{
	uint32_t generation = 0;
	uint8_t info_attr = 0;

	switch (exists) {
		case AS_POLICY_EXISTS_IGNORE:
			break;
			
		case AS_POLICY_EXISTS_UPDATE:
			info_attr |= AS_MSG_INFO3_UPDATE_ONLY;
			break;
			
		case AS_POLICY_EXISTS_CREATE_OR_REPLACE:
			info_attr |= AS_MSG_INFO3_CREATE_OR_REPLACE;
			break;
			
		case AS_POLICY_EXISTS_REPLACE:
			info_attr |= AS_MSG_INFO3_REPLACE_ONLY;
			break;
			
		case AS_POLICY_EXISTS_CREATE:
			write_attr |= AS_MSG_INFO2_CREATE_ONLY;
			break;
	}

	switch (gen_policy) {
		case AS_POLICY_GEN_IGNORE:
			break;
			
		case AS_POLICY_GEN_EQ:
			generation = gen;
			write_attr |= AS_MSG_INFO2_GENERATION;
			break;
			
		case AS_POLICY_GEN_GT:
			generation = gen;
			write_attr |= AS_MSG_INFO2_GENERATION_GT;
			break;
			
		default:
			break;
	}

	if (commit_level == AS_POLICY_COMMIT_LEVEL_MASTER) {
		info_attr |= AS_MSG_INFO3_COMMIT_MASTER;
	}

	if (consistency == AS_POLICY_CONSISTENCY_LEVEL_ALL) {
		read_attr |= AS_MSG_INFO1_CONSISTENCY_ALL;
	}

	cmd[8] = 22;
	cmd[9] = read_attr;
	cmd[10] = write_attr;
	cmd[11] = info_attr;
	*(uint16_t*)&cmd[12] = 0;
	*(uint32_t*)&cmd[14] = cf_swap_to_be32(generation);
	*(uint32_t*)&cmd[18] = cf_swap_to_be32(ttl);
	*(uint32_t*)&cmd[22] = cf_swap_to_be32(timeout_ms);
	*(uint16_t*)&cmd[26] = cf_swap_to_be16(n_fields);
	*(uint16_t*)&cmd[28] = cf_swap_to_be16(n_bins);
	return cmd + AS_HEADER_SIZE;
}

static uint8_t*
as_command_write_user_key(uint8_t* begin, const as_key* key)
{
	uint8_t* p = begin + AS_FIELD_HEADER_SIZE;
	as_val* val = (as_val*)key->valuep;
	uint32_t len;
	
	// Key must not be list or map.
	switch (val->type) {
		default:
		case AS_NIL: {
			*p++ = AS_BYTES_UNDEF;
			len = 0;
			break;
		}
		case AS_INTEGER: {
			as_integer* v = as_integer_fromval(val);
			*p++ = AS_BYTES_INTEGER;
			*(uint64_t*)p = cf_swap_to_be64(v->value);
			p += 8;
			len = 8;
			break;
		}
		case AS_STRING: {
			as_string* v = as_string_fromval(val);
			*p++ = AS_BYTES_STRING;
			// v->len should have been already set when calculating the digest.
			memcpy(p, v->value, v->len);
			p += v->len;
			len = (uint32_t)v->len;
			break;
		}
		case AS_BYTES: {
			as_bytes* v = as_bytes_fromval(val);
			// Note: v->type must be a blob type (AS_BYTES_BLOB, AS_BYTES_JAVA, AS_BYTES_PYTHON ...).
			// Otherwise, the particle type will be reassigned to a non-blob which causes a
			// mismatch between type and value.
			*p++ = v->type;
			memcpy(p, v->value, v->size);
			p += v->size;
			len = v->size;
			break;
		}
	}
	as_command_write_field_header(begin, AS_FIELD_KEY, ++len);
	return p;
}

uint8_t*
as_command_write_key(uint8_t* p, as_policy_key policy, const as_key* key)
{
	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);
	p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
	p = as_command_write_field_digest(p, &key->digest);
	
	if (policy == AS_POLICY_KEY_SEND) {
		p = as_command_write_user_key(p, key);
	}
	return p;
}

uint8_t*
as_command_write_bin(uint8_t* begin, uint8_t operation_type, const as_bin* bin, as_buffer* buffer)
{
	uint8_t* p = begin + AS_OPERATION_HEADER_SIZE;
	const char* name = bin->name;

	// Copy string, but do not transfer null byte.
	while (*name) {
		*p++ = *name++;
	}
	uint8_t name_len = p - begin - AS_OPERATION_HEADER_SIZE;
	as_val* val = (as_val*)bin->valuep;
	uint32_t val_len;
	uint8_t val_type;
	
	switch (val->type) {
		default:
		case AS_NIL: {
			val_len = 0;
			val_type = AS_BYTES_UNDEF;
			break;
		}
		case AS_INTEGER: {
			as_integer* v = as_integer_fromval(val);
			*(uint64_t*)p = cf_swap_to_be64(v->value);
			p += 8;
			val_len = 8;
			val_type = AS_BYTES_INTEGER;
			break;
		}
		case AS_STRING: {
			as_string* v = as_string_fromval(val);
			// v->len should have been already set by as_command_value_size().
			memcpy(p, v->value, v->len);
			p += v->len;
			val_len = (uint32_t)v->len;
			val_type = AS_BYTES_STRING;
			break;
		}
		case AS_BYTES: {
			as_bytes* v = as_bytes_fromval(val);
			memcpy(p, v->value, v->size);
			p += v->size;
			val_len = v->size;
			// Note: v->type must be a blob type (AS_BYTES_BLOB, AS_BYTES_JAVA, AS_BYTES_PYTHON ...).
			// Otherwise, the particle type will be reassigned to a non-blob which causes a
			// mismatch between type and value.
			val_type = v->type;
			break;
		}
		case AS_LIST: {
			memcpy(p, buffer->data, buffer->size);
			p += buffer->size;
			val_len = buffer->size;
			val_type = AS_BYTES_LIST;
			break;
		}
		case AS_MAP: {
			memcpy(p, buffer->data, buffer->size);
			p += buffer->size;
			val_len = buffer->size;
			val_type = AS_BYTES_MAP;
			break;
		}
	}
	*(uint32_t*)begin = cf_swap_to_be32(name_len + val_len + 4);
	begin += 4;
	*begin++ = operation_type;
	*begin++ = val_type;
	*begin++ = 0;
	*begin++ = name_len;
	return p;
}

as_status
as_command_execute(as_error * err, as_command_node* cn, uint8_t* command, size_t command_len,
	uint32_t timeout_ms, as_policy_retry retry,
	as_parse_results_fn parse_results_fn, void* parse_results_data
)
{
	uint64_t deadline_ms = as_socket_deadline(timeout_ms);
	uint32_t max_retries = retry + 1;
	uint32_t sleep_between_retries_ms = 0;
	uint32_t failed_nodes = 0;
	uint32_t failed_conns = 0;
	uint32_t iterations = 0;
	bool release_node;

	// Execute command until successful, timed out or maximum iterations have been reached.
	while (true) {
		as_node* node;
		
		if (cn->node) {
			node = cn->node;
			release_node = false;
		}
		else {
			node = as_node_get(cn->cluster, cn->ns, cn->digest, cn->write, cn->replica);
			release_node = true;
		}
		
		if (!node) {
			failed_nodes++;
			sleep_between_retries_ms = 10;
			goto Retry;
		}
		
		int fd;
		as_status status = as_node_get_connection(node, &fd);
		
		if (status) {
			if (release_node) {
				as_node_release(node);
			}
			failed_conns++;
			sleep_between_retries_ms = 1;
			goto Retry;
		}
		
		// Send command.
		status = as_socket_write_deadline(err, fd, command, command_len, deadline_ms);
		
		if (status) {
			// Socket errors are considered temporary anomalies.  Retry.
			// Close socket to flush out possible garbage.  Do not put back in pool.
			as_close(fd);
			if (release_node) {
				as_node_release(node);
			}
			sleep_between_retries_ms = 0;
			goto Retry;
		}
		
		// Parse results returned by server.
		status = parse_results_fn(err, fd, deadline_ms, parse_results_data);
		
		if (status) {
			switch (status) {
				// Retry on timeout.
				case AEROSPIKE_ERR_TIMEOUT:
					as_close(fd);
					if (release_node) {
						as_node_release(node);
					}
					sleep_between_retries_ms = 0;
					goto Retry;
				
				// Close socket on errors that can leave unread data in socket.
				case AEROSPIKE_ERR_QUERY_ABORTED:
				case AEROSPIKE_ERR_SCAN_ABORTED:
				case AEROSPIKE_ERR_CLIENT:
					as_close(fd);
					if (release_node) {
						as_node_release(node);
					}
					err->code = status;
					return status;
				
				default:
					err->code = status;
					break;
			}
		}
		
		// Put connection back in pool.
		as_node_put_connection(node, fd);
		
		// Release resources.
		if (release_node) {
			as_node_release(node);
		}
		return status;

Retry:
		// Check if max retries reached.
		if (++iterations > max_retries) {
			break;
		}
		
		// Check for client timeout.
		if (deadline_ms > 0) {
			int remaining_ms = (int)(deadline_ms - cf_getms() - sleep_between_retries_ms);
			
			if (remaining_ms <= 0) {
				break;
			}
			
			// Reset timeout in send buffer (destined for server).
			*(uint32_t*)(command + 22) = cf_swap_to_be32(remaining_ms);
		}
		
		if (sleep_between_retries_ms > 0) {
			// Sleep before trying again.
			usleep(sleep_between_retries_ms * 1000);
		}
	}
	
	return as_error_update(err, AEROSPIKE_ERR_TIMEOUT,
		"Client timeout: timeout=%d iterations=%u failedNodes=%u failedConns=%u",
		timeout_ms, iterations, failed_nodes, failed_conns);
}

as_status
as_command_parse_header(as_error* err, int fd, uint64_t deadline_ms, void* user_data)
{
	// Read header
	as_proto_msg* msg = user_data;
	as_status status = as_socket_read_deadline(err, fd, (uint8_t*)msg, sizeof(as_proto_msg), deadline_ms);
	
	if (status) {
		return status;
	}
	
	// Ensure that there is no data left to read.
	as_proto_swap_from_be(&msg->proto);
	size_t size = msg->proto.sz  - msg->m.header_sz;
	
	if (size > 0) {
		as_log_warn("Unexpected data received from socket after a write: fd=%d size=%zu", fd, size);
		
		// Verify size is not corrupted.
		if (size > 100000) {
			// The socket will be closed on this error, so we don't have to worry about emptying it.
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Unexpected data received from socket after a write: fd=%d size=%zu", fd, size);
		}
		
		// Empty socket.
		uint8_t* buf = cf_malloc(size);
		status = as_socket_read_deadline(err, fd, buf, size, deadline_ms);
		cf_free(buf);
		
		if (status) {
			return status;
		}
	}
	
	if (msg->m.result_code && msg->m.result_code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		return as_error_set_message(err, msg->m.result_code, as_error_string(msg->m.result_code));
	}
	return msg->m.result_code;
}

static int
as_command_bytes_to_int(uint8_t	*buf, int sz, int64_t *value)
{
	if (sz == 8) {
		// No need to worry about sign extension
		*value = cf_swap_from_be64(*(uint64_t *)buf);
		return 0;
	}
	
	// The server always returns sz == 8, so the rest of this function is just for legacy reasons.
	if (sz == 0) {
		*value = 0;
		return 0;
	}
	
	if (sz > 8)	{
		return -1;
	}
	
	if (sz == 1 && *buf < 0x7f) {
		*value = *buf;
		return 0;
	}
	
	// Negative numbers must be sign extended
	if (*buf & 0x80) {
		uint8_t	lg_buf[8];
		int i;
		for (i = 0; i < 8 - sz; i++) {
			lg_buf[i]=0xff;
		}
		memcpy(&lg_buf[i], buf, sz);
		*value = cf_swap_from_be64(*(uint64_t *)buf);
		return 0;
	}
	// Positive numbers don't.
	else {
		int64_t	v = 0;
		for (int i = 0; i < sz; i++, buf++) {
			v <<= 8;
			v |= *buf;
		}
		*value = v;
		return 0;
	}
	return 0;
}

uint8_t*
as_command_ignore_fields(uint8_t* p, uint32_t n_fields)
{
	if (n_fields > 0) {
		for (uint32_t i = 0; i < n_fields; i++) {
			p += cf_swap_from_be32(*(uint32_t*)p) + 4;
		}
	}
	return p;
}

uint8_t*
as_command_parse_key(uint8_t* p, uint32_t n_fields, as_key* key)
{
	uint32_t len;
	uint32_t size;
	
	for (uint32_t i = 0; i < n_fields; i++) {
		len = cf_swap_from_be32(*(uint32_t*)p) - 1;
		p += 4;
		
		switch (*p++) {
			case AS_FIELD_DIGEST:
				size = (len < AS_DIGEST_VALUE_SIZE) ? len : AS_DIGEST_VALUE_SIZE;
				memcpy(key->digest.value, p, size);
				break;
				
			case AS_FIELD_NAMESPACE:
				size = (len < (AS_NAMESPACE_MAX_SIZE-1)) ? len : (AS_NAMESPACE_MAX_SIZE-1);
				memcpy(key->ns, p, size);
				key->ns[size] = 0;
				break;
				
			case AS_FIELD_SETNAME:
				size = (len < (AS_SET_MAX_SIZE-1)) ? len : (AS_SET_MAX_SIZE-1);
				memcpy(key->set, p, size);
				key->set[size] = 0;
				break;
				
			case AS_FIELD_KEY:
				len--;
				uint8_t type = *p++;
				
				switch (type) {
					case AS_BYTES_INTEGER: {
						int64_t value;
						if (as_command_bytes_to_int(p, len, &value) == 0) {
							as_integer_init((as_integer*)&key->value, value);
							key->valuep = &key->value;
						}
						break;
					}
					case AS_BYTES_STRING: {
						char* value = malloc(len+1);
						memcpy(value, p, len);
						value[len] = 0;
						as_string_init_wlen((as_string*)&key->value, value, len, true);
						key->valuep = &key->value;
						break;
					}
					case AS_BYTES_BLOB: {
						void* value = malloc(len);
						memcpy(value, p, len);
						as_bytes_init_wrap((as_bytes*)&key->value, (uint8_t*)value, len, true);
						key->valuep = &key->value;
						break;
					}
					default: {
						as_log_error("Invalid key type: %d", type);
						break;
					}
				}
				break;
		}
		p += len;
	}
	return p;
}

static void
as_command_parse_value(uint8_t* p, uint8_t type, uint32_t value_size, as_val** value)
{
	// Allocate values on heap.
	switch (type) {
		case AS_BYTES_UNDEF: {
			*value = (as_val*)&as_nil;
			break;
		}
		case AS_BYTES_INTEGER: {
			int64_t v = 0;
			as_command_bytes_to_int(p, value_size, &v);
			*value = (as_val*)as_integer_new(v);
			break;
		}
		case AS_BYTES_STRING: {
			char* v = malloc(value_size + 1);
			memcpy(v, p, value_size);
			v[value_size] = 0;
			*value = (as_val*)as_string_new_wlen(v, value_size, true);
			break;
		}
		case AS_BYTES_LIST:
		case AS_BYTES_MAP: {
			as_buffer buffer;
			buffer.data = p;
			buffer.size = value_size;
			
			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_deserialize(&ser, &buffer, value);
			as_serializer_destroy(&ser);
			break;
		}
		default: {
			void* v = malloc(value_size);
			memcpy(v, p, value_size);
			*value = (as_val*)as_bytes_new_wrap(v, value_size, true);
			break;
		}
	}
}

uint8_t*
as_command_parse_success_failure_bins(uint8_t* p, as_error* err, as_msg* msg, as_val** value)
{
	p = as_command_ignore_fields(p, msg->n_fields);
		
	as_bin_name name;
	
	for (uint32_t i = 0; i < msg->n_ops; i++) {
		uint32_t op_size = cf_swap_from_be32(*(uint32_t*)p);
		p += 5;
		uint8_t type = *p;
		p += 2;
		
		uint8_t name_size = *p++;
		uint8_t name_len = (name_size <= AS_BIN_NAME_MAX_LEN)? name_size : AS_BIN_NAME_MAX_LEN;
		memcpy(name, p, name_len);
		name[name_len] = 0;
		p += name_size;
		
		uint32_t value_size = (op_size - (name_size + 4));

		if (strcmp(name, "SUCCESS") == 0) {
			if (value) {
				as_command_parse_value(p, type, value_size, value);
			}
			return p + value_size;
		}
		
		if (strcmp(name, "FAILURE") == 0) {
			as_val* val = 0;
			as_command_parse_value(p, type, value_size, &val);
			
			if (val == 0) {
				as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Received null FAILURE bin.");
			}
			else if (val->type == AS_STRING) {
				as_error_set_message(err, AEROSPIKE_ERR_CLIENT, ((as_string*)val)->value);
			}
			else {
				as_error_update(err, AEROSPIKE_ERR_CLIENT, "Expected string for FAILURE bin. Received %d", val->type);
			}
			as_val_destroy(val);
			return 0;
		}
		p += value_size;
	}
	as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find SUCCESS or FAILURE bin.");
	return 0;
}

static as_status
as_command_parse_udf_error(as_error* err, as_status status, as_val* val)
{
	if (val && val->type == AS_STRING) {
		char* begin = ((as_string*)val)->value;
		char* p = strrchr(begin, ':');
		
		if (p) {
			p = strrchr(++p, ':');
			
			if (p) {
				int code = atoi(++p);
				
				if (code > 0) {
					return as_error_set_message(err, code, begin);
				}
			}
		}
		return as_error_set_message(err, status, begin);
	}
	return as_error_set_message(err, status, as_error_string(status));
}

static as_status
as_command_parse_udf_failure(uint8_t* p, as_error* err, as_msg* msg, as_status status)
{
	p = as_command_ignore_fields(p, msg->n_fields);
	
	as_bin_name name;
	
	for (uint32_t i = 0; i < msg->n_ops; i++) {
		uint32_t op_size = cf_swap_from_be32(*(uint32_t*)p);
		p += 5;
		uint8_t type = *p;
		p += 2;
		
		uint8_t name_size = *p++;
		uint8_t name_len = (name_size <= AS_BIN_NAME_MAX_LEN)? name_size : AS_BIN_NAME_MAX_LEN;
		memcpy(name, p, name_len);
		name[name_len] = 0;
		p += name_size;
		
		uint32_t value_size = (op_size - (name_size + 4));
		
		if (strcmp(name, "FAILURE") == 0) {
			as_val* val = 0;
			as_command_parse_value(p, type, value_size, &val);
			status = as_command_parse_udf_error(err, status, val);
			as_val_destroy(val);
			return status;
		}
		p += value_size;
	}
	return as_error_set_message(err, status, as_error_string(status));
}

uint8_t*
as_command_parse_bins(as_record* rec, uint8_t* p, uint32_t n_bins, bool deserialize)
{
	as_bin* bin = rec->bins.entries;
	
	// Parse bins
	for (uint32_t i = 0; i < n_bins; i++, bin++) {
		uint32_t op_size = cf_swap_from_be32(*(uint32_t*)p);
		p += 5;
		uint8_t type = *p;
		p += 2;
		
		uint8_t name_size = *p++;
		uint8_t name_len = (name_size <= AS_BIN_NAME_MAX_LEN)? name_size : AS_BIN_NAME_MAX_LEN;
		memcpy(bin->name, p, name_len);
		bin->name[name_len] = 0;
		p += name_size;
		
		uint32_t value_size = (op_size - (name_size + 4));
		
		switch (type) {
			case AS_BYTES_UNDEF: {
				bin->valuep = (as_bin_value*)&as_nil;
				break;
			}
			case AS_BYTES_INTEGER: {
				int64_t value;
				if (as_command_bytes_to_int(p, value_size, &value) == 0) {
					as_integer_init((as_integer*)&bin->value, value);
					bin->valuep = &bin->value;
				}
				break;
			}
			case AS_BYTES_STRING: {
				char* value = malloc(value_size + 1);
				memcpy(value, p, value_size);
				value[value_size] = 0;
				as_string_init_wlen((as_string*)&bin->value, (char*)value, value_size, true);
				bin->valuep = &bin->value;
				break;
			}
			case AS_BYTES_LIST:
			case AS_BYTES_MAP: {
				if (deserialize) {
					as_val* value = 0;
					
					as_buffer buffer;
					buffer.data = p;
					buffer.size = value_size;
					
					as_serializer ser;
					as_msgpack_init(&ser);
					as_serializer_deserialize(&ser, &buffer, &value);
					as_serializer_destroy(&ser);
					
					bin->valuep = (as_bin_value*)value;
				}
				else {
					void* value = malloc(value_size);
					memcpy(value, p, value_size);
					as_bytes_init_wrap((as_bytes*)&bin->value, value, value_size, true);
					bin->value.bytes.type = (as_bytes_type)type;
					bin->valuep = &bin->value;
				}
				break;
			}
			default: {
				void* value = malloc(value_size);
				memcpy(value, p, value_size);
				as_bytes_init_wrap((as_bytes*)&bin->value, value, value_size, true);
				bin->value.bytes.type = (as_bytes_type)type;
				bin->valuep = &bin->value;
				break;
			}
		}
		rec->bins.size++;
		p += value_size;
	}
	return p;
}

as_status
as_command_parse_result(as_error* err, int fd, uint64_t deadline_ms, void* user_data)
{
	// Read header
	as_proto_msg msg;
	as_status status = as_socket_read_deadline(err, fd, (uint8_t*)&msg, sizeof(as_proto_msg), deadline_ms);
	
	if (status) {
		return status;
	}
	
	as_proto_swap_from_be(&msg.proto);
	as_msg_swap_header_from_be(&msg.m);
	size_t size = msg.proto.sz  - msg.m.header_sz;
	uint8_t* buf = 0;
	
	if (size > 0) {
		// Read remaining message bytes.
		buf = as_command_init(size);
		status = as_socket_read_deadline(err, fd, buf, size, deadline_ms);
		
		if (status) {
			as_command_free(buf, size);
			return status;
		}
	}
	
	// Parse result code and record.
	status = msg.m.result_code;
	as_record** record = user_data;
	
	switch (status) {
		case AEROSPIKE_OK: {
			if (record) {
				as_record* rec = *record;
				
				if (rec) {
					if (msg.m.n_ops > rec->bins.capacity) {
						if (rec->bins._free) {
							free(rec->bins.entries);
						}
						rec->bins.capacity = msg.m.n_ops;
						rec->bins.size = 0;
						rec->bins.entries = malloc(sizeof(as_bin) * msg.m.n_ops);
						rec->bins._free = true;
					}
				}
				else {
					rec = as_record_new(msg.m.n_ops);
					*record = rec;
				}
				rec->gen = msg.m.generation;
				rec->ttl = cf_server_void_time_to_ttl(msg.m.record_ttl);
				
				uint8_t* p = as_command_ignore_fields(buf, msg.m.n_fields);
				as_command_parse_bins(rec, p, msg.m.n_ops, true);
			}
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			status = as_command_parse_udf_failure(buf, err, &msg.m, status);
			break;
		}
			
		case AEROSPIKE_ERR_RECORD_NOT_FOUND:
		case AEROSPIKE_ERR_LARGE_ITEM_NOT_FOUND:
			break;

		default:
			as_error_set_message(err, status, as_error_string(status));
			break;
	}
	as_command_free(buf, size);
	return status;
}

as_status
as_command_parse_success_failure(as_error* err, int fd, uint64_t deadline_ms, void* user_data)
{
	// Read header
	as_proto_msg msg;
	as_status status = as_socket_read_deadline(err, fd, (uint8_t*)&msg, sizeof(as_proto_msg), deadline_ms);
	
	if (status) {
		return status;
	}
	
	as_proto_swap_from_be(&msg.proto);
	as_msg_swap_header_from_be(&msg.m);
	size_t size = msg.proto.sz  - msg.m.header_sz;
	uint8_t* buf = 0;
	
	if (size > 0) {
		// Read remaining message bytes.
		buf = as_command_init(size);
		status = as_socket_read_deadline(err, fd, buf, size, deadline_ms);
		
		if (status) {
			as_command_free(buf, size);
			return status;
		}
	}
	
	as_val** val = user_data;
	
	// Parse result code and record.
	status = msg.m.result_code;
	
	switch (status) {
		case AEROSPIKE_OK: {
			as_command_parse_success_failure_bins(buf, err, &msg.m, val);
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			status = as_command_parse_udf_failure(buf, err, &msg.m, status);
			*val = 0;
			break;
		}
			
		case AEROSPIKE_ERR_RECORD_NOT_FOUND:
		case AEROSPIKE_ERR_LARGE_ITEM_NOT_FOUND:
			*val = 0;
			break;
			
		default:
			as_error_set_message(err, status, as_error_string(status));
			*val = 0;
			break;
	}
	as_command_free(buf, size);
	return status;
}
