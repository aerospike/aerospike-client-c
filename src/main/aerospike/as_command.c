/*
 * Copyright 2008-2021 Aerospike, Inc.
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
#include <aerospike/as_event.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_record.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

/******************************************************************************
 * STATIC VARIABLES
 *****************************************************************************/

// These values must line up with as_operator enum.
static uint8_t as_protocol_types[] = {1, 2, 3, 4, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static as_status
as_command_read_messages(as_error* err, as_command* cmd, as_socket* sock, as_node* node);

static as_status
as_command_read_message(as_error* err, as_command* cmd, as_socket* sock, as_node* node);

as_status
as_batch_retry(as_command* cmd, as_error* err);

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
		case AS_DOUBLE: {
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
	
	if (policy == AS_POLICY_KEY_SEND && key->valuep) {
		size += as_command_user_key_size(key);
		(*n_fields)++;
	}
	return size;
}

size_t
as_command_value_size(as_val* val, as_queue* buffers)
{
	switch (val->type) {
		case AS_NIL: {
			return 0;
		}
		case AS_BOOLEAN: {
			return 1;
		}
		case AS_INTEGER: {
			return 8;
		}
		case AS_DOUBLE: {
			return 8;
		}
		case AS_STRING: {
			as_string* v = as_string_fromval(val);
			return as_string_len(v);
		}
		case AS_GEOJSON: {
			as_geojson* v = as_geojson_fromval(val);
			return 
				1 +					// as_particle_geojson_mem::flags
				2 +					// as_particle_geojson_mem::ncells
				(0 * 8) +			// <placeholder-cellids> EMPTY!
				as_geojson_len(v);
		}
		case AS_BYTES: {
			as_bytes* v = as_bytes_fromval(val);
			return v->size;
		}
		case AS_LIST:
		case AS_MAP: {
			as_buffer buffer;
			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_serialize(&ser, val, &buffer);
			as_serializer_destroy(&ser);
			as_queue_push(buffers, &buffer);
			return buffer.size;
		}
		default: {
			return 0;
		}
	}
}

uint8_t*
as_command_write_header_write(
	uint8_t* cmd, const as_policy_base* policy, as_policy_commit_level commit_level,
	as_policy_exists exists, as_policy_gen gen_policy, uint32_t gen, uint32_t ttl,
	uint16_t n_fields, uint16_t n_bins, bool durable_delete, uint8_t read_attr, uint8_t write_attr,
	uint8_t info_attr
	)
{
	switch (exists) {
		default:
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

	uint32_t generation;

	switch (gen_policy) {
		default:
		case AS_POLICY_GEN_IGNORE:
			generation = 0;
			break;
			
		case AS_POLICY_GEN_EQ:
			generation = gen;
			write_attr |= AS_MSG_INFO2_GENERATION;
			break;
			
		case AS_POLICY_GEN_GT:
			generation = gen;
			write_attr |= AS_MSG_INFO2_GENERATION_GT;
			break;
	}

	if (commit_level == AS_POLICY_COMMIT_LEVEL_MASTER) {
		info_attr |= AS_MSG_INFO3_COMMIT_MASTER;
	}

	if (durable_delete) {
		write_attr |= AS_MSG_INFO2_DURABLE_DELETE;
	}

#if defined USE_XDR
	read_attr |= AS_MSG_INFO1_XDR;
#endif

	cmd[8] = 22;
	cmd[9] = read_attr;
	cmd[10] = write_attr;
	cmd[11] = info_attr;
	*(uint16_t*)&cmd[12] = 0;
	*(uint32_t*)&cmd[14] = cf_swap_to_be32(generation);
	*(uint32_t*)&cmd[18] = cf_swap_to_be32(ttl);
	uint32_t timeout = as_command_server_timeout(policy);
	*(uint32_t*)&cmd[22] = cf_swap_to_be32(timeout);
	*(uint16_t*)&cmd[26] = cf_swap_to_be16(n_fields);
	*(uint16_t*)&cmd[28] = cf_swap_to_be16(n_bins);
	return cmd + AS_HEADER_SIZE;
}

uint8_t*
as_command_write_header_read(
	uint8_t* cmd, const as_policy_base* policy, as_policy_read_mode_ap read_mode_ap,
	as_policy_read_mode_sc read_mode_sc, uint32_t timeout, uint16_t n_fields, uint16_t n_bins,
	uint8_t read_attr
	)
{
	uint8_t info_attr = 0;
	as_command_set_attr_read(read_mode_ap, read_mode_sc, policy->compress, &read_attr,
							 &info_attr);

	cmd[8] = 22;
	cmd[9] = read_attr;
	cmd[10] = 0;
	cmd[11] = info_attr;
	memset(&cmd[12], 0, 10);
	*(uint32_t*)&cmd[22] = cf_swap_to_be32(timeout);
	*(uint16_t*)&cmd[26] = cf_swap_to_be16(n_fields);
	*(uint16_t*)&cmd[28] = cf_swap_to_be16(n_bins);
	return cmd + AS_HEADER_SIZE;
}

uint8_t*
as_command_write_header_read_header(
	uint8_t* cmd, const as_policy_base* policy, as_policy_read_mode_ap read_mode_ap,
	as_policy_read_mode_sc read_mode_sc, uint16_t n_fields, uint16_t n_bins, uint8_t read_attr
	)
{
	uint8_t info_attr = 0;
	as_command_set_attr_read_header(read_mode_ap, read_mode_sc, &read_attr, &info_attr);

	cmd[8] = 22;
	cmd[9] = read_attr;
	cmd[10] = 0;
	cmd[11] = info_attr;
	memset(&cmd[12], 0, 10);
	uint32_t timeout = as_command_server_timeout(policy);
	*(uint32_t*)&cmd[22] = cf_swap_to_be32(timeout);
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
		case AS_DOUBLE: {
			as_double* v = as_double_fromval(val);
			*p++ = AS_BYTES_DOUBLE;
			*(double*)p = cf_swap_to_big_float64(v->value);
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
	
	if (policy == AS_POLICY_KEY_SEND && key->valuep) {
		p = as_command_write_user_key(p, key);
	}
	return p;
}

uint8_t*
as_command_write_bin_name(uint8_t* cmd, const char* name)
{
	uint8_t* p = cmd + AS_OPERATION_HEADER_SIZE;
	
	// Copy string, but do not transfer null byte.
	while (*name) {
		*p++ = *name++;
	}
	uint8_t name_len = (uint8_t)(p - cmd - AS_OPERATION_HEADER_SIZE);
	*(uint32_t*)cmd = cf_swap_to_be32((uint32_t)name_len + 4);
	cmd += 4;
	*cmd++ = as_protocol_types[AS_OPERATOR_READ];
	*cmd++ = 0;
	*cmd++ = 0;
	*cmd++ = name_len;
	return p;
}

uint8_t*
as_command_write_bin(uint8_t* begin, as_operator op_type, const as_bin* bin, as_queue* buffers)
{
	uint8_t* p = begin + AS_OPERATION_HEADER_SIZE;
	const char* name = bin->name;

	// Copy string, but do not transfer null byte.
	while (*name) {
		*p++ = *name++;
	}
	uint8_t name_len = (uint8_t)(p - begin - AS_OPERATION_HEADER_SIZE);
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
		case AS_BOOLEAN: {
			as_boolean* v = as_boolean_fromval(val);
			*p++ = v->value;
			val_len = 1;
			val_type = AS_BYTES_BOOL;
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
		case AS_DOUBLE: {
			as_double* v = as_double_fromval(val);
			*(double*)p = cf_swap_to_big_float64(v->value);
			p += 8;
			val_len = 8;
			val_type = AS_BYTES_DOUBLE;
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
		case AS_GEOJSON: {
			// We send a cellid placeholder so we can fill in points
			// in place on the server w/o changing object size.

			as_geojson* v = as_geojson_fromval(val);
			// v->len should have been already set by as_command_value_size().

			// as_particle_geojson_mem::flags
			*p++ = 0;

			// as_particle_geojson_mem::ncells
			*(uint16_t *) p = cf_swap_to_be16(0);
			p += sizeof(uint16_t);
			
			// placeholder cellid
			// THIS LOOP EXECUTES 0 TIMES (still, it belongs here ...)
			for (int ii = 0; ii < 0; ++ii) {
				*(uint64_t *) p = cf_swap_to_be64(0);
				p += sizeof(uint64_t);
			}

			// json data itself
			memcpy(p, v->value, v->len);
			p += v->len;

			val_len = (uint32_t)(1 + 2 + (0 * 8) + v->len);
			val_type = AS_BYTES_GEOJSON;
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
			as_buffer buffer;
			as_queue_pop(buffers, &buffer);
			memcpy(p, buffer.data, buffer.size);
			p += buffer.size;
			val_len = buffer.size;
			val_type = AS_BYTES_LIST;
			cf_free(buffer.data);
			break;
		}
		case AS_MAP: {
			as_buffer buffer;
			as_queue_pop(buffers, &buffer);
			memcpy(p, buffer.data, buffer.size);
			p += buffer.size;
			val_len = buffer.size;
			val_type = AS_BYTES_MAP;
			cf_free(buffer.data);
			break;
		}
	}
	*(uint32_t*)begin = cf_swap_to_be32(name_len + val_len + 4);
	begin += 4;
	*begin++ = as_protocol_types[op_type];
	*begin++ = val_type;
	*begin++ = 0;
	*begin++ = name_len;
	return p;
}

size_t
as_command_compress_max_size(size_t cmd_sz)
{
	return compressBound((uLong)cmd_sz) + sizeof(as_compressed_proto);
}

as_status
as_command_compress(as_error* err, uint8_t* cmd, size_t cmd_sz, uint8_t* compressed_cmd, size_t* compressed_size)
{
	*compressed_size -= sizeof(as_compressed_proto);
	int ret_val = compress2(compressed_cmd + sizeof(as_compressed_proto), (uLongf*)compressed_size,
							cmd, (uLong)cmd_sz, Z_BEST_SPEED);
	
	if (ret_val) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Compress failed: %d", ret_val);
	}
	
	// compressed_size will now have to actual compressed size from compress2()
	as_command_compress_write_end(compressed_cmd, compressed_cmd + sizeof(as_compressed_proto) +
								  *compressed_size, cmd_sz);
	
	// Adjust the compressed size to include the header size
	*compressed_size += sizeof(as_compressed_proto);
	return AEROSPIKE_OK;
}

as_status
as_command_send(
	as_command* cmd, as_error* err, uint32_t comp_threshold, as_write_fn write_fn, void* udata
	)
{
	size_t capacity = cmd->buf_size;
	cmd->buf = as_command_buffer_init(capacity);
	cmd->buf_size = write_fn(udata, cmd->buf);

	if (comp_threshold > 0 && cmd->buf_size > comp_threshold) {
		// Compress command.
		size_t comp_capacity = as_command_compress_max_size(cmd->buf_size);
		size_t comp_size = comp_capacity;
		uint8_t* comp_buf = as_command_buffer_init(comp_capacity);
		as_status status = as_command_compress(err, cmd->buf, cmd->buf_size, comp_buf, &comp_size);
		as_command_buffer_free(cmd->buf, capacity);

		if (status != AEROSPIKE_OK) {
			as_command_buffer_free(comp_buf, comp_capacity);
			return status;
		}
		capacity = comp_capacity;
		cmd->buf = comp_buf;
		cmd->buf_size = comp_size;
	}

	as_command_start_timer(cmd);

	as_status status = as_command_execute(cmd, err);
	as_command_buffer_free(cmd->buf, capacity);
	return status;
}

static inline bool
is_server_timeout(as_error* err)
{
	// Server timeouts have a message.  Client timeouts do not have a message.
	return err->message[0];
}

as_status
as_command_execute(as_command* cmd, as_error* err)
{
	as_node* node = NULL;
	uint32_t command_sent_counter = 0;
	as_status status;
	bool release_node;

	// Execute command until successful, timed out or maximum iterations have been reached.
	while (true) {
		if (cmd->node) {
			node = cmd->node;
			release_node = false;
		}
		else {
			// node might already be destroyed on retry and is still set as the previous node.
			// This works because the previous node is only used for pointer comparison
			// and the previous node's contents are not examined during this call.
			node = as_partition_get_node(cmd->cluster, cmd->ns, cmd->partition, node, cmd->replica,
										 cmd->master);

			if (! node) {
				return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
									   "Node not found for partition %s:%u",
									   cmd->ns, cmd->partition_id);
			}
			as_node_reserve(node);
			release_node = true;
		}

		if (! as_node_valid_error_count(node)) {
			status = as_error_set_message(err, AEROSPIKE_MAX_ERROR_RATE, "Max error rate exceeded");
			goto Retry;
		}

		as_socket socket;
		status = as_node_get_connection(err, node, cmd->socket_timeout, cmd->deadline_ms, &socket);
		
		if (status != AEROSPIKE_OK) {
			// Do not retry on server error response such as invalid user/password.
			if (status > 0 && status != AEROSPIKE_ERR_TIMEOUT) {
				if (release_node) {
					as_node_release(node);
				}
				as_error_set_in_doubt(err, cmd->flags & AS_COMMAND_FLAGS_READ, command_sent_counter);
				return status;
			}
			goto Retry;
		}
		
		// Send command.
		status = as_socket_write_deadline(err, &socket, node, cmd->buf, cmd->buf_size,
										  cmd->socket_timeout, cmd->deadline_ms);
		
		if (status != AEROSPIKE_OK) {
			// Socket errors are considered temporary anomalies.  Retry.
			// Close socket to flush out possible garbage.	Do not put back in pool.
			as_node_close_conn_error(node, &socket, socket.pool);
			goto Retry;
		}
		command_sent_counter++;

		// Parse results returned by server.
		if (cmd->node) {
			status = as_command_read_messages(err, cmd, &socket, node);
		}
		else {
			status = as_command_read_message(err, cmd, &socket, node);
		}

		if (status == AEROSPIKE_OK) {
			// Reset error code if retry had occurred.
			if (cmd->iteration > 0) {
				as_error_reset(err);
			}
		}
		else {
			err->code = status;

			// Close socket on errors that can leave unread data in socket.
			switch (status) {
				case AEROSPIKE_ERR_DEVICE_OVERLOAD:
					as_node_put_conn_error(node, &socket);
					goto Retry;

				case AEROSPIKE_ERR_CONNECTION:
					as_node_close_conn_error(node, &socket, socket.pool);
					goto Retry;

				case AEROSPIKE_ERR_TIMEOUT:
					if (is_server_timeout(err)) {
						as_node_put_conn_error(node, &socket);
					}
					else {
						as_node_close_conn_error(node, &socket, socket.pool);
					}
					goto Retry;

				case AEROSPIKE_NOT_AUTHENTICATED:
				case AEROSPIKE_ERR_TLS_ERROR:
				case AEROSPIKE_ERR_QUERY_ABORTED:
				case AEROSPIKE_ERR_SCAN_ABORTED:
				case AEROSPIKE_ERR_CLIENT_ABORT:
				case AEROSPIKE_ERR_CLIENT:
					as_node_close_conn_error(node, &socket, socket.pool);
					if (release_node) {
						as_node_release(node);
					}
					as_error_set_in_doubt(err, cmd->flags & AS_COMMAND_FLAGS_READ, command_sent_counter);
					return status;
				
				default:
					as_error_set_in_doubt(err, cmd->flags & AS_COMMAND_FLAGS_READ, command_sent_counter);
					break;
			}
		}
		
		// Put connection back in pool.
		as_node_put_connection(node, &socket);
		
		// Release resources.
		if (release_node) {
			as_node_release(node);
		}
		return status;

Retry:
		// Check if max retries reached.
		if (++cmd->iteration > cmd->policy->max_retries) {
			break;
		}

		uint32_t sleep_between_retries;

		// Alternate between master and prole on socket errors or database reads.
		// Timeouts/NO_MORE_CONNECTIONS are not a good indicator of impending data migration.
		if (cmd->replica != AS_POLICY_REPLICA_MASTER && (
			((cmd->flags & AS_COMMAND_FLAGS_READ) && !(cmd->flags & AS_COMMAND_FLAGS_LINEARIZE)) ||
			(status != AEROSPIKE_ERR_TIMEOUT && status != AEROSPIKE_ERR_NO_MORE_CONNECTIONS &&
			 status != AEROSPIKE_MAX_ERROR_RATE)
			)) {
			// Note: SC session read will ignore this setting because it uses master only.
			cmd->master = !cmd->master;

			// Disable sleep on first failure because target node is likely to change.
			sleep_between_retries = (cmd->iteration == 1)? 0 : cmd->policy->sleep_between_retries;
		}
		else {
			// Sleep as defined because target node is not likely to change.
			sleep_between_retries = cmd->policy->sleep_between_retries;
		}

		if (cmd->deadline_ms > 0) {
			// Check for total timeout.
			int64_t remaining = cmd->deadline_ms - cf_getms() - sleep_between_retries;

			if (remaining <= 0) {
				break;
			}

			if (remaining < cmd->total_timeout) {
				cmd->total_timeout = (uint32_t)remaining;

				if (cmd->socket_timeout > cmd->total_timeout) {
					cmd->socket_timeout = cmd->total_timeout;
				}
			}
		}

		// Prepare for retry.
		if (release_node) {
			as_node_release(node);
		}

		if (sleep_between_retries > 0) {
			// Sleep before trying again.
			as_sleep(sleep_between_retries);
		}

		if (cmd->flags & AS_COMMAND_FLAGS_BATCH) {
			status = as_batch_retry(cmd, err);

			if (status != AEROSPIKE_USE_NORMAL_RETRY) {
				return status;
			}
		}
	}

	// Retries have been exhausted.
	// Fill in timeout stats if timeout occurred.
	if (err->code == AEROSPIKE_ERR_TIMEOUT) {
		// Server timeouts have a message.  Client timeouts do not have a message.
		const char* type = is_server_timeout(err)? "Server" : "Client";
		as_error_update(err, AEROSPIKE_ERR_TIMEOUT,
			"%s timeout: socket=%u total=%u iterations=%u lastNode=%s",
			type, cmd->policy->socket_timeout, cmd->policy->total_timeout, cmd->iteration,
			as_node_get_address_string(node));
	}

	if (release_node) {
		as_node_release(node);
	}
	as_error_set_in_doubt(err, cmd->flags & AS_COMMAND_FLAGS_READ, command_sent_counter);
	return err->code;
}

static as_status
as_command_read_messages(as_error* err, as_command* cmd, as_socket* sock, as_node* node)
{
	size_t capacity = 0;
	uint8_t* buf = NULL;
	size_t size;
	size_t capacity2 = 0;
	uint8_t* buf2 = NULL;
	size_t size2;
	as_proto proto;
	as_status status;

	while (true) {
		// Read header
		status = as_socket_read_deadline(err, sock, node, (uint8_t*)&proto, sizeof(as_proto),
										 cmd->socket_timeout, cmd->deadline_ms);
		
		if (status != AEROSPIKE_OK) {
			break;
		}

		status = as_proto_parse(err, &proto);

		if (status != AEROSPIKE_OK) {
			break;
		}

		size = proto.sz;

		if (size == 0) {
			continue;
		}

		// Prepare buffer
		if (size > capacity) {
			as_command_buffer_free(buf, capacity);
			capacity = (size + 16383) & ~16383; // Round up in 16KB increments.
			buf = as_command_buffer_init(capacity);
		}
		
		// Read remaining message bytes in group
		status = as_socket_read_deadline(err, sock, node, buf, size, cmd->socket_timeout,
										 cmd->deadline_ms);
		
		if (status != AEROSPIKE_OK) {
			break;
		}
		
		if (proto.type == AS_MESSAGE_TYPE) {
			status = cmd->parse_results_fn(err, node, buf, size, cmd->udata);
		}
		else if (proto.type == AS_COMPRESSED_MESSAGE_TYPE) {
			status = as_compressed_size_parse(err, buf, &size2);

			if (status != AEROSPIKE_OK) {
				break;
			}

			if (size2 > capacity2) {
				as_command_buffer_free(buf2, capacity2);
				capacity2 = (size2 + 16383) & ~16383; // Round up in 16KB increments.
				buf2 = as_command_buffer_init(capacity2);
			}

			status = as_proto_decompress(err, buf2, size2, buf, size);

			if (status != AEROSPIKE_OK) {
				break;
			}

			status = cmd->parse_results_fn(err, node, buf2 + sizeof(as_proto),
										   size2 - sizeof(as_proto), cmd->udata);
		}
		else {
			status = as_proto_type_error(err, &proto, AS_MESSAGE_TYPE);
			break;
		}

		if (status != AEROSPIKE_OK) {
			if (status == AEROSPIKE_NO_MORE_RECORDS) {
				status = AEROSPIKE_OK;
			}
			break;
		}
	}
	as_command_buffer_free(buf, capacity);
	as_command_buffer_free(buf2, capacity2);
	return status;
}

static as_status
as_command_read_message(as_error* err, as_command* cmd, as_socket* sock, as_node* node)
{
	as_proto proto;
	as_status status = as_socket_read_deadline(err, sock, node, (uint8_t*)&proto, sizeof(as_proto),
											   cmd->socket_timeout, cmd->deadline_ms);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	status = as_proto_parse(err, &proto);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	size_t size = proto.sz;

	if (size == 0) {
		return as_proto_size_error(err, size);
	}

	uint8_t* buf = as_command_buffer_init(size);
	status = as_socket_read_deadline(err, sock, node, buf, size, cmd->socket_timeout, cmd->deadline_ms);

	if (status != AEROSPIKE_OK) {
		as_command_buffer_free(buf, size);
		return status;
	}

	if (proto.type == AS_MESSAGE_TYPE) {
		status = cmd->parse_results_fn(err, node, buf, size, cmd->udata);
		as_command_buffer_free(buf, size);
		return status;
	}
	else if (proto.type == AS_COMPRESSED_MESSAGE_TYPE) {
		size_t size2;
		status = as_compressed_size_parse(err, buf, &size2);

		if (status != AEROSPIKE_OK) {
			as_command_buffer_free(buf, size);
			return status;
		}

		uint8_t* buf2 = as_command_buffer_init(size2);
		status = as_proto_decompress(err, buf2, size2, buf, size);
		as_command_buffer_free(buf, size);

		if (status != AEROSPIKE_OK) {
			as_command_buffer_free(buf2, size2);
			return status;
		}
		status = cmd->parse_results_fn(err, node, buf2 + sizeof(as_proto), size2 - sizeof(as_proto),
									   cmd->udata);
		as_command_buffer_free(buf2, size2);
		return status;
	}
	else {
		as_command_buffer_free(buf, size);
		return as_proto_type_error(err, &proto, AS_MESSAGE_TYPE);
	}
}

as_status
as_command_parse_header(as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata)
{
	as_msg* msg = (as_msg*)buf;
	as_status status = as_msg_parse(err, msg, size);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (msg->result_code) {
		return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
	}

	as_record** rec = udata;

	if (rec) {
		as_record* r = *rec;
		
		if (r == NULL) {
			r = as_record_new(0);
			*rec = r;
		}
		r->gen = (uint16_t)msg->generation;
		r->ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	}
	return AEROSPIKE_OK;
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
	for (uint32_t i = 0; i < n_fields; i++) {
		p += cf_swap_from_be32(*(uint32_t*)p) + 4;
	}
	return p;
}

uint8_t*
as_command_ignore_bins(uint8_t* p, uint32_t n_bins)
{
	for (uint32_t i = 0; i < n_bins; i++) {
		p += cf_swap_from_be32(*(uint32_t*)p) + 4;
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
				key->digest.init = true;
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
					case AS_BYTES_DOUBLE: {
						double value = cf_swap_from_big_float64(*(double*)p);
						as_double_init((as_double*)&key->value, value);
						key->valuep = &key->value;
						break;
					}
					case AS_BYTES_STRING: {
						char* value = cf_malloc(len+1);
						memcpy(value, p, len);
						value[len] = 0;
						as_string_init_wlen((as_string*)&key->value, value, len, true);
						key->valuep = &key->value;
						break;
					}
					case AS_BYTES_BLOB: {
						void* value = cf_malloc(len);
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
		case AS_BYTES_BOOL: {
			bool v = *p;
			*value = (as_val*)as_boolean_new(v);
			break;
		}
		case AS_BYTES_INTEGER: {
			int64_t v = 0;
			as_command_bytes_to_int(p, value_size, &v);
			*value = (as_val*)as_integer_new(v);
			break;
		}
		case AS_BYTES_DOUBLE: {
			double v = cf_swap_from_big_float64(*(double*)p);
			*value = (as_val*)as_double_new(v);
			break;
		}
		case AS_BYTES_STRING: {
			char* v = cf_malloc(value_size + 1);
			memcpy(v, p, value_size);
			v[value_size] = 0;
			*value = (as_val*)as_string_new_wlen(v, value_size, true);
			break;
		}
		case AS_BYTES_GEOJSON: {
			uint8_t * ptr = p;

			// skip flags
			ptr++;

			// ncells
			uint16_t ncells = cf_swap_from_be16(* (uint16_t *) ptr);
			ptr += sizeof(uint16_t);

			// skip any cells
			ptr += sizeof(uint64_t) * ncells;

			// Use the json bytes.
			size_t jsonsz = value_size - 1 - 2 - (ncells * sizeof(uint64_t));
			char* v = cf_malloc(jsonsz + 1);
			memcpy(v, ptr, jsonsz);
			v[jsonsz] = 0;
			*value = (as_val*) as_geojson_new_wlen(v, jsonsz, true);
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
		case AS_BYTES_HLL: {
			void* v = cf_malloc(value_size);
			memcpy(v, p, value_size);
			as_bytes* b = as_bytes_new_wrap(v, value_size, true);
			b->type = AS_BYTES_HLL;
			*value = (as_val*)b;
			break;
		}
		default: {
			void* v = cf_malloc(value_size);
			memcpy(v, p, value_size);
			*value = (as_val*)as_bytes_new_wrap(v, value_size, true);
			break;
		}
	}
}

as_status
as_command_parse_success_failure_bins(uint8_t** pp, as_error* err, as_msg* msg, as_val** value)
{
	uint8_t* p = *pp;
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
			*pp = p + value_size;
			return AEROSPIKE_OK;
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
			return err->code;
		}
		p += value_size;
	}
	return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find SUCCESS or FAILURE bin.");
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

as_status
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

static as_status
abort_record_memory(as_error* err, as_record* rec, size_t size)
{
	// Bin values prior to failure will be destroyed later when entire record is destroyed.
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "malloc failure: %zu", size);
}

as_status
as_command_parse_bins(uint8_t** pp, as_error* err, as_record* rec, uint32_t n_bins, bool deserialize)
{
	uint8_t* p = *pp;
	as_bin* bin = rec->bins.entries;

	// Reset size in case we are reusing a record.
	rec->bins.size = 0;

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
			case AS_BYTES_BOOL: {
				bool value = *p;
				as_boolean_init((as_boolean*)&bin->value, value);
				bin->valuep = &bin->value;
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
			case AS_BYTES_DOUBLE: {
				double value = cf_swap_from_big_float64(*(double*)p);
				as_double_init((as_double*)&bin->value, value);
				bin->valuep = &bin->value;
				break;
			}
			case AS_BYTES_STRING: {
				char* value = cf_malloc(value_size + 1);

				if (! value) {
					return abort_record_memory(err, rec, value_size + 1);
				}
				memcpy(value, p, value_size);
				value[value_size] = 0;
				as_string_init_wlen((as_string*)&bin->value, (char*)value, value_size, true);
				bin->valuep = &bin->value;
				break;
			}
			case AS_BYTES_GEOJSON: {
				uint8_t * ptr = p;

				// skip flags
				ptr++;

				// ncells
				uint16_t ncells = cf_swap_from_be16(* (uint16_t *) ptr);
				ptr += sizeof(uint16_t);

				// skip any cells
				ptr += sizeof(uint64_t) * ncells;

				// Use the json bytes.
				size_t jsonsz = value_size - 1 - 2 - (ncells * sizeof(uint64_t));
				char* v = cf_malloc(jsonsz + 1);

				if (! v) {
					return abort_record_memory(err, rec, jsonsz + 1);
				}
				memcpy(v, ptr, jsonsz);
				v[jsonsz] = 0;
				as_geojson_init_wlen((as_geojson*)&bin->value,
									 (char*)v, jsonsz, true);
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
					int rv = as_serializer_deserialize(&ser, &buffer, &value);
					as_serializer_destroy(&ser);

					if (rv != 0) {
						return as_error_update(err, AEROSPIKE_ERR_CLIENT, "deserialize error: %d", rv);
					}
					bin->valuep = (as_bin_value*)value;
				}
				else {
					void* value = cf_malloc(value_size);

					if (! value) {
						return abort_record_memory(err, rec, value_size);
					}
					memcpy(value, p, value_size);
					as_bytes_init_wrap((as_bytes*)&bin->value, value, value_size, true);
					bin->value.bytes.type = (as_bytes_type)type;
					bin->valuep = &bin->value;
				}
				break;
			}
			default: {
				void* value = cf_malloc(value_size);

				if (! value) {
					return abort_record_memory(err, rec, value_size);
				}
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
	*pp = p;
	return AEROSPIKE_OK;
}

as_status
as_command_parse_result(as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata)
{
	as_command_parse_result_data* data = udata;
	as_msg* msg = (as_msg*)buf;
	as_status status = as_msg_parse(err, msg, size);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	status = msg->result_code;

	uint8_t* p = buf + sizeof(as_msg);

	switch (status) {
		case AEROSPIKE_OK: {
			if (data->record) {
				as_record* rec = *data->record;
				bool free_on_error;
				
				if (rec) {
					// Must destroy existing record bin values before populating new bin values.
					as_bin* bin = rec->bins.entries;
					for (uint16_t i = 0; i < rec->bins.size; i++, bin++) {
						as_val_destroy((as_val*)bin->valuep);
						bin->valuep = NULL;
					}

					if (msg->n_ops > rec->bins.capacity) {
						if (rec->bins._free) {
							cf_free(rec->bins.entries);
						}
						rec->bins.capacity = msg->n_ops;
						rec->bins.size = 0;
						rec->bins.entries = cf_malloc(sizeof(as_bin) * msg->n_ops);
						rec->bins._free = true;
					}
					free_on_error = false;
				}
				else {
					rec = as_record_new(msg->n_ops);
					*data->record = rec;
					free_on_error = true;
				}
				rec->gen = msg->generation;
				rec->ttl = cf_server_void_time_to_ttl(msg->record_ttl);
				
				p = as_command_ignore_fields(p, msg->n_fields);
				status = as_command_parse_bins(&p, err, rec, msg->n_ops, data->deserialize);

				if (status != AEROSPIKE_OK && free_on_error) {
					as_record_destroy(rec);
				}
			}
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			status = as_command_parse_udf_failure(p, err, msg, status);
			break;
		}
			
		default:
			as_error_update(err, status, "%s %s", as_node_get_address_string(node),
							as_error_string(status));
			break;
	}
	return status;
}

as_status
as_command_parse_success_failure(
	as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata
	)
{
	as_val** val = udata;
	as_msg* msg = (as_msg*)buf;
	as_status status = as_msg_parse(err, msg, size);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	status = msg->result_code;

	uint8_t* p = buf + sizeof(as_msg);

	switch (status) {
		case AEROSPIKE_OK: {
			status = as_command_parse_success_failure_bins(&p, err, msg, val);
			
			if (status != AEROSPIKE_OK) {
				if (val) {
					*val = 0;
				}
			}
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			status = as_command_parse_udf_failure(p, err, msg, status);
			if (val) {
				*val = 0;
			}
			break;
		}

		default:
			as_error_update(err, status, "%s %s", as_node_get_address_string(node),
							as_error_string(status));
			if (val) {
				*val = 0;
			}
			break;
	}
	return status;
}
