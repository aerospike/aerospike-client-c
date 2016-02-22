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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_async.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_thread_pool.h>
#include <aerospike/as_val.h>
#include <citrusleaf/cf_clock.h>

/************************************************************************
 * 	TYPES
 ************************************************************************/

typedef struct as_batch_node_s {
	as_node* node;
	as_vector offsets;
} as_batch_node;

typedef struct as_batch_task_s {
	as_node* node;
	as_vector offsets;
	
	as_cluster* cluster;
	as_error* err;
	cf_queue* complete_q;
	uint32_t* error_mutex;
	as_vector* records;     // New aerospike_batch_read()
	const char* ns;         // Old aerospike_batch_get()
	as_key* keys;           // Old aerospike_batch_get()
	as_batch_read* results; // Old aerospike_batch_get()
	void* udata;            // XDR
	as_batch_callback_xdr callback_xdr; // XDR
	const char** bins;      // Old aerospike_batch_get()
	
	uint32_t n_bins;        // Old aerospike_batch_get()
	uint32_t index;         // Old aerospike_batch_get()
	uint32_t n_keys;
	uint32_t timeout_ms;
	uint32_t retry;
	
	uint8_t read_attr;      // Old aerospike_batch_get()
	bool use_batch_records;
	bool use_new_batch;
	bool allow_inline;
	bool send_set_name;
	bool deserialize;
} as_batch_task;

typedef struct as_batch_complete_task_s {
	as_node* node;
	as_status result;
} as_batch_complete_task;

typedef struct {
	as_event_executor executor;
	as_batch_read_records* records;
	as_async_batch_listener listener;
} as_async_batch_executor;

typedef struct as_async_batch_command {
	as_event_command command;
	uint8_t space[];
} as_async_batch_command;

/******************************************************************************
 *	STATIC FUNCTIONS
 *****************************************************************************/

static inline bool
as_batch_use_new(const as_policy_batch* policy, as_node* node)
{
	return ! policy->use_batch_direct && node->has_batch_index;
}

static uint8_t*
as_batch_parse_fields(uint8_t* p, uint32_t n_fields, uint8_t** digest)
{
	uint32_t len;
	
	for (uint32_t i = 0; i < n_fields; i++) {
		len = cf_swap_from_be32(*(uint32_t*)p);
		p += 4;
		
		if (*p++ == AS_FIELD_DIGEST) {
			*digest = p;
		}
		p += len - 1;
	}
	return p;
}

static inline uint8_t*
as_batch_parse_record(uint8_t* p, as_msg* msg, as_record* rec, bool deserialize)
{
	as_record_init(rec, msg->n_ops);
	rec->gen = msg->generation;
	rec->ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	return as_command_parse_bins(rec, p, msg->n_ops, deserialize);
}

static void
as_batch_complete_async(as_event_executor* executor, as_error* err)
{
	as_async_batch_executor* e = (as_async_batch_executor*)executor;
	e->listener(err, e->records, executor->udata, executor->event_loop);
}

static bool
as_batch_async_skip_records(as_event_command* cmd)
{
	uint8_t* p = cmd->buf;
	uint8_t* end = p + cmd->len;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code && msg->result_code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			as_error err;
			as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
			as_event_response_error(cmd, &err);
			return true;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			as_event_executor_complete(cmd);
			return true;
		}
		
		p = as_command_ignore_fields(p, msg->n_fields);
		p = as_command_ignore_bins(p, msg->n_ops);
	}
	return false;
}

static bool
as_batch_async_parse_records(as_event_command* cmd)
{
	as_async_batch_executor* executor = cmd->udata;  // udata is overloaded to contain executor.

	if (! executor->executor.valid) {
		// An error has already been returned to the user and records have been deleted.
		// Skip over remaining socket data so it's fully read and can be reused.
		return as_batch_async_skip_records(cmd);
	}
	
	as_vector* records = &executor->records->list;
	uint8_t* p = cmd->buf;
	uint8_t* end = p + cmd->len;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code && msg->result_code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			as_error err;
			as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
			as_event_response_error(cmd, &err);
			return true;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			as_event_executor_complete(cmd);
			return true;
		}
		
		uint32_t offset = msg->transaction_ttl; // overloaded to contain batch index
		
		uint8_t* digest = 0;
		p = as_batch_parse_fields(p, msg->n_fields, &digest);
		
		if (offset >= records->size) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Batch index %u >= batch size: %u", offset, records->size);
			as_event_response_error(cmd, &err);
			return true;
		}
		
		as_batch_read_record* record = as_vector_get(records, offset);
		
		if (digest && memcmp(digest, record->key.digest.value, AS_DIGEST_VALUE_SIZE) == 0) {
			record->result = msg->result_code;
			
			if (msg->result_code == AEROSPIKE_OK) {
				p = as_batch_parse_record(p, msg, &record->record, cmd->deserialize);
			}
		}
		else {
			char digest_string[64];
			cf_digest_string((cf_digest*)digest, digest_string);
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Unexpected batch key returned: %s,%u", digest_string, offset);
			as_event_response_error(cmd, &err);
			return true;
		}
	}
	return false;
}

static as_status
as_batch_parse_records(as_error* err, uint8_t* buf, size_t size, as_batch_task* task)
{
	uint8_t* p = buf;
	uint8_t* end = buf + size;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code && msg->result_code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}

		uint32_t offset;
		if (task->use_new_batch) {
			offset = msg->transaction_ttl;  // overloaded to contain batch index
		}
		else {
			offset = *(uint32_t*)as_vector_get(&task->offsets, task->index++);
		}

		if (offset >= task->n_keys) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Batch index %u >= batch size: %u", offset, task->n_keys);
		}

		uint8_t* digest = 0;
		p = as_batch_parse_fields(p, msg->n_fields, &digest);
		
		if (task->use_batch_records) {
			as_batch_read_record* record = as_vector_get(task->records, offset);

			if (digest && memcmp(digest, record->key.digest.value, AS_DIGEST_VALUE_SIZE) == 0) {
				record->result = msg->result_code;
				
				if (msg->result_code == AEROSPIKE_OK) {
					p = as_batch_parse_record(p, msg, &record->record, task->deserialize);
				}
			}
			else {
				char digest_string[64];
				cf_digest_string((cf_digest*)digest, digest_string);
				return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unexpected batch key returned: %s,%u", digest_string, offset);
			}
		}
		else {
			as_key* key = &task->keys[offset];
			if (digest && memcmp(digest, key->digest.value, AS_DIGEST_VALUE_SIZE) == 0) {
				if (task->callback_xdr) {
					if (msg->result_code == AEROSPIKE_OK) {
						as_record rec;
						p = as_batch_parse_record(p, msg, &rec, task->deserialize);
						bool rv = task->callback_xdr(key, &rec, task->udata);
						as_record_destroy(&rec);
						
						if (!rv) {
							return AEROSPIKE_ERR_CLIENT_ABORT;
						}
					}
				}
				else {
					as_batch_read* result = &task->results[offset];
					result->result = msg->result_code;
					
					if (msg->result_code == AEROSPIKE_OK) {
						p = as_batch_parse_record(p, msg, &result->record, task->deserialize);
					}
				}
			}
			else {
				char digest_string[64];
				cf_digest_string((cf_digest*)digest, digest_string);
				return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Unexpected batch key returned: %s,%s,%u,%u", task->ns, digest_string, task->index, offset);
			}
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_batch_parse(as_error* err, int fd, uint64_t deadline_ms, void* udata)
{
	as_batch_task* task = udata;
	as_status status = AEROSPIKE_OK;
	uint8_t* buf = 0;
	size_t capacity = 0;
	
	while (true) {
		// Read header
		as_proto proto;
		status = as_socket_read_deadline(err, fd, (uint8_t*)&proto, sizeof(as_proto), deadline_ms);
		
		if (status) {
			break;
		}
		as_proto_swap_from_be(&proto);
		size_t size = proto.sz;
		
		if (size > 0) {
			// Prepare buffer
			if (size > capacity) {
				as_command_free(buf, capacity);
				capacity = size;
				buf = as_command_init(capacity);
			}
			
			// Read remaining message bytes in group
			status = as_socket_read_deadline(err, fd, buf, size, deadline_ms);
			
			if (status) {
				break;
			}
			
			status = as_batch_parse_records(err, buf, size, task);
			
			if (status != AEROSPIKE_OK) {
				if (status == AEROSPIKE_NO_MORE_RECORDS) {
					status = AEROSPIKE_OK;
				}
				break;
			}
		}
	}
	as_command_free(buf, capacity);
	return status;
}

static size_t
as_batch_index_records_size(as_vector* records, as_vector* offsets, bool send_set_name)
{
	// Estimate buffer size.
	size_t size = AS_HEADER_SIZE + AS_FIELD_HEADER_SIZE + sizeof(uint32_t) + 1;
	as_batch_read_record* prev = 0;
	uint32_t n_offsets = offsets->size;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		as_batch_read_record* record = as_vector_get(records, offset);
		
		size += AS_DIGEST_VALUE_SIZE + sizeof(uint32_t);
		
		// Avoid relatively expensive full equality checks for performance reasons.
		// Use reference equality only in hope that common namespaces/bin names are set from
		// fixed variables.  It's fine if equality not determined correctly because it just
		// results in more space used. The batch will still be correct.
		if (prev && prev->key.ns == record->key.ns &&
			(! send_set_name || prev->key.set == record->key.set) &&
			prev->bin_names == record->bin_names && prev->read_all_bins == record->read_all_bins) {
			// Can set repeat previous namespace/bin names to save space.
			size++;
		}
		else {
			// Estimate full header, namespace and bin names.
			size += as_command_string_field_size(record->key.ns) + 6;
			
			if (send_set_name) {
				size += as_command_string_field_size(record->key.set);
			}
			
			if (record->bin_names) {
				for (uint32_t i = 0; i < record->n_bin_names; i++) {
					size += as_command_string_operation_size(record->bin_names[i]);
				}
			}
			prev = record;
		}
	}
	return size;
}

static size_t
as_batch_index_records_write(as_vector* records, as_vector* offsets, uint32_t timeout_ms, bool allow_inline, bool send_set_name, uint8_t* cmd)
{
	uint32_t n_offsets = offsets->size;
	uint8_t* p = as_command_write_header_read(cmd, AS_MSG_INFO1_READ | AS_MSG_INFO1_BATCH_INDEX, AS_POLICY_CONSISTENCY_LEVEL_ONE, timeout_ms, 1, 0);
	uint8_t* field_size_ptr = p;
	p = as_command_write_field_header(p, send_set_name ? AS_FIELD_BATCH_INDEX_WITH_SET : AS_FIELD_BATCH_INDEX, 0);  // Need to update size at end
	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = allow_inline? 1 : 0;
	
	as_batch_read_record* prev = 0;
	uint16_t field_count = send_set_name ? 2 : 1;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);
		
		as_batch_read_record* record = as_vector_get(records, offset);
		memcpy(p, record->key.digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		
		// Avoid relatively expensive full equality checks for performance reasons.
		// Use reference equality only in hope that common namespaces/bin names are set from
		// fixed variables.  It's fine if equality not determined correctly because it just
		// results in more space used. The batch will still be correct.
		if (prev && prev->key.ns == record->key.ns &&
			(! send_set_name || prev->key.set == record->key.set) &&
			prev->bin_names == record->bin_names && prev->read_all_bins == record->read_all_bins) {
			// Can set repeat previous namespace/bin names to save space.
			*p++ = 1;  // repeat
		}
		else {
			// Write full header, namespace and bin names.
			*p++ = 0;  // do not repeat
			
			if (record->bin_names && record->n_bin_names) {
				*p++ = AS_MSG_INFO1_READ;
				*(uint16_t*)p = cf_swap_to_be16(field_count);
				p += sizeof(uint16_t);
				*(uint16_t*)p = cf_swap_to_be16((uint16_t)record->n_bin_names);
				p += sizeof(uint16_t);
				p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, record->key.ns);
			
				if (send_set_name) {
					p = as_command_write_field_string(p, AS_FIELD_SETNAME, record->key.set);
				}
				
				for (uint32_t i = 0; i < record->n_bin_names; i++) {
					p = as_command_write_bin_name(p, record->bin_names[i]);
				}
			}
			else {
				*p++ = (AS_MSG_INFO1_READ | (record->read_all_bins? AS_MSG_INFO1_GET_ALL : AS_MSG_INFO1_GET_NOBINDATA));
				*(uint16_t*)p = cf_swap_to_be16(field_count);
				p += sizeof(uint16_t);
				*p++ = 0;  // n_bin_names
				*p++ = 0;  // n_bin_names
				p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, record->key.ns);
				
				if (send_set_name) {
					p = as_command_write_field_string(p, AS_FIELD_SETNAME, record->key.set);
				}
			}
			prev = record;
		}
	}
	// Write real field size.
	size_t size = p - field_size_ptr - 4;
	*(uint32_t*)field_size_ptr = cf_swap_to_be32((uint32_t)size);
	
	return as_command_write_end(cmd, p);
}

static as_status
as_batch_index_records_execute(as_batch_task* task)
{
	// Estimate buffer size.
	size_t size = as_batch_index_records_size(task->records, &task->offsets, task->send_set_name);
		
	// Write command
	uint8_t* cmd = as_command_init(size);
	size = as_batch_index_records_write(task->records, &task->offsets, task->timeout_ms, task->allow_inline, task->send_set_name, cmd);

	as_command_node cn;
	cn.node = task->node;

	as_error err;
	as_error_init(&err);
	as_status status = as_command_execute(task->cluster, &err, &cn, cmd, size, task->timeout_ms, task->retry, as_batch_parse, task);
	
	as_command_free(cmd, size);
	
	if (status) {
		// Copy error to main error only once.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			as_error_copy(task->err, &err);
		}
	}
	return status;
}

static as_status
as_batch_index_execute(as_batch_task* task)
{
	// Calculate size of bin names.
	uint16_t field_count = task->send_set_name ? 2 : 1;
	size_t bin_name_size = 0;
	
	if (task->n_bins) {
		for (uint32_t i = 0; i < task->n_bins; i++) {
			bin_name_size += as_command_string_operation_size(task->bins[i]);
		}
	}
	
	// Estimate buffer size.
	size_t size = AS_HEADER_SIZE + AS_FIELD_HEADER_SIZE + 5;
	as_key* prev = 0;
	uint32_t n_offsets = task->offsets.size;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_key* key = &task->keys[offset];
		
		size += 24;  // digest + int count.

		// Try reference equality in hope that namespace for all keys is set from a fixed variable.
		if (prev && prev->ns == key->ns && (! task->send_set_name || prev->set == key->set)) {
			// Can set repeat previous namespace/bin names to save space.
			size++;
		}
		else {
			// Estimate full header, namespace and bin names.
			size += as_command_string_field_size(key->ns) + 6;
			
			if (task->send_set_name) {
				size += as_command_string_field_size(key->set);
			}
			size += bin_name_size;
			prev = key;
		}
	}
	
	// Write command
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, task->read_attr | AS_MSG_INFO1_BATCH_INDEX, AS_POLICY_CONSISTENCY_LEVEL_ONE, task->timeout_ms, 1, 0);
	uint8_t* field_size_ptr = p;
	p = as_command_write_field_header(p, task->send_set_name ? AS_FIELD_BATCH_INDEX_WITH_SET : AS_FIELD_BATCH_INDEX, 0);  // Need to update size at end
	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = task->allow_inline? 1 : 0;
	
	prev = 0;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);
		
		as_key* key = &task->keys[offset];
		memcpy(p, key->digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		
		// Try reference equality in hope that namespace for all keys is set from a fixed variable.
		if (prev && prev->ns == key->ns && (! task->send_set_name || prev->set == key->set)) {
			// Can set repeat previous namespace/bin names to save space.
			*p++ = 1;  // repeat
		}
		else {
			// Write full header, namespace and bin names.
			*p++ = 0;  // do not repeat
			*p++ = task->read_attr;
			*(uint16_t*)p = cf_swap_to_be16(field_count);
			p += sizeof(uint16_t);
			*(uint16_t*)p = cf_swap_to_be16((uint16_t)task->n_bins);
			p += sizeof(uint16_t);
			p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);
			
			if (task->send_set_name) {
				p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
			}

			if (task->n_bins) {
				for (uint32_t i = 0; i < task->n_bins; i++) {
					p = as_command_write_bin_name(p, task->bins[i]);
				}
			}
			prev = key;
		}
	}
	// Write real field size.
	size = p - field_size_ptr - 4;
	*(uint32_t*)field_size_ptr = cf_swap_to_be32((uint32_t)size);
	
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	cn.node = task->node;
	
	as_error err;
	as_error_init(&err);
	as_status status = as_command_execute(task->cluster, &err, &cn, cmd, size, task->timeout_ms, task->retry, as_batch_parse, task);
	
	as_command_free(cmd, size);
	
	if (status) {
		// Copy error to main error only once.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			as_error_copy(task->err, &err);
		}
	}
	return status;
}

static as_status
as_batch_direct_execute(as_batch_task* task)
{
	size_t size = AS_HEADER_SIZE;
	size += as_command_string_field_size(task->ns);
	
	uint32_t n_offsets = task->offsets.size;
	uint32_t byte_size = n_offsets * AS_DIGEST_VALUE_SIZE;
	size += as_command_field_size(byte_size);
	
	if (task->n_bins) {
		for (uint32_t i = 0; i < task->n_bins; i++) {
			size += as_command_string_operation_size(task->bins[i]);
		}
	}
	
	uint8_t* cmd = as_command_init(size);
	uint8_t* p = as_command_write_header_read(cmd, task->read_attr, AS_POLICY_CONSISTENCY_LEVEL_ONE, task->timeout_ms, 2, task->n_bins);
	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, task->ns);
	p = as_command_write_field_header(p, AS_FIELD_DIGEST_ARRAY, byte_size);
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_key* key = &task->keys[offset];
		memcpy(p, key->digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
	}
	
	if (task->n_bins) {
		for (uint32_t i = 0; i < task->n_bins; i++) {
			p = as_command_write_bin_name(p, task->bins[i]);
		}
	}
	
	size = as_command_write_end(cmd, p);
	
	as_command_node cn;
	cn.node = task->node;
	
	as_error err;
	as_error_init(&err);
	as_status status = as_command_execute(task->cluster, &err, &cn, cmd, size, task->timeout_ms, task->retry, as_batch_parse, task);
	
	as_command_free(cmd, size);
	
	if (status) {
		// Copy error to main error only once.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			as_error_copy(task->err, &err);
		}
	}
	return status;
}

static inline as_status
as_batch_command_execute(as_batch_task* task)
{
	as_status status;
	
	if (task->use_new_batch) {
		// New batch protocol
		if (task->use_batch_records) {
			// Use as_batch_read_records referenced in aerospike_batch_read().
			status = as_batch_index_records_execute(task);
		}
		else {
			// Use as_batch referenced in aerospike_batch_get(), aerospike_batch_get_bins()
			// and aerospike_batch_exists().
			status = as_batch_index_execute(task);
		}
	}
	else {
		// Old batch protocol
		status = as_batch_direct_execute(task);
	}
	return status;
}

static void
as_batch_worker(void* data)
{
	as_batch_task* task = (as_batch_task*)data;
	
	as_batch_complete_task complete_task;
	complete_task.node = task->node;
	complete_task.result = as_batch_command_execute(task);
	
	cf_queue_push(task->complete_q, &complete_task);
}

static as_batch_node*
as_batch_node_find(as_batch_node* batch_nodes, uint32_t n_batch_nodes, as_node* node)
{
	as_batch_node* batch_node = batch_nodes;
	
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		if (batch_node->node == node) {
			return batch_node;
		}
		batch_node++;
	}
	return 0;
}

static void
as_batch_release_nodes(as_batch_node* batch_nodes, uint32_t n_batch_nodes)
{
	as_batch_node* batch_node = batch_nodes;
	
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_node_release(batch_node->node);
		as_vector_destroy(&batch_node->offsets);
		batch_node++;
	}
}

static as_status
as_batch_execute(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	int read_attr, const char** bins, uint32_t n_bins,
	aerospike_batch_read_callback callback, as_batch_callback_xdr callback_xdr, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	uint32_t n_keys = batch->keys.size;
	
	if (n_keys <= 0) {
		callback(0, 0, udata);
		return AEROSPIKE_OK;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Batch command failed because cluster is empty.");
	}
	
	// Allocate results array on stack.  May be an issue for huge batch.
	as_batch_read* results = (callback)? (as_batch_read*)alloca(sizeof(as_batch_read) * n_keys) : 0;
	
	as_batch_node* batch_nodes = alloca(sizeof(as_batch_node) * n_nodes);
	char* ns = batch->keys.entries[0].ns;
	uint32_t n_batch_nodes = 0;
	as_status status = AEROSPIKE_OK;
	
	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_capacity = n_keys / n_nodes;
	offsets_capacity += offsets_capacity >> 2;

	// The minimum key capacity is 10.
	if (offsets_capacity < 10) {
		offsets_capacity = 10;
	}
	
	// Map keys to server nodes.
	for (uint32_t i = 0; i < n_keys; i++) {
		as_key* key = &batch->keys.entries[i];
		
		if (callback) {
			as_batch_read* result = &results[i];
			result->key = key;
			result->result = AEROSPIKE_ERR_RECORD_NOT_FOUND;
			as_record_init(&result->record, 0);
		}
		
		status = as_key_set_digest(err, key);
		
		if (status != AEROSPIKE_OK) {
			as_batch_release_nodes(batch_nodes, n_batch_nodes);
			as_nodes_release(nodes);
			return status;
		}
		
		as_node* node = as_node_get(cluster, key->ns, key->digest.value, false, AS_POLICY_REPLICA_MASTER);
		
		if (! node) {
			as_batch_release_nodes(batch_nodes, n_batch_nodes);
			as_nodes_release(nodes);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find batch node for key.");
		}
		
		if (! as_batch_use_new(policy, node)) {
			// Batch direct only supports batch commands with all keys in the same namespace.
			if (strcmp(ns, key->ns)) {
				as_batch_release_nodes(batch_nodes, n_batch_nodes);
				as_nodes_release(nodes);
				return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Batch keys must all be in the same namespace.");
			}
		}
		
		as_batch_node* batch_node = as_batch_node_find(batch_nodes, n_batch_nodes, node);
		
		if (batch_node) {
			// Release duplicate node
			as_node_release(node);
		}
		else {
			// Add batch node.
			batch_node = &batch_nodes[n_batch_nodes++];
			batch_node->node = node;  // Transfer node
			as_vector_inita(&batch_node->offsets, sizeof(uint32_t), offsets_capacity);
		}
		as_vector_append(&batch_node->offsets, &i);
	}
	as_nodes_release(nodes);
	
	uint32_t error_mutex = 0;
	
	// Initialize task.
	as_batch_task task;
	memset(&task, 0, sizeof(as_batch_task));
	task.cluster = cluster;
	task.ns = ns;
	task.err = err;
	task.results = results;
	task.error_mutex = &error_mutex;
	task.n_keys = n_keys;
	task.bins = bins;
	task.n_bins = n_bins;
	task.keys = batch->keys.entries;
	task.timeout_ms = policy->timeout;
	task.index = 0;
	task.retry = 0;
	task.read_attr = read_attr;
	task.use_batch_records = false;
	task.allow_inline = policy->allow_inline;
	task.send_set_name = policy->send_set_name;
	task.deserialize = policy->deserialize;
	task.udata = udata;
	task.callback_xdr = callback_xdr;

	if (policy->concurrent && n_batch_nodes > 1) {
		// Run batch requests in parallel in separate threads.
		task.complete_q = cf_queue_create(sizeof(as_batch_complete_task), true);
		
		uint32_t n_wait_nodes = n_batch_nodes;
		
		// Run task for each node.
		for (uint32_t i = 0; i < n_batch_nodes; i++) {
			// Stack allocate task for each node.  It should be fine since the task
			// only needs to be valid within this function.
			as_batch_task* task_node = alloca(sizeof(as_batch_task));
			memcpy(task_node, &task, sizeof(as_batch_task));
			
			as_batch_node* batch_node = &batch_nodes[i];
			task_node->use_new_batch = as_batch_use_new(policy, batch_node->node);
			task_node->node = batch_node->node;
			memcpy(&task_node->offsets, &batch_node->offsets, sizeof(as_vector));
			
			int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_batch_worker, task_node);
			
			if (rc) {
				// Thread could not be added. Abort entire batch.
				if (ck_pr_fas_32(task.error_mutex, 1) == 0) {
					status = as_error_update(task.err, AEROSPIKE_ERR_CLIENT, "Failed to add batch thread: %d", rc);
				}
				
				// Reset node count to threads that were run.
				n_wait_nodes = i;
				break;
			}
		}
		
		// Wait for tasks to complete.
		for (uint32_t i = 0; i < n_wait_nodes; i++) {
			as_batch_complete_task complete;
			cf_queue_pop(task.complete_q, &complete, CF_QUEUE_FOREVER);
			
			if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
				status = complete.result;
			}
		}
		
		// Release temporary queue.
		cf_queue_destroy(task.complete_q);
	}
	else {
		// Run batch requests sequentially in same thread.
		for (uint32_t i = 0; status == AEROSPIKE_OK && i < n_batch_nodes; i++) {
			as_batch_node* batch_node = &batch_nodes[i];
			
			task.use_new_batch = as_batch_use_new(policy, batch_node->node);
			task.node = batch_node->node;
			task.index = 0;
			memcpy(&task.offsets, &batch_node->offsets, sizeof(as_vector));
			status = as_batch_command_execute(&task);
		}
	}
			
	// Release each node.
	as_batch_release_nodes(batch_nodes, n_batch_nodes);

	// Call user defined function with results.
	if (callback) {
		callback(task.results, n_keys, udata);
		
		// Destroy records. User is responsible for destroying keys with as_batch_destroy().
		for (uint32_t i = 0; i < n_keys; i++) {
			if (task.results[i].result == AEROSPIKE_OK) {
				as_record_destroy(&task.results[i].record);
			}
		}
	}
	return status;
}

static as_status
as_batch_read_execute_sync(
	as_cluster* cluster, as_error* err, const as_policy_batch* policy, as_vector* records,
	uint32_t n_keys, uint32_t n_batch_nodes, as_batch_node* batch_nodes
	)
{
	as_status status = AEROSPIKE_OK;
	uint32_t error_mutex = 0;

	// Initialize task.
	as_batch_task task;
	memset(&task, 0, sizeof(as_batch_task));
	task.cluster = cluster;
	task.err = err;
	task.records = records;
	task.error_mutex = &error_mutex;
	task.n_keys = n_keys;
	task.timeout_ms = policy->timeout;
	task.retry = 0;
	task.use_batch_records = true;
	task.allow_inline = policy->allow_inline;
	task.send_set_name = policy->send_set_name;
	task.deserialize = policy->deserialize;

	if (policy->concurrent && n_batch_nodes > 1) {
		// Run batch requests in parallel in separate threads.
		task.complete_q = cf_queue_create(sizeof(as_batch_complete_task), true);
		
		uint32_t n_wait_nodes = n_batch_nodes;
		
		// Run task for each node.
		for (uint32_t i = 0; i < n_batch_nodes; i++) {
			// Stack allocate task for each node.  It should be fine since the task
			// only needs to be valid within this function.
			as_batch_task* task_node = alloca(sizeof(as_batch_task));
			memcpy(task_node, &task, sizeof(as_batch_task));
			
			as_batch_node* batch_node = &batch_nodes[i];
			task_node->use_new_batch = true;
			task_node->node = batch_node->node;
			memcpy(&task_node->offsets, &batch_node->offsets, sizeof(as_vector));
			
			int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_batch_worker, task_node);
			
			if (rc) {
				// Thread could not be added. Abort entire batch.
				if (ck_pr_fas_32(task.error_mutex, 1) == 0) {
					status = as_error_update(task.err, AEROSPIKE_ERR_CLIENT, "Failed to add batch thread: %d", rc);
				}
				
				// Reset node count to threads that were run.
				n_wait_nodes = i;
				break;
			}
		}
		
		// Wait for tasks to complete.
		for (uint32_t i = 0; i < n_wait_nodes; i++) {
			as_batch_complete_task complete;
			cf_queue_pop(task.complete_q, &complete, CF_QUEUE_FOREVER);
			
			if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
				status = complete.result;
			}
		}
		
		// Release temporary queue.
		cf_queue_destroy(task.complete_q);
	}
	else {
		// Run batch requests sequentially in same thread.
		for (uint32_t i = 0; status == AEROSPIKE_OK && i < n_batch_nodes; i++) {
			as_batch_node* batch_node = &batch_nodes[i];
			
			task.use_new_batch = true;
			task.node = batch_node->node;
			memcpy(&task.offsets, &batch_node->offsets, sizeof(as_vector));
			status = as_batch_command_execute(&task);
		}
	}
	
	// Release each node.
	as_batch_release_nodes(batch_nodes, n_batch_nodes);
	return status;
}

static as_status
as_batch_read_execute_async(
	as_cluster* cluster, as_error* err, const as_policy_batch* policy, as_vector* records, uint32_t n_batch_nodes,
	as_batch_node* batch_nodes, as_async_batch_executor* executor
	)
{
	as_event_executor* exec = &executor->executor;
	exec->max_concurrent = exec->max = n_batch_nodes;
	
	as_status status = AEROSPIKE_OK;
	
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_batch_node* batch_node = &batch_nodes[i];
		
		// Estimate buffer size.
		size_t size = as_batch_index_records_size(records, &batch_node->offsets, policy->send_set_name);
		
		// Allocate enough memory to cover, then, round up memory size in 8KB increments to reduce
		// fragmentation and to allow socket read to reuse buffer.
		size_t s = (sizeof(as_async_batch_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;
		as_event_command* cmd = cf_malloc(s);
		cmd->event_loop = exec->event_loop;
		cmd->conn = 0;
		cmd->cluster = cluster;
		cmd->node = batch_node->node;
		cmd->udata = executor;  // Overload udata to be the executor.
		cmd->parse_results = as_batch_async_parse_records;
		cmd->buf = ((as_async_batch_command*)cmd)->space;
		cmd->capacity = (uint32_t)(s - sizeof(as_async_batch_command));
		cmd->pos = 0;
		cmd->auth_len = 0;
		cmd->timeout_ms = policy->timeout;
		cmd->type = AS_ASYNC_TYPE_BATCH;
		cmd->state = AS_ASYNC_STATE_UNREGISTERED;
		cmd->pipe_listener = NULL;
		cmd->deserialize = policy->deserialize;
		cmd->free_buf = false;
		cmd->len = (uint32_t)as_batch_index_records_write(records, &batch_node->offsets, policy->timeout, policy->allow_inline, policy->send_set_name, cmd->buf);
		
		status = as_event_command_execute(cmd, err);
		
		if (status != AEROSPIKE_OK) {
			as_event_executor_cancel(exec, i);
			break;
		}
	}
	return status;
}

static void
as_batch_read_cleanup(
	as_async_batch_executor* async_executor, as_nodes* nodes, as_batch_node* batch_nodes,
	uint32_t n_batch_nodes
	)
{
	as_batch_release_nodes(batch_nodes, n_batch_nodes);
	as_nodes_release(nodes);
	
	if (async_executor) {
		// Destroy batch async resources.
		// Assume no async commands have been queued.
		cf_free(async_executor);
	}
}

static as_status
as_batch_read_execute(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_read_records* records,
	as_async_batch_executor* async_executor
	)
{
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	as_vector* list = &records->list;
	uint32_t n_keys = records->list.size;
	
	if (n_keys <= 0) {
		return AEROSPIKE_OK;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_batch_read_cleanup(async_executor, nodes, NULL, 0);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Batch command failed because cluster is empty.");
	}
	
	as_batch_node* batch_nodes = alloca(sizeof(as_batch_node) * n_nodes);
	uint32_t n_batch_nodes = 0;
	as_status status = AEROSPIKE_OK;
	
	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_capacity = n_keys / n_nodes;
	offsets_capacity += offsets_capacity >> 2;
	
	// The minimum key capacity is 10.
	if (offsets_capacity < 10) {
		offsets_capacity = 10;
	}
	
	// Map keys to server nodes.
	for (uint32_t i = 0; i < n_keys; i++) {
		as_batch_read_record* record = as_vector_get(list, i);
		as_key* key = &record->key;
		
		record->result = AEROSPIKE_ERR_RECORD_NOT_FOUND;
		as_record_init(&record->record, 0);
		
		status = as_key_set_digest(err, key);
		
		if (status != AEROSPIKE_OK) {
			as_batch_read_cleanup(async_executor, nodes, batch_nodes, n_batch_nodes);
			return status;
		}
		
		as_node* node = as_node_get(cluster, key->ns, key->digest.value, false, AS_POLICY_REPLICA_MASTER);
		
		if (! node) {
			as_batch_read_cleanup(async_executor, nodes, batch_nodes, n_batch_nodes);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to find batch node for key.");
		}
		
		if (! as_batch_use_new(policy, node)) {
			as_batch_read_cleanup(async_executor, nodes, batch_nodes, n_batch_nodes);
			return as_error_set_message(err, AEROSPIKE_ERR_UNSUPPORTED_FEATURE, "aerospike_batch_read() requires a server that supports new batch index protocol.");
		}
		
		as_batch_node* batch_node = as_batch_node_find(batch_nodes, n_batch_nodes, node);
		
		if (batch_node) {
			// Release duplicate node
			as_node_release(node);
		}
		else {
			// Add batch node.
			batch_node = &batch_nodes[n_batch_nodes++];
			batch_node->node = node;  // Transfer node
			
			if (n_keys <= 5000) {
				// All keys and offsets should fit on stack.
				as_vector_inita(&batch_node->offsets, sizeof(uint32_t), offsets_capacity);
			}
			else {
				// Allocate vector on heap to avoid stack overflow.
				as_vector_init(&batch_node->offsets, sizeof(uint32_t), offsets_capacity);
			}
		}
		as_vector_append(&batch_node->offsets, &i);
	}
	as_nodes_release(nodes);
	
	if (async_executor) {
		return as_batch_read_execute_async(cluster, err, policy, list, n_batch_nodes, batch_nodes, async_executor);
	}
	
	return as_batch_read_execute_sync(cluster, err, policy, list, n_keys, n_batch_nodes, batch_nodes);
}

/******************************************************************************
 *	PUBLIC FUNCTIONS
 *****************************************************************************/

bool
aerospike_has_batch_index(aerospike* as)
{
	as_nodes* nodes = as_nodes_reserve(as->cluster);
	
	if (nodes->size == 0) {
		as_nodes_release(nodes);
		return false;
	}
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		if (! nodes->array[i]->has_batch_index) {
			as_nodes_release(nodes);
			return false;
		}
	}
	as_nodes_release(nodes);
	return true;
}

as_status
aerospike_batch_read(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_read_records* records
	)
{
	as_error_reset(err);
	return as_batch_read_execute(as, err, policy, records, 0);
}

as_status
aerospike_batch_read_async(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_read_records* records,
	as_async_batch_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_error_reset(err);
	
	// Check for empty batch.
	if (records->list.size == 0) {
		listener(0, records, udata, event_loop);
		return AEROSPIKE_OK;
	}
	
	// Batch will be split up into a command for each node.
	// Allocate batch data shared by each command.
	as_async_batch_executor* executor = cf_malloc(sizeof(as_async_batch_executor));
	as_event_executor* exec = &executor->executor;
	pthread_mutex_init(&exec->lock, NULL);
	exec->commands = 0;
	exec->event_loop = as_event_assign(event_loop);
	exec->complete_fn = as_batch_complete_async;
	exec->udata = udata;
	exec->max_concurrent = 0;
	exec->max = 0;
	exec->count = 0;
	exec->valid = true;
	executor->records = records;
	executor->listener = listener;
	
	return as_batch_read_execute(as, err, policy, records, executor);
}

/**
 *	Destroy keys and records in record list.  It's the responsility of the caller to
 *	free `as_batch_read_record.bin_names` when necessary.
 */
void
as_batch_read_destroy(as_batch_read_records* records)
{
	as_vector* list = &records->list;
	
	for (uint32_t i = 0; i < list->size; i++) {
		as_batch_read_record* record = as_vector_get(list, i);
		
		// Destroy key.
		as_key_destroy(&record->key);
		
		// Destroy record if exists.
		if (record->result == AEROSPIKE_OK) {
			as_record_destroy(&record->record);
		}
	}
	as_vector_destroy(list);
}

/**
 *	Look up multiple records by key, then return all bins.
 */
as_status
aerospike_batch_get(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_execute(as, err, policy, batch, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL, 0, 0, callback, 0, udata);
}

/**
 *	@private
 *	Perform batch reads for XDR.  The callback will be called for each record as soon as it's
 *	received in no particular order.
 */
as_status
aerospike_batch_get_xdr(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_callback_xdr callback, void* udata
	)
{
	return as_batch_execute(as, err, policy, batch, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL, 0, 0, 0, callback, udata);
}

/**
 *	Look up multiple records by key, then return specified bins.
 */
as_status
aerospike_batch_get_bins(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	const char** bins, uint32_t n_bins, aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_execute(as, err, policy, batch, AS_MSG_INFO1_READ, bins, n_bins, callback, 0, udata);
}

/**
 *	Test whether multiple records exist in the cluster.
 */
as_status
aerospike_batch_exists(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_execute(as, err, policy, batch, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA, 0, 0, callback, 0, udata);
}
