/*
 * Copyright 2008-2023 Aerospike, Inc.
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
#include <aerospike/as_exp.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_thread_pool.h>
#include <aerospike/as_val.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>

//---------------------------------
// Constants
//---------------------------------

 #define BATCH_MSG_READ 0x0
 #define BATCH_MSG_REPEAT 0x1
 #define BATCH_MSG_INFO 0x2
 #define BATCH_MSG_WRITE 0xe

 #define BATCH_TYPE_RECORDS 0
 #define BATCH_TYPE_KEYS 1
 #define BATCH_TYPE_KEYS_NO_CALLBACK 2

//---------------------------------
// Types
//---------------------------------

typedef struct {
	size_t size;
	as_exp* filter_exp;
	as_queue* buffers;
	uint16_t field_count_header;
	uint8_t read_attr; // old batch only
	bool batch_any;
} as_batch_builder;

typedef struct {
	as_exp* filter_exp;
	int ttl;
	uint16_t gen;
	uint8_t read_attr;
	uint8_t write_attr;
	uint8_t info_attr;
	bool has_write;
	bool send_key;
} as_batch_attr;

typedef struct {
	as_policy_replica replica;
	as_policy_replica replica_sc;
	uint8_t replica_index;
	uint8_t replica_index_sc;
} as_batch_replica;

typedef struct as_batch_node_s {
	as_node* node;
	as_vector offsets;
} as_batch_node;

typedef struct as_batch_task_s {
	as_node* node;
	as_vector offsets;
	as_cluster* cluster;
	const as_policy_batch* policy;
	as_error* err;
	uint32_t* error_mutex;
	bool* error_row;
	cf_queue* complete_q;
	uint32_t n_keys;
	as_policy_replica replica;
	as_policy_replica replica_sc;
	uint8_t replica_index;
	uint8_t replica_index_sc;
	uint8_t type;
	bool has_write;
} as_batch_task;

typedef struct as_batch_task_records_s {
	as_batch_task base;
	as_vector* records;
} as_batch_task_records;

typedef struct as_batch_task_keys_s {
	as_batch_task base;
	const char* ns;
	as_key* keys;
	const as_batch* batch;
	as_batch_result* results;
	as_batch_listener listener;
	void* udata;
	as_batch_base_record* rec;
	as_batch_attr* attr;
} as_batch_task_keys;

typedef struct as_batch_complete_task_s {
	as_node* node;
	as_status result;
} as_batch_complete_task;

typedef struct {
	as_event_executor executor;
	as_batch_records* records;
	as_async_batch_listener listener;
	as_policy_replica replica;
	as_policy_replica replica_sc;
	as_policy_read_mode_sc read_mode_sc;
	bool has_write;
	bool error_row;
} as_async_batch_executor;

typedef struct as_async_batch_command {
	as_event_command command;
	uint8_t* ubuf;
	uint32_t ubuf_size;
	uint32_t pad;
	uint8_t space[];
} as_async_batch_command;

typedef struct {
	as_node* node;
	as_vector offsets;
	uint32_t size;
	bool can_repeat;
} as_batch_retry_node;

typedef struct {
	uint8_t* begin;
	uint8_t* copy;
	uint32_t size;
} as_batch_retry_offset;

//---------------------------------
// Static Variables
//---------------------------------

// These values must line up with as_operator enum.
bool as_op_is_write[] = {
	false,
	true,
	false,
	true,
	false,
	true,
	true,
	false,
	true,
	true,
	true,
	true,
	false,
	true,
	true,
	false,
	true
};

static const char cluster_empty_error[] = "Batch command failed because cluster is empty.";

//---------------------------------
// Static Functions
//---------------------------------

static uint8_t*
as_batch_parse_fields(uint8_t* p, uint32_t n_fields)
{
	uint32_t len;
	
	for (uint32_t i = 0; i < n_fields; i++) {
		len = cf_swap_from_be32(*(uint32_t*)p);
		p += 4 + len;
	}
	return p;
}

static inline as_status
as_batch_parse_record(uint8_t** pp, as_error* err, as_msg* msg, as_record* rec, bool deserialize)
{
	as_record_init(rec, msg->n_ops);
	rec->gen = msg->generation;
	rec->ttl = cf_server_void_time_to_ttl(msg->record_ttl);

	as_status status = as_command_parse_bins(pp, err, rec, msg->n_ops, deserialize);

	if (status != AEROSPIKE_OK) {
		as_record_destroy(rec);
		return status;
	}
	return AEROSPIKE_OK;
}

static void
as_batch_complete_async(as_event_executor* executor)
{
	if (executor->notify) {
		as_async_batch_executor* e = (as_async_batch_executor*)executor;

		if (e->error_row && ! executor->err) {
			as_error err;
			as_error_set_message(&err, AEROSPIKE_BATCH_FAILED, "One or more batch sub-commands failed");
			e->listener(&err, e->records, executor->udata, executor->event_loop);
		}
		else {
			e->listener(executor->err, e->records, executor->udata, executor->event_loop);
		}
	}
}

static inline bool
as_batch_set_error_row(uint8_t res)
{
	return res != AEROSPIKE_ERR_RECORD_NOT_FOUND && res != AEROSPIKE_FILTERED_OUT;
}

static inline void
as_batch_destroy_ubuf(as_async_batch_command* bc)
{
	if (bc->ubuf) {
		cf_free(bc->ubuf);
		bc->ubuf = NULL;
	}
}

static inline bool
as_batch_in_doubt(bool has_write, uint32_t sent)
{
	return has_write && sent > 1;
}

static bool
as_batch_async_parse_records(as_event_command* cmd)
{
	as_error err;
	uint8_t* p = cmd->buf + cmd->pos;
	uint8_t* end = cmd->buf + cmd->len;
	as_async_batch_executor* executor = cmd->udata;  // udata is overloaded to contain executor.
	as_vector* records = &executor->records->list;

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			if (msg->result_code != AEROSPIKE_OK) {
				as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
				as_event_response_error(cmd, &err);
				return true;
			}
			as_batch_destroy_ubuf((as_async_batch_command*)cmd);
			as_event_batch_complete(cmd);
			return true;
		}
		
		uint32_t offset = msg->transaction_ttl; // overloaded to contain batch index
		
		if (offset >= records->size) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Batch index %u >= batch size: %u",
							offset, records->size);
			as_event_response_error(cmd, &err);
			return true;
		}
		
		p = as_batch_parse_fields(p, msg->n_fields);
		
		as_batch_base_record* rec = as_vector_get(records, offset);
		rec->result = msg->result_code;

		if (msg->result_code == AEROSPIKE_OK) {
			as_status status = as_batch_parse_record(&p, &err, msg, &rec->record,
													 cmd->flags & AS_ASYNC_FLAGS_DESERIALIZE);

			if (status != AEROSPIKE_OK) {
				as_event_response_error(cmd, &err);
				return true;
			}
		}
		else if (msg->result_code == AEROSPIKE_ERR_UDF) {
			rec->in_doubt = as_batch_in_doubt(rec->has_write, cmd->command_sent_counter);
			executor->error_row = true;

			// AEROSPIKE_ERR_UDF results in "FAILURE" bin that contains an error message.
			as_status status = as_batch_parse_record(&p, &err, msg, &rec->record,
													 cmd->flags & AS_ASYNC_FLAGS_DESERIALIZE);

			if (status != AEROSPIKE_OK) {
				as_event_response_error(cmd, &err);
				return true;
			}
		}
		else if (as_batch_set_error_row(msg->result_code)) {
			rec->in_doubt = as_batch_in_doubt(rec->has_write, cmd->command_sent_counter);
			executor->error_row = true;
		}
	}
	return false;
}

static as_status
as_batch_parse_records(as_error* err, as_command* cmd, as_node* node, uint8_t* buf, size_t size)
{
	as_batch_task* task = cmd->udata;
	bool deserialize = task->policy->deserialize;

	uint8_t* p = buf;
	uint8_t* end = buf + size;

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			if (msg->result_code != AEROSPIKE_OK) {
				return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
			}
			return AEROSPIKE_NO_MORE_RECORDS;
		}

		uint32_t offset = msg->transaction_ttl;  // overloaded to contain batch index

		if (offset >= task->n_keys) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Batch index %u >= batch size: %u",
								   offset, task->n_keys);
		}

		p = as_batch_parse_fields(p, msg->n_fields);

		switch (task->type)
		{
			case BATCH_TYPE_RECORDS: {
				as_batch_task_records* btr = (as_batch_task_records*)task;
				as_batch_base_record* rec = as_vector_get(btr->records, offset);
				rec->result = msg->result_code;

				if (msg->result_code == AEROSPIKE_OK) {
					as_status status = as_batch_parse_record(&p, err, msg, &rec->record, deserialize);

					if (status != AEROSPIKE_OK) {
						return status;
					}
				}
				else if (msg->result_code == AEROSPIKE_ERR_UDF) {
					rec->in_doubt = as_batch_in_doubt(rec->has_write, cmd->sent);
					*task->error_row = true;

					// AEROSPIKE_ERR_UDF results in "FAILURE" bin that contains an error message.
					as_status status = as_batch_parse_record(&p, err, msg, &rec->record, deserialize);

					if (status != AEROSPIKE_OK) {
						return status;
					}
				}
				else if (as_batch_set_error_row(msg->result_code)) {
					rec->in_doubt = as_batch_in_doubt(rec->has_write, cmd->sent);
					*task->error_row = true;
				}
				break;
			}

			case BATCH_TYPE_KEYS: {
				as_batch_task_keys* btk = (as_batch_task_keys*)task;
				as_batch_result* res = &btk->results[offset];
				res->result = msg->result_code;

				if (msg->result_code == AEROSPIKE_OK) {
					as_status status = as_batch_parse_record(&p, err, msg, &res->record, deserialize);

					if (status != AEROSPIKE_OK) {
						return status;
					}
				}
				else if (msg->result_code == AEROSPIKE_ERR_UDF) {
					res->in_doubt = as_batch_in_doubt(task->has_write, cmd->sent);
					*task->error_row = true;
					as_status status = as_batch_parse_record(&p, err, msg, &res->record, deserialize);

					if (status != AEROSPIKE_OK) {
						return status;
					}
				}
				else if (as_batch_set_error_row(msg->result_code)) {
					res->in_doubt = as_batch_in_doubt(task->has_write, cmd->sent);
					*task->error_row = true;
				}
				break;
			}

			case BATCH_TYPE_KEYS_NO_CALLBACK: {
				as_record rec;

				if (msg->result_code == AEROSPIKE_OK) {
					as_status status = as_batch_parse_record(&p, err, msg, &rec, deserialize);

					if (status != AEROSPIKE_OK) {
						return status;
					}
				}
				else if (msg->result_code == AEROSPIKE_ERR_UDF) {
					*task->error_row = true;
					as_status status = as_batch_parse_record(&p, err, msg, &rec, deserialize);

					if (status != AEROSPIKE_OK) {
						return status;
					}
				}
				else if (as_batch_set_error_row(msg->result_code)) {
					*task->error_row = true;
				}
				as_record_destroy(&rec);
				break;
			}
		}
	}
	return AEROSPIKE_OK;
}

static inline uint8_t
as_batch_get_flags(const as_policy_batch* policy) {
	uint8_t flags = 0x8;

	if (policy->allow_inline) {
		flags |= 0x1;
	}

	if (policy->allow_inline_ssd) {
		flags |= 0x2;
	}

	if (policy->respond_all_keys) {
		flags |= 0x4;
	}
	return flags;
}

static as_status
as_batch_estimate_ops(
	const as_operations* ops, as_error* err, as_queue* buffers, size_t* sp, bool allow_write
	)
{
	size_t size = 0;
	uint32_t n_operations = ops->binops.size;

	if (n_operations == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No operations defined");
	}

	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];

		if (as_op_is_write[op->op]) {
			if (! allow_write) {
				return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
											"Write operations not allowed in batch read");
			}
			size += 6; // Extra write specific fields.
		}

		as_status status = as_command_bin_size(&op->bin, buffers, &size, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	*sp = size;
	return AEROSPIKE_OK;
}

static inline uint8_t*
as_batch_write_fields(
	uint8_t* p, as_key* key, uint16_t n_fields, uint16_t n_ops
	)
{
	n_fields += 2;
	*(uint16_t*)p = cf_swap_to_be16(n_fields);
	p += sizeof(uint16_t);
	*(uint16_t*)p = cf_swap_to_be16(n_ops);
	p += sizeof(uint16_t);
	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);
	p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
	return p;
}

static inline uint8_t*
as_batch_write_ops(uint8_t* p, const as_operations* ops, as_queue* buffers)
{
	uint32_t n_operations = ops->binops.size;

	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];
		p = as_command_write_bin(p, op->op, &op->bin, buffers);
	}
	return p;
}

static size_t
as_batch_trailer_write(uint8_t* cmd, uint8_t* p, uint8_t* batch_field)
{
	// Write batch field size.
	size_t size = p - batch_field - 4;
	*(uint32_t*)batch_field = cf_swap_to_be32((uint32_t)size);
	return as_command_write_end(cmd, p);
}

//-----------------------------
// Old Batch Protocol Functions
//-----------------------------

static as_status
as_batch_read_record_size_old(
	as_key* key, as_batch_read_record* rec, as_batch_builder* bb, as_error* err
	)
{
	bb->size += 6;
	bb->size += as_command_string_field_size(key->ns);
	bb->size += as_command_string_field_size(key->set);

	if (rec->n_bin_names) {
		for (uint32_t i = 0; i < rec->n_bin_names; i++) {
			bb->size += as_command_string_operation_size(rec->bin_names[i]);
		}
	}
	else if (rec->ops) {
		size_t s = 0;
		as_status status = as_batch_estimate_ops(rec->ops, err, bb->buffers, &s, false);

		if (status != AEROSPIKE_OK) {
			return status;
		}
		bb->size += s;
	}
	return AEROSPIKE_OK;
}

static as_status
as_batch_records_size_old(
	as_vector* records, as_vector* offsets, as_batch_builder* bb, as_error* err
	)
{
	as_batch_read_record* prev = 0;
	uint32_t n_offsets = offsets->size;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		as_batch_read_record* rec = as_vector_get(records, offset);
		
		if (rec->type != AS_BATCH_READ) {
			return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
										"Batch write operations not supported on older servers");
		}

		bb->size += AS_DIGEST_VALUE_SIZE + sizeof(uint32_t);

		if (prev && strcmp(prev->key.ns, rec->key.ns) == 0 &&
			strcmp(prev->key.set, rec->key.set) == 0 &&
			prev->bin_names == rec->bin_names && prev->read_all_bins == rec->read_all_bins &&
			prev->ops == rec->ops) {
			// Can set repeat flag to save space.
			bb->size++;
		}
		else {
			// Size full message.
			as_status status = as_batch_read_record_size_old(&rec->key, rec, bb, err);

			if (status != AEROSPIKE_OK) {
				return status;
			}
			prev = rec;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_batch_keys_size_old(
	as_key* keys, as_vector* offsets, as_batch_read_record* rec, as_batch_builder* bb, as_error* err
	)
{
	as_key* prev = 0;
	uint32_t n_offsets = offsets->size;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		as_key* key = &keys[offset];

		bb->size += AS_DIGEST_VALUE_SIZE + sizeof(uint32_t);

		if (prev && strcmp(prev->ns, key->ns) == 0 && strcmp(prev->set, key->set) == 0) {
			// Can set repeat flag to save space.
			bb->size++;
		}
		else {
			// Size full message.
			as_status status = as_batch_read_record_size_old(key, rec, bb, err);

			if (status != AEROSPIKE_OK) {
				return status;
			}
			prev = key;
		}
	}
	return AEROSPIKE_OK;
}

static uint8_t*
as_batch_header_write_old(
	uint8_t* p, const as_policy_batch* policy, uint32_t n_offsets, as_batch_builder* bb
	)
{
	bb->read_attr = AS_MSG_INFO1_READ;

	if (policy->read_mode_ap == AS_POLICY_READ_MODE_AP_ALL) {
		bb->read_attr |= AS_MSG_INFO1_READ_MODE_AP_ALL;
	}

	p = as_command_write_header_read(p, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, policy->base.total_timeout, bb->field_count_header, 0,
		bb->read_attr | AS_MSG_INFO1_BATCH_INDEX, 0);

	if (bb->filter_exp) {
		p = as_exp_write(bb->filter_exp, p);
	}
	return p;
}

static size_t
as_batch_records_write_old(
	const as_policy_batch* policy, as_vector* records, as_vector* offsets, as_batch_builder* bb,
	uint8_t* cmd
	)
{
	uint32_t n_offsets = offsets->size;
	uint8_t* p = as_batch_header_write_old(cmd, policy, n_offsets, bb);

	uint8_t* batch_field = p;
	p = as_command_write_field_header(p, AS_FIELD_BATCH_INDEX, 0);
	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = policy->allow_inline? 1 : 0;

	as_batch_read_record* prev = 0;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);
		
		as_batch_read_record* rec = as_vector_get(records, offset);
		memcpy(p, rec->key.digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		
		if (prev && strcmp(prev->key.ns, rec->key.ns) == 0 &&
			strcmp(prev->key.set, rec->key.set) == 0 &&
			prev->bin_names == rec->bin_names && prev->read_all_bins == rec->read_all_bins &&
			prev->ops == rec->ops) {
			// Can set repeat flag to save space.
			*p++ = BATCH_MSG_REPEAT;
		}
		else {
			// Write full message.
			*p++ = BATCH_MSG_READ;
			
			if (rec->bin_names) {
				*p++ = bb->read_attr;
				p = as_batch_write_fields(p, &rec->key, 0, (uint16_t)rec->n_bin_names);

				for (uint32_t i = 0; i < rec->n_bin_names; i++) {
					p = as_command_write_bin_name(p, rec->bin_names[i]);
				}
			}
			else if (rec->ops) {
				*p++ = bb->read_attr;
				p = as_batch_write_fields(p, &rec->key, 0, rec->ops->binops.size);
				p = as_batch_write_ops(p, rec->ops, bb->buffers);
			}
			else {
				*p++ = (bb->read_attr | (rec->read_all_bins? AS_MSG_INFO1_GET_ALL :
														 AS_MSG_INFO1_GET_NOBINDATA));

				p = as_batch_write_fields(p, &rec->key, 0, 0);
			}
			prev = rec;
		}
	}
	return as_batch_trailer_write(cmd, p, batch_field);
}

static size_t
as_batch_keys_write_old(
	const as_policy_batch* policy, as_key* keys, as_vector* offsets, as_batch_read_record* rec,
	as_batch_attr* attr, as_batch_builder* bb, uint8_t* cmd
	)
{
	uint32_t n_offsets = offsets->size;
	uint8_t* p = as_batch_header_write_old(cmd, policy, n_offsets, bb);

	uint8_t* batch_field = p;
	p = as_command_write_field_header(p, AS_FIELD_BATCH_INDEX, 0);
	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = policy->allow_inline? 1 : 0;

	as_key* prev = 0;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);

		as_key* key = &keys[offset];
		memcpy(p, key->digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;

		if (prev && strcmp(prev->ns, key->ns) == 0 && strcmp(prev->set, key->set) == 0) {
			// Can set repeat flag to save space.
			*p++ = BATCH_MSG_REPEAT;
		}
		else {
			// Write full message.
			*p++ = BATCH_MSG_READ;
			
			if (rec->bin_names) {
				*p++ = bb->read_attr;
				p = as_batch_write_fields(p, key, 0, (uint16_t)rec->n_bin_names);

				for (uint32_t i = 0; i < rec->n_bin_names; i++) {
					p = as_command_write_bin_name(p, rec->bin_names[i]);
				}
			}
			else if (rec->ops) {
				*p++ = bb->read_attr;
				p = as_batch_write_fields(p, key, 0, rec->ops->binops.size);
				p = as_batch_write_ops(p, rec->ops, bb->buffers);
			}
			else {
				*p++ = (bb->read_attr | attr->read_attr);
				p = as_batch_write_fields(p, key, 0, 0);
			}
			prev = key;
		}
	}
	return as_batch_trailer_write(cmd, p, batch_field);
}

//-----------------------------
// New Batch Protocol Functions
//-----------------------------

static uint8_t*
as_batch_header_write_new(
	uint8_t* p, const as_policy_batch* policy, uint32_t n_offsets, as_batch_builder* bb
	)
{
	uint8_t read_attr = AS_MSG_INFO1_BATCH_INDEX;

	if (policy->base.compress) {
		read_attr |= AS_MSG_INFO1_COMPRESS_RESPONSE;
	}

	p += 8;
	*p++ = 22;
	*p++ = read_attr;
	memset(p, 0, 12);
	p += 12;
	*(uint32_t*)p = cf_swap_to_be32(policy->base.total_timeout);
	p += sizeof(uint32_t);
	*(uint16_t*)p = cf_swap_to_be16(bb->field_count_header);
	p += sizeof(uint16_t);
	*(uint16_t*)p = 0;
	p += sizeof(uint16_t);

	if (bb->filter_exp) {
		p = as_exp_write(bb->filter_exp, p);
	}
	return p;
}

static inline bool
as_batch_equals_read(as_batch_read_record* prev, as_batch_read_record* rec)
{
	return prev->bin_names == rec->bin_names && prev->ops == rec->ops &&
		   prev->policy == rec->policy && prev->read_all_bins == rec->read_all_bins;
}

static inline bool
as_batch_equals_write(as_batch_write_record* prev, as_batch_write_record* rec)
{
	return prev->ops == rec->ops && prev->policy == rec->policy;
}

static inline bool
as_batch_equals_apply(as_batch_apply_record* prev, as_batch_apply_record* rec)
{
	return prev->function == rec->function && prev->arglist == rec->arglist &&
		   prev->module == rec->module && prev->policy == rec->policy;
}

static inline bool
as_batch_equals_remove(as_batch_remove_record* prev, as_batch_remove_record* rec)
{
	return prev->policy == rec->policy;
}

static bool
as_batch_equals(as_batch_base_record* prev, as_batch_base_record* rec, as_batch_builder* bb)
{
	if (! (prev && prev->type == rec->type && strcmp(prev->key.ns, rec->key.ns) == 0 &&
		strcmp(prev->key.set, rec->key.set) == 0)) {
		return false;
	}

	switch (rec->type) {
	case AS_BATCH_READ:
		return as_batch_equals_read((as_batch_read_record*)prev, (as_batch_read_record*)rec);

	case AS_BATCH_WRITE:
		return as_batch_equals_write((as_batch_write_record*)prev, (as_batch_write_record*)rec);

	case AS_BATCH_APPLY:
		return as_batch_equals_apply((as_batch_apply_record*)prev, (as_batch_apply_record*)rec);

	case AS_BATCH_REMOVE:
		return as_batch_equals_remove((as_batch_remove_record*)prev, (as_batch_remove_record*)rec);

	default:
		return false;
	}
}

static as_status
as_batch_read_record_size(
	as_key* key, as_batch_read_record* rec, as_batch_builder* bb, as_error* err
	)
{
	if (rec->policy) {
		if (rec->policy->filter_exp) {
			bb->size += rec->policy->filter_exp->packed_sz + AS_FIELD_HEADER_SIZE;
		}
	}

	if (rec->bin_names) {
		for (uint32_t j = 0; j < rec->n_bin_names; j++) {
			bb->size += as_command_string_operation_size(rec->bin_names[j]);
		}
	}
	else if (rec->ops) {
		size_t s = 0;
		as_status status = as_batch_estimate_ops(rec->ops, err, bb->buffers, &s, false);

		if (status != AEROSPIKE_OK) {
			return status;
		}
		bb->size += s;
	}
	return AEROSPIKE_OK;
}

static as_status
as_batch_write_record_size(
	as_key* key, as_batch_write_record* rec, as_batch_builder* bb, as_error* err
	)
{
	bb->size += 6; // gen(2) + ttl(4) = 6

	if (rec->policy) {
		if (rec->policy->filter_exp) {
			bb->size += rec->policy->filter_exp->packed_sz + AS_FIELD_HEADER_SIZE;
		}

		if (rec->policy->key == AS_POLICY_KEY_SEND) {
			bb->size += as_command_user_key_size(key);
		}
	}

	bool has_write = false;

	as_operations* ops = rec->ops;
	uint32_t n_operations = ops->binops.size;

	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];

		if (as_op_is_write[op->op]) {
			has_write = true;
		}

		as_status status = as_command_bin_size(&op->bin, bb->buffers, &bb->size, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}

	if (! has_write) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
									"Batch write operations do not contain a write");
	}
	return AEROSPIKE_OK;
}

static void
as_batch_apply_record_size(as_key* key, as_batch_apply_record* rec, as_batch_builder* bb)
{
	bb->size += 6; // gen(2) + ttl(4) = 6

	if (rec->policy) {
		if (rec->policy->filter_exp) {
			bb->size += rec->policy->filter_exp->packed_sz + AS_FIELD_HEADER_SIZE;
		}

		if (rec->policy->key == AS_POLICY_KEY_SEND) {
			bb->size += as_command_user_key_size(key);
		}
	}

	bb->size += as_command_string_field_size(rec->module);
	bb->size += as_command_string_field_size(rec->function);

	as_buffer buffer;
	as_serializer ser;
	as_msgpack_init(&ser);
	as_serializer_serialize(&ser, (as_val*)rec->arglist, &buffer);
	as_serializer_destroy(&ser);
	as_queue_push(bb->buffers, &buffer);
	bb->size += as_command_field_size(buffer.size);
}

static void
as_batch_remove_record_size(as_key* key, as_batch_remove_record* rec, as_batch_builder* bb)
{
	bb->size += 6; // gen(2) + ttl(4) = 6

	if (rec->policy) {
		if (rec->policy->filter_exp) {
			bb->size += rec->policy->filter_exp->packed_sz + AS_FIELD_HEADER_SIZE;
		}

		if (rec->policy->key == AS_POLICY_KEY_SEND) {
			bb->size += as_command_user_key_size(key);
		}
	}
}

static as_status
as_batch_record_size(
	as_key* key, as_batch_base_record* rec, as_batch_builder* bb, as_error* err
	)
{
	bb->size += 8;
	bb->size += as_command_string_field_size(key->ns);
	bb->size += as_command_string_field_size(key->set);

	switch (rec->type) {
	case AS_BATCH_READ:
		return as_batch_read_record_size(key, (as_batch_read_record*)rec, bb, err);
	case AS_BATCH_WRITE:
		return as_batch_write_record_size(key, (as_batch_write_record*)rec, bb, err);
	case AS_BATCH_APPLY:
		as_batch_apply_record_size(key, (as_batch_apply_record*)rec, bb);
		return AEROSPIKE_OK;
	case AS_BATCH_REMOVE:
		as_batch_remove_record_size(key, (as_batch_remove_record*)rec, bb);
		return AEROSPIKE_OK;
	default:
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid batch rec type: %u", rec->type);
	}
}

static as_status
as_batch_records_size_new(
	as_vector* records, as_vector* offsets, as_batch_builder* bb, as_error* err
	)
{
	as_batch_base_record* prev = 0;
	as_status status;
	uint32_t n_offsets = offsets->size;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		as_batch_base_record* rec = as_vector_get(records, offset);
		
		bb->size += AS_DIGEST_VALUE_SIZE + sizeof(uint32_t);

		if (as_batch_equals(prev, rec, bb)) {
			// Can set repeat flag to save space.
			bb->size++;
		}
		else {
			// Size full message.
			status = as_batch_record_size(&rec->key, rec, bb, err);

			if (status != AEROSPIKE_OK) {
				return status;
			}
			prev = rec;
		}
	}
	return AEROSPIKE_OK;
}

static void
as_batch_init_size(as_batch_builder* bb)
{
	bb->size = AS_HEADER_SIZE + AS_FIELD_HEADER_SIZE + sizeof(uint32_t) + 1;

	if (bb->filter_exp) {
		bb->size += AS_FIELD_HEADER_SIZE + bb->filter_exp->packed_sz;
		bb->field_count_header = 2;
	}
	else {
		bb->field_count_header = 1;
	}
}

static as_status
as_batch_records_size(as_vector* records, as_vector* offsets, as_batch_builder* bb, as_error* err)
{
	as_batch_init_size(bb);

	if (bb->batch_any) {
		return as_batch_records_size_new(records, offsets, bb, err);
	}
	else {
		return as_batch_records_size_old(records, offsets, bb, err);
	}
}

static void
as_batch_attr_read_header(as_batch_attr* attr, const as_policy_batch* p)
{
	attr->filter_exp = NULL;
	attr->read_attr = AS_MSG_INFO1_READ;

	if (p->read_mode_ap == AS_POLICY_READ_MODE_AP_ALL) {
		attr->read_attr |= AS_MSG_INFO1_READ_MODE_AP_ALL;
	}

	attr->write_attr = 0;

	switch (p->read_mode_sc) {
	default:
	case AS_POLICY_READ_MODE_SC_SESSION:
		attr->info_attr = 0;
		break;
	case AS_POLICY_READ_MODE_SC_LINEARIZE:
		attr->info_attr = AS_MSG_INFO3_SC_READ_TYPE;
		break;
	case AS_POLICY_READ_MODE_SC_ALLOW_REPLICA:
		attr->info_attr = AS_MSG_INFO3_SC_READ_RELAX;
		break;
	case AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE:
		attr->info_attr = AS_MSG_INFO3_SC_READ_TYPE | AS_MSG_INFO3_SC_READ_RELAX;
		break;
	}
	attr->ttl = 0;
	attr->gen = 0;
	attr->has_write = false;
	attr->send_key = false;
}

static void
as_batch_attr_read_row(as_batch_attr* attr, const as_policy_batch_read* p)
{
	attr->filter_exp = p->filter_exp;
	attr->read_attr = AS_MSG_INFO1_READ;

	if (p->read_mode_ap == AS_POLICY_READ_MODE_AP_ALL) {
		attr->read_attr |= AS_MSG_INFO1_READ_MODE_AP_ALL;
	}

	attr->write_attr = 0;

	switch (p->read_mode_sc) {
	default:
	case AS_POLICY_READ_MODE_SC_SESSION:
		attr->info_attr = 0;
		break;
	case AS_POLICY_READ_MODE_SC_LINEARIZE:
		attr->info_attr = AS_MSG_INFO3_SC_READ_TYPE;
		break;
	case AS_POLICY_READ_MODE_SC_ALLOW_REPLICA:
		attr->info_attr = AS_MSG_INFO3_SC_READ_RELAX;
		break;
	case AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE:
		attr->info_attr = AS_MSG_INFO3_SC_READ_TYPE | AS_MSG_INFO3_SC_READ_RELAX;
		break;
	}
	attr->ttl = 0;
	attr->gen = 0;
	attr->has_write = false;
	attr->send_key = false;
}

static void
as_batch_attr_read_adjust_ops(as_batch_attr* attr, as_operations* ops)
{
	for (uint16_t i = 0; i < ops->binops.size; i++) {
		as_binop* op = &ops->binops.entries[i];

		if (op->op == AS_OPERATOR_READ && op->bin.name[0] == 0) {
			attr->read_attr |= AS_MSG_INFO1_GET_ALL;
			return;
		}
	}
}

static inline void
as_batch_attr_read_adjust(as_batch_attr* attr, bool read_all_bins)
{
	if (read_all_bins) {
		attr->read_attr |= AS_MSG_INFO1_GET_ALL;
	}
	else {
		attr->read_attr |= AS_MSG_INFO1_GET_NOBINDATA;
	}
}

static void
as_batch_attr_write_header(as_batch_attr* attr, as_operations* ops)
{
	attr->filter_exp = NULL;
	attr->read_attr = 0;
	attr->write_attr = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_RESPOND_ALL_OPS;

	for (uint16_t i = 0; i < ops->binops.size; i++) {
		as_binop* op = &ops->binops.entries[i];

		if (! as_op_is_write[op->op]) {
			attr->read_attr |= AS_MSG_INFO1_READ;
		}

		if (op->op == AS_OPERATOR_READ && op->bin.name[0] == 0) {
			attr->read_attr |= AS_MSG_INFO1_GET_ALL;
			// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
			attr->write_attr &= ~AS_MSG_INFO2_RESPOND_ALL_OPS;
		}
	}

	attr->info_attr = 0;
	attr->ttl = ops->ttl;
	attr->gen = 0;
	attr->has_write = true;
	attr->send_key = false;
}

static void
as_batch_attr_write_row(as_batch_attr* attr, const as_policy_batch_write* p, as_operations* ops)
{
	as_batch_attr_write_header(attr, ops);
	attr->filter_exp = p->filter_exp;
	attr->send_key = (p->key == AS_POLICY_KEY_SEND);

	switch (p->gen) {
	default:
	case AS_POLICY_GEN_IGNORE:
		break;
	case AS_POLICY_GEN_EQ:
		attr->gen = ops->gen;
		attr->write_attr |= AS_MSG_INFO2_GENERATION;
		break;
	case AS_POLICY_GEN_GT:
		attr->gen = ops->gen;
		attr->write_attr |= AS_MSG_INFO2_GENERATION_GT;
		break;
	}

	switch (p->exists) {
	case AS_POLICY_EXISTS_IGNORE:
		break;
	case AS_POLICY_EXISTS_UPDATE:
		attr->info_attr |= AS_MSG_INFO3_UPDATE_ONLY;
		break;
	case AS_POLICY_EXISTS_CREATE_OR_REPLACE:
		attr->info_attr |= AS_MSG_INFO3_CREATE_OR_REPLACE;
		break;
	case AS_POLICY_EXISTS_REPLACE:
		attr->info_attr |= AS_MSG_INFO3_REPLACE_ONLY;
		break;
	case AS_POLICY_EXISTS_CREATE:
		attr->write_attr |= AS_MSG_INFO2_CREATE_ONLY;
		break;
	}

	if (p->durable_delete) {
		attr->write_attr |= AS_MSG_INFO2_DURABLE_DELETE;
	}

	if (p->commit_level == AS_POLICY_COMMIT_LEVEL_MASTER) {
		attr->info_attr |= AS_MSG_INFO3_COMMIT_MASTER;
	}
}

static void
as_batch_attr_apply_header(as_batch_attr* attr)
{
	attr->filter_exp = NULL;
	attr->read_attr = 0;
	attr->write_attr = AS_MSG_INFO2_WRITE;
	attr->info_attr = 0;
	attr->ttl = 0;
	attr->gen = 0;
	attr->has_write = true;
	attr->send_key = false;
}

static void
as_batch_attr_apply_row(as_batch_attr* attr, const as_policy_batch_apply* p)
{
	attr->filter_exp = p->filter_exp;
	attr->read_attr = 0;
	attr->write_attr = AS_MSG_INFO2_WRITE;
	attr->info_attr = 0;
	attr->ttl = p->ttl;
	attr->gen = 0;
	attr->has_write = true;
	attr->send_key = (p->key == AS_POLICY_KEY_SEND);

	if (p->durable_delete) {
		attr->write_attr |= AS_MSG_INFO2_DURABLE_DELETE;
	}

	if (p->commit_level == AS_POLICY_COMMIT_LEVEL_MASTER) {
		attr->info_attr |= AS_MSG_INFO3_COMMIT_MASTER;
	}
}

static void
as_batch_attr_remove_header(as_batch_attr* attr)
{
	attr->filter_exp = NULL;
	attr->read_attr = 0;
	attr->write_attr = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_RESPOND_ALL_OPS | AS_MSG_INFO2_DELETE;
	attr->info_attr = 0;
	attr->ttl = 0;
	attr->gen = 0;
	attr->has_write = true;
	attr->send_key = false;
}

static void
as_batch_attr_remove_row(as_batch_attr* attr, const as_policy_batch_remove* p)
{
	attr->filter_exp = p->filter_exp;
	attr->read_attr = 0;
	attr->write_attr = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_RESPOND_ALL_OPS | AS_MSG_INFO2_DELETE;
	attr->info_attr = 0;
	attr->ttl = 0;
	attr->gen = 0;
	attr->has_write = true;
	attr->send_key = (p->key == AS_POLICY_KEY_SEND);

	switch (p->gen) {
	default:
	case AS_POLICY_GEN_IGNORE:
		break;
	case AS_POLICY_GEN_EQ:
		attr->gen = p->generation;
		attr->write_attr |= AS_MSG_INFO2_GENERATION;
		break;
	case AS_POLICY_GEN_GT:
		attr->gen = p->generation;
		attr->write_attr |= AS_MSG_INFO2_GENERATION_GT;
		break;
	}

	if (p->durable_delete) {
		attr->write_attr |= AS_MSG_INFO2_DURABLE_DELETE;
	}

	if (p->commit_level == AS_POLICY_COMMIT_LEVEL_MASTER) {
		attr->info_attr |= AS_MSG_INFO3_COMMIT_MASTER;
	}
}

static uint8_t*
as_batch_write_fields_filter(
	uint8_t* p, as_key* key, as_exp* filter, uint16_t n_fields, uint16_t n_ops
	)
{
	if (filter) {
		n_fields++;
		p = as_batch_write_fields(p, key, n_fields, n_ops);
		p = as_exp_write(filter, p);
	}
	else {
		p = as_batch_write_fields(p, key, n_fields, n_ops);
	}
	return p;
}

static uint8_t*
as_batch_write_read(
	uint8_t* p, as_key* key, as_batch_attr* attr, as_exp* filter, uint16_t n_ops
	)
{
	*p++ = BATCH_MSG_INFO;
	*p++ = attr->read_attr;
	*p++ = attr->write_attr;
	*p++ = attr->info_attr;
	p = as_batch_write_fields_filter(p, key, filter, 0, n_ops);
	return p;
}

static uint8_t*
as_batch_write_write(
	uint8_t* p, as_key* key, as_batch_attr* attr, as_exp* filter, uint16_t n_fields, uint16_t n_ops
	)
{
	*p++ = BATCH_MSG_WRITE;
	*p++ = attr->read_attr;
	*p++ = attr->write_attr;
	*p++ = attr->info_attr;
	*(uint16_t*)p = cf_swap_to_be16(attr->gen);
	p += sizeof(uint16_t);
	*(uint32_t*)p = cf_swap_to_be32(attr->ttl);
	p += sizeof(uint32_t);

	if (attr->send_key) {
		n_fields++;
		p = as_batch_write_fields_filter(p, key, filter, n_fields, n_ops);
		p = as_command_write_user_key(p, key);
	}
	else {
		p = as_batch_write_fields_filter(p, key, filter, n_fields, n_ops);
	}
	return p;
}

static uint8_t*
as_batch_write_bin_names(
	uint8_t* p, as_key* key, as_batch_attr* attr, as_exp* filter, const char** bins, uint16_t n_bins
	)
{
	p = as_batch_write_read(p, key, attr, filter, n_bins);

	for (uint32_t j = 0; j < n_bins; j++) {
		p = as_command_write_bin_name(p, bins[j]);
	}
	return p;
}

static uint8_t*
as_batch_write_operations(
	uint8_t* p, as_key* key, as_batch_attr* attr, as_exp* filter, const as_operations* ops,
	as_queue* buffers
	)
{
	uint16_t n_ops = (uint16_t)ops->binops.size;

	if (attr->has_write) {
		p = as_batch_write_write(p, key, attr, filter, 0, n_ops);
	}
	else {
		p = as_batch_write_read(p, key, attr, filter, n_ops);
	}
	p = as_batch_write_ops(p, ops, buffers);
	return p;
}

static uint8_t*
as_batch_write_udf(
	uint8_t* p, as_key* key, as_batch_apply_record* rec, as_batch_attr* attr, as_exp* filter,
	as_queue* buffers
	)
{
	p = as_batch_write_write(p, key, attr, filter, 3, 0);
	p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, rec->module);
	p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, rec->function);

	as_buffer buffer;
	as_queue_pop(buffers, &buffer);
	p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &buffer);
	as_buffer_destroy(&buffer);
	return p;
}

static size_t
as_batch_records_write_new(
	const as_policy_batch* policy, as_vector* records, as_vector* offsets, as_batch_builder* bb,
	uint8_t* cmd
	)
{
	uint32_t n_offsets = offsets->size;
	uint8_t* p = as_batch_header_write_new(cmd, policy, n_offsets, bb);

	uint8_t* batch_field = p;
	p = as_command_write_field_header(p, AS_FIELD_BATCH_INDEX, 0);
	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = as_batch_get_flags(policy);

	as_batch_base_record* prev = 0;
	as_batch_attr attr;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);
		
		as_batch_base_record* rec = as_vector_get(records, offset);
		memcpy(p, rec->key.digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		
		if (as_batch_equals(prev, rec, bb)) {
			// Can set repeat flag to save space.
			*p++ = BATCH_MSG_REPEAT;
		}
		else {
			// Write full message.
			switch (rec->type) {
				case AS_BATCH_READ: {
					as_batch_read_record* br = (as_batch_read_record*)rec;

					if (br->policy) {
						as_batch_attr_read_row(&attr, br->policy);
					}
					else {
						as_batch_attr_read_header(&attr, policy);
					}

					if (br->bin_names) {
						p = as_batch_write_bin_names(p, &br->key, &attr, attr.filter_exp,
							(const char**)br->bin_names, br->n_bin_names);
					}
					else if (br->ops) {
						as_batch_attr_read_adjust_ops(&attr, br->ops);
						p = as_batch_write_operations(p, &br->key, &attr, attr.filter_exp, br->ops,
							bb->buffers);
					}
					else {
						as_batch_attr_read_adjust(&attr, br->read_all_bins);
						p = as_batch_write_read(p, &br->key, &attr, attr.filter_exp, 0);
					}
					break;
				}

				case AS_BATCH_WRITE: {
					as_batch_write_record* bw = (as_batch_write_record*)rec;

					if (bw->policy) {
						as_batch_attr_write_row(&attr, bw->policy, bw->ops);
					}
					else {
						as_batch_attr_write_header(&attr, bw->ops);
					}
					p = as_batch_write_operations(p, &bw->key, &attr, attr.filter_exp, bw->ops,
						bb->buffers);
					break;
				}

				case AS_BATCH_APPLY: {
					as_batch_apply_record* ba = (as_batch_apply_record*)rec;

					if (ba->policy) {
						as_batch_attr_apply_row(&attr, ba->policy);
					}
					else {
						as_batch_attr_apply_header(&attr);
					}
					p = as_batch_write_udf(p, &ba->key, ba, &attr, attr.filter_exp, bb->buffers);
					break;
				}

				case AS_BATCH_REMOVE: {
					as_batch_remove_record* brm = (as_batch_remove_record*)rec;

					if (brm->policy) {
						as_batch_attr_remove_row(&attr, brm->policy);
					}
					else {
						as_batch_attr_remove_header(&attr);
					}
					p = as_batch_write_write(p, &brm->key, &attr, attr.filter_exp, 0, 0);
					break;
				}
			}
			prev = rec;
		}
	}
	return as_batch_trailer_write(cmd, p, batch_field);
}

static inline size_t
as_batch_records_write(
	const as_policy_batch* policy, as_vector* records, as_vector* offsets, as_batch_builder* bb,
	uint8_t* cmd
	)
{
	if (bb->batch_any) {
		return as_batch_records_write_new(policy, records, offsets, bb, cmd);
	}
	else {
		return as_batch_records_write_old(policy, records, offsets, bb, cmd);
	}
}

static void
as_batch_replica_init(as_batch_replica* rep, const as_policy_batch* policy, bool has_write)
{
	if (has_write) {
		rep->replica = as_command_write_replica(policy->replica);
		rep->replica_sc = rep->replica;
		rep->replica_index = 0;
		rep->replica_index_sc = 0;
		return;
	}

	rep->replica = policy->replica;
	rep->replica_index = as_replica_index_init_read(rep->replica);

	switch (policy->read_mode_sc) {
		case AS_POLICY_READ_MODE_SC_SESSION:
			rep->replica_sc = AS_POLICY_REPLICA_MASTER;
			rep->replica_index_sc = 0;
			break;

		case AS_POLICY_READ_MODE_SC_LINEARIZE:
			if  (rep->replica == AS_POLICY_REPLICA_PREFER_RACK) {
				rep->replica_sc = AS_POLICY_REPLICA_SEQUENCE;
				rep->replica_index_sc = 0;
			}
			else {
				rep->replica_sc = rep->replica;
				rep->replica_index_sc = rep->replica_index;
			}
			break;

		default:
			rep->replica_sc = rep->replica;
			rep->replica_index_sc = rep->replica_index;
			break;
	}
}

static as_status
as_batch_get_node(
	as_cluster* cluster, const as_key* key, const as_batch_replica* rep, bool has_write,
	as_node* prev_node, as_node** node_pp
	)
{
	as_error err;
	as_partition_info pi;
	as_status status = as_partition_info_init(&pi, cluster, &err, key);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_policy_replica replica;
	uint8_t replica_index;

	if (has_write || !pi.sc_mode) {
		replica = rep->replica;
		replica_index = rep->replica_index;
	}
	else {
		replica = rep->replica_sc;
		replica_index = rep->replica_index_sc;
	}

	as_node* node = as_partition_get_node(cluster, pi.ns, pi.partition, prev_node, replica,
		pi.replica_size, &replica_index);

	if (! node) {
		*node_pp = NULL;
		return AEROSPIKE_ERR_INVALID_NODE;
	}

	*node_pp = node;
	return AEROSPIKE_OK;
}

static inline void
as_batch_command_init(
	as_command* cmd, as_batch_task* task, const as_policy_batch* policy, uint8_t* buf, size_t size,
	const as_command* parent
	)
{
	cmd->cluster = task->cluster;
	cmd->policy = &policy->base;
	cmd->node = task->node;
	cmd->ns = NULL;        // Not referenced when node set.
	cmd->partition = NULL; // Not referenced when node set.
	cmd->parse_results_fn = as_batch_parse_records;
	cmd->udata = task;
	cmd->buf = buf;
	cmd->buf_size = size;
	cmd->partition_id = 0; // Not referenced when node set.
	cmd->replica = task->replica;

	// Note: Do not set flags to AS_COMMAND_FLAGS_LINEARIZE because AP and SC replicas
	// are tracked separately for batch (cmd->master and cmd->master_sc).
	// SC master/replica switch is done in as_batch_retry().
	cmd->flags = AS_COMMAND_FLAGS_BATCH;

	if (! task->has_write) {
		cmd->flags |= AS_COMMAND_FLAGS_READ;
	}

	if (! parent) {
		// Normal batch.
		cmd->replica_index = task->replica_index;
		cmd->replica_index_sc = task->replica_index_sc;
		as_command_start_timer(cmd);
	}
	else {
		// Split retry mode.  Do not reset timer.
		cmd->replica_index = parent->replica_index;
		cmd->replica_index_sc = parent->replica_index_sc;
		cmd->iteration = parent->iteration;
		cmd->socket_timeout = parent->socket_timeout;
		cmd->total_timeout = parent->total_timeout;
		cmd->max_retries = parent->max_retries;
		cmd->deadline_ms = parent->deadline_ms;
		cmd->sent = parent->sent;
	}
}

static inline void
as_batch_builder_set_node(as_batch_builder* bb, as_node* node)
{
	bb->batch_any = (node->features & AS_FEATURES_BATCH_ANY);
}

static inline void
as_batch_builder_destroy(as_batch_builder* bb)
{
	as_buffers_destroy(bb->buffers);
}

static void
as_batch_set_doubt_records(as_batch_task_records* btr, as_error* err)
{
	uint32_t offsets_size = btr->base.offsets.size;

	for (uint32_t i = 0; i < offsets_size; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&btr->base.offsets, i);
		as_batch_base_record* rec = as_vector_get(btr->records, offset);

		if (rec->result == AEROSPIKE_NO_RESPONSE && rec->has_write) {
			rec->in_doubt = err->in_doubt;
		}
	}
}

static void
as_batch_set_doubt_keys(as_batch_task_keys* btk, as_error* err)
{
	uint32_t offsets_size = btk->base.offsets.size;

	for (uint32_t i = 0; i < offsets_size; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&btk->base.offsets, i);
		as_batch_result* res = &btk->results[offset];

		if (res->result == AEROSPIKE_NO_RESPONSE) {
			res->in_doubt = err->in_doubt;
		}
	}
}

static as_status
as_batch_execute_records(as_batch_task_records* btr, as_error* err, as_command* parent)
{
	as_batch_task* task = &btr->base;
	const as_policy_batch* policy = task->policy;

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), 8);

	as_batch_builder bb = {
		.filter_exp = policy->base.filter_exp,
		.buffers = &buffers
	};

	as_batch_builder_set_node(&bb, task->node);

	as_status status = as_batch_records_size(btr->records, &task->offsets, &bb, err);

	if (status != AEROSPIKE_OK) {
		as_batch_builder_destroy(&bb);
		return status;
	}

	// Write command
	size_t capacity = bb.size;
	uint8_t* buf = as_command_buffer_init(capacity);
	size_t size = as_batch_records_write(policy, btr->records, &task->offsets, &bb, buf);
	as_batch_builder_destroy(&bb);

	if (policy->base.compress && size > AS_COMPRESS_THRESHOLD) {
		// Compress command.
		size_t comp_capacity = as_command_compress_max_size(size);
		size_t comp_size = comp_capacity;
		uint8_t* comp_buf = as_command_buffer_init(comp_capacity);
		status = as_command_compress(err, buf, size, comp_buf, &comp_size);
		as_command_buffer_free(buf, capacity);

		if (status != AEROSPIKE_OK) {
			as_command_buffer_free(comp_buf, comp_capacity);
			return status;
		}
		capacity = comp_capacity;
		buf = comp_buf;
		size = comp_size;
	}

	as_command cmd;
	as_batch_command_init(&cmd, task, policy, buf, size, parent);

	status = as_command_execute(&cmd, err);

	// Set in_doubt for keys associated this batch command when
	// the command was not retried and split. If a split retry occurred,
	// those new subcommands have already set in_doubt on the affected
	// subset of keys.
	if (status != AEROSPIKE_OK && !(cmd.flags & AS_COMMAND_FLAGS_SPLIT_RETRY)) {
		as_batch_set_doubt_records(btr, err);
	}

	as_command_buffer_free(buf, capacity);
	return status;
}

static as_status
as_batch_keys_size_new(
	as_key* keys, as_vector* offsets, as_batch_base_record* rec, as_batch_builder* bb, as_error* err
	)
{
	as_key* prev = 0;
	uint32_t n_offsets = offsets->size;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		as_key* key = &keys[offset];

		bb->size += AS_DIGEST_VALUE_SIZE + sizeof(uint32_t);

		if (prev && strcmp(prev->ns, key->ns) == 0 && strcmp(prev->set, key->set) == 0) {
			// Can set repeat flag to save space.
			bb->size++;
		}
		else {
			// Size full message.
			as_status status = as_batch_record_size(key, rec, bb, err);

			if (status != AEROSPIKE_OK) {
				return status;
			}
			prev = key;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_batch_keys_size(
	as_key* keys, as_vector* offsets, as_batch_base_record* rec, as_batch_builder* bb,
	as_error* err
	)
{
	as_batch_init_size(bb);

	if (bb->batch_any) {
		return as_batch_keys_size_new(keys, offsets, rec, bb, err);
	}
	else {
		if (rec->type != AS_BATCH_READ) {
			return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
										"Batch write operations not supported on older servers");
		}
		return as_batch_keys_size_old(keys, offsets, (as_batch_read_record*)rec, bb, err);
	}
}

static size_t
as_batch_keys_write_new(
	const as_policy_batch* policy, as_key* keys, as_vector* offsets, as_batch_base_record* rec,
	as_batch_attr* attr, as_batch_builder* bb, uint8_t* cmd
	)
{
	uint32_t n_offsets = offsets->size;
	uint8_t* p = as_batch_header_write_new(cmd, policy, n_offsets, bb);

	uint8_t* batch_field = p;
	p = as_command_write_field_header(p, AS_FIELD_BATCH_INDEX, 0);
	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = as_batch_get_flags(policy);

	as_key* prev = 0;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);

		as_key* key = &keys[offset];
		memcpy(p, key->digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;

		if (prev && strcmp(prev->ns, key->ns) == 0 && strcmp(prev->set, key->set) == 0) {
			// Can set repeat flag to save space.
			*p++ = BATCH_MSG_REPEAT;
		}
		else {
			// Write full message.
			switch (rec->type) {
				case AS_BATCH_READ: {
					as_batch_read_record* br = (as_batch_read_record*)rec;

					if (br->bin_names) {
						p = as_batch_write_bin_names(p, key, attr, NULL,
							(const char**)br->bin_names, br->n_bin_names);
					}
					else if (br->ops) {
						p = as_batch_write_operations(p, key, attr, NULL, br->ops,
							bb->buffers);
					}
					else {
						p = as_batch_write_read(p, key, attr, NULL, 0);
					}
					break;
				}

				case AS_BATCH_WRITE: {
					as_batch_write_record* bw = (as_batch_write_record*)rec;
					p = as_batch_write_operations(p, key, attr, NULL, bw->ops, bb->buffers);
					break;
				}

				case AS_BATCH_APPLY: {
					as_batch_apply_record* ba = (as_batch_apply_record*)rec;
					p = as_batch_write_udf(p, key, ba, attr, NULL, bb->buffers);
					break;
				}

				case AS_BATCH_REMOVE: {
					p = as_batch_write_write(p, key, attr, NULL, 0, 0);
					break;
				}
			}
			prev = key;
		}
	}
	return as_batch_trailer_write(cmd, p, batch_field);
}

static inline size_t
as_batch_keys_write(
	const as_policy_batch* policy, as_key* keys, as_vector* offsets, as_batch_base_record* rec,
	as_batch_attr* attr, as_batch_builder* bb, uint8_t* cmd
	)
{
	if (bb->batch_any) {
		return as_batch_keys_write_new(policy, keys, offsets, rec, attr, bb, cmd);
	}
	else {
		return as_batch_keys_write_old(policy, keys, offsets, (as_batch_read_record*)rec, attr, bb,
			cmd);
	}
}

static as_status
as_batch_execute_keys(as_batch_task_keys* btk, as_error* err, as_command* parent)
{
	as_batch_task* task = &btk->base;
	const as_policy_batch* policy = task->policy;

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), 8);

	as_batch_builder bb = {
		.filter_exp = btk->attr->filter_exp ? btk->attr->filter_exp : policy->base.filter_exp,
		.buffers = &buffers
	};

	as_batch_builder_set_node(&bb, task->node);

	as_status status = as_batch_keys_size(btk->keys, &task->offsets, btk->rec, &bb, err);

	if (status != AEROSPIKE_OK) {
		as_batch_builder_destroy(&bb);
		return status;
	}

	size_t capacity = bb.size;
	uint8_t* buf = as_command_buffer_init(capacity);
	size_t size = as_batch_keys_write(policy, btk->keys, &task->offsets, btk->rec, btk->attr, &bb,
		buf);
	as_batch_builder_destroy(&bb);

	if (policy->base.compress && size > AS_COMPRESS_THRESHOLD) {
		// Compress command.
		size_t comp_capacity = as_command_compress_max_size(size);
		size_t comp_size = comp_capacity;
		uint8_t* comp_buf = as_command_buffer_init(comp_capacity);
		status = as_command_compress(err, buf, size, comp_buf, &comp_size);
		as_command_buffer_free(buf, capacity);

		if (status != AEROSPIKE_OK) {
			as_command_buffer_free(comp_buf, comp_capacity);
			return status;
		}
		capacity = comp_capacity;
		buf = comp_buf;
		size = comp_size;
	}

	as_command cmd;
	as_batch_command_init(&cmd, task, policy, buf, size, parent);

	status = as_command_execute(&cmd, err);

	// Set in_doubt for keys associated this batch command when
	// the command was not retried and split. If a split retry occurred,
	// those new subcommands have already set in_doubt on the affected
	// subset of keys.
	if (btk->listener && status != AEROSPIKE_OK && !(cmd.flags & AS_COMMAND_FLAGS_SPLIT_RETRY)
		&& btk->rec->has_write) {
		as_batch_set_doubt_keys(btk, err);
	}

	as_command_buffer_free(buf, capacity);
	return status;
}

static void
as_batch_worker(void* data)
{
	as_batch_task* task = (as_batch_task*)data;
	
	as_batch_complete_task complete_task;
	complete_task.node = task->node;

	as_error err;
	as_error_init(&err);

	if (task->type == BATCH_TYPE_RECORDS) {
		// Execute batch referenced in aerospike_batch_read().
		complete_task.result = as_batch_execute_records((as_batch_task_records*)task, &err, NULL);
	}
	else {
		// Execute batch referenced in aerospike_batch_get(), aerospike_batch_get_bins()
		// and aerospike_batch_exists().
		complete_task.result = as_batch_execute_keys((as_batch_task_keys*)task, &err, NULL);
	}

	if (complete_task.result != AEROSPIKE_OK) {
		// Copy error to main error only once.
		if (as_fas_uint32(task->error_mutex, 1) == 0) {
			as_error_copy(task->err, &err);
		}
	}
	cf_queue_push(task->complete_q, &complete_task);
}

static as_batch_node*
as_batch_node_find(as_vector* batch_nodes, as_node* node)
{
	as_batch_node* batch_node = batch_nodes->list;
	uint32_t n_batch_nodes = batch_nodes->size;

	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		if (batch_node->node == node) {
			return batch_node;
		}
		batch_node++;
	}
	return NULL;
}

static void
as_batch_release_nodes(as_vector* batch_nodes)
{
	as_batch_node* batch_node = batch_nodes->list;
	uint32_t n_batch_nodes = batch_nodes->size;
	
	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_node_release(batch_node->node);
		as_vector_destroy(&batch_node->offsets);
		batch_node++;
	}
	as_vector_destroy(batch_nodes);
}

static inline void
as_batch_release_nodes_cancel_async(as_vector* batch_nodes, uint32_t start)
{
	as_batch_node* batch_node = batch_nodes->list;
	uint32_t n_batch_nodes = batch_nodes->size;

	// Release each node that was not processed.
	for (uint32_t i = start; i < n_batch_nodes; i++) {
		as_node_release(batch_node[i].node);
	}
}

static inline void
as_batch_release_nodes_after_async(as_vector* batch_nodes)
{
	// Do not release each node here because those nodes are released
	// after each async command completes.
	as_batch_node* batch_node = batch_nodes->list;
	uint32_t n_batch_nodes = batch_nodes->size;

	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_vector_destroy(&batch_node->offsets);
		batch_node++;
	}
	as_vector_destroy(batch_nodes);
}

static as_status
as_batch_keys_execute_seq(
	as_error* err, as_batch_task_keys* btk, as_vector* batch_nodes, as_command* parent
	)
{
	as_status status = AEROSPIKE_OK;
	as_error e;

	for (uint32_t i = 0; i < batch_nodes->size; i++) {
		as_batch_node* batch_node = as_vector_get(batch_nodes, i);
		
		btk->base.node = batch_node->node;
		memcpy(&btk->base.offsets, &batch_node->offsets, sizeof(as_vector));
		as_error_init(&e);

		as_status s = as_batch_execute_keys(btk, &e, parent);

		if (s != AEROSPIKE_OK) {
			if (btk->base.policy->respond_all_keys) {
				if (status == AEROSPIKE_OK) {
					as_error_copy(err, &e);
					status = s;
				}
			}
			else {
				as_error_copy(err, &e);
				status = s;
				break;
			}
		}
	}
	return status;
}

static as_status
as_batch_keys_execute(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_base_record* rec, as_batch_attr* attr, as_batch_listener listener, void* udata
	)
{
	uint32_t n_keys = batch->keys.size;
	
	if (n_keys == 0) {
		if (listener) {
			listener(0, 0, udata);
		}
		return AEROSPIKE_OK;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	as_nodes_release(nodes);

	if (n_nodes == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, cluster_empty_error);
	}
	
	// Allocate results array on stack.  May be an issue for huge batch.
	as_batch_result* results = (listener)? (as_batch_result*)alloca(sizeof(as_batch_read) * n_keys) : 0;

	as_vector batch_nodes;
	as_vector_inita(&batch_nodes, sizeof(as_batch_node), n_nodes);

	char* ns = batch->keys.entries[0].ns;
	as_status status = AEROSPIKE_OK;
	
	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_capacity = n_keys / n_nodes;
	offsets_capacity += offsets_capacity >> 2;

	// The minimum key capacity is 10.
	if (offsets_capacity < 10) {
		offsets_capacity = 10;
	}

	as_batch_replica rep;
	as_batch_replica_init(&rep, policy, rec->has_write);

	bool error_row = false;

	// Map keys to server nodes.
	for (uint32_t i = 0; i < n_keys; i++) {
		as_key* key = &batch->keys.entries[i];
		
		if (listener) {
			as_batch_result* result = &results[i];
			result->key = key;
			result->result = AEROSPIKE_NO_RESPONSE;
			result->in_doubt = false;
			as_record_init(&result->record, 0);
		}
		
		status = as_key_set_digest(err, key);
		
		if (status != AEROSPIKE_OK) {
			as_batch_release_nodes(&batch_nodes);
			return status;
		}

		as_node* node;
		status = as_batch_get_node(cluster, key, &rep, rec->has_write, NULL, &node);

		if (status != AEROSPIKE_OK) {
			rec->result = status;
			error_row = true;
			continue;
		}

		as_batch_node* batch_node = as_batch_node_find(&batch_nodes, node);
		
		if (! batch_node) {
			// Add batch node.
			as_node_reserve(node);
			batch_node = as_vector_reserve(&batch_nodes);
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

	// Fatal if no key requests were generated on initialization.
	if (batch_nodes.size == 0) {
		return as_error_set_message(err, AEROSPIKE_BATCH_FAILED, "Nodes not found");
	}

	uint8_t type;

	if (listener) {
		type = BATCH_TYPE_KEYS;
	}
	else {
		type = BATCH_TYPE_KEYS_NO_CALLBACK;
	}

	uint32_t error_mutex = 0;

	// Initialize task.
	as_batch_task_keys btk;
	memset(&btk, 0, sizeof(as_batch_task_keys));
	btk.base.cluster = cluster;
	btk.base.policy = policy;
	btk.base.err = err;
	btk.base.error_mutex = &error_mutex;
	btk.base.error_row = &error_row;
	btk.base.n_keys = n_keys;
	btk.base.replica = rep.replica;
	btk.base.replica_sc = rep.replica_sc;
	btk.base.type = type;
	btk.base.has_write = rec->has_write;
	btk.base.replica_index = rep.replica_index;
	btk.base.replica_index_sc = rep.replica_index_sc;
	btk.ns = ns;
	btk.keys = batch->keys.entries;
	btk.batch = batch;
	btk.results = results;
	btk.listener = listener;
	btk.udata = udata;
	btk.rec = rec;
	btk.attr = attr;

	if (policy->concurrent && batch_nodes.size > 1) {
		// Run batch requests in parallel in separate threads.
		btk.base.complete_q = cf_queue_create(sizeof(as_batch_complete_task), true);
		
		uint32_t n_wait_nodes = 0;
		
		// Run task for each node.
		for (uint32_t i = 0; i < batch_nodes.size; i++) {
			// Stack allocate task for each node.  It should be fine since the task
			// only needs to be valid within this function.
			as_batch_task_keys* btk_node = alloca(sizeof(as_batch_task_keys));
			memcpy(btk_node, &btk, sizeof(as_batch_task_keys));
			
			as_batch_node* batch_node = as_vector_get(&batch_nodes, i);
			btk_node->base.node = batch_node->node;
			memcpy(&btk_node->base.offsets, &batch_node->offsets, sizeof(as_vector));

			int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_batch_worker, btk_node);
			
			if (rc == 0) {
				n_wait_nodes++;
			}
			else {
				// Thread could not be added.
				if (as_fas_uint32(btk.base.error_mutex, 1) == 0) {
					status = as_error_update(btk.base.err, AEROSPIKE_ERR_CLIENT,
											 "Failed to add batch thread: %d", rc);
				}
			}
		}
		
		// Wait for tasks to complete.
		for (uint32_t i = 0; i < n_wait_nodes; i++) {
			as_batch_complete_task complete;
			cf_queue_pop(btk.base.complete_q, &complete, CF_QUEUE_FOREVER);
			
			if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
				status = complete.result;
			}
		}
		
		// Release temporary queue.
		cf_queue_destroy(btk.base.complete_q);
	}
	else {
		// Run batch requests sequentially in same thread.
		status = as_batch_keys_execute_seq(err, &btk, &batch_nodes, NULL);
	}

	// Release each node.
	as_batch_release_nodes(&batch_nodes);

	// Call user defined function with results.
	if (listener) {
		listener(btk.results, n_keys, udata);
		
		// Destroy records. User is responsible for destroying keys with as_batch_destroy().
		for (uint32_t i = 0; i < n_keys; i++) {
			as_batch_result* br = &btk.results[i];
			as_record_destroy(&br->record);
		}
	}

	if (status == AEROSPIKE_OK && error_row) {
		return as_error_set_message(err, AEROSPIKE_BATCH_FAILED,
			"One or more batch sub-commands failed");
	}
	return status;
}

static as_status
as_batch_execute_sync(
	as_cluster* cluster, as_error* err, const as_policy_batch* policy, bool has_write,
	as_batch_replica* rep, as_vector* records, uint32_t n_keys, as_vector* batch_nodes,
	as_command* parent, bool* error_row
	)
{
	as_status status = AEROSPIKE_OK;
	uint32_t error_mutex = 0;
	uint32_t n_batch_nodes = batch_nodes->size;

	// Initialize task.
	as_batch_task_records btr;
	memset(&btr, 0, sizeof(as_batch_task_records));
	btr.base.cluster = cluster;
	btr.base.policy = policy;
	btr.base.err = err;
	btr.base.error_mutex = &error_mutex;
	btr.base.error_row = error_row;
	btr.base.n_keys = n_keys;
	btr.base.replica = rep->replica;
	btr.base.replica_sc = rep->replica_sc;
	btr.base.type = BATCH_TYPE_RECORDS;
	btr.base.has_write = has_write;
	btr.base.replica_index = rep->replica_index;
	btr.base.replica_index_sc = rep->replica_index_sc;
	btr.records = records;

	if (policy->concurrent && n_batch_nodes > 1 && parent == NULL) {
		// Run batch requests in parallel in separate threads.
		btr.base.complete_q = cf_queue_create(sizeof(as_batch_complete_task), true);
		
		uint32_t n_wait_nodes = 0;
		
		// Run task for each node.
		for (uint32_t i = 0; i < n_batch_nodes; i++) {
			// Stack allocate task for each node.  It should be fine since the task
			// only needs to be valid within this function.
			as_batch_task_records* btr_node = alloca(sizeof(as_batch_task_records));
			memcpy(btr_node, &btr, sizeof(as_batch_task_records));
			
			as_batch_node* batch_node = as_vector_get(batch_nodes, i);
			btr_node->base.node = batch_node->node;
			memcpy(&btr_node->base.offsets, &batch_node->offsets, sizeof(as_vector));

			int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_batch_worker, btr_node);

			if (rc == 0) {
				n_wait_nodes++;
			}
			else {
				// Thread could not be added.
				if (as_fas_uint32(btr.base.error_mutex, 1) == 0) {
					status = as_error_update(btr.base.err, AEROSPIKE_ERR_CLIENT,
											 "Failed to add batch thread: %d", rc);
				}
			}
		}
		
		// Wait for tasks to complete.
		for (uint32_t i = 0; i < n_wait_nodes; i++) {
			as_batch_complete_task complete;
			cf_queue_pop(btr.base.complete_q, &complete, CF_QUEUE_FOREVER);
			
			if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
				status = complete.result;
			}
		}
		
		// Release temporary queue.
		cf_queue_destroy(btr.base.complete_q);
	}
	else {
		// Run batch requests sequentially in same thread.
		as_error e;

		for (uint32_t i = 0; i < n_batch_nodes; i++) {
			as_batch_node* batch_node = as_vector_get(batch_nodes, i);

			btr.base.node = batch_node->node;
			memcpy(&btr.base.offsets, &batch_node->offsets, sizeof(as_vector));
			as_error_init(&e);

			as_status s = as_batch_execute_records(&btr, &e, parent);

			if (s != AEROSPIKE_OK) {
				if (policy->respond_all_keys) {
					if (status == AEROSPIKE_OK) {
						as_error_copy(err, &e);
						status = s;
					}
				}
				else {
					as_error_copy(err, &e);
					status = s;
					break;
				}
			}
		}
	}
	
	// Release each node.
	as_batch_release_nodes(batch_nodes);
	return status;
}

static inline as_async_batch_command*
as_batch_command_create(
	as_cluster* cluster, const as_policy_batch* policy, as_batch_replica* rep, as_node* node,
	as_async_batch_executor* executor, size_t size, uint8_t flags, uint8_t* ubuf, uint32_t ubuf_size
	)
{
	// Allocate enough memory to cover, then, round up memory size in 8KB increments to reduce
	// fragmentation and to allow socket read to reuse buffer.
	size_t s = (sizeof(as_async_batch_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;

	as_async_batch_command* bc = cf_malloc(s);
	bc->ubuf = ubuf;
	bc->ubuf_size = ubuf_size;

	as_event_command* cmd = &bc->command;
	cmd->total_deadline = policy->base.total_timeout;
	cmd->socket_timeout = policy->base.socket_timeout;
	cmd->max_retries = policy->base.max_retries;
	cmd->iteration = 0;
	cmd->replica = rep->replica;
	cmd->event_loop = executor->executor.event_loop;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->ns = NULL;
	cmd->partition = NULL;
	cmd->udata = executor;  // Overload udata to be the executor.
	cmd->parse_results = as_batch_async_parse_records;
	cmd->pipe_listener = NULL;
	cmd->buf = ((as_async_batch_command*)cmd)->space;
	cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_batch_command));
	cmd->type = AS_ASYNC_TYPE_BATCH;
	cmd->proto_type = AS_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->flags = flags;

	if (policy->deserialize) {
		cmd->flags |= AS_ASYNC_FLAGS_DESERIALIZE;
	}

	// Batch does not reference cmd->replica_size because it varies per key.
	// cmd->replica_size = 1;
	cmd->replica_index = rep->replica_index;
	cmd->replica_index_sc = rep->replica_index_sc;
	return bc;
}

static as_status
as_batch_execute_async(
	as_cluster* cluster, as_error* err, const as_policy_batch* policy, as_batch_replica* rep,
	as_vector* records, as_vector* batch_nodes, as_async_batch_executor* executor
	)
{
	uint32_t n_batch_nodes = batch_nodes->size;
	as_event_executor* exec = &executor->executor;
	exec->max_concurrent = exec->max = exec->queued = n_batch_nodes;
	executor->replica = rep->replica;
	executor->replica_sc = rep->replica_sc;

	// Note: Do not set flags to AS_ASYNC_FLAGS_LINEARIZE because AP and SC replicas
	// are tracked separately for batch. SC replica_index switch is done in as_batch_retry_async().
	uint8_t flags = 0;

	if (! executor->has_write) {
		flags |= AS_ASYNC_FLAGS_READ;
	}

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), 8);

	as_batch_builder bb = {
		.filter_exp = policy->base.filter_exp,
		.buffers = &buffers
	};

	as_status status = AEROSPIKE_OK;

	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_batch_node* batch_node = as_vector_get(batch_nodes, i);
		as_batch_builder_set_node(&bb, batch_node->node);

		// Estimate buffer size.
		status = as_batch_records_size(records, &batch_node->offsets, &bb, err);

		if (status != AEROSPIKE_OK) {
			as_event_executor_cancel(exec, i);
			as_batch_release_nodes_cancel_async(batch_nodes, i);
			break;
		}

		if (! (policy->base.compress && bb.size > AS_COMPRESS_THRESHOLD)) {
			// Send uncompressed command.
			as_async_batch_command* bc = as_batch_command_create(cluster, policy, rep,
				batch_node->node, executor, bb.size, flags, NULL, 0);

			as_event_command* cmd = &bc->command;

			cmd->write_len = (uint32_t)as_batch_records_write(policy, records, &batch_node->offsets,
				&bb, cmd->buf);

			status = as_event_command_execute(cmd, err);
		}
		else {
			// Send compressed command.
			// First write uncompressed buffer.
			size_t capacity = bb.size;
			uint8_t* ubuf = cf_malloc(capacity);
			size_t size = as_batch_records_write(policy, records, &batch_node->offsets, &bb, ubuf);

			// Allocate command with compressed upper bound.
			size_t comp_size = as_command_compress_max_size(size);

			as_async_batch_command* bc = as_batch_command_create(cluster, policy, rep,
				batch_node->node, executor, comp_size, flags, ubuf, (uint32_t)size);

			as_event_command* cmd = &bc->command;

			// Compress buffer and execute.
			status = as_command_compress(err, ubuf, size, cmd->buf, &comp_size);

			if (status != AEROSPIKE_OK) {
				as_event_executor_cancel(exec, i);
				// Current node not released, so start at current node.
				as_batch_release_nodes_cancel_async(batch_nodes, i);
				cf_free(ubuf);
				cf_free(cmd);
				break;
			}
			cmd->write_len = (uint32_t)comp_size;
			status = as_event_command_execute(cmd, err);
		}

		if (status != AEROSPIKE_OK) {
			as_event_executor_cancel(exec, i);
			// Current node was released in as_event_command_execute(), so start at current node + 1.
			as_batch_release_nodes_cancel_async(batch_nodes, i + 1);
			break;
		}
	}
	as_batch_builder_destroy(&bb);
	as_batch_release_nodes_after_async(batch_nodes);
	return status;
}

static void
as_batch_records_cleanup(
	as_async_batch_executor* async_executor, as_vector* batch_nodes
	)
{
	if (batch_nodes) {
		as_batch_release_nodes(batch_nodes);
	}

	if (async_executor) {
		// Destroy batch async resources.
		// Assume no async commands have been queued.
		cf_free(async_executor);
	}
}

static as_status
as_batch_records_execute(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records,
	as_async_batch_executor* async_executor, bool has_write
	)
{
	as_vector* list = &records->list;
	uint32_t n_keys = records->list.size;
	
	if (n_keys == 0) {
		return AEROSPIKE_OK;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	as_nodes_release(nodes);
	
	if (n_nodes == 0) {
		as_batch_records_cleanup(async_executor, NULL);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, cluster_empty_error);
	}
	
	as_vector batch_nodes;
	as_vector_inita(&batch_nodes, sizeof(as_batch_node), n_nodes);

	as_status status = AEROSPIKE_OK;
	
	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_capacity = n_keys / n_nodes;
	offsets_capacity += offsets_capacity >> 2;
	
	// The minimum key capacity is 10.
	if (offsets_capacity < 10) {
		offsets_capacity = 10;
	}
	
	as_batch_replica rep;
	as_batch_replica_init(&rep, policy, has_write);

	bool error_row = false;

	// Map keys to server nodes.
	for (uint32_t i = 0; i < n_keys; i++) {
		as_batch_base_record* rec = as_vector_get(list, i);
		as_key* key = &rec->key;
		
		rec->result = AEROSPIKE_NO_RESPONSE;
		as_record_init(&rec->record, 0);
		
		status = as_key_set_digest(err, key);
		
		if (status != AEROSPIKE_OK) {
			as_batch_records_cleanup(async_executor, &batch_nodes);
			return status;
		}
		
		as_node* node;
		status = as_batch_get_node(cluster, key, &rep, rec->has_write, NULL, &node);

		if (status != AEROSPIKE_OK) {
			rec->result = status;
			error_row = true;
			continue;
		}

		as_batch_node* batch_node = as_batch_node_find(&batch_nodes, node);
		
		if (! batch_node) {
			// Add batch node.
			as_node_reserve(node);
			batch_node = as_vector_reserve(&batch_nodes);
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

	// Fatal if no key requests were generated on initialization.
	if (batch_nodes.size == 0) {
		if (async_executor) {
			// Destroy batch async resources.
			cf_free(async_executor);
		}
		return as_error_set_message(err, AEROSPIKE_BATCH_FAILED, "Nodes not found");
	}

	if (async_executor) {
		async_executor->error_row = error_row;
		return as_batch_execute_async(cluster, err, policy, &rep, list, &batch_nodes,
			async_executor);
	}
	else {
		status = as_batch_execute_sync(cluster, err, policy, has_write, &rep, list, n_keys,
			&batch_nodes, NULL, &error_row);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		if (error_row) {
			return as_error_set_message(err, AEROSPIKE_BATCH_FAILED,
				"One or more batch sub-commands failed");
		}
		return AEROSPIKE_OK;
	}
}

static as_status
as_batch_records_execute_async(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records,
	as_async_batch_listener listener, void* udata, as_event_loop* event_loop, bool has_write
	)
{
	// Check for empty batch.
	if (records->list.size == 0) {
		listener(0, records, udata, event_loop);
		return AEROSPIKE_OK;
	}
	
	// Batch will be split up into a command for each node.
	// Allocate batch data shared by each command.
	as_async_batch_executor* be = cf_malloc(sizeof(as_async_batch_executor));
	be->records = records;
	be->listener = listener;
	// replica/replica_sc are set later in as_batch_execute_async().
	be->read_mode_sc = policy->read_mode_sc;
	be->has_write = has_write;
	be->error_row = false;

	as_event_executor* exec = &be->executor;
	pthread_mutex_init(&exec->lock, NULL);
	exec->commands = 0;
	exec->event_loop = as_event_assign(event_loop);
	exec->complete_fn = as_batch_complete_async;
	exec->udata = udata;
	exec->err = NULL;
	exec->ns = NULL;
	exec->cluster_key = 0;
	exec->max_concurrent = 0;
	exec->max = 0;
	exec->count = 0;
	exec->queued = 0;
	exec->notify = true;
	exec->valid = true;

	return as_batch_records_execute(as, err, policy, records, be, has_write);
}

//---------------------------------
// Retry Functions
//---------------------------------

static as_status
as_batch_retry_records(as_batch_task_records* btr, as_command* parent, as_error* err)
{
	as_batch_task* task = &btr->base;
	as_vector* list = btr->records;
	as_cluster* cluster = task->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	as_nodes_release(nodes);

	if (n_nodes == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, cluster_empty_error);
	}

	as_vector batch_nodes;
	as_vector_inita(&batch_nodes, sizeof(as_batch_node), n_nodes);

	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_size = task->offsets.size;
	uint32_t offsets_capacity = offsets_size / n_nodes;
	offsets_capacity += offsets_capacity >> 2;

	// The minimum key capacity is 10.
	if (offsets_capacity < 10) {
		offsets_capacity = 10;
	}

	as_batch_replica rep;
	rep.replica = task->replica;
	rep.replica_sc = task->replica_sc;
	rep.replica_index = parent->replica_index;
	rep.replica_index_sc = parent->replica_index_sc;

	// Map keys to server nodes.
	for (uint32_t i = 0; i < offsets_size; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_batch_read_record* rec = as_vector_get(btr->records, offset);

		if (rec->result != AEROSPIKE_NO_RESPONSE) {
			// Do not retry keys that already have a response.
			continue;
		}

		as_key* key = &rec->key;

		as_node* node;
		as_status status = as_batch_get_node(cluster, key, &rep, rec->has_write, parent->node, &node);

		if (status != AEROSPIKE_OK) {
			rec->result = status;
			*task->error_row = true;
			continue;
		}

		as_batch_node* batch_node = as_batch_node_find(&batch_nodes, node);

		if (! batch_node) {
			// Add batch node.
			as_node_reserve(node);
			batch_node = as_vector_reserve(&batch_nodes);
			batch_node->node = node;  // Transfer node

			// Allocate vector on heap to avoid stack overflow.
			as_vector_init(&batch_node->offsets, sizeof(uint32_t), offsets_capacity);
		}
		as_vector_append(&batch_node->offsets, &offset);
	}

	if (batch_nodes.size == 0) {
		return AEROSPIKE_USE_NORMAL_RETRY;
	}

	if (batch_nodes.size == 1) {
		as_batch_node* batch_node = as_vector_get(&batch_nodes, 0);

		if (batch_node->node == task->node) {
			// Batch node is the same.
			as_batch_release_nodes(&batch_nodes);
			return AEROSPIKE_USE_NORMAL_RETRY;
		}
	}
	parent->flags |= AS_COMMAND_FLAGS_SPLIT_RETRY;

	return as_batch_execute_sync(cluster, err, task->policy, task->has_write, &rep,
		list, task->n_keys, &batch_nodes, parent, task->error_row);
}

static as_status
as_batch_retry_keys(as_batch_task_keys* btk, as_command* parent, as_error* err)
{
	as_batch_task* task = &btk->base;
	as_cluster* cluster = task->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	as_nodes_release(nodes);

	if (n_nodes == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, cluster_empty_error);
	}

	as_vector batch_nodes;
	as_vector_inita(&batch_nodes, sizeof(as_batch_node), n_nodes);

	as_status status = AEROSPIKE_OK;

	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_size = task->offsets.size;
	uint32_t offsets_capacity = offsets_size / n_nodes;
	offsets_capacity += offsets_capacity >> 2;

	// The minimum key capacity is 10.
	if (offsets_capacity < 10) {
		offsets_capacity = 10;
	}

	as_batch_replica rep;
	rep.replica = task->replica;
	rep.replica_sc = task->replica_sc;
	rep.replica_index = parent->replica_index;
	rep.replica_index_sc = parent->replica_index_sc;

	as_batch_base_record* rec = btk->rec;

	// Map keys to server nodes.
	for (uint32_t i = 0; i < offsets_size; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_key* key = &btk->batch->keys.entries[offset];
		as_batch_result* res = &btk->results[offset];

		if (res->result != AEROSPIKE_NO_RESPONSE) {
			// Do not retry keys that already have a response.
			continue;
		}

		as_node* node;
		status = as_batch_get_node(cluster, key, &rep, rec->has_write, parent->node, &node);

		if (status != AEROSPIKE_OK) {
			res->result = status;
			*task->error_row = true;
			continue;
		}

		as_batch_node* batch_node = as_batch_node_find(&batch_nodes, node);

		if (! batch_node) {
			// Add batch node.
			as_node_reserve(node);
			batch_node = as_vector_reserve(&batch_nodes);
			batch_node->node = node;  // Transfer node

			// Allocate vector on heap to avoid stack overflow.
			as_vector_init(&batch_node->offsets, sizeof(uint32_t), offsets_capacity);
		}
		as_vector_append(&batch_node->offsets, &offset);
	}

	if (batch_nodes.size == 1) {
		as_batch_node* batch_node = as_vector_get(&batch_nodes, 0);

		if (batch_node->node == task->node) {
			// Batch node is the same.
			as_batch_release_nodes(&batch_nodes);
			return AEROSPIKE_USE_NORMAL_RETRY;
		}
	}
	parent->flags |= AS_COMMAND_FLAGS_SPLIT_RETRY;

	// Run batch retries sequentially in same thread.
	status = as_batch_keys_execute_seq(err, btk, &batch_nodes, parent);

	// Release each node.
	as_batch_release_nodes(&batch_nodes);
	return status;
}

as_status
as_batch_retry(as_command* parent, as_error* err)
{
	// Retry requires keys for this node to be split among other nodes.
	// This is both recursive and exponential.
	as_batch_task* task = parent->udata;

	if (task->replica == AS_POLICY_REPLICA_MASTER) {
		// Node assignment will not change.
		return AEROSPIKE_USE_NORMAL_RETRY;
	}

	if (err->code != AEROSPIKE_ERR_TIMEOUT ||
		task->policy->read_mode_sc != AS_POLICY_READ_MODE_SC_LINEARIZE) {
		parent->replica_index_sc++;
	}

	if (task->type == BATCH_TYPE_RECORDS) {
		return as_batch_retry_records((as_batch_task_records*)task, parent, err);
	}
	else {
		return as_batch_retry_keys((as_batch_task_keys*)task, parent, err);
	}
}

static inline as_async_batch_command*
as_batch_retry_command_create(
	as_event_command* parent, as_node* node, as_batch_replica* rep, size_t size, uint64_t deadline,
	uint8_t flags, uint8_t* ubuf, uint32_t ubuf_size
	)
{
	// Allocate enough memory to cover, then, round up memory size in 8KB increments to reduce
	// fragmentation and to allow socket read to reuse buffer.
	size_t s = (sizeof(as_async_batch_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;

	as_async_batch_command* bc = cf_malloc(s);
	bc->ubuf = ubuf;
	bc->ubuf_size = ubuf_size;

	as_event_command* cmd = &bc->command;
	cmd->total_deadline = deadline;
	cmd->socket_timeout = parent->socket_timeout;
	cmd->max_retries = parent->max_retries;
	cmd->iteration = parent->iteration;
	cmd->replica = parent->replica;
	cmd->event_loop = parent->event_loop;
	cmd->cluster = parent->cluster;
	cmd->node = node;
	cmd->ns = NULL;
	cmd->partition = NULL;
	cmd->udata = parent->udata;  // Overload udata to be the executor.
	cmd->parse_results = parent->parse_results;
	cmd->pipe_listener = parent->pipe_listener;
	cmd->buf = ((as_async_batch_command*)cmd)->space;
	cmd->command_sent_counter = parent->command_sent_counter;
	cmd->write_len = (uint32_t)size;
	cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_batch_command));
	cmd->type = AS_ASYNC_TYPE_BATCH;
	cmd->proto_type = AS_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->flags = flags;
	// Batch does not reference cmd->replica_size because it varies per key.
	// cmd->replica_size = 1;
	cmd->replica_index = rep->replica_index;
	cmd->replica_index_sc = rep->replica_index_sc;
	return bc;
}

static as_batch_retry_node*
as_batch_retry_node_find(as_vector* bnodes, as_node* node)
{
	as_batch_retry_node* bnode = bnodes->list;
	uint32_t n_bnodes = bnodes->size;

	for (uint32_t i = 0; i < n_bnodes; i++) {
		if (bnode->node == node) {
			return bnode;
		}
		bnode++;
	}
	return NULL;
}

static size_t
as_batch_retry_write(
	uint8_t* buf, uint8_t* header, uint32_t header_size, uint8_t header_flags, uint8_t* batch_field,
	as_vector* offsets
	)
{
	uint8_t* p = buf;
	memcpy(p, header, header_size);
	p += header_size;

	*(uint32_t*)p = cf_swap_to_be32(offsets->size);
	p += sizeof(uint32_t);

	*p++ = header_flags;

	for (uint32_t i = 0; i < offsets->size; i++) {
		as_batch_retry_offset* off = as_vector_get(offsets, i);

		if (off->copy) {
			size_t hsz = sizeof(uint32_t) + AS_DIGEST_VALUE_SIZE;
			memcpy(p, off->begin, hsz);
			p += hsz;
			size_t sz = off->size - hsz;
			memcpy(p, off->copy + hsz, sz);
			p += sz;
		}
		else {
			memcpy(p, off->begin, off->size);
			p += off->size;
		}
	}
	return as_batch_trailer_write(buf, p, batch_field);
}

static void
as_batch_retry_release_nodes(as_vector* bnodes)
{
	as_batch_retry_node* bnode = bnodes->list;
	uint32_t n_bnodes = bnodes->size;
	
	for (uint32_t i = 0; i < n_bnodes; i++) {
		as_node_release(bnode->node);
		as_vector_destroy(&bnode->offsets);
		bnode++;
	}
	as_vector_destroy(bnodes);
}

static inline void
as_batch_retry_release_nodes_cancel_async(as_vector* bnodes, uint32_t start)
{
	as_batch_retry_node* bnode = bnodes->list;
	uint32_t n_bnodes = bnodes->size;

	// Release each node that was not processed.
	for (uint32_t i = start; i < n_bnodes; i++) {
		as_node_release(bnode[i].node);
	}
}

static void
as_batch_retry_release_nodes_after_async(as_vector* bnodes)
{
	// Do not release each node here because those nodes are released
	// after each async command completes.
	as_batch_retry_node* bnode = bnodes->list;
	uint32_t n_bnodes = bnodes->size;

	for (uint32_t i = 0; i < n_bnodes; i++) {
		as_vector_destroy(&bnode->offsets);
		bnode++;
	}
	as_vector_destroy(bnodes);
}

static inline uint8_t*
as_batch_retry_get_ubuf(as_async_batch_command* bc)
{
	// Return saved uncompressed buffer when compression is enabled.
	// Return command buffer when compression is not enabled.
	return bc->ubuf ? bc->ubuf : (uint8_t*)bc + bc->command.write_offset;
}

static uint8_t*
as_batch_retry_parse_row(uint8_t* p, uint8_t* type)
{
	p += sizeof(uint32_t) + AS_DIGEST_VALUE_SIZE;

	*type = *p++;

	switch (*type) {
	case BATCH_MSG_REPEAT:
		return p;
	case BATCH_MSG_READ:
		p++;
		break;
	case BATCH_MSG_INFO:
		p += 3;
		break;
	case BATCH_MSG_WRITE:
		p += 9;
		break;
	}

	uint16_t n_fields = cf_swap_from_be16(*(uint16_t*)p);
	p += sizeof(uint16_t);
	uint16_t n_bins = cf_swap_from_be16(*(uint16_t*)p);
	p += sizeof(uint16_t);

	for (uint16_t j = 0; j < n_fields; j++) {
		uint32_t sz = cf_swap_from_be32(*(uint32_t*)p);
		p += sizeof(uint32_t) + sz;
	}

	for (uint32_t j = 0; j < n_bins; j++) {
		uint32_t sz = cf_swap_from_be32(*(uint32_t*)p);
		p += sizeof(uint32_t) + sz;
	}
	return p;
}

int
as_batch_retry_async(as_event_command* parent, bool timeout)
{
	as_async_batch_command* parent_bc = (as_async_batch_command*)parent;
	as_async_batch_executor* be = parent->udata; // udata is overloaded to contain executor.

	if (parent->replica == AS_POLICY_REPLICA_MASTER) {
		// Node assignment will not change. Go through normal retry.
		return 1;
	}

	as_cluster* cluster = parent->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	as_nodes_release(nodes);

	if (n_nodes == 0) {
		return 1;  // Go through normal retry.
	}

	if (! timeout || (!be->has_write && be->read_mode_sc != AS_POLICY_READ_MODE_SC_LINEARIZE)) {
		parent->replica_index++;
	}

	// Batch offsets, read/write operations and other arguments are out of scope in async batch
	// retry, so they must be parsed from the parent command's send buffer.
	uint8_t* header = as_batch_retry_get_ubuf(parent_bc);
	uint8_t* p = header;

	p += AS_HEADER_SIZE;

	// Field ID is located after field size.
	if (*(p + sizeof(uint32_t)) == AS_FIELD_FILTER) {
		p += cf_swap_from_be32(*(uint32_t*)p) + sizeof(uint32_t);
	}

	// Field ID must be AS_FIELD_BATCH_INDEX at this point.
	if (*(p + sizeof(uint32_t)) != AS_FIELD_BATCH_INDEX) {
		as_log_error("Batch retry buffer is corrupt");
		return -2;  // Defer to original error.
	}

	uint8_t* batch_field = p;
	p += AS_FIELD_HEADER_SIZE;

	uint32_t header_size = (uint32_t)(p - header);
	uint32_t n_offsets = cf_swap_from_be32(*(uint32_t*)p);
	p += sizeof(uint32_t);

	uint8_t header_flags = *p++;

	as_vector bnodes;
	as_vector_inita(&bnodes, sizeof(as_batch_retry_node), n_nodes);

	as_batch_replica rep;
	rep.replica = be->replica;
	rep.replica_sc = be->replica_sc;
	rep.replica_index = parent->replica_index;
	rep.replica_index_sc = parent->replica_index_sc;

	as_vector* records = &be->records->list;

	as_batch_retry_offset full = {0};
	as_batch_retry_offset off;

	// Map keys to server nodes.
	for (uint32_t i = 0; i < n_offsets; i++) {
		off.begin = p;
		off.copy = NULL;

		uint32_t offset = cf_swap_from_be32(*(uint32_t*)p);

		uint8_t type;
		p = as_batch_retry_parse_row(p, &type);

		off.size = (uint32_t)(p - off.begin);

		as_batch_base_record* rec = as_vector_get(records, offset);

		if (type != BATCH_MSG_REPEAT) {
			// Full message.
			full.size = off.size;
			full.begin = off.begin;

			// Disallow repeat on new nodes.
			for (uint32_t j = 0; j < bnodes.size; j++) {
				as_batch_retry_node* bn = as_vector_get(&bnodes, j);
				bn->can_repeat = false;
			}
		}

		if (rec->result != AEROSPIKE_NO_RESPONSE) {
			// Do not retry keys that already have a response.
			continue;
		}

		as_key* key = &rec->key;
		as_node* node;

		as_status status = as_batch_get_node(cluster, key, &rep, rec->has_write, parent->node, &node);

		if (status != AEROSPIKE_OK) {
			rec->result = status;
			be->error_row = true;
			continue;
		}

		as_batch_retry_node* bnode = as_batch_retry_node_find(&bnodes, node);

		if (! bnode) {
			// Add batch node.
			as_node_reserve(node);
			bnode = as_vector_reserve(&bnodes);
			bnode->node = node;  // Transfer node
			bnode->size = header_size + 5;  // Add n_offsets(4) + flags(1) to header.
			as_vector_init(&bnode->offsets, sizeof(as_batch_retry_offset), n_offsets);
		}

		if (type != BATCH_MSG_REPEAT) {
			// Full message. Allow repeat on assigned node.
			// full size/begin has already been set.
			bnode->can_repeat = true;
		}
		else {
			// Repeat message.
			if (!bnode->can_repeat) {
				// Copy last full message.
				off.copy = full.begin;
				off.size = full.size;

				// Allow repeat on assigned node.
				bnode->can_repeat = true;
			}
		}
		bnode->size += off.size;
		as_vector_append(&bnode->offsets, &off);
	}

	if (bnodes.size == 0) {
		return 1;  // Go through normal retry.
	}

	if (bnodes.size == 1) {
		as_batch_retry_node* bnode = as_vector_get(&bnodes, 0);

		if (bnode->node == parent->node) {
			// Batch node is the same.  Go through normal retry.
			as_batch_retry_release_nodes(&bnodes);
			return 1;  // Go through normal retry.
		}
	}

	uint64_t deadline = parent->total_deadline;

	if (deadline > 0) {
		// Convert deadline back to timeout.
		uint64_t now = cf_getms();

		if (deadline > now) {
			deadline -= now;
		}
		else {
			// Timeout occurred.
			as_batch_retry_release_nodes(&bnodes);
			return -2;  // Timeout occurred, defer to original error.
		}
	}

	as_event_executor* e = &be->executor;
	pthread_mutex_lock(&e->lock);
	e->max += bnodes.size - 1;
	e->max_concurrent = e->max;
	e->queued = e->max;
	pthread_mutex_unlock(&e->lock);

	uint8_t flags = parent->flags &
		(AS_ASYNC_FLAGS_READ | AS_ASYNC_FLAGS_DESERIALIZE | AS_ASYNC_FLAGS_HEAP_REC);

	for (uint32_t i = 0; i < bnodes.size; i++) {
		as_batch_retry_node* bnode = as_vector_get(&bnodes, i);

		if (! (parent_bc->ubuf && bnode->size > AS_COMPRESS_THRESHOLD)) {
			as_async_batch_command* bc = as_batch_retry_command_create(parent, bnode->node, &rep,
				bnode->size, deadline, flags, NULL, 0);

			as_event_command* cmd = &bc->command;

			cmd->write_len = (uint32_t)as_batch_retry_write(cmd->buf, header, header_size,
				header_flags, batch_field, &bnode->offsets);

			// Retry command at the end of the queue so other commands have a chance to run first.
			as_event_command_schedule(cmd);
		}
		else {
			// Send compressed command.
			// First write uncompressed buffer.
			size_t capacity = bnode->size;
			uint8_t* ubuf = cf_malloc(capacity);
			size_t size = as_batch_retry_write(ubuf, header, header_size, header_flags, batch_field,
				&bnode->offsets);

			// Allocate command with compressed upper bound.
			size_t comp_size = as_command_compress_max_size(bnode->size);

			as_async_batch_command* bc = as_batch_retry_command_create(parent, bnode->node, &rep,
				comp_size, deadline, flags, ubuf, (uint32_t)size);

			as_event_command* cmd = &bc->command;

			// Compress buffer and execute.
			as_error err;
			as_status status = as_command_compress(&err, ubuf, size, cmd->buf, &comp_size);

			if (status != AEROSPIKE_OK) {
				as_event_executor_error(e, &err, bnodes.size - i);
				// Current node not released, so start at current node.
				as_batch_retry_release_nodes_cancel_async(&bnodes, i);
				cf_free(ubuf);
				cf_free(bc);
				break;
			}
			cmd->write_len = (uint32_t)comp_size;

			// Retry command at the end of the queue so other commands have a chance to run first.
			as_event_command_schedule(cmd);
		}
	}
	as_batch_retry_release_nodes_after_async(&bnodes);
	as_batch_destroy_ubuf(parent_bc);

	// Close parent command.
	as_event_timer_stop(parent);
	as_event_command_release(parent);
	return 0;  // Split retry was initiated.
}

void
as_async_batch_error(as_event_command* cmd, as_error* err)
{
	as_async_batch_command* bc = (as_async_batch_command*)cmd;
	as_async_batch_executor* be = cmd->udata;  // udata is overloaded to contain executor.

	be->error_row = true;

	// Set error/in_doubt in each key contained in the command.
	// Batch offsets are out of scope, so they must be parsed
	// from the parent command's send buffer.
	uint8_t* p = as_batch_retry_get_ubuf(bc);

	p += AS_HEADER_SIZE;

	// Field ID is located after field size.
	if (*(p + sizeof(uint32_t)) == AS_FIELD_FILTER) {
		p += cf_swap_from_be32(*(uint32_t*)p) + sizeof(uint32_t);
	}

	// Field ID must be AS_FIELD_BATCH_INDEX at this point.
	if (*(p + sizeof(uint32_t)) != AS_FIELD_BATCH_INDEX) {
		as_log_error("Batch retry buffer is corrupt");
		as_batch_destroy_ubuf(bc);
		return;
	}
	p += AS_FIELD_HEADER_SIZE;

	uint32_t n_offsets = cf_swap_from_be32(*(uint32_t*)p);
	p += sizeof(uint32_t) + 1; // Skip over n_offsets and header flags.

	as_vector* records = &be->records->list;

	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = cf_swap_from_be32(*(uint32_t*)p);
		as_batch_base_record* rec = as_vector_get(records, offset);

		if (rec->result == AEROSPIKE_NO_RESPONSE && rec->has_write) {
			rec->in_doubt = err->in_doubt;
		}

		uint8_t type;
		p = as_batch_retry_parse_row(p, &type);
	}
	as_batch_destroy_ubuf(bc);
}

//---------------------------------
// Public Functions
//---------------------------------

as_status
aerospike_batch_read(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records
	)
{
	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.batch;
	}

	return as_batch_records_execute(as, err, policy, records, NULL, false);
}

as_status
aerospike_batch_read_async(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records,
	as_async_batch_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}

	return as_batch_records_execute_async(as, err, policy, records, listener, udata, event_loop,
		false);
}

as_status
aerospike_batch_write(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records
	)
{
	as_error_reset(err);

	if (! policy) {
		policy = &as->config.policies.batch_parent_write;
	}

	return as_batch_records_execute(as, err, policy, records, NULL, true);
}

as_status
aerospike_batch_write_async(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_records* records,
	as_async_batch_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch_parent_write;
	}

	return as_batch_records_execute_async(as, err, policy, records, listener, udata, event_loop,
		true);
}

void
as_batch_records_destroy(as_batch_records* records)
{
	as_vector* list = &records->list;
	
	for (uint32_t i = 0; i < list->size; i++) {
		as_batch_base_record* record = as_vector_get(list, i);
		
		as_key_destroy(&record->key);
		as_record_destroy(&record->record);
	}
	as_vector_destroy(list);
}

as_status
aerospike_batch_get(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_listener listener, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	as_batch_read_record rec = {
		.type = AS_BATCH_READ,
		.read_all_bins = true
	};

	as_batch_attr attr;
	as_batch_attr_read_header(&attr, policy);
	attr.read_attr |= AS_MSG_INFO1_GET_ALL;

	return as_batch_keys_execute(as, err, policy, batch, (as_batch_base_record*)&rec, &attr,
		listener, udata);
}

as_status
aerospike_batch_get_bins(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	const char** bins, uint32_t n_bins, as_batch_listener listener, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	as_batch_read_record rec = {
		.type = AS_BATCH_READ,
		// Cast to maintain backwards compatibility. Field is not really modified.
		.bin_names = (char**)bins,
		.n_bin_names = n_bins
	};

	as_batch_attr attr;
	as_batch_attr_read_header(&attr, policy);

	return as_batch_keys_execute(as, err, policy, batch, (as_batch_base_record*)&rec, &attr,
		listener, udata);
}

as_status
aerospike_batch_get_ops(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_operations* ops, as_batch_listener listener, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	as_batch_read_record rec = {
		.type = AS_BATCH_READ,
		.ops = ops
	};

	as_batch_attr attr;
	as_batch_attr_read_header(&attr, policy);
	as_batch_attr_read_adjust_ops(&attr, ops);

	return as_batch_keys_execute(as, err, policy, batch, (as_batch_base_record*)&rec, &attr,
		listener, udata);
}

as_status
aerospike_batch_exists(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_listener listener, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	as_batch_read_record rec = {
		.type = AS_BATCH_READ
	};

	as_batch_attr attr;
	as_batch_attr_read_header(&attr, policy);
	attr.read_attr |= AS_MSG_INFO1_GET_NOBINDATA;

	return as_batch_keys_execute(as, err, policy, batch, (as_batch_base_record*)&rec, &attr,
		listener, udata);
}

as_status
aerospike_batch_operate(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_write* policy_write, const as_batch* batch,
	as_operations* ops, as_batch_listener listener, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch_parent_write;
	}
	
	if (! policy_write) {
		policy_write = &as->config.policies.batch_write;
	}

	as_batch_write_record rec = {
		.type = AS_BATCH_WRITE,
		.has_write = true,
		.policy = policy_write,
		.ops = ops
	};

	as_batch_attr attr;
	as_batch_attr_write_row(&attr, policy_write, ops);

	return as_batch_keys_execute(as, err, policy, batch, (as_batch_base_record*)&rec, &attr,
		listener, udata);
}

as_status
aerospike_batch_apply(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_apply* policy_apply, const as_batch* batch,
	const char* module, const char* function, as_list* arglist,
	as_batch_listener listener, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch_parent_write;
	}
	
	if (! policy_apply) {
		policy_apply = &as->config.policies.batch_apply;
	}

	as_batch_apply_record rec = {
		.type = AS_BATCH_APPLY,
		.has_write = true,
		.policy = policy_apply,
		.module = module,
		.function = function,
		.arglist = arglist
	};

	as_batch_attr attr;
	as_batch_attr_apply_row(&attr, policy_apply);

	return as_batch_keys_execute(as, err, policy, batch, (as_batch_base_record*)&rec, &attr,
		listener, udata);
}

as_status
aerospike_batch_remove(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_remove* policy_remove, const as_batch* batch,
	as_batch_listener listener, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch_parent_write;
	}
	
	if (! policy_remove) {
		policy_remove = &as->config.policies.batch_remove;
	}

	as_batch_remove_record rec = {
		.type = AS_BATCH_REMOVE,
		.has_write = true,
		.policy = policy_remove
	};

	as_batch_attr attr;
	as_batch_attr_remove_row(&attr, policy_remove);

	return as_batch_keys_execute(as, err, policy, batch, (as_batch_base_record*)&rec, &attr,
		listener, udata);
}
