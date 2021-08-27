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
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_async.h>
#include <aerospike/as_command.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_key.h>
#include <aerospike/as_list.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_predexp.h>
#include <aerospike/as_record.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_thread_pool.h>
#include <aerospike/as_val.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_digest.h>

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
	const as_policy_batch* policy;
	as_error* err;
	uint32_t* error_mutex;
	cf_queue* complete_q;
	uint32_t n_keys;
	as_policy_replica replica_sc;
	bool use_batch_records;
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
	as_batch_read* results;
	aerospike_batch_read_callback callback;
	as_batch_callback_xdr callback_xdr;
	void* udata;
	as_operations* ops;
	const char** bins;
	uint32_t n_bins;
	uint8_t read_attr;
} as_batch_task_keys;

typedef struct as_batch_complete_task_s {
	as_node* node;
	as_status result;
} as_batch_complete_task;

typedef struct {
	as_event_executor executor;
	as_batch_read_records* records;
	as_async_batch_listener listener;
	as_policy_replica replica_sc;
} as_async_batch_executor;

typedef struct as_async_batch_command {
	as_event_command command;
	uint8_t space[];
} as_async_batch_command;

/******************************************************************************
 * STATIC VARIABLES
 *****************************************************************************/

// These values must line up with as_operator enum.
static bool as_op_is_write[] = {
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

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

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
	return as_command_parse_bins(pp, err, rec, msg->n_ops, deserialize);
}

static void
as_batch_complete_async(as_event_executor* executor)
{
	as_async_batch_executor* e = (as_async_batch_executor*)executor;
	e->listener(executor->err, e->records, executor->udata, executor->event_loop);
}

static inline bool
as_batch_parse_stop(uint8_t rc)
{
	return rc && rc != AEROSPIKE_ERR_RECORD_NOT_FOUND && rc != AEROSPIKE_FILTERED_OUT;
}

static bool
as_batch_async_skip_records(as_event_command* cmd, uint8_t* p, uint8_t* end)
{
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (as_batch_parse_stop(msg->result_code)) {
			as_error err;
			as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
			as_event_response_error(cmd, &err);
			return true;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			as_event_batch_complete(cmd);
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
	uint8_t* p = cmd->buf + cmd->pos;
	uint8_t* end = cmd->buf + cmd->len;
	as_async_batch_executor* executor = cmd->udata;  // udata is overloaded to contain executor.

	if (! executor->executor.valid) {
		// An error has already been returned to the user and records have been deleted.
		// Skip over remaining socket data so it's fully read and can be reused.
		return as_batch_async_skip_records(cmd, p, end);
	}
	
	as_error err;
	as_vector* records = &executor->records->list;

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (as_batch_parse_stop(msg->result_code)) {
			as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
			as_event_response_error(cmd, &err);
			return true;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
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
		
		as_batch_read_record* record = as_vector_get(records, offset);
		record->result = msg->result_code;
		
		if (msg->result_code == AEROSPIKE_OK) {
			as_status status = as_batch_parse_record(&p, &err, msg, &record->record,
													 cmd->flags2 & AS_ASYNC_FLAGS2_DESERIALIZE);

			if (status != AEROSPIKE_OK) {
				as_event_response_error(cmd, &err);
				return true;
			}
		}
	}
	return false;
}

static as_status
as_batch_parse_records(as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata)
{
	as_batch_task* task = udata;
	bool deserialize = task->policy->deserialize;

	uint8_t* p = buf;
	uint8_t* end = buf + size;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (as_batch_parse_stop(msg->result_code)) {
			return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}

		uint32_t offset = msg->transaction_ttl;  // overloaded to contain batch index

		if (offset >= task->n_keys) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Batch index %u >= batch size: %u",
								   offset, task->n_keys);
		}

		p = as_batch_parse_fields(p, msg->n_fields);
		
		if (task->use_batch_records) {
			as_batch_task_records* btr = (as_batch_task_records*)task;
			as_batch_read_record* record = as_vector_get(btr->records, offset);
			record->result = msg->result_code;
			
			if (msg->result_code == AEROSPIKE_OK) {
				as_status status = as_batch_parse_record(&p, err, msg, &record->record,
														 deserialize);

				if (status != AEROSPIKE_OK) {
					return status;
				}
			}
		}
		else {
			as_batch_task_keys* btk = (as_batch_task_keys*)task;
			as_key* key = &btk->keys[offset];

			if (btk->callback_xdr) {
				if (msg->result_code == AEROSPIKE_OK) {
					as_record rec;
					as_status status = as_batch_parse_record(&p, err, msg, &rec, deserialize);

					if (status != AEROSPIKE_OK) {
						as_record_destroy(&rec);
						return status;
					}

					bool rv = btk->callback_xdr(key, &rec, btk->udata);
					as_record_destroy(&rec);
					
					if (!rv) {
						return AEROSPIKE_ERR_CLIENT_ABORT;
					}
				}
			}
			else {
				as_batch_read* result = &btk->results[offset];
				result->result = msg->result_code;
				
				if (msg->result_code == AEROSPIKE_OK) {
					as_status status = as_batch_parse_record(&p, err, msg, &result->record,
															 deserialize);

					if (status != AEROSPIKE_OK) {
						return status;
					}
				}
			}
		}
	}
	return AEROSPIKE_OK;
}

typedef struct {
	size_t size;
	as_queue* buffers;
	uint8_t* filter_field;
	uint32_t filter_size;
	uint16_t field_count_header;
} as_batch_builder;

static inline void
as_batch_builder_init(
	as_batch_builder* bb, as_queue* buffers, uint8_t* filter_field, uint32_t filter_size
	)
{
	bb->buffers = buffers;
	bb->filter_field = filter_field;
	bb->filter_size = filter_size;
}

static inline void
as_batch_builder_destroy(as_batch_builder* bb)
{
	as_buffers_destroy(bb->buffers);
}

static as_status
as_batch_estimate_ops(const as_operations* ops, as_error* err, as_queue* buffers, size_t* sp)
{
	size_t size = 0;
	uint32_t n_operations = ops->binops.size;

	if (n_operations == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "No operations defined");
	}

	for (uint32_t i = 0; i < n_operations; i++) {
		as_binop* op = &ops->binops.entries[i];

		if (as_op_is_write[op->op]) {
			return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
										"Write operations not allowed in batch read");
		}
		size += as_command_bin_size(&op->bin, buffers);
	}
	*sp = size;
	return AEROSPIKE_OK;
}

static as_status
as_batch_size_records(
	const as_policy_batch* policy, as_vector* records, as_vector* offsets, as_batch_builder* bb,
	as_error* err
	)
{
	// Estimate buffer size.
	size_t size = AS_HEADER_SIZE + AS_FIELD_HEADER_SIZE + sizeof(uint32_t) + 1;

	if (policy->base.filter_exp) {
		size += AS_FIELD_HEADER_SIZE + policy->base.filter_exp->packed_sz;
		bb->filter_size = (uint32_t)size;
		bb->field_count_header = 2;
	}
	else if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &bb->filter_size);
		bb->field_count_header = 2;
	}
	else if (bb->filter_field) {
		// filter_field is only set on async batch retry with a filter expression.
		// filter_size is already set in this case.
		size += bb->filter_size;
		bb->field_count_header = 2;
	}
	else {
		bb->filter_size = 0;
		bb->field_count_header = 1;
	}

	as_batch_read_record* prev = 0;
	uint32_t n_offsets = offsets->size;
	bool send_set_name = policy->send_set_name;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		as_batch_read_record* record = as_vector_get(records, offset);
		
		size += AS_DIGEST_VALUE_SIZE + sizeof(uint32_t);
		
		if (prev && strcmp(prev->key.ns, record->key.ns) == 0 &&
			(! send_set_name || strcmp(prev->key.set, record->key.set) == 0) &&
			prev->bin_names == record->bin_names && prev->read_all_bins == record->read_all_bins &&
			prev->ops == record->ops) {
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
				for (uint32_t j = 0; j < record->n_bin_names; j++) {
					size += as_command_string_operation_size(record->bin_names[j]);
				}
			}
			else if (record->ops) {
				size_t s = 0;
				as_status status = as_batch_estimate_ops(record->ops, err, bb->buffers, &s);

				if (status != AEROSPIKE_OK) {
					return status;
				}
				size += s;
			}
			prev = record;
		}
	}
	bb->size = size;
	return AEROSPIKE_OK;
}

static inline uint8_t*
as_batch_write_fields(
	uint8_t* p, const as_policy_batch* policy, as_key* key, uint16_t field_count, uint16_t op_count
	)
{
	*(uint16_t*)p = cf_swap_to_be16(field_count);
	p += sizeof(uint16_t);
	*(uint16_t*)p = cf_swap_to_be16(op_count);
	p += sizeof(uint16_t);
	p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, key->ns);

	if (policy->send_set_name) {
		p = as_command_write_field_string(p, AS_FIELD_SETNAME, key->set);
	}
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
as_batch_index_records_write(
	const as_policy_batch* policy, as_vector* records, as_vector* offsets, as_batch_builder* bb,
	uint8_t* cmd
	)
{
	uint8_t read_attr = AS_MSG_INFO1_READ;

	if (policy->read_mode_ap == AS_POLICY_READ_MODE_AP_ALL) {
		read_attr |= AS_MSG_INFO1_READ_MODE_AP_ALL;
	}

	uint32_t n_offsets = offsets->size;
	uint8_t* p = as_command_write_header_read(cmd, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, policy->base.total_timeout, bb->field_count_header, 0,
		read_attr | AS_MSG_INFO1_BATCH_INDEX);

	if (policy->base.filter_exp) {
		p = as_exp_write(policy->base.filter_exp, p);
	}
	else if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, bb->filter_size, p);
	}
	else if (bb->filter_field) {
		// filter_field is only set on async batch retry with filter expression.
		memcpy(p, bb->filter_field, bb->filter_size);
		p += bb->filter_size;
	}

	uint8_t* field_size_ptr = p;

	p = as_command_write_field_header(p,
			policy->send_set_name ? AS_FIELD_BATCH_INDEX_WITH_SET : AS_FIELD_BATCH_INDEX, 0);

	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = policy->allow_inline? 1 : 0;
	
	as_batch_read_record* prev = 0;
	uint16_t field_count = policy->send_set_name ? 2 : 1;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);
		
		as_batch_read_record* record = as_vector_get(records, offset);
		memcpy(p, record->key.digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		
		if (prev && strcmp(prev->key.ns, record->key.ns) == 0 &&
			(! policy->send_set_name || strcmp(prev->key.set, record->key.set) == 0) &&
			prev->bin_names == record->bin_names && prev->read_all_bins == record->read_all_bins &&
			prev->ops == record->ops) {
			// Can set repeat previous namespace/bin names to save space.
			*p++ = 1;  // repeat
		}
		else {
			// Write full header, namespace and bin names.
			*p++ = 0;  // do not repeat
			
			if (record->bin_names) {
				*p++ = read_attr;
				p = as_batch_write_fields(p, policy, &record->key, field_count,
					(uint16_t)record->n_bin_names);

				for (uint32_t j = 0; j < record->n_bin_names; j++) {
					p = as_command_write_bin_name(p, record->bin_names[j]);
				}
			}
			else if (record->ops) {
				*p++ = read_attr;
				p = as_batch_write_fields(p, policy, &record->key, field_count,
					record->ops->binops.size);

				p = as_batch_write_ops(p, record->ops, bb->buffers);
			}
			else {
				*p++ = (read_attr | (record->read_all_bins? AS_MSG_INFO1_GET_ALL :
															AS_MSG_INFO1_GET_NOBINDATA));

				p = as_batch_write_fields(p, policy, &record->key, field_count, 0);
			}
			prev = record;
		}
	}
	// Write real field size.
	size_t size = p - field_size_ptr - 4;
	*(uint32_t*)field_size_ptr = cf_swap_to_be32((uint32_t)size);

	return as_command_write_end(cmd, p);
}

static inline as_policy_replica
as_batch_get_replica_sc(const as_policy_batch* policy)
{
	switch (policy->read_mode_sc) {
		case AS_POLICY_READ_MODE_SC_SESSION:
			return AS_POLICY_REPLICA_MASTER;

		case AS_POLICY_READ_MODE_SC_LINEARIZE:
			return (policy->replica != AS_POLICY_REPLICA_PREFER_RACK) ?
					policy->replica : AS_POLICY_REPLICA_SEQUENCE;

		default:
			return policy->replica;
	}
}

static as_status
as_batch_get_node(
	as_cluster* cluster, as_error* err, const as_key* key, as_policy_replica replica,
	as_policy_replica replica_sc, bool master, bool master_sc, as_node* prev_node, as_node** node_pp
	)
{
	as_partition_info pi;
	as_status status = as_partition_info_init(&pi, cluster, err, key);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	if (pi.sc_mode) {
		replica = replica_sc;
		master = master_sc;
	}

	as_node* node = as_partition_get_node(cluster, pi.ns, pi.partition, prev_node, replica, master);

	if (! node) {
		*node_pp = NULL;
		return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE, "Node not found for partition %s:%u",
							   pi.ns, pi.partition_id);
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
	cmd->replica = policy->replica;

	// Note: Do not set flags to AS_COMMAND_FLAGS_LINEARIZE because AP and SC replicas
	// are tracked separately for batch (cmd->master and cmd->master_sc).
	// SC master/replica switch is done in as_batch_retry().
	cmd->flags = AS_COMMAND_FLAGS_READ | AS_COMMAND_FLAGS_BATCH;

	if (! parent) {
		// Normal batch.
		cmd->master_sc = true;
		as_command_start_timer(cmd);
	}
	else {
		// Split retry mode.  Do not reset timer.
		cmd->master_sc = parent->master_sc;
		cmd->iteration = parent->iteration;
		cmd->master = parent->master;
		cmd->socket_timeout = parent->socket_timeout;
		cmd->total_timeout = parent->total_timeout;
		cmd->deadline_ms = parent->deadline_ms;
	}
}

static as_status
as_batch_execute_records(as_batch_task_records* btr, as_error* err, as_command* parent)
{
	as_error_reset(err);

	as_batch_task* task = &btr->base;
	const as_policy_batch* policy = task->policy;

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), 8);

	as_batch_builder bb;
	as_batch_builder_init(&bb, &buffers, NULL, 0);

	// Estimate buffer size.
	as_status status = as_batch_size_records(policy, btr->records, &task->offsets, &bb, err);

	if (status != AEROSPIKE_OK) {
		as_batch_builder_destroy(&bb);
		return status;
	}

	// Write command
	size_t capacity = bb.size;
	uint8_t* buf = as_command_buffer_init(capacity);
	size_t size = as_batch_index_records_write(policy, btr->records, &task->offsets, &bb, buf);
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
	as_command_buffer_free(buf, capacity);
	return status;
}

static as_status
as_batch_execute_keys(as_batch_task_keys* btk, as_error* err, as_command* parent)
{
	as_error_reset(err);

	as_batch_task* task = &btk->base;
	const as_policy_batch* policy = task->policy;

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), 8);

	// Estimate buffer size.
	size_t size = AS_HEADER_SIZE + AS_FIELD_HEADER_SIZE + 5;
	uint32_t pred_size = 0;
	uint16_t field_count_header = 1;

	if (policy->base.filter_exp) {
		size += AS_FIELD_HEADER_SIZE + policy->base.filter_exp->packed_sz;
		field_count_header++;
	}
	else if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &pred_size);
		field_count_header++;
	}

	uint16_t field_count = policy->send_set_name ? 2 : 1;
	as_key* prev = 0;
	uint32_t n_offsets = task->offsets.size;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_key* key = &btk->keys[offset];
		
		size += 24;  // digest + int count.

		if (prev && strcmp(prev->ns, key->ns) == 0 &&
			(! policy->send_set_name || strcmp(prev->set, key->set) == 0)) {
			// Can set repeat previous namespace/bin names to save space.
			size++;
		}
		else {
			// Estimate full header, namespace and bin names.
			size += as_command_string_field_size(key->ns) + 6;
			
			if (policy->send_set_name) {
				size += as_command_string_field_size(key->set);
			}

			if (btk->n_bins) {
				for (uint32_t j = 0; j < btk->n_bins; j++) {
					size += as_command_string_operation_size(btk->bins[j]);
				}
			}
			else if (btk->ops) {
				size_t s = 0;
				as_status status = as_batch_estimate_ops(btk->ops, err, &buffers, &s);

				if (status != AEROSPIKE_OK) {
					as_buffers_destroy(&buffers);
					return status;
				}
				size += s;
			}
			prev = key;
		}
	}

	if (policy->read_mode_ap == AS_POLICY_READ_MODE_AP_ALL) {
		btk->read_attr |= AS_MSG_INFO1_READ_MODE_AP_ALL;
	}

	// Write command
	size_t capacity = size;
	uint8_t* buf = as_command_buffer_init(capacity);

	uint8_t* p = as_command_write_header_read(buf, &policy->base, policy->read_mode_ap,
		policy->read_mode_sc, policy->base.total_timeout, field_count_header, 0,
		btk->read_attr | AS_MSG_INFO1_BATCH_INDEX);

	if (policy->base.filter_exp) {
		p = as_exp_write(policy->base.filter_exp, p);
	}
	else if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, pred_size, p);
	}

	uint8_t* field_size_ptr = p;
	p = as_command_write_field_header(p, policy->send_set_name ? AS_FIELD_BATCH_INDEX_WITH_SET :
																 AS_FIELD_BATCH_INDEX, 0);

	*(uint32_t*)p = cf_swap_to_be32(n_offsets);
	p += sizeof(uint32_t);
	*p++ = policy->allow_inline? 1 : 0;
	
	prev = 0;
	
	for (uint32_t i = 0; i < n_offsets; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		*(uint32_t*)p = cf_swap_to_be32(offset);
		p += sizeof(uint32_t);
		
		as_key* key = &btk->keys[offset];
		memcpy(p, key->digest.value, AS_DIGEST_VALUE_SIZE);
		p += AS_DIGEST_VALUE_SIZE;
		
		if (prev && strcmp(prev->ns, key->ns) == 0 &&
			(! policy->send_set_name || strcmp(prev->set, key->set) == 0)) {
			// Can set repeat previous namespace/bin names to save space.
			*p++ = 1;  // repeat
		}
		else {
			// Write full header, namespace and bin names.
			*p++ = 0;  // do not repeat
			*p++ = btk->read_attr;

			if (btk->n_bins) {
				p = as_batch_write_fields(p, policy, key, field_count, (uint16_t)btk->n_bins);

				for (uint32_t j = 0; j < btk->n_bins; j++) {
					p = as_command_write_bin_name(p, btk->bins[j]);
				}
			}
			else if (btk->ops) {
				p = as_batch_write_fields(p, policy, key, field_count, btk->ops->binops.size);
				p = as_batch_write_ops(p, btk->ops, &buffers);
			}
			else {
				p = as_batch_write_fields(p, policy, key, field_count, 0);
			}
			prev = key;
		}
	}
	as_buffers_destroy(&buffers);

	// Write real field size.
	size = p - field_size_ptr - 4;
	*(uint32_t*)field_size_ptr = cf_swap_to_be32((uint32_t)size);
	
	size = as_command_write_end(buf, p);
	
	as_status status;

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

	if (task->use_batch_records) {
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
as_batch_keys_execute(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	int read_attr, const char** bins, uint32_t n_bins, as_operations* ops,
	aerospike_batch_read_callback callback, as_batch_callback_xdr callback_xdr, void* udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.batch;
	}
	
	uint32_t n_keys = batch->keys.size;
	
	if (n_keys == 0) {
		callback(0, 0, udata);
		return AEROSPIKE_OK;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, cluster_empty_error);
	}
	
	// Allocate results array on stack.  May be an issue for huge batch.
	as_batch_read* results = (callback)? (as_batch_read*)alloca(sizeof(as_batch_read) * n_keys) : 0;

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

	as_policy_replica replica_sc = as_batch_get_replica_sc(policy);

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
			as_batch_release_nodes(&batch_nodes);
			as_nodes_release(nodes);
			return status;
		}

		as_node* node;
		status = as_batch_get_node(cluster, err, key, policy->replica, replica_sc, true, true,
								   NULL, &node);

		if (status != AEROSPIKE_OK) {
			as_batch_release_nodes(&batch_nodes);
			as_nodes_release(nodes);
			return status;
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
	as_nodes_release(nodes);
	
	uint32_t error_mutex = 0;
	
	// Initialize task.
	as_batch_task_keys btk;
	memset(&btk, 0, sizeof(as_batch_task_keys));
	btk.base.cluster = cluster;
	btk.base.policy = policy;
	btk.base.err = err;
	btk.base.error_mutex = &error_mutex;
	btk.base.n_keys = n_keys;
	btk.base.replica_sc = replica_sc;
	btk.base.use_batch_records = false;
	btk.ns = ns;
	btk.keys = batch->keys.entries;
	btk.batch = batch;
	btk.results = results;
	btk.callback = callback;
	btk.callback_xdr = callback_xdr;
	btk.udata = udata;
	btk.ops = ops;
	btk.bins = bins;
	btk.n_bins = n_bins;
	btk.read_attr = read_attr;

	if (policy->concurrent && batch_nodes.size > 1) {
		// Run batch requests in parallel in separate threads.
		btk.base.complete_q = cf_queue_create(sizeof(as_batch_complete_task), true);
		
		uint32_t n_wait_nodes = batch_nodes.size;
		
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
			
			if (rc) {
				// Thread could not be added. Abort entire batch.
				if (as_fas_uint32(btk.base.error_mutex, 1) == 0) {
					status = as_error_update(btk.base.err, AEROSPIKE_ERR_CLIENT,
											 "Failed to add batch thread: %d", rc);
				}
				
				// Reset node count to threads that were run.
				n_wait_nodes = i;
				break;
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
		for (uint32_t i = 0; status == AEROSPIKE_OK && i < batch_nodes.size; i++) {
			as_batch_node* batch_node = as_vector_get(&batch_nodes, i);
			
			btk.base.node = batch_node->node;
			memcpy(&btk.base.offsets, &batch_node->offsets, sizeof(as_vector));
			status = as_batch_execute_keys(&btk, err, NULL);
		}
	}
			
	// Release each node.
	as_batch_release_nodes(&batch_nodes);

	// Call user defined function with results.
	if (callback) {
		callback(btk.results, n_keys, udata);
		
		// Destroy records. User is responsible for destroying keys with as_batch_destroy().
		for (uint32_t i = 0; i < n_keys; i++) {
			as_batch_read* br = &btk.results[i];
			if (br->result == AEROSPIKE_OK) {
				as_record_destroy(&br->record);
			}
		}
	}
	return status;
}

static as_status
as_batch_read_execute_sync(
	as_cluster* cluster, as_error* err, const as_policy_batch* policy, as_policy_replica replica_sc,
	as_vector* records, uint32_t n_keys, as_vector* batch_nodes, as_command* parent
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
	btr.base.n_keys = n_keys;
	btr.base.replica_sc = replica_sc;
	btr.base.use_batch_records = true;
	btr.records = records;

	if (policy->concurrent && n_batch_nodes > 1 && parent == NULL) {
		// Run batch requests in parallel in separate threads.
		btr.base.complete_q = cf_queue_create(sizeof(as_batch_complete_task), true);
		
		uint32_t n_wait_nodes = n_batch_nodes;
		
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
			
			if (rc) {
				// Thread could not be added. Abort entire batch.
				if (as_fas_uint32(btr.base.error_mutex, 1) == 0) {
					status = as_error_update(btr.base.err, AEROSPIKE_ERR_CLIENT,
											 "Failed to add batch thread: %d", rc);
				}
				
				// Reset node count to threads that were run.
				n_wait_nodes = i;
				break;
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
		for (uint32_t i = 0; status == AEROSPIKE_OK && i < n_batch_nodes; i++) {
			as_batch_node* batch_node = as_vector_get(batch_nodes, i);
			
			btr.base.node = batch_node->node;
			memcpy(&btr.base.offsets, &batch_node->offsets, sizeof(as_vector));
			status = as_batch_execute_records(&btr, err, parent);
		}
	}
	
	// Release each node.
	as_batch_release_nodes(batch_nodes);
	return status;
}

static inline as_event_command*
as_batch_read_command_create(
	as_cluster* cluster, const as_policy_batch* policy, as_node* node,
	as_async_batch_executor* executor, size_t size, uint8_t flags
	)
{
	// Allocate enough memory to cover, then, round up memory size in 8KB increments to reduce
	// fragmentation and to allow socket read to reuse buffer.
	size_t s = (sizeof(as_async_batch_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;
	as_event_command* cmd = cf_malloc(s);
	cmd->total_deadline = policy->base.total_timeout;
	cmd->socket_timeout = policy->base.socket_timeout;
	cmd->max_retries = policy->base.max_retries;
	cmd->iteration = 0;
	cmd->replica = policy->replica;
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
	cmd->flags2 = policy->deserialize ? AS_ASYNC_FLAGS2_DESERIALIZE : 0;
	return cmd;
}

static as_status
as_batch_read_execute_async(
	as_cluster* cluster, as_error* err, const as_policy_batch* policy, as_policy_replica replica_sc,
	as_vector* records, as_vector* batch_nodes, as_async_batch_executor* executor
	)
{
	uint32_t n_batch_nodes = batch_nodes->size;
	as_event_executor* exec = &executor->executor;
	exec->max_concurrent = exec->max = exec->queued = n_batch_nodes;
	executor->replica_sc = replica_sc;

	// Note: Do not set flags to AS_ASYNC_FLAGS_LINEARIZE because AP and SC replicas
	// are tracked separately for batch (AS_ASYNC_FLAGS_MASTER and AS_ASYNC_FLAGS_MASTER_SC).
	// SC master/replica switch is done in as_batch_retry_async().
	uint8_t flags = AS_ASYNC_FLAGS_READ | AS_ASYNC_FLAGS_MASTER | AS_ASYNC_FLAGS_MASTER_SC;

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), 8);

	as_batch_builder bb;
	as_batch_builder_init(&bb, &buffers, NULL, 0);

	as_status status = AEROSPIKE_OK;

	for (uint32_t i = 0; i < n_batch_nodes; i++) {
		as_batch_node* batch_node = as_vector_get(batch_nodes, i);
		
		// Estimate buffer size.
		status = as_batch_size_records(policy, records, &batch_node->offsets, &bb, err);

		if (status != AEROSPIKE_OK) {
			as_event_executor_cancel(exec, i);
			as_batch_release_nodes_cancel_async(batch_nodes, i);
			break;
		}

		if (! (policy->base.compress && bb.size > AS_COMPRESS_THRESHOLD)) {
			// Send uncompressed command.
			as_event_command* cmd = as_batch_read_command_create(cluster, policy, batch_node->node,
				executor, bb.size, flags);

			cmd->write_len = (uint32_t)as_batch_index_records_write(policy, records,
				&batch_node->offsets, &bb, cmd->buf);

			status = as_event_command_execute(cmd, err);
		}
		else {
			// Send compressed command.
			// First write uncompressed buffer.
			size_t capacity = bb.size;
			uint8_t* buf = as_command_buffer_init(capacity);
			size_t size = as_batch_index_records_write(policy, records, &batch_node->offsets, &bb,
				buf);

			// Allocate command with compressed upper bound.
			size_t comp_size = as_command_compress_max_size(size);

			as_event_command* cmd = as_batch_read_command_create(cluster, policy, batch_node->node,
				executor, comp_size, flags);

			// Compress buffer and execute.
			status = as_command_compress(err, buf, size, cmd->buf, &comp_size);
			as_command_buffer_free(buf, capacity);

			if (status != AEROSPIKE_OK) {
				as_event_executor_cancel(exec, i);
				// Current node not released, so start at current node.
				as_batch_release_nodes_cancel_async(batch_nodes, i);
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
as_batch_read_cleanup(
	as_async_batch_executor* async_executor, as_nodes* nodes, as_vector* batch_nodes
	)
{
	if (batch_nodes) {
		as_batch_release_nodes(batch_nodes);
	}

	as_nodes_release(nodes);
	
	if (async_executor) {
		// Destroy batch async resources.
		// Assume no async commands have been queued.
		cf_free(async_executor);
	}
}

static as_status
as_batch_records_execute(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_read_records* records,
	as_async_batch_executor* async_executor
	)
{
	if (! policy) {
		policy = &as->config.policies.batch;
	}

	as_vector* list = &records->list;
	uint32_t n_keys = records->list.size;
	
	if (n_keys == 0) {
		return AEROSPIKE_OK;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_batch_read_cleanup(async_executor, nodes, NULL);
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
	
	as_policy_replica replica_sc = as_batch_get_replica_sc(policy);

	// Map keys to server nodes.
	for (uint32_t i = 0; i < n_keys; i++) {
		as_batch_read_record* record = as_vector_get(list, i);
		as_key* key = &record->key;
		
		record->result = AEROSPIKE_ERR_RECORD_NOT_FOUND;
		as_record_init(&record->record, 0);
		
		status = as_key_set_digest(err, key);
		
		if (status != AEROSPIKE_OK) {
			as_batch_read_cleanup(async_executor, nodes, &batch_nodes);
			return status;
		}
		
		as_node* node;
		status = as_batch_get_node(cluster, err, key, policy->replica, replica_sc, true, true,
								   NULL, &node);

		if (status != AEROSPIKE_OK) {
			as_batch_read_cleanup(async_executor, nodes, &batch_nodes);
			return status;
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
	as_nodes_release(nodes);
	
	if (async_executor) {
		return as_batch_read_execute_async(cluster, err, policy, replica_sc, list,
										   &batch_nodes, async_executor);
	}
	
	return as_batch_read_execute_sync(cluster, err, policy, replica_sc, list, n_keys,
									  &batch_nodes, NULL);
}

/******************************************************************************
 * RETRY FUNCTIONS
 *****************************************************************************/

static as_status
as_batch_retry_records(as_batch_task_records* btr, as_command* parent, as_error* err)
{
	as_batch_task* task = &btr->base;
	as_vector* list = btr->records;
	as_cluster* cluster = task->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;

	if (n_nodes == 0) {
		as_nodes_release(nodes);
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

	// Map keys to server nodes.
	for (uint32_t i = 0; i < offsets_size; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_batch_read_record* record = as_vector_get(btr->records, offset);
		as_key* key = &record->key;

		as_node* node;
		status = as_batch_get_node(cluster, err, key, task->policy->replica, task->replica_sc,
								   parent->master, parent->master_sc, parent->node, &node);

		if (status != AEROSPIKE_OK) {
			as_batch_release_nodes(&batch_nodes);
			as_nodes_release(nodes);
			return status;
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
	as_nodes_release(nodes);

	if (batch_nodes.size == 1) {
		as_batch_node* batch_node = as_vector_get(&batch_nodes, 0);

		if (batch_node->node == task->node) {
			// Batch node is the same.
			as_batch_release_nodes(&batch_nodes);
			return AEROSPIKE_USE_NORMAL_RETRY;
		}
	}

	return as_batch_read_execute_sync(cluster, err, task->policy, task->replica_sc, list,
									  task->n_keys, &batch_nodes, parent);
}

static as_status
as_batch_retry_keys(as_batch_task_keys* btk, as_command* parent, as_error* err)
{
	as_batch_task* task = &btk->base;
	as_cluster* cluster = task->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;

	if (n_nodes == 0) {
		as_nodes_release(nodes);
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

	// Map keys to server nodes.
	for (uint32_t i = 0; i < offsets_size; i++) {
		uint32_t offset = *(uint32_t*)as_vector_get(&task->offsets, i);
		as_key* key = &btk->batch->keys.entries[offset];

		as_node* node;
		status = as_batch_get_node(cluster, err, key, task->policy->replica, task->replica_sc,
								   parent->master, parent->master_sc, parent->node, &node);

		if (status != AEROSPIKE_OK) {
			as_batch_release_nodes(&batch_nodes);
			as_nodes_release(nodes);
			return status;
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
	as_nodes_release(nodes);

	if (batch_nodes.size == 1) {
		as_batch_node* batch_node = as_vector_get(&batch_nodes, 0);

		if (batch_node->node == task->node) {
			// Batch node is the same.
			as_batch_release_nodes(&batch_nodes);
			return AEROSPIKE_USE_NORMAL_RETRY;
		}
	}

	// Run batch retries sequentially in same thread.
	for (uint32_t i = 0; status == AEROSPIKE_OK && i < batch_nodes.size; i++) {
		as_batch_node* batch_node = as_vector_get(&batch_nodes, i);

		task->node = batch_node->node;
		memcpy(&task->offsets, &batch_node->offsets, sizeof(as_vector));
		status = as_batch_execute_keys(btk, err, parent);
	}

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

	if (as_load_uint32(task->error_mutex)) {
		// No reason to retry when entire batch will fail.
		return err->code;
	}

	const as_policy_batch* policy = task->policy;
	as_policy_replica replica = policy->replica;

	if (!(replica == AS_POLICY_REPLICA_SEQUENCE || replica == AS_POLICY_REPLICA_PREFER_RACK)) {
		// Node assignment will not change.
		return AEROSPIKE_USE_NORMAL_RETRY;
	}

	if (err->code != AEROSPIKE_ERR_TIMEOUT ||
		policy->read_mode_sc != AS_POLICY_READ_MODE_SC_LINEARIZE) {
		parent->master_sc = ! parent->master_sc;
	}

	if (task->use_batch_records) {
		return as_batch_retry_records((as_batch_task_records*)task, parent, err);
	}
	else {
		return as_batch_retry_keys((as_batch_task_keys*)task, parent, err);
	}
}

static inline as_event_command*
as_batch_retry_command_create(
	as_event_command* parent, as_node* node, size_t size, uint64_t deadline, uint8_t flags
	)
{
	// Allocate enough memory to cover, then, round up memory size in 8KB increments to reduce
	// fragmentation and to allow socket read to reuse buffer.
	size_t s = (sizeof(as_async_batch_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;
	as_event_command* cmd = cf_malloc(s);
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
	cmd->write_len = (uint32_t)size;
	cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_batch_command));
	cmd->type = AS_ASYNC_TYPE_BATCH;
	cmd->proto_type = AS_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->flags = flags;
	cmd->flags2 = parent->flags2;
	return cmd;
}

int
as_batch_retry_async(as_event_command* parent, bool timeout)
{
	as_async_batch_executor* executor = parent->udata; // udata is overloaded to contain executor.

	if (! executor->executor.valid) {
		return -2;  // Defer to original error.
	}

	if (!(parent->replica == AS_POLICY_REPLICA_SEQUENCE ||
		  parent->replica == AS_POLICY_REPLICA_PREFER_RACK)) {
		return 1;  // Go through normal retry.
	}

	as_vector* records = &executor->records->list;
	as_cluster* cluster = parent->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;

	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return 1;  // Go through normal retry.
	}

	as_status status;
	as_error err;

	// Batch policy and offsets are out of scope, so they
	// must be parsed from the parent command's send buffer.
	as_policy_batch policy;
	as_policy_batch_init(&policy);
	policy.replica = parent->replica;

	uint8_t* p = (uint8_t*)parent + parent->write_offset;
	uint8_t* ubuf = NULL;
	uint8_t type = (uint8_t)((as_proto*)p)->type;

	if (type == AS_MESSAGE_TYPE) {
		// Buffer already uncompressed.
		p += 9;
	}
	else if (type == AS_COMPRESSED_MESSAGE_TYPE) {
		// Uncompress. Be careful not to modify original compressed buffer.
		uint64_t cproto = cf_swap_from_be64(*(uint64_t*)p);
		size_t csize = (size_t)(cproto & 0xFFFFFFFFFFFFUL);
		p += sizeof(uint64_t);
		size_t usize = (size_t)cf_swap_from_be64(*(uint64_t*)p);
		ubuf = cf_malloc(usize);

		if (as_proto_decompress(&err, ubuf, usize, p, csize) != AEROSPIKE_OK) {
			as_log_warn("Batch retry as_proto_decompress failed: %d:%s", err.code, err.message)
			as_nodes_release(nodes);
			cf_free(ubuf);
			return 1;  // Go through normal retry.
		}
		p = ubuf + 9;
	}
	else {
		as_proto_type_error(&err, (as_proto*)p, AS_MESSAGE_TYPE);
		as_log_warn("Batch retry failed: %d:%s", err.code, err.message)
		as_nodes_release(nodes);
		return 1;  // Go through normal retry.
	}

	uint8_t read_attr = *p;
	p += 2;

	if (read_attr & AS_MSG_INFO1_READ_MODE_AP_ALL) {
		policy.read_mode_ap = AS_POLICY_READ_MODE_AP_ALL;
	}

	if (read_attr & AS_MSG_INFO1_COMPRESS_RESPONSE) {
		policy.base.compress = true;
	}

	uint8_t info3 = *p;

	if (info3 & AS_MSG_INFO3_SC_READ_TYPE) {
		if (info3 & AS_MSG_INFO3_SC_READ_RELAX) {
			policy.read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE;
		}
		else {
			policy.read_mode_sc = AS_POLICY_READ_MODE_SC_LINEARIZE;
		}
	}
	else {
		if (info3 & AS_MSG_INFO3_SC_READ_RELAX) {
			policy.read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_REPLICA;
		}
		else {
			policy.read_mode_sc = AS_POLICY_READ_MODE_SC_SESSION;
		}
	}

	p += 19;
	uint8_t* filter_field = p;
	p += sizeof(uint32_t);
	uint32_t filter_size;

	if (*p == AS_FIELD_FILTER) {
		// filter_size defined as full field size (including header) in this special case.
		filter_size = cf_swap_from_be32(*(uint32_t*)filter_field) + sizeof(uint32_t);
		p += filter_size;
	}
	else {
		filter_field = NULL;
		filter_size = 0;
	}

	policy.send_set_name = (*p++ == AS_FIELD_BATCH_INDEX_WITH_SET);

	uint32_t offsets_size = cf_swap_from_be32(*(uint32_t*)p);
	p += sizeof(uint32_t);
	policy.allow_inline = (bool)*p++;

	// Create initial key capacity for each node as average + 25%.
	uint32_t offsets_capacity = offsets_size / n_nodes;
	offsets_capacity += offsets_capacity >> 2;

	// The minimum key capacity is 10.
	if (offsets_capacity < 10) {
		offsets_capacity = 10;
	}

	if (! timeout || policy.read_mode_sc != AS_POLICY_READ_MODE_SC_LINEARIZE) {
		parent->flags ^= AS_ASYNC_FLAGS_MASTER_SC;  // Alternate between SC master and prole.
	}

	as_vector batch_nodes;
	as_vector_inita(&batch_nodes, sizeof(as_batch_node), n_nodes);

	// Map keys to server nodes.
	for (uint32_t i = 0; i < offsets_size; i++) {
		uint32_t offset = cf_swap_from_be32(*(uint32_t*)p);
		p += sizeof(uint32_t);

		as_batch_read_record* record = as_vector_get(records, offset);
		as_key* key = &record->key;

		as_node* node;
		status = as_batch_get_node(cluster, &err, key, policy.replica, executor->replica_sc,
								   parent->flags & AS_ASYNC_FLAGS_MASTER,
								   parent->flags & AS_ASYNC_FLAGS_MASTER_SC,
								   parent->node, &node);

		if (status != AEROSPIKE_OK) {
			as_batch_release_nodes(&batch_nodes);
			as_nodes_release(nodes);

			// Close parent command with error.
			as_event_timer_stop(parent);
			as_event_error_callback(parent, &err);

			if (ubuf) {
				cf_free(ubuf);
			}
			return -1;  // Abort all retries.
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

		p += AS_DIGEST_VALUE_SIZE;

		if (*p++ == 0) {
			p++;  // read_attr
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
		}
	}
	as_nodes_release(nodes);

	if (batch_nodes.size == 1) {
		as_batch_node* batch_node = as_vector_get(&batch_nodes, 0);

		if (batch_node->node == parent->node) {
			// Batch node is the same.  Go through normal retry.
			as_batch_release_nodes(&batch_nodes);

			if (ubuf) {
				cf_free(ubuf);
			}
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
			as_batch_release_nodes(&batch_nodes);

			if (ubuf) {
				cf_free(ubuf);
			}
			return -2;  // Timeout occurred, defer to original error.
		}
	}

	as_event_executor* e = &executor->executor;
	pthread_mutex_lock(&e->lock);
	e->max += batch_nodes.size - 1;
	e->max_concurrent = e->max;
	e->queued = e->max;
	pthread_mutex_unlock(&e->lock);

	uint8_t flags = AS_ASYNC_FLAGS_READ | (parent->flags & AS_ASYNC_FLAGS_MASTER) |
					(parent->flags & AS_ASYNC_FLAGS_MASTER_SC);

	as_queue buffers;
	as_queue_inita(&buffers, sizeof(as_buffer), 8);

	as_batch_builder bb;
	as_batch_builder_init(&bb, &buffers, filter_field, filter_size);

	for (uint32_t i = 0; i < batch_nodes.size; i++) {
		as_batch_node* batch_node = as_vector_get(&batch_nodes, i);

		// Estimate buffer size.
		status = as_batch_size_records(&policy, records, &batch_node->offsets, &bb, &err);

		if (status != AEROSPIKE_OK) {
			as_event_executor_error(e, &err, batch_nodes.size - i);
			as_batch_release_nodes_cancel_async(&batch_nodes, i);
			break;
		}

		if (! (policy.base.compress && bb.size > AS_COMPRESS_THRESHOLD)) {
			as_event_command* cmd = as_batch_retry_command_create(parent, batch_node->node, bb.size,
									deadline, flags);

			cmd->write_len = (uint32_t)as_batch_index_records_write(&policy, records,
				&batch_node->offsets, &bb, cmd->buf);

			// Retry command at the end of the queue so other commands have a chance to run first.
			as_event_command_schedule(cmd);
		}
		else {
			// Send compressed command.
			// First write uncompressed buffer.
			size_t capacity = bb.size;
			uint8_t* buf = as_command_buffer_init(capacity);
			size_t size = as_batch_index_records_write(&policy, records, &batch_node->offsets, &bb, buf);

			// Allocate command with compressed upper bound.
			size_t comp_size = as_command_compress_max_size(size);

			as_event_command* cmd = as_batch_retry_command_create(parent, batch_node->node,
									comp_size, deadline, flags);

			// Compress buffer and execute.
			status = as_command_compress(&err, buf, size, cmd->buf, &comp_size);
			as_command_buffer_free(buf, capacity);

			if (status != AEROSPIKE_OK) {
				as_event_executor_error(e, &err, batch_nodes.size - i);
				// Current node not released, so start at current node.
				as_batch_release_nodes_cancel_async(&batch_nodes, i);
				cf_free(cmd);
				break;
			}

			cmd->write_len = (uint32_t)comp_size;

			// Retry command at the end of the queue so other commands have a chance to run first.
			as_event_command_schedule(cmd);
		}
	}

	as_batch_builder_destroy(&bb);
	as_batch_release_nodes_after_async(&batch_nodes);

	// Close parent command.
	as_event_timer_stop(parent);
	as_event_command_release(parent);

	if (ubuf) {
		cf_free(ubuf);
	}
	return 0;  // Split retry was initiated.
}

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

as_status
aerospike_batch_read(
	aerospike* as, as_error* err, const as_policy_batch* policy, as_batch_read_records* records
	)
{
	as_error_reset(err);
	return as_batch_records_execute(as, err, policy, records, 0);
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
	exec->err = NULL;
	exec->ns = NULL;
	exec->cluster_key = 0;
	exec->max_concurrent = 0;
	exec->max = 0;
	exec->count = 0;
	exec->queued = 0;
	exec->notify = true;
	exec->valid = true;
	executor->records = records;
	executor->listener = listener;

	return as_batch_records_execute(as, err, policy, records, executor);
}

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
 * Look up multiple records by key, then return all bins.
 */
as_status
aerospike_batch_get(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_keys_execute(as, err, policy, batch, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL,
								 NULL, 0, NULL, callback, NULL, udata);
}

/**
 * @private
 * Perform batch reads for XDR.  The callback will be called for each record as soon as it's
 * received in no particular order.
 */
as_status
aerospike_batch_get_xdr(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_callback_xdr callback, void* udata
	)
{
	return as_batch_keys_execute(as, err, policy, batch, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL,
								 NULL, 0, NULL, NULL, callback, udata);
}

/**
 * Look up multiple records by key, then return specified bins.
 */
as_status
aerospike_batch_get_bins(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	const char** bins, uint32_t n_bins, aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_keys_execute(as, err, policy, batch, AS_MSG_INFO1_READ, bins, n_bins, NULL,
								 callback, NULL, udata);
}

as_status
aerospike_batch_get_ops(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_operations* ops, aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_keys_execute(as, err, policy, batch, AS_MSG_INFO1_READ, NULL, 0, ops,
								 callback, NULL, udata);
}

/**
 * Test whether multiple records exist in the cluster.
 */
as_status
aerospike_batch_exists(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	aerospike_batch_read_callback callback, void* udata
	)
{
	return as_batch_keys_execute(as, err, policy, batch,
								 AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA, NULL, 0, NULL,
								 callback, NULL, udata);
}
