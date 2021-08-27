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
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_async.h>
#include <aerospike/as_command.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_job.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_partition_tracker.h>
#include <aerospike/as_query_validate.h>
#include <aerospike/as_random.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_sleep.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_thread_pool.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_queue.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_scan_task_s {
	as_node* node;
	as_node_partitions* np;

	as_partition_tracker* pt;
	as_cluster* cluster;
	const as_policy_scan* policy;
	const as_scan* scan;
	aerospike_scan_foreach_callback callback;
	void* udata;
	as_error* err;
	cf_queue* complete_q;
	uint32_t* error_mutex;
	uint64_t task_id;
	uint64_t cluster_key;
	bool first;
} as_scan_task;

typedef struct as_scan_complete_task_s {
	as_node* node;
	uint64_t task_id;
	as_status result;
} as_scan_complete_task;

typedef struct as_async_scan_executor {
	as_event_executor executor;
	as_async_scan_listener listener;
	as_cluster* cluster;
	as_partition_tracker* pt;
	uint8_t* cmd_buf;
	uint32_t cmd_size;
	uint32_t cmd_size_pre;
	uint32_t cmd_size_post;
	uint32_t task_id_offset;
	uint16_t n_fields;
	bool concurrent;
	bool deserialize_list_map;
} as_async_scan_executor;

typedef struct as_async_scan_command {
	as_event_command command;
	as_node_partitions* np;
	uint8_t space[];
} as_async_scan_command;

typedef struct as_scan_builder {
	as_partition_tracker* pt;
	as_node_partitions* np;
	as_buffer argbuffer;
	as_queue* opsbuffers;
	uint64_t max_records;
	uint32_t predexp_size;
	uint32_t task_id_offset;
	uint32_t parts_full_size;
	uint32_t parts_partial_size;
	uint32_t cmd_size_pre;
	uint32_t cmd_size_post;
	uint16_t n_fields;
} as_scan_builder;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_status
as_scan_partition_retry_async(as_async_scan_executor* se, as_error* err);

static inline void
as_scan_partition_executor_destroy(as_async_scan_executor* se)
{
	as_partition_tracker_destroy(se->pt);
	cf_free(se->pt);
	cf_free(se->cmd_buf);
}

static void
as_scan_partition_notify(as_async_scan_executor* se, as_error* err)
{
	as_scan_partition_executor_destroy(se);
	se->listener(err, NULL, se->executor.udata, se->executor.event_loop);
}

static void
as_scan_partition_complete_async(as_event_executor* ee)
{
	as_async_scan_executor* se = (as_async_scan_executor*)ee;

	// Handle error.
	if (ee->err) {
		as_scan_partition_notify(se, ee->err);
		return;
	}

	// Check if all partitions received.
	as_error err;
	as_status status = as_partition_tracker_is_complete(se->pt, &err);

	if (status == AEROSPIKE_OK) {
		// Scan complete.
		as_scan_partition_notify(se, NULL);
		return;
	}

	// Stop on all errors except AEROSPIKE_ERR_CLIENT.
	if (status != AEROSPIKE_ERR_CLIENT) {
		as_scan_partition_notify(se, &err);
		return;
	}

	// Reassign incomplete partitions to nodes.
	status = as_partition_tracker_assign(se->pt, se->cluster, ee->ns, &err);

	if (status != AEROSPIKE_OK) {
		as_scan_partition_notify(se, &err);
		return;
	}

	// Retry scan.
	as_scan_partition_retry_async(se, &err);
}

static as_status
as_scan_parse_record_async(as_event_command* cmd, uint8_t** pp, as_msg* msg, as_error* err)
{
	as_async_scan_command* sc = (as_async_scan_command*)cmd;
	as_async_scan_executor* se = cmd->udata;  // udata is overloaded to contain executor.

	if (sc->np) {
		if (msg->info3 & AS_MSG_INFO3_PARTITION_DONE) {
			as_partition_tracker_part_done(se->pt, sc->np, msg->generation);
			return AEROSPIKE_OK;
		}
	}

	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	*pp = as_command_parse_key(*pp, msg->n_fields, &rec.key);

	if (sc->np) {
		as_partition_tracker_set_digest(se->pt, sc->np, &rec.key.digest, sc->command.cluster->n_partitions);
	}

	as_status status = as_command_parse_bins(pp, err, &rec, msg->n_ops,
											 cmd->flags2 & AS_ASYNC_FLAGS2_DESERIALIZE);

	if (status != AEROSPIKE_OK) {
		as_record_destroy(&rec);
		return status;
	}

	bool rv = se->listener(0, &rec, se->executor.udata, se->executor.event_loop);
	as_record_destroy(&rec);

	if (! rv) {
		se->executor.notify = false;
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT_ABORT, "");
	}
	return AEROSPIKE_OK;
}

static bool
as_scan_parse_records_async(as_event_command* cmd)
{
	as_error err;
	as_event_executor* executor = cmd->udata;  // udata is overloaded to contain executor.
	uint8_t* p = cmd->buf + cmd->pos;
	uint8_t* end = cmd->buf + cmd->len;

	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code) {
			// Special case - if we scan a set name that doesn't exist on a
			// node, it will return "not found".
			if (msg->result_code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
				as_event_query_complete(cmd);
				return true;
			}
			as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
			as_event_response_error(cmd, &err);
			return true;
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			as_event_query_complete(cmd);
			return true;
		}
		
		if (! executor->valid) {
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT_ABORT, "");
			as_event_response_error(cmd, &err);
			return true;
		}

		if (as_scan_parse_record_async(cmd, &p, msg, &err) != AEROSPIKE_OK) {
			as_event_response_error(cmd, &err);
			return true;
		}
	}
	return false;
}

static as_status
as_scan_parse_record(uint8_t** pp, as_msg* msg, as_scan_task* task, as_error* err)
{
	if (task->pt) {
		if (msg->info3 & AS_MSG_INFO3_PARTITION_DONE) {
			as_partition_tracker_part_done(task->pt, task->np, msg->generation);
			return AEROSPIKE_OK;
		}
	}

	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	*pp = as_command_parse_key(*pp, msg->n_fields, &rec.key);

	if (task->pt) {
		as_partition_tracker_set_digest(task->pt, task->np, &rec.key.digest, task->cluster->n_partitions);
	}

	as_status status = as_command_parse_bins(pp, err, &rec, msg->n_ops, task->scan->deserialize_list_map);

	if (status != AEROSPIKE_OK) {
		as_record_destroy(&rec);
		return status;
	}

	bool rv = true;

	if (task->callback) {
		rv = task->callback((as_val*)&rec, task->udata);
	}
	as_record_destroy(&rec);
	return rv ? AEROSPIKE_OK : AEROSPIKE_ERR_CLIENT_ABORT;
}

static as_status
as_scan_parse_records(as_error* err, as_node* node, uint8_t* buf, size_t size, void* udata)
{
	as_scan_task* task = udata;
	uint8_t* p = buf;
	uint8_t* end = buf + size;
	as_status status;
	
	while (p < end) {
		as_msg* msg = (as_msg*)p;
		as_msg_swap_header_from_be(msg);
		
		if (msg->result_code) {
			// Special case - if we scan a set name that doesn't exist on a
			// node, it will return "not found" - we unify this with the
			// case where OK is returned and no callbacks were made. [AKG]
			// We are sending "no more records back" to the caller which will
			// send OK to the main worker thread.
			if (msg->result_code == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
				return AEROSPIKE_NO_MORE_RECORDS;
			}
			return as_error_set_message(err, msg->result_code, as_error_string(msg->result_code));
		}
		p += sizeof(as_msg);
		
		if (msg->info3 & AS_MSG_INFO3_LAST) {
			return AEROSPIKE_NO_MORE_RECORDS;
		}
		
		status = as_scan_parse_record(&p, msg, task, err);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
		
		if (as_load_uint32(task->error_mutex)) {
			err->code = AEROSPIKE_ERR_SCAN_ABORTED;
			return err->code;
		}
	}
	return AEROSPIKE_OK;
}

static size_t
as_scan_command_size(const as_policy_scan* policy, const as_scan* scan, as_scan_builder* sb)
{
	size_t size = AS_HEADER_SIZE;
	uint32_t predexp_size = 0;
	uint16_t n_fields = 0;

	if (sb->np) {
		sb->parts_full_size = sb->np->parts_full.size * 2;
		sb->parts_partial_size = sb->np->parts_partial.size * 20;
	}
	else {
		sb->parts_full_size = 0;
		sb->parts_partial_size = 0;
	}

	if (scan->ns[0]) {
		size += as_command_string_field_size(scan->ns);
		n_fields++;
	}
	
	if (scan->set[0]) {
		size += as_command_string_field_size(scan->set);
		n_fields++;
	}
	
	if (policy->records_per_second > 0) {
		size += as_command_field_size(sizeof(uint32_t));
		n_fields++;
	}

	// Estimate scan timeout size.
	size += as_command_field_size(sizeof(uint32_t));
	n_fields++;

	// Estimate taskId size.
	size += as_command_field_size(8);
	n_fields++;
	
	// Estimate background function size.
	as_buffer_init(&sb->argbuffer);
	
	if (scan->apply_each.function[0]) {
		size += as_command_field_size(1);
		size += as_command_string_field_size(scan->apply_each.module);
		size += as_command_string_field_size(scan->apply_each.function);
		
		if (scan->apply_each.arglist) {
			// If the query has a udf w/ arglist, then serialize it.
			as_serializer ser;
			as_msgpack_init(&ser);
			as_serializer_serialize(&ser, (as_val*)scan->apply_each.arglist, &sb->argbuffer);
			as_serializer_destroy(&ser);
		}
		size += as_command_field_size(sb->argbuffer.size);
		n_fields += 4;
	}
	
	if (scan->predexp.size > 0) {
		size += AS_FIELD_HEADER_SIZE;
		for (uint16_t ii = 0; ii < scan->predexp.size; ++ii) {
			as_predexp_base * bp = scan->predexp.entries[ii];
			predexp_size += (uint32_t)((*bp->size_fn)(bp));
		}
		size += predexp_size;
		n_fields++;
		sb->predexp_size = predexp_size;
	}
	else if (policy->base.filter_exp) {
		size += AS_FIELD_HEADER_SIZE + policy->base.filter_exp->packed_sz;
		n_fields++;
		sb->predexp_size = 0;
	}
	else if (policy->base.predexp) {
		size += as_predexp_list_size(policy->base.predexp, &predexp_size);
		n_fields++;
		sb->predexp_size = predexp_size;
	}

	if (sb->parts_full_size > 0) {
		size += as_command_field_size(sb->parts_full_size);
		n_fields++;
	}

	if (sb->parts_partial_size > 0) {
		size += as_command_field_size(sb->parts_partial_size);
		n_fields++;
	}

	if (sb->max_records > 0) {
		size += as_command_field_size(8);
		n_fields++;
	}

	sb->n_fields = n_fields;

	// Operations (used in background scans) and bin names (used in foreground scans)
	// are mutually exclusive.
	if (scan->ops) {
		// Estimate size for background operations.
		as_operations* ops = scan->ops;

		for (uint16_t i = 0; i < ops->binops.size; i++) {
			as_binop* op = &ops->binops.entries[i];
			size += as_command_bin_size(&op->bin, sb->opsbuffers);
		}
	}
	else {
		// Estimate size for selected bin names.
		for (uint16_t i = 0; i < scan->select.size; i++) {
			size += as_command_string_operation_size(scan->select.entries[i]);
		}
	}
	return size;
}

static size_t
as_scan_command_init(
	uint8_t* cmd, const as_policy_scan* policy, const as_scan* scan, uint64_t task_id,
	as_scan_builder* sb
	)
{
	uint16_t n_ops = (scan->ops) ? scan->ops->binops.size : scan->select.size;
	uint8_t* p;
	
	if (scan->apply_each.function[0] || scan->ops) {
		p = as_command_write_header_write(cmd, &policy->base, AS_POLICY_COMMIT_LEVEL_ALL,
				AS_POLICY_EXISTS_IGNORE, AS_POLICY_GEN_IGNORE, 0, 0, sb->n_fields, n_ops,
				policy->durable_delete, 0, AS_MSG_INFO2_WRITE, 0);
	}
	else {
		uint8_t read_attr = (scan->no_bins)? AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA : AS_MSG_INFO1_READ;
		p = as_command_write_header_read(cmd, &policy->base, AS_POLICY_READ_MODE_AP_ONE,
				AS_POLICY_READ_MODE_SC_SESSION, policy->base.total_timeout, sb->n_fields, n_ops,
				read_attr);
	}
	
	if (scan->ns[0]) {
		p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, scan->ns);
	}
	
	if (scan->set[0]) {
		p = as_command_write_field_string(p, AS_FIELD_SETNAME, scan->set);
	}

	if (policy->records_per_second > 0) {
		p = as_command_write_field_uint32(p, AS_FIELD_SCAN_RPS, policy->records_per_second);
	}

	// Write socket timeout.
	p = as_command_write_field_uint32(p, AS_FIELD_SCAN_TIMEOUT, policy->base.socket_timeout);

	// Write task_id field.
	p = as_command_write_field_uint64(p, AS_FIELD_TASK_ID, task_id);
	sb->task_id_offset = ((uint32_t)(p - cmd)) - sizeof(uint64_t);
	
	// Write background function
	if (scan->apply_each.function[0]) {
		p = as_command_write_field_header(p, AS_FIELD_UDF_OP, 1);
		*p++ = 2;
		p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, scan->apply_each.module);
		p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, scan->apply_each.function);
		p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, &sb->argbuffer);
	}
	as_buffer_destroy(&sb->argbuffer);
	
	// Write predicate expressions.
	if (scan->predexp.size > 0) {
		p = as_command_write_field_header(p, AS_FIELD_FILTER, sb->predexp_size);
		for (uint16_t ii = 0; ii < scan->predexp.size; ++ii) {
			as_predexp_base * bp = scan->predexp.entries[ii];
			p = (*bp->write_fn)(bp, p);
		}
	}
	else if (policy->base.filter_exp) {
		p = as_exp_write(policy->base.filter_exp, p);
	}
	else if (policy->base.predexp) {
		p = as_predexp_list_write(policy->base.predexp, sb->predexp_size, p);
	}

	sb->cmd_size_pre = (uint32_t)(p - cmd);

	if (sb->parts_full_size > 0) {
		p = as_command_write_field_header(p, AS_FIELD_PID_ARRAY, sb->parts_full_size);

		as_vector* list = &sb->np->parts_full;

		for (uint32_t i = 0; i < list->size; i++) {
			uint16_t part_id = as_partition_tracker_get_id(list, i);
			*(uint16_t*)p = cf_swap_to_le16(part_id);
			p += sizeof(uint16_t);
		}
	}

	if (sb->parts_partial_size > 0) {
		p = as_command_write_field_header(p, AS_FIELD_DIGEST_ARRAY, sb->parts_partial_size);

		as_partition_tracker* pt = sb->pt;
		as_vector* list = &sb->np->parts_partial;

		for (uint32_t i = 0; i < list->size; i++) {
			as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
			memcpy(p, ps->digest.value, AS_DIGEST_VALUE_SIZE);
			p += AS_DIGEST_VALUE_SIZE;
		}
	}

	if (sb->max_records > 0) {
		p = as_command_write_field_uint64(p, AS_FIELD_SCAN_MAX_RECORDS, sb->max_records);
	}

	if (scan->ops) {
		as_operations* ops = scan->ops;

		for (uint16_t i = 0; i < ops->binops.size; i++) {
			as_binop* op = &ops->binops.entries[i];
			p = as_command_write_bin(p, op->op, &op->bin, sb->opsbuffers);
		}
		as_buffers_destroy(sb->opsbuffers);
	}
	else {
		for (uint16_t i = 0; i < scan->select.size; i++) {
			p = as_command_write_bin_name(p, scan->select.entries[i]);
		}
	}

	sb->cmd_size_post = ((uint32_t)(p - cmd)) - sb->cmd_size_pre;
	return as_command_write_end(cmd, p);
}

static as_status
as_scan_command_execute(as_scan_task* task)
{
	as_error err;
	as_error_init(&err);

	as_status status;

	if (task->cluster_key && ! task->first) {
		status = as_query_validate(&err, task->node, task->scan->ns, 10000, task->cluster_key);

		if (status) {
			// Set main error only once.
			if (as_fas_uint32(task->error_mutex, 1) == 0) {
				as_error_copy(task->err, &err);
			}
			return status;
		}
	}

	as_queue opsbuffers;

	if (task->scan->ops) {
		as_queue_inita(&opsbuffers, sizeof(as_buffer), task->scan->ops->binops.size);
	}

	as_scan_builder sb;
	sb.pt = task->pt;
	sb.np = task->np;
	sb.opsbuffers = &opsbuffers;

	if (task->pt) {
		sb.max_records = task->np->record_max;
	}
	else {
		sb.max_records = 0;
	}

	size_t size = as_scan_command_size(task->policy, task->scan, &sb);
	uint8_t* buf = as_command_buffer_init(size);
	size = as_scan_command_init(buf, task->policy, task->scan, task->task_id, &sb);

	as_command cmd;
	cmd.cluster = task->cluster;
	cmd.policy = &task->policy->base;
	cmd.node = task->node;
	cmd.ns = NULL;        // Not referenced when node set.
	cmd.partition = NULL; // Not referenced when node set.
	cmd.parse_results_fn = as_scan_parse_records;
	cmd.udata = task;
	cmd.buf = buf;
	cmd.buf_size = size;
	cmd.partition_id = 0; // Not referenced when node set.
	cmd.replica = AS_POLICY_REPLICA_MASTER;
	cmd.flags = AS_COMMAND_FLAGS_READ;

	as_command_start_timer(&cmd);

	status = as_command_execute(&cmd, &err);

	// Free command memory.
	as_command_buffer_free(buf, size);

	if (status) {
		if (task->pt && as_partition_tracker_should_retry(task->pt, status)) {
			return AEROSPIKE_OK;
		}

		// Set main error only once.
		if (as_fas_uint32(task->error_mutex, 1) == 0) {
			// Don't set error when user aborts query,
			if (status != AEROSPIKE_ERR_CLIENT_ABORT) {
				as_error_copy(task->err, &err);
			}
		}
		return status;
	}

	if (task->cluster_key) {
		status = as_query_validate(&err, task->node, task->scan->ns, 10000, task->cluster_key);

		if (status) {
			// Set main error only once.
			if (as_fas_uint32(task->error_mutex, 1) == 0) {
				as_error_copy(task->err, &err);
			}
			return status;
		}
	}
	return status;
}

static void
as_scan_worker(void* data)
{
	as_scan_task* task = (as_scan_task*)data;
	
	as_scan_complete_task complete_task;
	complete_task.node = task->node;
	complete_task.task_id = task->task_id;

	if (as_load_uint32(task->error_mutex) == 0) {
		complete_task.result = as_scan_command_execute(task);
	}
	else {
		complete_task.result = AEROSPIKE_ERR_SCAN_ABORTED;
	}

	cf_queue_push(task->complete_q, &complete_task);
}

static inline as_status
as_scan_validate(as_error* err, const as_policy_scan* policy, const as_scan* scan)
{
	as_error_reset(err);
	return AEROSPIKE_OK;
}

static as_status
as_scan_generic(
	as_cluster* cluster, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	aerospike_scan_foreach_callback callback, void* udata, uint64_t* task_id_ptr
	)
{
	as_status status = as_scan_validate(err, policy, scan);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_nodes* nodes;
	status = as_cluster_reserve_all_nodes(cluster, err, &nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	uint64_t cluster_key = 0;
	uint64_t task_id = as_task_id_resolve(task_id_ptr);

	// Initialize task.
	uint32_t error_mutex = 0;
	as_scan_task task;
	task.np = NULL;
	task.pt = NULL;
	task.cluster = cluster;
	task.policy = policy;
	task.scan = scan;
	task.callback = callback;
	task.udata = udata;
	task.err = err;
	task.error_mutex = &error_mutex;
	task.task_id = task_id;
	task.cluster_key = cluster_key;
	task.first = true;

	if (scan->concurrent) {
		uint32_t n_wait_nodes = nodes->size;
		task.complete_q = cf_queue_create(sizeof(as_scan_complete_task), true);

		// Run node scans in parallel.
		for (uint32_t i = 0; i < nodes->size; i++) {
			// Stack allocate task for each node.  It should be fine since the task
			// only needs to be valid within this function.
			as_scan_task* task_node = alloca(sizeof(as_scan_task));
			memcpy(task_node, &task, sizeof(as_scan_task));
			task_node->node = nodes->array[i];

			int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_scan_worker, task_node);
			
			if (rc) {
				// Thread could not be added. Abort entire scan.
				if (as_fas_uint32(task.error_mutex, 1) == 0) {
					status = as_error_update(task.err, AEROSPIKE_ERR_CLIENT, "Failed to add scan thread: %d", rc);
				}
				
				// Reset node count to threads that were run.
				n_wait_nodes = i;
				break;
			}
			task.first = false;
		}

		// Wait for tasks to complete.
		for (uint32_t i = 0; i < n_wait_nodes; i++) {
			as_scan_complete_task complete;
			cf_queue_pop(task.complete_q, &complete, CF_QUEUE_FOREVER);
			
			if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
				status = complete.result;
			}
		}
		
		// Release temporary queue.
		cf_queue_destroy(task.complete_q);
	}
	else {
		task.complete_q = 0;
		
		// Run node scans in series.
		for (uint32_t i = 0; i < nodes->size && status == AEROSPIKE_OK; i++) {
			task.node = nodes->array[i];
			status = as_scan_command_execute(&task);
			task.first = false;
		}
	}

	as_cluster_release_all_nodes(nodes);

	// If user aborts query, command is considered successful.
	if (status == AEROSPIKE_ERR_CLIENT_ABORT) {
		status = AEROSPIKE_OK;
	}

	// If completely successful, make the callback that signals completion.
	if (callback && status == AEROSPIKE_OK) {
		callback(NULL, udata);
	}
	return status;
}

static as_status
as_scan_partitions_validate(
	as_cluster* cluster, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	uint32_t* n_nodes
	)
{
	as_status status = as_scan_validate(err, policy, scan);

	if (status != AEROSPIKE_OK) {
		return status;
	}
	return as_cluster_validate_size(cluster, err, n_nodes);
}

static as_status
as_scan_partitions(
	as_cluster* cluster, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	as_partition_tracker* pt, aerospike_scan_foreach_callback callback, void* udata)
{
	as_status status;

	while (true) {
		uint64_t task_id = as_random_get_uint64();
		status = as_partition_tracker_assign(pt, cluster, scan->ns, err);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		uint32_t n_nodes = pt->node_parts.size;

		// Initialize task.
		uint32_t error_mutex = 0;
		as_scan_task task;
		task.pt = pt;
		task.cluster = cluster;
		task.policy = policy;
		task.scan = scan;
		task.callback = callback;
		task.udata = udata;
		task.err = err;
		task.error_mutex = &error_mutex;
		task.task_id = task_id;
		task.cluster_key = 0;
		task.first = false;

		if (scan->concurrent && n_nodes > 1) {
			uint32_t n_wait_nodes = n_nodes;
			task.complete_q = cf_queue_create(sizeof(as_scan_complete_task), true);

			// Run node scans in parallel.
			for (uint32_t i = 0; i < n_nodes; i++) {
				// Stack allocate task for each node.  It should be fine since the task
				// only needs to be valid within this function.
				as_scan_task* task_node = alloca(sizeof(as_scan_task));
				memcpy(task_node, &task, sizeof(as_scan_task));

				task_node->np = as_vector_get(&pt->node_parts, i);
				task_node->node = task_node->np->node;

				int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_scan_worker, task_node);
				
				if (rc) {
					// Thread could not be added. Abort entire scan.
					if (as_fas_uint32(task.error_mutex, 1) == 0) {
						status = as_error_update(task.err, AEROSPIKE_ERR_CLIENT, "Failed to add scan thread: %d", rc);
					}
					
					// Reset node count to threads that were run.
					n_wait_nodes = i;
					break;
				}
			}

			// Wait for tasks to complete.
			for (uint32_t i = 0; i < n_wait_nodes; i++) {
				as_scan_complete_task complete;
				cf_queue_pop(task.complete_q, &complete, CF_QUEUE_FOREVER);
				
				if (complete.result != AEROSPIKE_OK && status == AEROSPIKE_OK) {
					status = complete.result;
				}
			}
			
			// Release temporary queue.
			cf_queue_destroy(task.complete_q);
		}
		else {
			task.complete_q = 0;
			
			// Run node scans in series.
			for (uint32_t i = 0; i < n_nodes && status == AEROSPIKE_OK; i++) {
				task.np = as_vector_get(&pt->node_parts, i);
				task.node = task.np->node;
				status = as_scan_command_execute(&task);
			}
		}

		// If user aborts query, command is considered successful.
		if (status == AEROSPIKE_ERR_CLIENT_ABORT) {
			status = AEROSPIKE_OK;
			break;
		}

		if (status != AEROSPIKE_OK) {
			return status;
		}

		status = as_partition_tracker_is_complete(pt, err);

		// Stop on ok and all errors except AEROSPIKE_ERR_CLIENT.
		if (status != AEROSPIKE_ERR_CLIENT) {
			break;
		}

		if (pt->sleep_between_retries > 0) {
			// Sleep before trying again.
			as_sleep(pt->sleep_between_retries);
		}
	}

	if (status == AEROSPIKE_OK) {
		callback(NULL, udata);
	}
	return status;
}

static as_status
as_scan_partition_execute_async(as_async_scan_executor* se, as_partition_tracker* pt, as_error* err)
{
	as_event_executor* ee = &se->executor;
	uint32_t n_nodes = pt->node_parts.size;

	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_partitions* np = as_vector_get(&pt->node_parts, i);
		uint32_t parts_full_size = np->parts_full.size * 2;
		uint32_t parts_partial_size = np->parts_partial.size * 20;
		size_t size = se->cmd_size;
		uint16_t n_fields = se->n_fields;

		if (parts_full_size > 0) {
			size += parts_full_size + AS_FIELD_HEADER_SIZE;
			n_fields++;
		}

		if (parts_partial_size > 0) {
			size += parts_partial_size + AS_FIELD_HEADER_SIZE;
			n_fields++;
		}

		if (np->record_max > 0) {
			size += as_command_field_size(8);
			n_fields++;
		}

		// Allocate enough memory to cover, then, round up memory size in 8KB increments to reduce
		// fragmentation and to allow socket read to reuse buffer.
		size_t s = (sizeof(as_async_scan_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;
		as_async_scan_command* scmd = cf_malloc(s);
		scmd->np = np;

		as_event_command* cmd = (as_event_command*)scmd;
		cmd->buf = scmd->space;

		uint8_t* p = cmd->buf;

		// Copy first part of generic command.
		memcpy(p, se->cmd_buf, se->cmd_size_pre);

		// Update n_fields in header.
		*(uint16_t*)&p[26] = cf_swap_to_be16(n_fields);
		p += se->cmd_size_pre;

		// Write node specific partitions.
		if (parts_full_size > 0) {
			p = as_command_write_field_header(p, AS_FIELD_PID_ARRAY, parts_full_size);

			as_vector* list = &np->parts_full;

			for (uint32_t i = 0; i < list->size; i++) {
				uint16_t part_id = as_partition_tracker_get_id(list, i);
				*(uint16_t*)p = cf_swap_to_le16(part_id);
				p += sizeof(uint16_t);
			}
		}

		// Write node specific digests.
		if (parts_partial_size > 0) {
			p = as_command_write_field_header(p, AS_FIELD_DIGEST_ARRAY, parts_partial_size);

			as_vector* list = &np->parts_partial;

			for (uint32_t i = 0; i < list->size; i++) {
				as_partition_status* ps = as_partition_tracker_get_status(pt, list, i);
				memcpy(p, ps->digest.value, AS_DIGEST_VALUE_SIZE);
				p += AS_DIGEST_VALUE_SIZE;
			}
		}

		// Write record limit.
		if (np->record_max > 0) {
			p = as_command_write_field_uint64(p, AS_FIELD_SCAN_MAX_RECORDS, np->record_max);
		}

		memcpy(p, se->cmd_buf + se->cmd_size_pre, se->cmd_size_post);
		p += se->cmd_size_post;
		size = as_command_write_end(cmd->buf, p);

		cmd->total_deadline = pt->total_timeout;
		cmd->socket_timeout = pt->socket_timeout;
		cmd->max_retries = 0;
		cmd->iteration = 0;
		cmd->replica = AS_POLICY_REPLICA_MASTER;
		cmd->event_loop = ee->event_loop;
		cmd->cluster = se->cluster;
		cmd->node = np->node;
		// Reserve node because as_event_command_free() will release node
		// on command completion.
		as_node_reserve(cmd->node);
		cmd->ns = NULL;
		cmd->partition = NULL;
		cmd->udata = se;  // Overload udata to be the executor.
		cmd->parse_results = as_scan_parse_records_async;
		cmd->pipe_listener = NULL;
		cmd->write_len = (uint32_t)size;
		cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_scan_command));
		cmd->type = AS_ASYNC_TYPE_SCAN_PARTITION;
		cmd->proto_type = AS_MESSAGE_TYPE;
		cmd->state = AS_ASYNC_STATE_UNREGISTERED;
		cmd->flags = AS_ASYNC_FLAGS_MASTER;
		cmd->flags2 = se->deserialize_list_map ? AS_ASYNC_FLAGS2_DESERIALIZE : 0;
		ee->commands[i] = cmd;
	}

	// Run scan commands.
	uint32_t max = ee->max_concurrent;

	for (uint32_t i = 0; i < max; i++) {
		ee->queued++;

		as_event_command* cmd = ee->commands[i];
		as_status status = as_event_command_execute(cmd, err);

		if (status != AEROSPIKE_OK) {
			// as_event_executor_destroy() will release nodes that were not queued.
			// as_event_executor_cancel() or as_event_executor_error() will eventually
			// call as_event_executor_destroy().
			if (pt->iteration == 0) {
				// On first scan attempt, cleanup and do not call listener.
				as_scan_partition_executor_destroy(se);
				as_event_executor_cancel(ee, i);
			}
			else {
				// On scan retry, caller will cleanup and call listener.
				as_event_executor_error(ee, err, n_nodes - i);
			}
			return status;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_scan_partition_retry_async(as_async_scan_executor* se_old, as_error* err)
{
	as_async_scan_executor* se = cf_malloc(sizeof(as_async_scan_executor));
	se->listener = se_old->listener;
	se->cluster = se_old->cluster;
	se->pt = se_old->pt;
	se->cmd_buf = se_old->cmd_buf;
	se->cmd_size = se_old->cmd_size;
	se->cmd_size_pre = se_old->cmd_size_pre;
	se->cmd_size_post = se_old->cmd_size_post;
	se->task_id_offset = se_old->task_id_offset;
	se->n_fields = se_old->n_fields;
	se->concurrent = se_old->concurrent;
	se->deserialize_list_map = se_old->deserialize_list_map;

	// Must change task_id each round. Otherwise, server rejects command.
	uint64_t task_id = as_random_get_uint64();
	*(uint64_t*)(se->cmd_buf + se->task_id_offset) = task_id;

	uint32_t n_nodes = se->pt->node_parts.size;

	as_event_executor* ee_old = &se_old->executor;
	as_event_executor* ee = &se->executor;
	pthread_mutex_init(&ee->lock, NULL);
	ee->max = n_nodes;
	ee->max_concurrent = se->concurrent ? n_nodes : 1;
	ee->commands = cf_malloc(sizeof(as_event_command*) * n_nodes);
	ee->event_loop = ee_old->event_loop;
	ee->complete_fn = ee_old->complete_fn;
	ee->udata = ee_old->udata;
	ee->err = NULL;
	ee->ns = ee_old->ns;
	ee_old->ns = NULL;
	ee->cluster_key = 0;
	ee->count = 0;
	ee->queued = 0;
	ee->notify = true;
	ee->valid = true;

	return as_scan_partition_execute_async(se, se->pt, err);
}

static as_status
as_scan_partition_async(
	as_cluster* cluster, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	as_partition_tracker* pt, as_async_scan_listener listener, void* udata,
	as_event_loop* event_loop
	)
{
	pt->sleep_between_retries = 0;
	as_status status = as_partition_tracker_assign(pt, cluster, scan->ns, err);

	if (status != AEROSPIKE_OK) {
		as_partition_tracker_destroy(pt);
		cf_free(pt);
		return status;
	}

	as_queue opsbuffers;

	if (scan->ops) {
		as_queue_inita(&opsbuffers, sizeof(as_buffer), scan->ops->binops.size);
	}

	// Create scan command buffer without partition fields.
	// The partition fields will be added later.
	uint64_t task_id = as_random_get_uint64();
	as_scan_builder sb;
	sb.pt = NULL;
	sb.np = NULL;
	sb.opsbuffers = &opsbuffers;
	sb.max_records = 0;

	size_t cmd_size = as_scan_command_size(policy, scan, &sb);
	uint8_t* cmd_buf = cf_malloc(cmd_size);
	cmd_size = as_scan_command_init(cmd_buf, policy, scan, task_id, &sb);

	as_async_scan_executor* se = cf_malloc(sizeof(as_async_scan_executor));
	se->listener = listener;
	se->cluster = cluster;
	se->pt = pt;
	se->cmd_buf = cmd_buf;
	se->cmd_size = (uint32_t)cmd_size;
	se->cmd_size_pre = sb.cmd_size_pre;
	se->cmd_size_post = sb.cmd_size_post;
	se->task_id_offset = sb.task_id_offset;
	se->n_fields = sb.n_fields;
	se->concurrent = scan->concurrent;
	se->deserialize_list_map = scan->deserialize_list_map;

	uint32_t n_nodes = pt->node_parts.size;

	as_event_executor* ee = &se->executor;
	pthread_mutex_init(&ee->lock, NULL);
	ee->max = n_nodes;
	ee->max_concurrent = scan->concurrent ? n_nodes : 1;
	ee->commands = cf_malloc(sizeof(as_event_command*) * n_nodes);
	ee->event_loop = as_event_assign(event_loop);
	ee->complete_fn = as_scan_partition_complete_async;
	ee->udata = udata;
	ee->err = NULL;
	ee->ns = cf_strdup(scan->ns);
	ee->cluster_key = 0;
	ee->count = 0;
	ee->queued = 0;
	ee->notify = true;
	ee->valid = true;

	return as_scan_partition_execute_async(se, pt, err);
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

bool
as_async_scan_should_retry(void* udata, as_status status)
{
	as_async_scan_executor* ase = udata;
	return as_partition_tracker_should_retry(ase->pt, status);
}

as_status
aerospike_scan_background(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	uint64_t* scan_id
	)
{
	if (! policy) {
		policy = &as->config.policies.scan;
	}

	return as_scan_generic(as->cluster, err, policy, scan, 0, 0, scan_id);
}

as_status
aerospike_scan_wait(
	aerospike* as, as_error* err, const as_policy_info* policy, uint64_t scan_id,
	uint32_t interval_ms
	)
{
	return aerospike_job_wait(as, err, policy, "scan", scan_id, interval_ms);
}

as_status
aerospike_scan_info(
	aerospike* as, as_error* err, const as_policy_info* policy, uint64_t scan_id, as_scan_info* info
	)
{
	as_job_info job_info;
	as_status status = aerospike_job_info(as, err, policy, "scan", scan_id, false, &job_info);
	
	if (status == AEROSPIKE_OK) {
		switch (job_info.status) {
			case AS_JOB_STATUS_COMPLETED:
				info->status = AS_SCAN_STATUS_COMPLETED;
				break;
				
			case AS_JOB_STATUS_INPROGRESS:
				info->status = AS_SCAN_STATUS_INPROGRESS;
				break;
			
			default:
				info->status = AS_SCAN_STATUS_UNDEF;
				break;
		}
		info->progress_pct = job_info.progress_pct;
		info->records_scanned = job_info.records_read;
	}
	return status;
}

as_status
aerospike_scan_foreach(
	aerospike* as, as_error* err, const as_policy_scan* policy, as_scan* scan,
	aerospike_scan_foreach_callback callback, void* udata
	)
{
	if (! policy) {
		policy = &as->config.policies.scan;
	}

	as_cluster* cluster = as->cluster;
	uint32_t n_nodes;
	as_status status = as_scan_partitions_validate(cluster, err, policy, scan, &n_nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_partition_tracker pt;
	as_partition_tracker_init_nodes(&pt, cluster, policy, scan, n_nodes);
	status = as_scan_partitions(cluster, err, policy, scan, &pt, callback, udata);
	as_partition_tracker_destroy(&pt);
	return status;
}

as_status
aerospike_scan_node(
	aerospike* as, as_error* err, const as_policy_scan* policy, as_scan* scan,
	const char* node_name, aerospike_scan_foreach_callback callback, void* udata
	)
{
	if (! policy) {
		policy = &as->config.policies.scan;
	}

	as_cluster* cluster = as->cluster;

	// Retrieve node.
	as_node* node = as_node_get_by_name(cluster, node_name);
		
	if (! node) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid node name: %s", node_name);
	}

	as_status status = as_scan_validate(err, policy, scan);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_partition_tracker pt;
	as_partition_tracker_init_node(&pt, cluster, policy, scan, node);
	status = as_scan_partitions(cluster, err, policy, scan, &pt, callback, udata);
	as_partition_tracker_destroy(&pt);
	as_node_release(node);
	return status;
}

as_status
aerospike_scan_partitions(
	aerospike* as, as_error* err, const as_policy_scan* policy, as_scan* scan,
	as_partition_filter* pf, aerospike_scan_foreach_callback callback, void* udata
	)
{
	as_cluster* cluster = as->cluster;

	if (! policy) {
		policy = &as->config.policies.scan;
	}

	uint32_t n_nodes;
	as_status status = as_scan_partitions_validate(cluster, err, policy, scan, &n_nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_partition_tracker pt;
	status = as_partition_tracker_init_filter(&pt, cluster, policy, scan, n_nodes, pf, err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	status = as_scan_partitions(cluster, err, policy, scan, &pt, callback, udata);
	as_partition_tracker_destroy(&pt);
	return status;
}

as_status
aerospike_scan_async(
	aerospike* as, as_error* err, const as_policy_scan* policy, as_scan* scan,
	uint64_t* scan_id, as_async_scan_listener listener, void* udata, as_event_loop* event_loop
	)
{
	if (! policy) {
		policy = &as->config.policies.scan;
	}

	as_status status = as_scan_validate(err, policy, scan);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_cluster* cluster = as->cluster;
	uint32_t n_nodes;
	status = as_cluster_validate_size(cluster, err, &n_nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_partition_tracker* pt = cf_malloc(sizeof(as_partition_tracker));
	as_partition_tracker_init_nodes(pt, cluster, policy, scan, n_nodes);
	return as_scan_partition_async(cluster, err, policy, scan, pt, listener, udata, event_loop);
}

as_status
aerospike_scan_node_async(
	aerospike* as, as_error* err, const as_policy_scan* policy, as_scan* scan,
	uint64_t* scan_id, const char* node_name, as_async_scan_listener listener, void* udata,
	as_event_loop* event_loop
	)
{
	if (! policy) {
		policy = &as->config.policies.scan;
	}

	as_status status = as_scan_validate(err, policy, scan);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_cluster* cluster = as->cluster;

	// Retrieve and reserve node.
	as_node* node = as_node_get_by_name(cluster, node_name);
	
	if (! node) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid node name: %s", node_name);
	}

	as_partition_tracker* pt = cf_malloc(sizeof(as_partition_tracker));
	as_partition_tracker_init_node(pt, cluster, policy, scan, node);
	status = as_scan_partition_async(cluster, err, policy, scan, pt, listener, udata,
									 event_loop);
	as_node_release(node);
	return status;
}

as_status
aerospike_scan_partitions_async(
	aerospike* as, as_error* err, const as_policy_scan* policy, as_scan* scan,
	as_partition_filter* pf, as_async_scan_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_cluster* cluster = as->cluster;

	if (! policy) {
		policy = &as->config.policies.scan;
	}

	uint32_t n_nodes;
	as_status status = as_scan_partitions_validate(cluster, err, policy, scan, &n_nodes);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_partition_tracker* pt = cf_malloc(sizeof(as_partition_tracker));
	status = as_partition_tracker_init_filter(pt, cluster, policy, scan, n_nodes, pf, err);

	if (status != AEROSPIKE_OK) {
		cf_free(pt);
		return status;
	}
	return as_scan_partition_async(cluster, err, policy, scan, pt, listener, udata, event_loop);
}
