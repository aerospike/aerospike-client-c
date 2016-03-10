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
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/as_async.h>
#include <aerospike/as_command.h>
#include <aerospike/as_job.h>
#include <aerospike/as_key.h>
#include <aerospike/as_log.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_random.h>
#include <aerospike/as_serializer.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_thread_pool.h>
#include <citrusleaf/cf_clock.h>
#include <citrusleaf/cf_queue.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

typedef struct as_scan_task_s {
	as_node* node;
	
	as_cluster* cluster;
	const as_policy_scan* policy;
	const as_scan* scan;
	aerospike_scan_foreach_callback callback;
	void* udata;
	as_error* err;
	cf_queue* complete_q;
	uint32_t* error_mutex;
	uint64_t task_id;
	
	uint8_t* cmd;
	size_t cmd_size;
} as_scan_task;

typedef struct as_scan_complete_task_s {
	as_node* node;
	uint64_t task_id;
	as_status result;
} as_scan_complete_task;

typedef struct as_async_scan_executor {
	as_event_executor executor;
	as_async_scan_listener listener;
} as_async_scan_executor;

typedef struct as_async_scan_command {
	as_event_command command;
	uint8_t space[];
} as_async_scan_command;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static void
as_scan_complete_async(as_event_executor* executor, as_error* err)
{
	((as_async_scan_executor*)executor)->listener(err, 0, executor->udata, executor->event_loop);
}

static bool
as_scan_parse_record_async(as_event_command* cmd, uint8_t** pp, as_msg* msg)
{
	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	
	uint8_t* p = *pp;
	p = as_command_parse_key(p, msg->n_fields, &rec.key);
	p = as_command_parse_bins(&rec, p, msg->n_ops, cmd->deserialize);
	*pp = p;
	
	as_event_executor* executor = cmd->udata;  // udata is overloaded to contain executor.
	bool rv = ((as_async_scan_executor*)executor)->listener(0, &rec, executor->udata, executor->event_loop);
	as_record_destroy(&rec);
	return rv;
}

static bool
as_scan_parse_records_async(as_event_command* cmd)
{
	as_event_executor* executor = cmd->udata;  // udata is overloaded to contain executor.
	uint8_t* p = cmd->buf;
	uint8_t* end = p + cmd->len;
	
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
				return true;
			}
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
		
		if (! executor->valid) {
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT_ABORT, "");
			as_event_response_error(cmd, &err);
			return true;
		}

		if (! as_scan_parse_record_async(cmd, &p, msg)) {
			executor->valid = false;
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT_ABORT, "");
			as_event_response_error(cmd, &err);
			return true;
		}
	}
	return false;
}

static as_status
as_scan_parse_record(uint8_t** pp, as_msg* msg, as_scan_task* task)
{
	as_record rec;
	as_record_inita(&rec, msg->n_ops);
	
	rec.gen = msg->generation;
	rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
	
	uint8_t* p = *pp;
	p = as_command_parse_key(p, msg->n_fields, &rec.key);
	p = as_command_parse_bins(&rec, p, msg->n_ops, task->scan->deserialize_list_map);
	*pp = p;
	
	bool rv = true;

	if (task->callback) {
		rv = task->callback((as_val*)&rec, task->udata);
	}
	as_record_destroy(&rec);
	return rv ? AEROSPIKE_OK : AEROSPIKE_ERR_CLIENT_ABORT;
}

static as_status
as_scan_parse_records(uint8_t* buf, size_t size, as_scan_task* task, as_error* err)
{
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
		
		status = as_scan_parse_record(&p, msg, task);
		
		if (status != AEROSPIKE_OK) {
			return status;
		}
		
		if (ck_pr_load_32(task->error_mutex)) {
			err->code = AEROSPIKE_ERR_SCAN_ABORTED;
			return err->code;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_scan_parse(as_error* err, int fd, uint64_t deadline_ms, void* udata)
{
	as_scan_task* task = udata;
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
			
			status = as_scan_parse_records(buf, size, task, err);
			
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

static as_status
as_scan_command_execute(as_scan_task* task)
{
	as_command_node cn;
	cn.node = task->node;
	
	as_error err;
	as_error_init(&err);
	as_status status = as_command_execute(task->cluster, &err, &cn, task->cmd, task->cmd_size, task->policy->timeout, 0, as_scan_parse, task);
	
	if (status) {
		// Set main error only once.
		if (ck_pr_fas_32(task->error_mutex, 1) == 0) {
			// Don't set error when user aborts query,
			if (status != AEROSPIKE_ERR_CLIENT_ABORT) {
				as_error_copy(task->err, &err);
			}
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
	complete_task.result = as_scan_command_execute(task);
		
	cf_queue_push(task->complete_q, &complete_task);
}

static size_t
as_scan_command_size(const as_scan* scan, uint16_t* fields, as_buffer* argbuffer)
{
	// Build Command.  It's okay to share command across threads because scan does not have retries.
	// If retries were allowed, the timeout field in the command would change on retry which
	// would conflict with other threads.
	size_t size = AS_HEADER_SIZE;
	uint16_t n_fields = 0;
	
	if (scan->ns) {
		size += as_command_string_field_size(scan->ns);
		n_fields++;
	}
	
	if (scan->set) {
		size += as_command_string_field_size(scan->set);
		n_fields++;
	}
	
	// Estimate scan options size.
	size += as_command_field_size(2);
	n_fields++;
	
	// Estimate taskId size.
	size += as_command_field_size(8);
	n_fields++;
	
	// Estimate background function size.
	as_buffer_init(argbuffer);
	
	if (scan->apply_each.function[0]) {
		size += as_command_field_size(1);
		size += as_command_string_field_size(scan->apply_each.module);
		size += as_command_string_field_size(scan->apply_each.function);
		
		if (scan->apply_each.arglist) {
			// If the query has a udf w/ arglist, then serialize it.
			as_serializer ser;
			as_msgpack_init(&ser);
            as_serializer_serialize(&ser, (as_val*)scan->apply_each.arglist, argbuffer);
			as_serializer_destroy(&ser);
		}
		size += as_command_field_size(argbuffer->size);
		n_fields += 4;
	}
	
	// Estimate size for selected bin names.
	if (scan->select.size > 0) {
		for (uint16_t i = 0; i < scan->select.size; i++) {
			size += as_command_string_operation_size(scan->select.entries[i]);
		}
	}
	*fields = n_fields;
	return size;
}

static size_t
as_scan_command_init(uint8_t* cmd, const as_policy_scan* policy, const as_scan* scan,
	uint64_t task_id, uint16_t n_fields, as_buffer* argbuffer)
{
	uint8_t* p;
	
	if (scan->apply_each.function[0]) {
		p = as_command_write_header(cmd, AS_MSG_INFO1_READ, AS_MSG_INFO2_WRITE,
			AS_POLICY_COMMIT_LEVEL_ALL, AS_POLICY_CONSISTENCY_LEVEL_ONE, AS_POLICY_EXISTS_IGNORE,
			AS_POLICY_GEN_IGNORE, 0, 0, policy->timeout, n_fields, 0);
	}
	else {
		uint8_t read_attr = (scan->no_bins)? AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_NOBINDATA : AS_MSG_INFO1_READ;
		p = as_command_write_header_read(cmd, read_attr, AS_POLICY_CONSISTENCY_LEVEL_ONE, policy->timeout, n_fields, scan->select.size);
	}
	
	if (scan->ns) {
		p = as_command_write_field_string(p, AS_FIELD_NAMESPACE, scan->ns);
	}
	
	if (scan->set) {
		p = as_command_write_field_string(p, AS_FIELD_SETNAME, scan->set);
	}
	
	// Write scan options
	p = as_command_write_field_header(p, AS_FIELD_SCAN_OPTIONS, 2);
	uint8_t priority = scan->priority << 4;
	
	if (policy->fail_on_cluster_change) {
		priority |= 0x08;
	}
	
	if (scan->include_ldt) {
		priority |= 0x02;
	}
	
	*p++ = priority;
	*p++ = scan->percent;
	
	// Write taskId field
	p = as_command_write_field_uint64(p, AS_FIELD_TASK_ID, task_id);
	
	// Write background function
	if (scan->apply_each.function[0]) {
		p = as_command_write_field_header(p, AS_FIELD_UDF_OP, 1);
		*p++ = 2;
		p = as_command_write_field_string(p, AS_FIELD_UDF_PACKAGE_NAME, scan->apply_each.module);
		p = as_command_write_field_string(p, AS_FIELD_UDF_FUNCTION, scan->apply_each.function);
		p = as_command_write_field_buffer(p, AS_FIELD_UDF_ARGLIST, argbuffer);
	}
    as_buffer_destroy(argbuffer);
	
	if (scan->select.size > 0) {
		for (uint16_t i = 0; i < scan->select.size; i++) {
			p = as_command_write_bin_name(p, scan->select.entries[i]);
		}
	}
	return as_command_write_end(cmd, p);
}

static as_status
as_scan_generic(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	aerospike_scan_foreach_callback callback, void* udata, uint64_t* task_id_ptr)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.scan;
	}
	
	as_cluster* cluster = as->cluster;
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Scan command failed because cluster is empty.");
	}
	
	// Reserve each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_reserve(nodes->array[i]);
	}
	
	uint64_t task_id;
	if (task_id_ptr) {
		if (*task_id_ptr == 0) {
			*task_id_ptr = as_random_get_uint64();
		}
		task_id = *task_id_ptr;
	}
	else {
		task_id = as_random_get_uint64();
	}

	// Create scan command
	as_buffer argbuffer;
	uint16_t n_fields = 0;
	size_t size = as_scan_command_size(scan, &n_fields, &argbuffer);
	uint8_t* cmd = as_command_init(size);
	size = as_scan_command_init(cmd, policy, scan, task_id, n_fields, &argbuffer);
	
	// Initialize task.
	uint32_t error_mutex = 0;
	as_scan_task task;
	task.cluster = as->cluster;
	task.policy = policy;
	task.scan = scan;
	task.callback = callback;
	task.udata = udata;
	task.err = err;
	task.error_mutex = &error_mutex;
	task.task_id = task_id;
	task.cmd = cmd;
	task.cmd_size = size;
	
	as_status status = AEROSPIKE_OK;
	
	if (scan->concurrent) {
		uint32_t n_wait_nodes = n_nodes;
		task.complete_q = cf_queue_create(sizeof(as_scan_complete_task), true);

		// Run node scans in parallel.
		for (uint32_t i = 0; i < n_nodes; i++) {
			// Stack allocate task for each node.  It should be fine since the task
			// only needs to be valid within this function.
			as_scan_task* task_node = alloca(sizeof(as_scan_task));
			memcpy(task_node, &task, sizeof(as_scan_task));
			task_node->node = nodes->array[i];
			
			int rc = as_thread_pool_queue_task(&cluster->thread_pool, as_scan_worker, task_node);
			
			if (rc) {
				// Thread could not be added. Abort entire scan.
				if (ck_pr_fas_32(task.error_mutex, 1) == 0) {
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
			task.node = nodes->array[i];
			status = as_scan_command_execute(&task);
		}
	}
	
	// Release each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_release(nodes->array[i]);
	}
	
	// Release nodes array.
	as_nodes_release(nodes);

	// Free command memory.
	as_command_free(cmd, size);

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
as_scan_async(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan, uint64_t* scan_id,
	as_async_scan_listener listener, void* udata, as_event_loop* event_loop,
	as_node** nodes, uint32_t n_nodes
	)
{
	if (! policy) {
		policy = &as->config.policies.scan;
	}
	
	// Assign task id.
	uint64_t task_id;
	if (scan_id) {
		if (*scan_id == 0) {
			*scan_id = as_random_get_uint64();
		}
		task_id = *scan_id;
	}
	else {
		task_id = as_random_get_uint64();
	}
	
	bool daisy_chain = ! (scan->concurrent || n_nodes == 1);

	// Scan will be split up into a command for each node.
	// Allocate scan data shared by each command.
	as_async_scan_executor* executor = cf_malloc(sizeof(as_async_scan_executor));
	as_event_executor* exec = &executor->executor;
	pthread_mutex_init(&exec->lock, NULL);
	exec->event_loop = as_event_assign(event_loop);
	exec->complete_fn = as_scan_complete_async;
	exec->udata = udata;
	exec->max = n_nodes;
	exec->count = 0;
	exec->valid = true;
	executor->listener = listener;
	
	if (daisy_chain) {
		exec->commands = cf_malloc(sizeof(as_event_command*) * n_nodes);
		exec->max_concurrent = 1;
	}
	else {
		exec->commands = 0;
		exec->max_concurrent = n_nodes;
	}

	// Create scan command buffer.
	as_buffer argbuffer;
	uint16_t n_fields = 0;
	size_t size = as_scan_command_size(scan, &n_fields, &argbuffer);
	uint8_t* cmd_buf = as_command_init(size);
	size = as_scan_command_init(cmd_buf, policy, scan, task_id, n_fields, &argbuffer);
	
	// Allocate enough memory to cover, then, round up memory size in 8KB increments to allow socket
	// read to reuse buffer.
	size_t s = (sizeof(as_async_scan_command) + size + AS_AUTHENTICATION_MAX_SIZE + 8191) & ~8191;

	as_status status = AEROSPIKE_OK;

	// Create all scan commands.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_event_command* cmd = cf_malloc(s);
		cmd->event_loop = exec->event_loop;
		cmd->conn = 0;
		cmd->cluster = as->cluster;
		cmd->node = nodes[i];
		cmd->udata = executor;  // Overload udata to be the executor.
		cmd->parse_results = as_scan_parse_records_async;
		cmd->buf = ((as_async_scan_command*)cmd)->space;
		cmd->capacity = (uint32_t)(s - sizeof(as_async_scan_command));
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->auth_len = 0;
		cmd->timeout_ms = policy->timeout;
		cmd->type = AS_ASYNC_TYPE_SCAN;
		cmd->state = AS_ASYNC_STATE_UNREGISTERED;
		cmd->pipe_listener = NULL;
		cmd->deserialize = scan->deserialize_list_map;
		cmd->free_buf = false;
		memcpy(cmd->buf, cmd_buf, size);
		
		if (daisy_chain) {
			exec->commands[i] = cmd;
		}
		else {
			status = as_event_command_execute(cmd, err);
			
			if (status != AEROSPIKE_OK) {
				as_event_executor_cancel(exec, i);
				break;
			}
		}
	}
	
	// Free command buffer.
	as_command_free(cmd_buf, size);

	// If scanning one node at a time, start first command.
	if (status == AEROSPIKE_OK && daisy_chain) {
		as_event_command* cmd = exec->commands[0];
		status = as_event_command_execute(cmd, err);
		
		if (status != AEROSPIKE_OK) {
			as_event_executor_cancel(exec, 0);
		}
	}
	return status;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_status
aerospike_scan_background(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	uint64_t* scan_id
	)
{
	return as_scan_generic(as, err, policy, scan, 0, 0, scan_id);
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
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	aerospike_scan_foreach_callback callback, void* udata
	)
{
	return as_scan_generic(as, err, policy, scan, callback, udata, 0);
}

as_status
aerospike_scan_node(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan,
	const char* node_name, aerospike_scan_foreach_callback callback, void * udata
	)
{
	as_error_reset(err);
	
	if (! policy) {
		policy = &as->config.policies.scan;
	}

	// Retrieve node.
	as_node* node = as_node_get_by_name(as->cluster, node_name);
	
	if (! node) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid node name: %s", node_name);
	}

	// Create scan command
	uint64_t task_id = as_random_get_uint64();
	as_buffer argbuffer;
	uint16_t n_fields = 0;
	size_t size = as_scan_command_size(scan, &n_fields, &argbuffer);
	uint8_t* cmd = as_command_init(size);
	size = as_scan_command_init(cmd, policy, scan, task_id, n_fields, &argbuffer);
	
	// Initialize task.
	uint32_t error_mutex = 0;
	as_scan_task task;
	task.node = node;
	task.cluster = as->cluster;
	task.policy = policy;
	task.scan = scan;
	task.callback = callback;
	task.udata = udata;
	task.err = err;
	task.complete_q = 0;
	task.error_mutex = &error_mutex;
	task.task_id = task_id;
	task.cmd = cmd;
	task.cmd_size = size;
	
	// Run scan.
	as_status status = as_scan_command_execute(&task);
		
	// Free command memory.
	as_command_free(cmd, size);
	
	// Release node.
	as_node_release(node);
	
	// If completely successful, make the callback that signals completion.
	if (callback && status == AEROSPIKE_OK) {
		callback(NULL, udata);
	}
	return status;
}

as_status
aerospike_scan_async(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan, uint64_t* scan_id,
	as_async_scan_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_error_reset(err);

	as_nodes* nodes = as_nodes_reserve(as->cluster);
	uint32_t n_nodes = nodes->size;
	
	if (n_nodes == 0) {
		as_nodes_release(nodes);
		return as_error_set_message(err, AEROSPIKE_ERR_SERVER, "Scan command failed because cluster is empty.");
	}

	// Reserve each node in cluster.
	for (uint32_t i = 0; i < n_nodes; i++) {
		as_node_reserve(nodes->array[i]);
	}
	
	as_status status = as_scan_async(as, err, policy, scan, scan_id, listener, udata, event_loop, nodes->array, n_nodes);
	as_nodes_release(nodes);
	return status;
}

as_status
aerospike_scan_node_async(
	aerospike* as, as_error* err, const as_policy_scan* policy, const as_scan* scan, uint64_t* scan_id,
	const char* node_name, as_async_scan_listener listener, void* udata, as_event_loop* event_loop
	)
{
	as_error_reset(err);

	// Retrieve and reserve node.
	as_node* node = as_node_get_by_name(as->cluster, node_name);
	
	if (! node) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid node name: %s", node_name);
	}
	
	return as_scan_async(as, err, policy, scan, scan_id, listener, udata, event_loop, &node, 1);
}
