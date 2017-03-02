/*
 * Copyright 2008-2017 Aerospike, Inc.
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
#include <aerospike/as_event.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_command.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_pipe.h>
#include <aerospike/as_proto.h>
#include <citrusleaf/alloc.h>
#include <errno.h>
#include <pthread.h>

/******************************************************************************
 * GLOBALS
 *****************************************************************************/

as_event_loop* as_event_loops = 0;
as_event_loop* as_event_loop_current = 0;
uint32_t as_event_loop_capacity = 0;
uint32_t as_event_loop_size = 0;
int as_event_send_buffer_size = 0;
int as_event_recv_buffer_size = 0;
bool as_event_threads_created = false;

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

// Force link error on event initialization when event library not defined.
#if AS_EVENT_LIB_DEFINED

static bool
as_event_initialize_loops(uint32_t capacity)
{
	if (capacity == 0) {
		return false;
	}
	
	as_event_send_buffer_size = as_pipe_get_send_buffer_size();
	as_event_recv_buffer_size = as_pipe_get_recv_buffer_size();

	as_event_loops = cf_calloc(capacity, sizeof(as_event_loop));
	
	if (! as_event_loops) {
		return false;
	}

	as_event_loop_capacity = capacity;
	as_event_loop_current = as_event_loops;
	
	// Initialize first loop to circular linked list for efficient round-robin
	// event loop distribution.
	as_event_loops->next = as_event_loops;
	return true;
}

as_event_loop*
as_event_create_loops(uint32_t capacity)
{
	if (! as_event_initialize_loops(capacity)) {
		return 0;
	}
	
	as_event_threads_created = true;
	
	for (uint32_t i = 0; i < capacity; i++) {
		as_event_loop* event_loop = &as_event_loops[i];

		event_loop->loop = 0;
		pthread_mutex_init(&event_loop->lock, 0);
		event_loop->thread = 0;
		event_loop->index = i;
		as_queue_init(&event_loop->pipe_cb_queue, sizeof(as_queued_pipe_cb), AS_EVENT_QUEUE_INITIAL_CAPACITY);
		event_loop->pipe_cb_calling = false;

		if (! as_event_create_loop(event_loop)) {
			as_event_close_loops();
			return 0;
		}
		
		if (i > 0) {
			// This loop points to first loop to create circular round-robin linked list.
			event_loop->next = as_event_loops;
			
			// Adjust previous loop to point to this loop.
			as_event_loops[i - 1].next = event_loop;
		}
		as_event_loop_size++;
	}
	return as_event_loops;
}

bool
as_event_set_external_loop_capacity(uint32_t capacity)
{
	if (! as_event_initialize_loops(capacity)) {
		return 0;
	}
	
	as_event_threads_created = false;
	return true;
}

#endif

as_event_loop*
as_event_set_external_loop(void* loop)
{
	uint32_t current = ck_pr_faa_32(&as_event_loop_size, 1);
	
	if (current >= as_event_loop_capacity) {
		as_log_error("Failed to add external loop. Capacity is %u", as_event_loop_capacity);
		return 0;
	}
	
	as_event_loop* event_loop = &as_event_loops[current];
	event_loop->loop = loop;
	pthread_mutex_init(&event_loop->lock, 0);
	event_loop->thread = pthread_self();  // Current thread must be same as event loop thread!
	event_loop->index = current;
	as_queue_init(&event_loop->pipe_cb_queue, sizeof(as_queued_pipe_cb), AS_EVENT_QUEUE_INITIAL_CAPACITY);
	event_loop->pipe_cb_calling = false;
	as_event_register_external_loop(event_loop);

	if (current > 0) {
		// This loop points to first loop to create circular round-robin linked list.
		event_loop->next = as_event_loops;
		
		// Adjust previous loop to point to this loop.
		// Warning: not synchronized with as_event_loop_get()
		as_event_loops[current - 1].next = event_loop;
	}
	return event_loop;
}

as_event_loop*
as_event_loop_find(void* loop)
{
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_loop* event_loop = &as_event_loops[i];
		
		if (event_loop->loop == loop) {
			return event_loop;
		}
	}
	return NULL;
}

bool
as_event_close_loops()
{
	if (! as_event_loops) {
		return false;
	}
	
	bool status = true;
	
	// Close or send close signal to all event loops.
	// This will eventually release resources associated with each event loop.
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_loop* event_loop = &as_event_loops[i];
	
		// Calling close directly can cause previously queued commands to be dropped.
		// Therefore, always queue close command to event loop.
		if (! as_event_send_close_loop(event_loop)) {
			as_log_error("Failed to send stop command to event loop");
			status = false;
		}
	}

	// Only join threads if event loops were created internally.
	// It is not possible to join on externally created event loop threads.
	if (as_event_threads_created && status) {
		for (uint32_t i = 0; i < as_event_loop_size; i++) {
			as_event_loop* event_loop = &as_event_loops[i];
			pthread_join(event_loop->thread, NULL);
		}
		as_event_destroy_loops();
	}
	return status;
}

void
as_event_destroy_loops()
{
	if (as_event_loops) {
		cf_free(as_event_loops);
		as_event_loops = NULL;
		as_event_loop_size = 0;
	}
}

/******************************************************************************
 * PRIVATE FUNCTIONS
 *****************************************************************************/

as_status
as_event_command_execute(as_event_command* cmd, as_error* err)
{
	ck_pr_inc_32(&cmd->cluster->async_pending);
	
	// Only do this after the above increment to avoid a race with as_cluster_destroy().
	if (!cmd->cluster->valid) {
		as_event_command_free(cmd);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Client shutting down");
	}

	// Use pointer comparison for performance.
	// If portability becomes an issue, use "pthread_equal(event_loop->thread, pthread_self())"
	// instead.
	if (cmd->event_loop->thread == pthread_self()) {
		// We are already in event loop thread, so start processing.
		as_event_command_begin(cmd);
	}
	else {
		if (cmd->timeout_ms) {
			// Store current time in first 8 bytes which is not used yet.
			*(uint64_t*)cmd = cf_getms();
		}
		
		// Send command through queue so it can be executed in event loop thread.
		if (! as_event_send(cmd)) {
			as_event_command_free(cmd);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to queue command");
		}
	}
	return AEROSPIKE_OK;
}

static inline void
as_event_release_async_connection(as_event_command* cmd)
{
	as_queue* queue = &cmd->node->async_conn_qs[cmd->event_loop->index];
	as_event_release_connection(cmd->cluster, cmd->conn, queue);
}

static inline void
as_event_put_connection(as_event_command* cmd)
{
	as_event_set_conn_last_used(cmd->conn);
	as_queue* queue = &cmd->node->async_conn_qs[cmd->event_loop->index];

	if (as_queue_push(queue, &cmd->conn)) {
		ck_pr_inc_32(&cmd->cluster->async_conn_pool);
	} else {
		as_event_release_connection(cmd->cluster, cmd->conn, queue);
	}
}

static inline void
as_event_response_complete(as_event_command* cmd)
{
	if (cmd->pipe_listener != NULL) {
		as_pipe_response_complete(cmd);
		return;
	}
	
	as_event_stop_timer(cmd);
	as_event_stop_watcher(cmd, cmd->conn);
	as_event_put_connection(cmd);
}

static inline void
as_event_executor_destroy(as_event_executor* executor)
{
	pthread_mutex_destroy(&executor->lock);
	
	if (executor->commands) {
		cf_free(executor->commands);
	}
	
	cf_free(executor);
}

static void
as_event_executor_error(as_event_executor* executor, as_error* err, int queued_count)
{
	pthread_mutex_lock(&executor->lock);
	bool notify = executor->valid;
	executor->valid = false;

	if (queued_count >= 0) {
		// Add tasks that were never queued.
		executor->count += (executor->max - queued_count);
	}
	else {
		executor->count++;
	}

	bool complete = executor->count == executor->max;
	pthread_mutex_unlock(&executor->lock);

	// Notify user only once on first error.
	if (notify) {
		executor->complete_fn(executor, err);
	}

	// If all commands complete, destroy executor.
	if (complete) {
		as_event_executor_destroy(executor);
	}
}

void
as_event_executor_cancel(as_event_executor* executor, int queued_count)
{
	// Cancel group of commands that already have been queued.
	// We are cancelling commands running in the event loop thread when this method
	// is NOT running in the event loop thread.  Enforce thread-safety.
	pthread_mutex_lock(&executor->lock);
	executor->valid = false;
	
	// Add tasks that were never queued.
	executor->count += (executor->max - queued_count);
	
	bool complete = executor->count == executor->max;
	pthread_mutex_unlock(&executor->lock);

	if (complete) {
		as_event_executor_destroy(executor);
	}
}

void
as_event_executor_complete(as_event_command* cmd)
{
	as_event_response_complete(cmd);
	
	as_event_executor* executor = cmd->udata;
	pthread_mutex_lock(&executor->lock);
	bool notify = executor->valid;
	executor->count++;
	bool complete = executor->count == executor->max;
	int next = executor->count + executor->max_concurrent - 1;
	bool start_new_command = next < executor->max && executor->valid;
	pthread_mutex_unlock(&executor->lock);

	if (complete) {
		// All commands completed.
		if (notify) {
			executor->complete_fn(executor, 0);
		}
		as_event_executor_destroy(executor);
	}
	else {
		// Determine if a new command needs to be started.
		if (start_new_command) {
			as_error err;
			as_status status = as_event_command_execute(executor->commands[next], &err);
			
			if (status != AEROSPIKE_OK) {
				as_event_executor_error(executor, &err, next);
			}
		}
	}
	as_event_command_release(cmd);
}

as_connection_status
as_event_get_connection(as_event_command* cmd)
{
	as_queue* queue = &cmd->node->async_conn_qs[cmd->event_loop->index];
	as_async_connection* conn;

	// Find connection.
	while (as_queue_pop(queue, &conn)) {
		ck_pr_dec_32(&cmd->cluster->async_conn_pool);
		
		// Verify that socket is active and receive buffer is empty.
		int len = as_event_validate_connection(&conn->base);
		
		if (len == 0) {
			conn->cmd = cmd;
			cmd->conn = (as_event_connection*)conn;
			return AS_CONNECTION_FROM_POOL;
		}
		
		as_log_debug("Invalid async socket from pool: %d", len);
		as_event_release_connection(cmd->cluster, &conn->base, queue);
	}
	
	// Create connection structure only when node connection count within queue limit.
	if (as_queue_incr_total(queue)) {
		ck_pr_inc_32(&cmd->cluster->async_conn_count);
		conn = cf_malloc(sizeof(as_async_connection));
		conn->base.pipeline = false;
#if defined(AS_USE_LIBEV) || defined(AS_USE_LIBEVENT)
		conn->base.watching = 0;
#endif
		conn->cmd = cmd;
		cmd->conn = &conn->base;
		return AS_CONNECTION_NEW;
	}
	else {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_NO_MORE_CONNECTIONS,
						"Max node/event loop %s async connections would be exceeded: %u",
						cmd->node->name, queue->capacity);
		as_event_stop_timer(cmd);
		as_event_error_callback(cmd, &err);
		return AS_CONNECTION_TOO_MANY;
	}
}

void
as_event_error_callback(as_event_command* cmd, as_error* err)
{
	switch (cmd->type) {
		case AS_ASYNC_TYPE_WRITE:
			((as_async_write_command*)cmd)->listener(err, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_RECORD:
			((as_async_record_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_VALUE:
			((as_async_value_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
			
		default:
			// Handle command that is part of a group (batch, scan, query).
			as_event_executor_error(cmd->udata, err, -1);
			break;
	}

	as_event_command_release(cmd);
}

void
as_event_socket_error(as_event_command* cmd, as_error* err)
{
	if (cmd->pipe_listener != NULL) {
		as_pipe_socket_error(cmd, err);
		return;
	}
	
	// Socket read/write failure.
	as_event_stop_watcher(cmd, cmd->conn);
	
	// Stop timer.
	as_event_stop_timer(cmd);
	
	// Do not put connection back in pool.
	as_event_release_async_connection(cmd);
	as_event_error_callback(cmd, err);
}

void
as_event_response_error(as_event_command* cmd, as_error* err)
{
	if (cmd->pipe_listener != NULL) {
		as_pipe_response_error(cmd, err);
		return;
	}
	
	// Server sent back error.
	// Release resources, make callback and free command.
	as_event_stop_timer(cmd);
	as_event_stop_watcher(cmd, cmd->conn);
	
	// Close socket on errors that can leave unread data in socket.
	switch (err->code) {
		case AEROSPIKE_ERR_QUERY_ABORTED:
		case AEROSPIKE_ERR_SCAN_ABORTED:
		case AEROSPIKE_ERR_ASYNC_CONNECTION:
		case AEROSPIKE_ERR_TLS_ERROR:
		case AEROSPIKE_ERR_CLIENT_ABORT:
		case AEROSPIKE_ERR_CLIENT:
		case AEROSPIKE_NOT_AUTHENTICATED:
			as_event_release_async_connection(cmd);
			break;
			
		default:
			as_event_put_connection(cmd);
			break;
	}
	as_event_error_callback(cmd, err);
}

void
as_event_timeout(as_event_command* cmd)
{
	if (cmd->pipe_listener != NULL) {
		as_pipe_timeout(cmd);
		return;
	}
	
	as_error err;
	as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
	
	// Command has timed out.
	// Stop watcher if it has been initialized.
	if (cmd->state > AS_ASYNC_STATE_UNREGISTERED) {
		as_event_stop_watcher(cmd, cmd->conn);
	}
	
	// Assume timer has already been stopped.
	// Do not put connection back in pool.
	as_event_release_async_connection(cmd);
	as_event_error_callback(cmd, &err);
}

bool
as_event_command_parse_header(as_event_command* cmd)
{
	as_msg* msg = (as_msg*)cmd->buf;
	
	if (msg->result_code == AEROSPIKE_OK) {
		as_event_response_complete(cmd);
		((as_async_write_command*)cmd)->listener(0, cmd->udata, cmd->event_loop);
		as_event_command_release(cmd);
	}
	else {
		as_error err;
		as_error_set_message(&err, msg->result_code, as_error_string(msg->result_code));
		as_event_response_error(cmd, &err);
	}
	return true;
}

bool
as_event_command_parse_result(as_event_command* cmd)
{
	as_error err;
	as_msg* msg = (as_msg*)cmd->buf;
	as_msg_swap_header_from_be(msg);
	uint8_t* p = cmd->buf + sizeof(as_msg);
	as_status status = msg->result_code;
	
	switch (status) {
		case AEROSPIKE_OK: {
			as_record rec;
			
			if (msg->n_ops < 1000) {
				as_record_inita(&rec, msg->n_ops);
			}
			else {
				as_record_init(&rec, msg->n_ops);
			}
			
			rec.gen = msg->generation;
			rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
			
			p = as_command_ignore_fields(p, msg->n_fields);
			status = as_command_parse_bins(&p, &err, &rec, msg->n_ops, cmd->deserialize);

			if (status == AEROSPIKE_OK) {
				as_event_response_complete(cmd);
				((as_async_record_command*)cmd)->listener(0, &rec, cmd->udata, cmd->event_loop);
				as_event_command_release(cmd);
			}
			else {
				as_event_response_error(cmd, &err);
			}
			as_record_destroy(&rec);
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			as_command_parse_udf_failure(p, &err, msg, status);
			as_event_response_error(cmd, &err);
			break;
		}
			
		default: {
			as_error_set_message(&err, status, as_error_string(status));
			as_event_response_error(cmd, &err);
			break;
		}
	}
	return true;
}

bool
as_event_command_parse_success_failure(as_event_command* cmd)
{
	as_msg* msg = (as_msg*)cmd->buf;
	as_msg_swap_header_from_be(msg);
	uint8_t* p = cmd->buf + sizeof(as_msg);
	as_status status = msg->result_code;
	
	switch (status) {
		case AEROSPIKE_OK: {
			as_error err;
			as_val* val = 0;
			status = as_command_parse_success_failure_bins(&p, &err, msg, &val);
			
			if (status == AEROSPIKE_OK) {
				as_event_response_complete(cmd);
				((as_async_value_command*)cmd)->listener(0, val, cmd->udata, cmd->event_loop);
				as_event_command_release(cmd);
				as_val_destroy(val);
			}
			else {
				as_event_response_error(cmd, &err);
			}
			break;
		}
			
		case AEROSPIKE_ERR_UDF: {
			as_error err;
			as_command_parse_udf_failure(p, &err, msg, status);
			as_event_response_error(cmd, &err);
			break;
		}
			
		default: {
			as_error err;
			as_error_set_message(&err, status, as_error_string(status));
			as_event_response_error(cmd, &err);
			break;
		}
	}
	return true;
}

void
as_event_command_free(as_event_command* cmd)
{
	as_node_release(cmd->node);
	
	as_cluster* cluster = cmd->cluster;
	bool destroy;
	
	ck_pr_dec_32_zero(&cluster->async_pending, &destroy);
	
	// Only destroy cluster if cluster was closed and there are no pending async commands.
	if (destroy) {
		as_cluster_destroy(cluster);
	}
	
	if (cmd->free_buf) {
		cf_free(cmd->buf);
	}
	cf_free(cmd);
}
