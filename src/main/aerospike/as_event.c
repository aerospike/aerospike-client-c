/*
 * Copyright 2008-2019 Aerospike, Inc.
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
#include <aerospike/as_info.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_monitor.h>
#include <aerospike/as_pipe.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_query_validate.h>
#include <aerospike/as_shm_cluster.h>
#include <citrusleaf/alloc.h>
#include <pthread.h>

// Use pointer comparison for performance.  If portability becomes an issue, use
// "pthread_equal(event_loop->thread, pthread_self())" instead.
#if !defined(_MSC_VER)
#define as_in_event_loop(_t1) ((_t1) == pthread_self())
#else
#define as_in_event_loop(_t1) ((_t1).p == pthread_self().p)
#endif

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
bool as_event_single_thread = false;

as_status aerospike_library_init(as_error* err);
int as_batch_retry_async(as_event_command* cmd);

/******************************************************************************
 * PUBLIC FUNCTIONS
 *****************************************************************************/

static as_status
as_event_validate_policy(as_error* err, as_policy_event* policy)
{
	if (policy->max_commands_in_process < 0 || (policy->max_commands_in_process > 0 && policy->max_commands_in_process < 5)) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "max_commands_in_process %u must be 0 or >= 5", policy->max_commands_in_process);
	}
	return AEROSPIKE_OK;
}

static void
as_event_initialize_loop(as_policy_event* policy, as_event_loop* event_loop, uint32_t index)
{
	pthread_mutex_init(&event_loop->lock, 0);
	as_queue_init(&event_loop->queue, sizeof(as_event_commander), AS_EVENT_QUEUE_INITIAL_CAPACITY);

	if (policy->max_commands_in_process > 0) {
		as_queue_init(&event_loop->delay_queue, sizeof(as_event_command*), policy->queue_initial_capacity);
	}
	else {
		memset(&event_loop->delay_queue, 0, sizeof(as_queue));
	}
	as_queue_init(&event_loop->pipe_cb_queue, sizeof(as_queued_pipe_cb), AS_EVENT_QUEUE_INITIAL_CAPACITY);
	event_loop->index = index;
	event_loop->max_commands_in_queue = policy->max_commands_in_queue;
	event_loop->max_commands_in_process = policy->max_commands_in_process;
	event_loop->pending = 0;
	event_loop->errors = 0;
	event_loop->using_delay_queue = false;
	event_loop->pipe_cb_calling = false;
}

// Force link error on event initialization when event library not defined.
#if AS_EVENT_LIB_DEFINED

static as_status
as_event_initialize_loops(as_error* err, uint32_t capacity)
{
	as_status status = aerospike_library_init(err);

	if (status != AEROSPIKE_OK) {
		return status;
	}

#if defined(_MSC_VER)
	// Call WSAStartup() on event loops initialization on windows.
	WORD version = MAKEWORD(2, 2);
	WSADATA data;
	if (WSAStartup(version, &data) != 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "WSAStartup failed");
	}
#endif

	if (capacity == 0) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid capacity: %u", capacity);
	}
	
	as_event_send_buffer_size = as_pipe_get_send_buffer_size();
	as_event_recv_buffer_size = as_pipe_get_recv_buffer_size();

	as_event_loops = cf_calloc(capacity, sizeof(as_event_loop));
	
	if (! as_event_loops) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "as_event_loops calloc() failed");
	}

	as_event_loop_capacity = capacity;
	as_event_loop_current = as_event_loops;
	
	// Initialize first loop to circular linked list for efficient round-robin
	// event loop distribution.
	as_event_loops->next = as_event_loops;
	return AEROSPIKE_OK;
}

as_event_loop*
as_event_create_loops(uint32_t capacity)
{
	as_error err;
	as_event_loop* event_loops = NULL;

	if (as_create_event_loops(&err, NULL, capacity, &event_loops) != AEROSPIKE_OK) {
		as_log_error(err.message);
		return NULL;
	}
	return event_loops;
}

as_status
as_create_event_loops(as_error* err, as_policy_event* policy, uint32_t capacity, as_event_loop** event_loops)
{
	as_error_reset(err);

	as_status status;
	as_policy_event pol_local;

	if (policy) {
		status = as_event_validate_policy(err, policy);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	else {
		policy = &pol_local;
		as_policy_event_init(policy);
	}

	status = as_event_initialize_loops(err, capacity);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_event_threads_created = true;
	
	for (uint32_t i = 0; i < capacity; i++) {
		as_event_loop* event_loop = &as_event_loops[i];
		as_event_initialize_loop(policy, event_loop, i);
		event_loop->loop = NULL;

#if !defined(_MSC_VER)
		event_loop->thread = 0;
#else
		memset(&event_loop->thread, 0, sizeof(pthread_t));
#endif

		if (! as_event_create_loop(event_loop)) {
			as_event_close_loops();
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to create event_loop: %u", i);
		}
		
		if (i > 0) {
			// This loop points to first loop to create circular round-robin linked list.
			event_loop->next = as_event_loops;
			
			// Adjust previous loop to point to this loop.
			as_event_loops[i - 1].next = event_loop;
		}
		as_event_loop_size++;
	}

	if (event_loops) {
		*event_loops = as_event_loops;
	}
	return AEROSPIKE_OK;
}

bool
as_event_set_external_loop_capacity(uint32_t capacity)
{
	as_error err;
	as_status status = as_event_initialize_loops(&err, capacity);

	if (status != AEROSPIKE_OK) {
		as_log_error(err.message);
		return false;
	}

	as_event_threads_created = false;
	return true;
}

#endif

as_event_loop*
as_event_set_external_loop(void* loop)
{
	as_error err;
	as_event_loop* event_loop = NULL;

	if (as_set_external_event_loop(&err, NULL, loop, &event_loop) != AEROSPIKE_OK) {
		as_log_error(err.message);
		return NULL;
	}
	return event_loop;
}

as_status
as_set_external_event_loop(as_error* err, as_policy_event* policy, void* loop, as_event_loop** event_loop_out)
{
	as_error_reset(err);

	as_policy_event pol_local;

	if (policy) {
		as_status status = as_event_validate_policy(err, policy);

		if (status != AEROSPIKE_OK) {
			return status;
		}
	}
	else {
		policy = &pol_local;
		as_policy_event_init(policy);
	}

	uint32_t current = as_faa_uint32(&as_event_loop_size, 1);
	
	if (current >= as_event_loop_capacity) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to add external loop. Capacity is %u", as_event_loop_capacity);
	}
	
	as_event_loop* event_loop = &as_event_loops[current];
	as_event_initialize_loop(policy, event_loop, current);
	event_loop->loop = loop;
	event_loop->thread = pthread_self();  // Current thread must be same as event loop thread!

	as_event_register_external_loop(event_loop);

	if (current > 0) {
		// This loop points to first loop to create circular round-robin linked list.
		event_loop->next = as_event_loops;
		
		// Adjust previous loop to point to this loop.
		// Warning: not synchronized with as_event_loop_get()
		as_event_loops[current - 1].next = event_loop;
	}
	*event_loop_out = event_loop;
	return AEROSPIKE_OK;
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
		if (! as_event_execute(event_loop, NULL, NULL)) {
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
#if defined(_MSC_VER)
	// Call WSACleanup() on event loops destroy on windows.
	WSACleanup();
#endif

	if (as_event_loops) {
		cf_free(as_event_loops);
		as_event_loops = NULL;
		as_event_loop_size = 0;
	}
}

/******************************************************************************
 * PRIVATE FUNCTIONS
 *****************************************************************************/

static void as_event_command_execute_in_loop(as_event_command* cmd);
static void as_event_command_begin(as_event_command* cmd);

as_status
as_event_command_execute(as_event_command* cmd, as_error* err)
{
	// Initialize read buffer (buf) to be located after write buffer.
	cmd->write_offset = (uint32_t)(cmd->buf - (uint8_t*)cmd);
	cmd->buf += cmd->write_len;
	cmd->command_sent_counter = 0;
	cmd->conn = NULL;

	as_event_loop* event_loop = cmd->event_loop;

	// Avoid recursive error death spiral by forcing command to be queued to
	// event loop when consecutive recursive errors reaches an approximate limit.
	if (as_in_event_loop(event_loop->thread) && event_loop->errors < 5) {
		// We are already in event loop thread, so start processing.
		as_event_command_execute_in_loop(cmd);
	}
	else {
		// Send command through queue so it can be executed in event loop thread.
		if (cmd->total_deadline > 0) {
			// Convert total timeout to deadline.
			cmd->total_deadline += cf_getms();
		}
		cmd->state = AS_ASYNC_STATE_REGISTERED;

		if (! as_event_execute(cmd->event_loop, (as_event_executable)as_event_command_execute_in_loop, cmd)) {
			event_loop->errors++;  // Not in event loop thread, so not exactly accurate.
			if (cmd->node) {
				as_node_release(cmd->node);
			}
			cf_free(cmd);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to queue command");
		}
	}
	return AEROSPIKE_OK;
}

static void
as_event_execute_from_delay_queue(as_event_loop* event_loop)
{
	event_loop->using_delay_queue = true;

	as_event_command* cmd;

	while (event_loop->pending < event_loop->max_commands_in_process &&
		   as_queue_pop(&event_loop->delay_queue, &cmd)) {

		if (cmd->state == AS_ASYNC_STATE_QUEUE_ERROR) {
			// Command timed out and user has already been notified.
			as_event_command_release(cmd);
			continue;
		}

		if (cmd->socket_timeout > 0) {
			if (cmd->total_deadline > 0) {
				if (cmd->socket_timeout < cmd->total_deadline - cf_getms()) {
					// Transition from total timer to socket timer.
					as_event_stop_timer(cmd);
					as_event_set_socket_timer(cmd);
					cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER | AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
				}
			}
			else {
				// Use socket timer.
				as_event_init_socket_timer(cmd);
				cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER | AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
			}
		}

		event_loop->pending++;
		as_event_command_begin(cmd);
	}
	event_loop->using_delay_queue = false;
}

static inline void
as_event_prequeue_error(as_event_loop* event_loop, as_event_command* cmd, as_error* err)
{
	event_loop->errors++;
	cmd->state = AS_ASYNC_STATE_QUEUE_ERROR;
	as_event_error_callback(cmd, err);
}

static void
as_event_command_execute_in_loop(as_event_command* cmd)
{
	as_event_loop* event_loop = cmd->event_loop;

	if (cmd->cluster->pending[event_loop->index]++ == -1) {
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_CLIENT, "Cluster has been closed");
		as_event_prequeue_error(event_loop, cmd, &err);
		return;
	}

	uint64_t total_timeout = 0;

	if (cmd->total_deadline > 0) {
		uint64_t now = cf_getms();

		if (cmd->state == AS_ASYNC_STATE_REGISTERED) {
			// Command was queued to event loop thread.
			if (now >= cmd->total_deadline) {
				// Command already timed out.
				as_error err;
				as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, "Register timeout");
				as_event_prequeue_error(event_loop, cmd, &err);
				return;
			}
			total_timeout = cmd->total_deadline - now;
		}
		else {
			// Convert total timeout to deadline.
			total_timeout = cmd->total_deadline;
			cmd->total_deadline += now;
		}
	}

	if (event_loop->max_commands_in_process > 0) {
		// Delay queue takes precedence over new commands.
		as_event_execute_from_delay_queue(event_loop);

		// Handle new command.
		if (event_loop->pending >= event_loop->max_commands_in_process) {
			// Pending queue full. Append new command to delay queue.
			bool status;

			if (event_loop->max_commands_in_queue > 0) {
				uint32_t size = as_queue_size(&event_loop->delay_queue);

				if (size < event_loop->max_commands_in_queue) {
					status = as_queue_push(&event_loop->delay_queue, &cmd);
				}
				else {
					status = false;
				}
			}
			else {
				status = as_queue_push(&event_loop->delay_queue, &cmd);
			}

			if (! status) {
				as_error err;
				as_error_update(&err, AEROSPIKE_ERR_ASYNC_QUEUE_FULL, "Async delay queue full: %u",
								event_loop->max_commands_in_queue);
				as_event_prequeue_error(event_loop, cmd, &err);
				return;
			}

			if (total_timeout > 0) {
				as_event_init_total_timer(cmd, total_timeout);
				cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER;
			}

			cmd->state = AS_ASYNC_STATE_DELAY_QUEUE;
			return;
		}
	}

	if (total_timeout > 0) {
		if (cmd->socket_timeout > 0 && cmd->socket_timeout < total_timeout) {
			// Use socket timer.
			as_event_init_socket_timer(cmd);
			cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER | AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
		}
		else {
			// Use total timer.
			as_event_init_total_timer(cmd, total_timeout);
			cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER;
		}
	}
	else if (cmd->socket_timeout > 0) {
		// Use socket timer.
		as_event_init_socket_timer(cmd);
		cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER | AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
	}
	
	// Start processing.
	event_loop->pending++;
	as_event_command_begin(cmd);
}

static void
as_event_command_begin(as_event_command* cmd)
{
	cmd->state = AS_ASYNC_STATE_CONNECT;

	if (cmd->partition) {
		// If in retry, need to release node from prior attempt.
		if (cmd->node) {
			as_node_release(cmd->node);
		}

		if (cmd->cluster->shm_info) {
			cmd->node = as_partition_shm_get_node(cmd->cluster, cmd->ns, cmd->partition,
												  cmd->replica, cmd->flags & AS_ASYNC_FLAGS_MASTER);
		}
		else {
			cmd->node = as_partition_get_node(cmd->cluster, cmd->ns, cmd->partition, cmd->replica,
											  cmd->flags & AS_ASYNC_FLAGS_MASTER);
		}

		if (! cmd->node) {
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_CLIENT, "Cluster is empty");

			if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
				as_event_stop_timer(cmd);
			}
			as_event_error_callback(cmd, &err);
			return;
		}
		as_node_reserve(cmd->node);
	}

	if (cmd->pipe_listener) {
		as_pipe_get_connection(cmd);
		return;
	}

	as_conn_pool* pool = &cmd->node->async_conn_pools[cmd->event_loop->index];
	as_async_connection* conn;

	// Find connection.
	while (as_conn_pool_get(pool, &conn)) {
		// Verify that socket is active and receive buffer is empty.
		int len = as_event_validate_connection(&conn->base);

		if (len == 0) {
			conn->cmd = cmd;
			cmd->conn = (as_event_connection*)conn;
			cmd->event_loop->errors = 0;  // Reset errors on valid connection.
			as_event_command_write_start(cmd);
			return;
		}

		as_log_debug("Invalid async socket from pool: %d", len);
		as_event_release_connection(&conn->base, pool);
	}

	// Create connection structure only when node connection count within queue limit.
	if (as_conn_pool_inc(pool)) {
		conn = cf_malloc(sizeof(as_async_connection));
		conn->base.pipeline = false;
		conn->base.watching = 0;
		conn->cmd = cmd;
		cmd->conn = &conn->base;
		as_event_connect(cmd);
		return;
	}

	cmd->event_loop->errors++;

	if (! as_event_command_retry(cmd, true)) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_NO_MORE_CONNECTIONS,
						"Max node/event loop %s async connections would be exceeded: %u",
						cmd->node->name, pool->limit);

		if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
			as_event_stop_timer(cmd);
		}
		as_event_error_callback(cmd, &err);
	}
}

void
as_event_socket_timeout(as_event_command* cmd)
{
	if (cmd->flags & AS_ASYNC_FLAGS_EVENT_RECEIVED) {
		// Event(s) received within socket timeout period.
		cmd->flags &= ~AS_ASYNC_FLAGS_EVENT_RECEIVED;

		if (cmd->total_deadline > 0) {
			// Check total timeout.
			uint64_t now = cf_getms();

			if (now >= cmd->total_deadline) {
				as_event_stop_timer(cmd);
				as_event_total_timeout(cmd);
				return;
			}

			uint64_t remaining = cmd->total_deadline - now;

			if (remaining <= cmd->socket_timeout) {
				// Transition to total timer.
				cmd->flags &= ~AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
				as_event_stop_timer(cmd);
				as_event_set_total_timer(cmd, remaining);
			}
			else {
				as_event_repeat_socket_timer(cmd);
			}
		}
		else {
			as_event_repeat_socket_timer(cmd);
		}
		return;
	}

	if (cmd->pipe_listener) {
		as_pipe_timeout(cmd, true);
		return;
	}

	// Node should not be null at this point.
	as_event_connection_timeout(cmd, &cmd->node->async_conn_pools[cmd->event_loop->index]);

	// Attempt retry.
	// Read commands shift to prole node on timeout.
	if (! as_event_command_retry(cmd, cmd->flags & AS_ASYNC_FLAGS_READ)) {
		as_event_stop_timer(cmd);

		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_TIMEOUT, "Client timeout: iterations=%u lastNode=%s",
						cmd->iteration + 1, as_node_get_address_string(cmd->node));

		as_event_error_callback(cmd, &err);
	}
}

void
as_event_total_timeout(as_event_command* cmd)
{
	if (cmd->state == AS_ASYNC_STATE_DELAY_QUEUE) {
		cmd->state = AS_ASYNC_STATE_QUEUE_ERROR;

		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, "Delay queue timeout");

		// Notify user, but do not destroy command.
		as_event_notify_error(cmd, &err);
		return;
	}

	if (cmd->pipe_listener) {
		as_pipe_timeout(cmd, false);
		return;
	}

	// Node should not be null at this point.
	as_event_connection_timeout(cmd, &cmd->node->async_conn_pools[cmd->event_loop->index]);

	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_TIMEOUT, "Client timeout: iterations=%u lastNode=%s",
					cmd->iteration + 1, as_node_get_address_string(cmd->node));
	as_event_error_callback(cmd, &err);
}

bool
as_event_command_retry(as_event_command* cmd, bool alternate)
{
	// Check max retries.
	if (++(cmd->iteration) > cmd->max_retries) {
		return false;
	}

	if (cmd->total_deadline > 0) {
		// Check total timeout.
		uint64_t now = cf_getms();

		if (now >= cmd->total_deadline) {
			return false;
		}

		if (cmd->flags & AS_ASYNC_FLAGS_USING_SOCKET_TIMER) {
			uint64_t remaining = cmd->total_deadline - now;

			if (remaining <= cmd->socket_timeout) {
				// Transition to total timer.
				cmd->flags &= ~AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
				as_event_stop_timer(cmd);
				as_event_set_total_timer(cmd, remaining);
			}
			else {
				as_event_repeat_socket_timer(cmd);
			}
		}
	}
	else if (cmd->flags & AS_ASYNC_FLAGS_USING_SOCKET_TIMER) {
		as_event_repeat_socket_timer(cmd);
	}

	if (alternate) {
		cmd->flags ^= AS_ASYNC_FLAGS_MASTER;  // Alternate between master and prole.
	}

	// Old connection should already be closed or is closing.
	// Reset command connection so timeout watcher knows not to close connection twice.
	cmd->conn = NULL;

	// Batch retries can be split into multiple retries to different nodes.
	if (cmd->type == AS_ASYNC_TYPE_BATCH) {
		int rv = as_batch_retry_async(cmd);

		// 1:  Split retry not attempted.  Go through normal retry.
		// 0:  Split retry started.
		// -1: Split retry failed to start. Error has been handled.
		// -2: Split retry failed to start. Defer to original error.
		if (rv <= 0) {
			// This command should have been closed in as_batch_retry_async().
			return rv >= -1;
		}
	}

	// Retry command at the end of the queue so other commands have a chance to run first.
	return as_event_execute(cmd->event_loop, (as_event_executable)as_event_command_begin, cmd);
}

static inline void
as_event_put_connection(as_event_command* cmd, as_conn_pool* pool)
{
	as_event_set_conn_last_used(cmd->conn, cmd->cluster->max_socket_idle);

	if (! as_conn_pool_put(pool, &cmd->conn)) {
		as_event_release_connection(cmd->conn, pool);
	}
}

static inline void
as_event_response_complete(as_event_command* cmd)
{
	if (cmd->pipe_listener != NULL) {
		as_pipe_response_complete(cmd);
		return;
	}
	
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		as_event_stop_timer(cmd);
	}
	as_event_stop_watcher(cmd, cmd->conn);

	as_conn_pool* pool = &cmd->node->async_conn_pools[cmd->event_loop->index];
	as_event_put_connection(cmd, pool);
}

static void
as_event_executor_destroy(as_event_executor* executor)
{
	pthread_mutex_destroy(&executor->lock);
	
	if (executor->commands) {
		// Free commands not started yet.
		for (uint32_t i = executor->queued; i < executor->max; i++) {
			// Destroy command before it was started.
			as_event_command* cmd = executor->commands[i];
			as_event_command_destroy(cmd);
		}
		cf_free(executor->commands);
	}

	if (executor->err) {
		cf_free(executor->err);
	}

	if (executor->ns) {
		cf_free(executor->ns);
	}
	
	cf_free(executor);
}

void
as_event_executor_error(as_event_executor* executor, as_error* err, uint32_t command_count)
{
	pthread_mutex_lock(&executor->lock);
	bool first_error = executor->valid;
	executor->valid = false;
	executor->count += command_count;
	bool complete = executor->count == executor->max;
	pthread_mutex_unlock(&executor->lock);

	if (complete) {
		// All commands have completed.
		// If scan or query user callback already returned false,
		// do not re-notify user that an error occurred.
		if (executor->notify) {
			if (first_error) {
				// Original error can be used directly.
				executor->err = err;
				executor->complete_fn(executor);
				executor->err = NULL;
			}
			else {
				// Use saved error.
				executor->complete_fn(executor);
			}
		}
		as_event_executor_destroy(executor);
	}
	else if (first_error)
	{
		// Save first error only.
		executor->err = cf_malloc(sizeof(as_error));
		as_error_copy(executor->err, err);
	}
}

void
as_event_executor_cancel(as_event_executor* executor, uint32_t queued_count)
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
		// Do not call user listener because an error will be returned
		// on initial batch, scan or query call.
		as_event_executor_destroy(executor);
	}
}

void
as_event_executor_complete(as_event_command* cmd)
{
	as_event_executor* executor = cmd->udata;
	pthread_mutex_lock(&executor->lock);
	executor->count++;
	uint32_t next = executor->count + executor->max_concurrent - 1;
	bool complete = executor->count == executor->max;
	bool start_new_command = next < executor->max && executor->valid;
	pthread_mutex_unlock(&executor->lock);

	if (complete) {
		// All commands completed.
		// If scan or query user callback already returned false,
		// do not re-notify user that an error occurred.
		if (executor->notify) {
			executor->complete_fn(executor);
		}
		as_event_executor_destroy(executor);
	}
	else {
		// Determine if a new command needs to be started.
		if (start_new_command) {
			if (executor->cluster_key) {
				as_query_validate_next_async(executor, next);
			}
			else {
				as_error err;
				executor->queued++;

				if (as_event_command_execute(executor->commands[next], &err) != AEROSPIKE_OK) {
					as_event_executor_error(executor, &err, executor->max - next);
				}
			}
		}
	}
	as_event_command_release(cmd);
}

void
as_event_query_complete(as_event_command* cmd)
{
	as_event_response_complete(cmd);

	as_event_executor* executor = cmd->udata;

	if (executor->cluster_key) {
		// Verify migrations did not occur during scan/query.
		as_query_validate_end_async(cmd);
		return;
	}
	as_event_executor_complete(cmd);
}

void
as_event_batch_complete(as_event_command* cmd)
{
	as_event_response_complete(cmd);
	as_event_executor_complete(cmd);
}

void
as_event_notify_error(as_event_command* cmd, as_error* err)
{
	as_error_set_in_doubt(err, cmd->flags & AS_ASYNC_FLAGS_READ, cmd->command_sent_counter);

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
		case AS_ASYNC_TYPE_INFO:
			((as_async_info_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;

		default:
			// Handle command that is part of a group (batch, scan, query).
			as_event_executor_error(cmd->udata, err, 1);
			break;
	}
}

void
as_event_parse_error(as_event_command* cmd, as_error* err)
{
	if (cmd->pipe_listener) {
		as_pipe_socket_error(cmd, err, false);
		return;
	}

	// Close connection.
	as_event_stop_watcher(cmd, cmd->conn);
	as_event_release_async_connection(cmd);

	// Stop timer.
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		as_event_stop_timer(cmd);
	}

	as_event_error_callback(cmd, err);
}

void
as_event_socket_error(as_event_command* cmd, as_error* err)
{
	if (cmd->pipe_listener) {
		// Retry pipeline commands.
		as_pipe_socket_error(cmd, err, true);
		return;
	}

	// Connection should already have been closed before calling this function.
	// Stop timer.
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		as_event_stop_timer(cmd);
	}

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
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		as_event_stop_timer(cmd);
	}
	as_event_stop_watcher(cmd, cmd->conn);
	
	as_conn_pool* pool = &cmd->node->async_conn_pools[cmd->event_loop->index];

	// Close socket on errors that can leave unread data in socket.
	switch (err->code) {
		case AEROSPIKE_ERR_QUERY_ABORTED:
		case AEROSPIKE_ERR_SCAN_ABORTED:
		case AEROSPIKE_ERR_ASYNC_CONNECTION:
		case AEROSPIKE_ERR_TLS_ERROR:
		case AEROSPIKE_ERR_CLIENT_ABORT:
		case AEROSPIKE_ERR_CLIENT:
		case AEROSPIKE_NOT_AUTHENTICATED: {
			as_event_release_connection(cmd->conn, pool);
			break;
		}
			
		default:
			as_event_put_connection(cmd, pool);
			break;
	}
	as_event_error_callback(cmd, err);
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
			as_error_update(&err, status, "%s %s", as_node_get_address_string(cmd->node), as_error_string(status));
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
			as_error_update(&err, status, "%s %s", as_node_get_address_string(cmd->node), as_error_string(status));
			as_event_response_error(cmd, &err);
			break;
		}
	}
	return true;
}

bool
as_event_command_parse_info(as_event_command* cmd)
{
	char* response = (char*)cmd->buf;
	response[cmd->len] = 0;

	char* error = 0;
	as_status status = as_info_validate(response, &error);

	if (status == AEROSPIKE_OK) {
		as_event_response_complete(cmd);
		((as_async_info_command*)cmd)->listener(NULL, response, cmd->udata, cmd->event_loop);
		as_event_command_release(cmd);
	}
	else {
		as_error err;
		as_error_set_message(&err, status, as_error_string(status));
		as_event_response_error(cmd, &err);
	}
	return true;
}

void
as_event_command_free(as_event_command* cmd)
{
	as_event_loop* event_loop = cmd->event_loop;

	if (cmd->state != AS_ASYNC_STATE_QUEUE_ERROR) {
		event_loop->pending--;
	}
	cmd->cluster->pending[event_loop->index]--;

	if (cmd->node) {
		as_node_release(cmd->node);
	}

	if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
		cf_free(cmd->buf);
	}
	cf_free(cmd);

	if (event_loop->max_commands_in_process > 0 && ! event_loop->using_delay_queue) {
		// Try executing commands from the delay queue.
		as_event_execute_from_delay_queue(event_loop);
	}
}

/******************************************************************************
 * CLUSTER CLOSE FUNCTIONS
 *****************************************************************************/

typedef struct {
	as_monitor* monitor;
	as_cluster* cluster;
	as_event_loop* event_loop;
	uint32_t* event_loop_count;
} as_event_close_state;

static void
as_event_close_cluster_event_loop(as_event_close_state* state)
{
	state->cluster->pending[state->event_loop->index] = -1;

	if (as_aaf_uint32(state->event_loop_count, -1) == 0) {
		as_cluster_destroy(state->cluster);
		cf_free(state->event_loop_count);

		if (state->monitor) {
			as_monitor_notify(state->monitor);
		}
	}
	cf_free(state);
}

static void
as_event_close_cluster_cb(as_event_close_state* state)
{
	int pending = state->cluster->pending[state->event_loop->index];

	if (pending < 0) {
		// Cluster's event loop connections are already closed.
		return;
	}

	if (pending > 0) {
		// Cluster has pending commands.
		// Check again after all other commands run.
		if (as_event_execute(state->event_loop, (as_event_executable)as_event_close_cluster_cb, state)) {
			return;
		}
		as_log_error("Failed to queue cluster close command");
	}

	as_event_close_cluster_event_loop(state);
}

void
as_event_close_cluster(as_cluster* cluster)
{
	// Determine if current thread is an event loop thread.
	bool in_event_loop = false;

	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_loop* event_loop = &as_event_loops[i];

		if (as_in_event_loop(event_loop->thread)) {
			in_event_loop = true;
			break;
		}
	}

	as_monitor* monitor = NULL;

	if (! in_event_loop) {
		monitor = cf_malloc(sizeof(as_monitor));
		as_monitor_init(monitor);
	}

	uint32_t* event_loop_count = cf_malloc(sizeof(uint32_t));
	*event_loop_count = as_event_loop_size;

	// Send cluster close notification to async event loops.
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_loop* event_loop = &as_event_loops[i];

		as_event_close_state* state = cf_malloc(sizeof(as_event_close_state));
		state->monitor = monitor;
		state->cluster = cluster;
		state->event_loop = event_loop;
		state->event_loop_count = event_loop_count;

		if (! as_event_execute(event_loop, (as_event_executable)as_event_close_cluster_cb, state)) {
			as_log_error("Failed to queue cluster close command");
			as_event_close_cluster_event_loop(state);
		}
	}

	// Deadlock would occur if we wait from an event loop thread.
	// Only wait when not in event loop thread.
	if (monitor) {
		as_monitor_wait(monitor);
		as_monitor_destroy(monitor);
		cf_free(monitor);
	}
}
