/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_txn.h>
#include <citrusleaf/alloc.h>
#include <pthread.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

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
static pthread_mutex_t as_event_lock = PTHREAD_MUTEX_INITIALIZER;

as_status aerospike_library_init(as_error* err);
int as_batch_retry_async(as_event_command* cmd, bool timeout);

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

	// Synchronize event loop registration calls that are coming from separate
	// event loop threads.
	pthread_mutex_lock(&as_event_lock);

	uint32_t current = as_event_loop_size;

	if (current >= as_event_loop_capacity) {
		pthread_mutex_unlock(&as_event_lock);
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

	// Set as_event_loop_size now that event loop has been fully initialized.
	as_event_loop_size = current + 1;

	pthread_mutex_unlock(&as_event_lock);

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
as_event_close_loops(void)
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
as_event_destroy_loops(void)
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

static void as_event_command_execute_in_loop(as_event_loop* event_loop, as_event_command* cmd);
static void as_event_command_begin(as_event_loop* event_loop, as_event_command* cmd);
static void as_event_execute_from_delay_queue(as_event_loop* event_loop);
static void connector_error(as_event_command* cmd, as_error* err);

as_status
as_event_command_execute(as_event_command* cmd, as_error* err)
{
	cmd->command_sent_counter = 0;

	as_event_loop* event_loop = cmd->event_loop;

	if (as_in_event_loop(event_loop->thread)) {
		// We are already in the event loop thread.
		if (event_loop->errors < 5) {
			// Start processing immediately.
			as_event_command_execute_in_loop(event_loop, cmd);
		}
		else {
			// Avoid recursive error death spiral by giving other commands
			// a chance to run first.
			as_event_command_schedule(cmd);
		}
	}
	else {
		// Send command through queue so it can be executed in event loop thread.
		if (cmd->total_deadline > 0) {
			// Convert total timeout to deadline.
			cmd->total_deadline += cf_getms();
		}
		cmd->state = AS_ASYNC_STATE_REGISTERED;

		if (! as_event_execute(cmd->event_loop,
			(as_event_executable)as_event_command_execute_in_loop, cmd)) {

			cmd->event_loop->errors++;  // May not be in event loop thread, so not exactly accurate.
			as_event_command_destroy(cmd);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to queue command");
		}
	}
	return AEROSPIKE_OK;
}

void
as_event_command_schedule(as_event_command* cmd)
{
	// Schedule command to execute in next event loop iteration.
	// Must be run in event loop thread.
	if (cmd->total_deadline > 0) {
		// Convert total timeout to deadline.
		cmd->total_deadline += cf_getms();
	}

	// Callback is as_event_process_timer().
	cmd->state = AS_ASYNC_STATE_REGISTERED;
	as_event_timer_once(cmd, 0);
}

static inline void
as_event_prequeue_error(as_event_loop* event_loop, as_event_command* cmd, as_error* err)
{
	event_loop->errors++;
	cmd->state = AS_ASYNC_STATE_QUEUE_ERROR;
	as_event_error_callback(cmd, err);
}

void
as_event_command_execute_in_loop(as_event_loop* event_loop, as_event_command* cmd)
{
	// Initialize read buffer (buf) to be located after write buffer.
	cmd->begin = 0;
	cmd->write_offset = (uint32_t)(cmd->buf - (uint8_t*)cmd);
	cmd->buf += cmd->write_len;
	cmd->conn = NULL;
	cmd->proto_type_rcv = 0;
	cmd->event_state = &cmd->cluster->event_state[event_loop->index];
	cmd->metrics_enabled = cmd->cluster->metrics_enabled;

	if (cmd->event_state->closed) {
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

			cmd->state = AS_ASYNC_STATE_DELAY_QUEUE;

			if (total_timeout > 0) {
				as_event_timer_once(cmd, total_timeout);
			}
			return;
		}
	}

	if (total_timeout > 0) {
		if (cmd->socket_timeout > 0 && cmd->socket_timeout < total_timeout) {
			// Use socket timer.
			as_event_timer_repeat(cmd, cmd->socket_timeout);
		}
		else {
			// Use total timer.
			as_event_timer_once(cmd, total_timeout);
		}
	}
	else if (cmd->socket_timeout > 0) {
		// Use socket timer.
		as_event_timer_repeat(cmd, cmd->socket_timeout);
	}

	// Start processing.
	event_loop->pending++;
	cmd->event_state->pending++;

	as_event_command_begin(event_loop, cmd);
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
					as_event_timer_stop(cmd);
					as_event_timer_repeat(cmd, cmd->socket_timeout);
				}
			}
			else {
				// Use socket timer.
				as_event_timer_repeat(cmd, cmd->socket_timeout);
			}
		}

		event_loop->pending++;
		cmd->event_state->pending++;

		as_event_command_begin(event_loop, cmd);
	}
	event_loop->using_delay_queue = false;
}

static void
as_event_create_connection(as_event_command* cmd, as_async_conn_pool* pool)
{
	as_async_connection* conn = cf_malloc(sizeof(as_async_connection));
	conn->base.pipeline = false;
	conn->base.watching = 0;
	conn->cmd = cmd;
	cmd->conn = &conn->base;
	as_event_connect(cmd, pool);
}

static inline void
as_event_add_latency(as_event_command* cmd, as_latency_type type)
{
	uint64_t elapsed = cf_getns() - cmd->begin;
	as_node_add_latency(cmd->node, type, elapsed);
}

void
as_event_connection_complete(as_event_command* cmd)
{
	if (cmd->metrics_enabled) {
		as_event_add_latency(cmd, AS_LATENCY_TYPE_CONN);
	}
}

static void
as_event_command_begin(as_event_loop* event_loop, as_event_command* cmd)
{
	cmd->state = AS_ASYNC_STATE_CONNECT;

	if (cmd->partition) {
		// If in retry, need to release node from prior attempt.
		if (cmd->node) {
			as_node_release(cmd->node);
		}

		// cmd->node might already be destroyed on retry and is still set as the previous node.
		// This works because the previous node is only used for pointer comparison
		// and the previous node's contents are not examined during this call.
		cmd->node = as_partition_get_node(cmd->cluster, cmd->ns, cmd->partition, cmd->node,
										  cmd->replica, cmd->replica_size, &cmd->replica_index);

		if (! cmd->node) {
			event_loop->errors++;

			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_INVALID_NODE, "Node not found for partition %s",
							cmd->ns);

			as_event_timer_stop(cmd);
			as_event_error_callback(cmd, &err);
			return;
		}
		as_node_reserve(cmd->node);
	}

	if (! as_node_valid_error_rate(cmd->node)) {
		event_loop->errors++;

		if (as_event_command_retry(cmd, true)) {
			return;
		}

		as_error err;
		as_error_set_message(&err, AEROSPIKE_MAX_ERROR_RATE, "Max error rate exceeded");

		as_event_timer_stop(cmd);
		as_event_error_callback(cmd, &err);
		return;
	}

	if (cmd->metrics_enabled) {
		cmd->begin = cf_getns();
	}

	if (cmd->pipe_listener) {
		as_pipe_get_connection(cmd);
		return;
	}

	as_async_conn_pool* pool = &cmd->node->async_conn_pools[event_loop->index];
	as_async_connection* conn;

	// Find connection.
	while (as_queue_pop(&pool->queue, &conn)) {
		// Verify that socket is active.
		if (! as_event_conn_current_tran(&conn->base, cmd->cluster->max_socket_idle_ns_tran)) {
			as_event_release_connection(&conn->base, pool);
			continue;
		}

		// Verify that socket is active and receive buffer is empty.
		int len = as_event_conn_validate(&conn->base);

		if (len != 0) {
			as_log_debug("Invalid async socket from pool: %d", len);
			as_event_release_connection(&conn->base, pool);
			as_node_incr_error_rate(cmd->node);
			continue;
		}

		conn->cmd = cmd;
		cmd->conn = (as_event_connection*)conn;
		event_loop->errors = 0;  // Reset errors on valid connection.
		as_event_command_write_start(cmd);
		return;
	}

	// Create connection only when connection count within limit.
	if (as_async_conn_pool_incr_total(pool)) {
		as_event_create_connection(cmd, pool);
		return;
	}

	event_loop->errors++;

	// AEROSPIKE_ERR_NO_MORE_CONNECTIONS should be handled as timeout (true) because
	// it's not an indicator of impending data migration.  This retry is recursive.
	if (as_event_command_retry(cmd, true)) {
		return;
	}

	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_NO_MORE_CONNECTIONS,
					"Max node/event loop %s async connections would be exceeded: %u",
					cmd->node->name, pool->limit);

	as_event_timer_stop(cmd);
	as_event_error_callback(cmd, &err);
}

bool
as_event_proto_parse(as_event_command* cmd, as_proto* proto)
{
	if (proto->version != AS_PROTO_VERSION) {
		as_error err;
		as_proto_version_error(&err, proto);
		as_event_parse_error(cmd, &err);
		return false;
	}

	if (proto->type != cmd->proto_type && proto->type != AS_COMPRESSED_MESSAGE_TYPE) {
		as_error err;
		as_proto_type_error(&err, proto, cmd->proto_type);
		as_event_parse_error(cmd, &err);
		return false;
	}

	cmd->proto_type_rcv = (uint8_t)proto->type;
	as_proto_swap_from_be(proto);

	if (proto->sz > PROTO_SIZE_MAX) {
		as_error err;
		as_proto_size_error(&err, (size_t)proto->sz);
		as_event_parse_error(cmd, &err);
		return false;
	}
	return true;
}

bool
as_event_proto_parse_auth(as_event_command* cmd, as_proto* proto)
{
	if (proto->version != AS_PROTO_VERSION) {
		as_error err;
		as_proto_version_error(&err, proto);
		as_event_parse_error(cmd, &err);
		return false;
	}

	if (proto->type != AS_ADMIN_MESSAGE_TYPE) {
		as_error err;
		as_proto_type_error(&err, proto, cmd->proto_type);
		as_event_parse_error(cmd, &err);
		return false;
	}

	as_proto_swap_from_be(proto);

	if (proto->sz > PROTO_SIZE_MAX) {
		as_error err;
		as_proto_size_error(&err, (size_t)proto->sz);
		as_event_parse_error(cmd, &err);
		return false;
	}
	return true;
}

bool
as_event_decompress(as_event_command* cmd)
{
	as_error err;
	size_t size = (size_t)cf_swap_from_be64(*(uint64_t*)cmd->buf);

	if (size > PROTO_SIZE_MAX) {
		as_proto_size_error(&err, size);
		as_event_parse_error(cmd, &err);
		return false;
	}

	uint8_t* buf = cf_malloc(size);

	if (as_proto_decompress(&err, buf, size, cmd->buf, cmd->len) != AEROSPIKE_OK) {
		cf_free(buf);
		as_event_parse_error(cmd, &err);
		return false;
	}

	if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
		cf_free(cmd->buf);
	}
	cmd->buf = buf;
	cmd->len = (uint32_t)size;
	cmd->pos = sizeof(as_proto);
	cmd->read_capacity = cmd->len;
	cmd->flags |= AS_ASYNC_FLAGS_FREE_BUF;
	return true;
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
				as_event_timer_stop(cmd);
				as_event_total_timeout(cmd);
				return;
			}

			uint64_t remaining = cmd->total_deadline - now;

			if (remaining <= cmd->socket_timeout) {
				// Transition to total timer.
				cmd->flags &= ~AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
				as_event_timer_stop(cmd);
				as_event_timer_once(cmd, remaining);
			}
			else {
				as_event_timer_again(cmd);
			}
		}
		else {
			as_event_timer_again(cmd);
		}
		return;
	}

	as_node_add_timeout(cmd->node);

	if (cmd->pipe_listener) {
		as_pipe_timeout(cmd, true);
		return;
	}

	// Node should not be null at this point.
	as_event_connection_timeout(cmd, &cmd->node->async_conn_pools[cmd->event_loop->index]);

	if (! as_event_command_retry(cmd, true)) {
		as_event_timer_stop(cmd);

		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_TIMEOUT, "Client timeout: iterations=%u lastNode=%s",
						cmd->iteration, as_node_get_address_string(cmd->node));

		as_event_error_callback(cmd, &err);
	}
}

static void
as_event_delay_timeout(as_event_command* cmd)
{
	cmd->state = AS_ASYNC_STATE_QUEUE_ERROR;

	if (cmd->metrics_enabled) {
		as_cluster_add_delay_queue_timeout(cmd->cluster);
	}

	as_error err;
	as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, "Delay queue timeout");

	// Notify user, but do not destroy command.
	as_event_notify_error(cmd, &err);
}

void
as_event_process_timer(as_event_command* cmd)
{
	switch (cmd->state) {
		case AS_ASYNC_STATE_REGISTERED:
			// Start command from the beginning.
			as_event_command_execute_in_loop(cmd->event_loop, cmd);
			break;

		case AS_ASYNC_STATE_DELAY_QUEUE:
			// Command timed out in delay queue.
			as_event_delay_timeout(cmd);
			break;

		case AS_ASYNC_STATE_RETRY:
			// Execute retry.
			as_event_execute_retry(cmd);
			break;

		default:
			// Total timeout.
			as_event_total_timeout(cmd);
			break;
	}
}

void
as_event_total_timeout(as_event_command* cmd)
{
	// Node should not be null at this point.
	as_node_add_timeout(cmd->node);
	
	if (cmd->pipe_listener) {
		as_pipe_timeout(cmd, false);
		return;
	}

	as_event_connection_timeout(cmd, &cmd->node->async_conn_pools[cmd->event_loop->index]);

	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_TIMEOUT, "Client timeout: iterations=%u lastNode=%s",
					cmd->iteration + 1, as_node_get_address_string(cmd->node));
	as_event_error_callback(cmd, &err);
}

bool
as_event_command_retry(as_event_command* cmd, bool timeout)
{
	// Check max retries.
	if (++(cmd->iteration) > cmd->max_retries) {
		return false;
	}

	// Alternate between master and prole on socket errors or database reads.
	// Timeouts are not a good indicator of impending data migration.
	if (! timeout || ((cmd->flags & AS_ASYNC_FLAGS_READ) &&
					  !(cmd->flags & AS_ASYNC_FLAGS_LINEARIZE))) {
		// Note: SC session read will ignore this setting because it uses master only.
		cmd->replica_index++;
	}

	// Old connection should already be closed or is closing.
	// Reset command connection so timeout watcher knows not to close connection twice.
	cmd->conn = NULL;

	// Batch retries can be split into multiple retries to different nodes.
	if (cmd->type == AS_ASYNC_TYPE_BATCH) {
		int rv = as_batch_retry_async(cmd, timeout);

		// 1:  Split retry not attempted.  Go through normal retry.
		// 0:  Split retry started.
		// -1: Split retry failed to start. Error has been handled.
		// -2: Split retry failed to start. Defer to original error.
		if (rv <= 0) {
			// This command should have been closed in as_batch_retry_async().
			return rv >= -1;
		}
	}

	// Disable timeout.
	as_event_timer_stop(cmd);

	// Retry command at the end of the queue so other commands have a chance to run first.
	// Initialize event to eventually call as_event_execute_retry().
	cmd->state = AS_ASYNC_STATE_RETRY;
	as_event_timer_once(cmd, 0);
	return true;
}

void
as_event_execute_retry(as_event_command* cmd)
{
	// Restore timer that was reset for retry.
	if (cmd->total_deadline > 0) {
		// Check total timeout.
		uint64_t now = cf_getms();

		if (now >= cmd->total_deadline) {
			as_event_total_timeout(cmd);
			return;
		}

		uint64_t remaining = cmd->total_deadline - now;

		if (cmd->flags & AS_ASYNC_FLAGS_USING_SOCKET_TIMER) {
			if (remaining <= cmd->socket_timeout) {
				// Restore total timer.
				cmd->flags &= ~AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
				as_event_timer_once(cmd, remaining);
			}
			else {
				// Restore socket timer.
				cmd->flags &= ~AS_ASYNC_FLAGS_EVENT_RECEIVED;
				as_event_timer_repeat(cmd, cmd->socket_timeout);
			}
		}
		else {
			// Restore total timer.
			as_event_timer_once(cmd, remaining);
		}
	}
	else if (cmd->flags & AS_ASYNC_FLAGS_USING_SOCKET_TIMER) {
		// Restore socket timer.
		cmd->flags &= ~AS_ASYNC_FLAGS_EVENT_RECEIVED;
		as_event_timer_repeat(cmd, cmd->socket_timeout);
	}

	// Retry command.
	as_cluster_add_retry(cmd->cluster);
	as_event_command_begin(cmd->event_loop, cmd);
}

static inline void
as_event_put_connection(as_event_command* cmd, as_async_conn_pool* pool)
{
	as_event_set_conn_last_used(cmd->conn);

	if (! as_async_conn_pool_push_head(pool, cmd->conn)) {
		as_event_release_connection(cmd->conn, pool);
	}
}

void
as_event_response_complete(as_event_command* cmd)
{
	if (cmd->metrics_enabled && cmd->latency_type != AS_LATENCY_TYPE_NONE) {
		as_event_add_latency(cmd, cmd->latency_type);
	}
	
	if (cmd->pipe_listener != NULL) {
		as_pipe_response_complete(cmd);
		return;
	}
	
	as_event_timer_stop(cmd);
	as_event_stop_watcher(cmd, cmd->conn);

	as_async_conn_pool* pool = &cmd->node->async_conn_pools[cmd->event_loop->index];
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
	bool complete;

	pthread_mutex_lock(&executor->lock);

	bool first_error = executor->valid;
	executor->valid = false;

	if (executor->max_concurrent == 1) {
		// Add current command that failed when running commands in sequence.
		executor->count++;
		complete = executor->count == executor->queued;
	}
	else {
		// Add current command and any remaining commands.
		executor->count += command_count;
		complete = executor->count == executor->max;
	}

	pthread_mutex_unlock(&executor->lock);

	if (complete) {
		// All commands have completed.
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
		as_event_executor_destroy(executor);
	}
	else if (first_error) {
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
	bool complete;

	pthread_mutex_lock(&executor->lock);

	// Do not call user listener because an error will be returned
	// on initial batch, scan or query call.
	executor->notify = false;
	executor->valid = false;

	if (executor->max_concurrent == 1) {
		// Add current task that failed when running commands in sequence.
		executor->count++;
		complete = executor->count == executor->queued;
	}
	else {
		// Add tasks that were never queued.
		executor->count += (executor->max - queued_count);
		complete = executor->count == executor->max;
	}

	pthread_mutex_unlock(&executor->lock);

	if (complete) {
		as_event_executor_destroy(executor);
	}
}

void
as_event_executor_complete(as_event_executor* executor)
{
	pthread_mutex_lock(&executor->lock);
	executor->count++;
	uint32_t next = executor->count + executor->max_concurrent - 1;
	bool complete = executor->count == executor->max;
	bool start_new_command = next < executor->max && executor->valid;
	pthread_mutex_unlock(&executor->lock);

	if (complete) {
		// All commands completed.
		executor->complete_fn(executor);
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
}

void
as_event_query_complete(as_event_command* cmd)
{
	as_event_response_complete(cmd);

	as_event_executor* executor = cmd->udata;

	if (executor->cluster_key) {
		// Verify migrations did not occur during query.
		as_event_loop* event_loop = cmd->event_loop;
		as_node* node = cmd->node;

		// Reserve node again because the node will be released in as_event_command_release().
		// Node must be available for as_query_validate_end_async().
		as_node_reserve(node);
		as_event_command_release(cmd);
		as_query_validate_end_async(executor, node, event_loop);
	}
	else {
		as_event_command_release(cmd);
		as_event_executor_complete(executor);
	}
}

void
as_event_batch_complete(as_event_command* cmd)
{
	as_event_executor* executor = cmd->udata;
	as_event_response_complete(cmd);
	as_event_command_release(cmd);
	as_event_executor_complete(executor);
}

bool as_async_scan_should_retry(as_event_command* cmd, as_status status);
bool as_async_query_should_retry(as_event_command* cmd, as_status status);

void
as_event_error_callback(as_event_command* cmd, as_error* err)
{
	if ((cmd->type == AS_ASYNC_TYPE_SCAN_PARTITION &&
		as_async_scan_should_retry(cmd, err->code)) ||
	    (cmd->type == AS_ASYNC_TYPE_QUERY_PARTITION &&
		as_async_query_should_retry(cmd, err->code))) {
		as_event_executor* executor = cmd->udata;
		as_event_command_release(cmd);
		as_event_executor_complete(executor);
		return;
	}
	as_event_notify_error(cmd, err);
	as_event_command_release(cmd);
}

static as_status
as_event_command_parse_set_digest(as_event_command* cmd, as_error* err, char* set, uint8_t* digest) {
	// The key has fallen out of scope, so the key's set and digest have to be
	// parsed from the command's send buffer
	uint8_t* p = as_event_get_ubuf(cmd);
	p += AS_HEADER_SIZE;

	// Field ID is located after field size.
	// Skip namespace.
	uint8_t field_id = *(uint8_t*)(p + sizeof(uint32_t));

	if (field_id == AS_FIELD_NAMESPACE) {
		p += cf_swap_from_be32(*(uint32_t*)p) + sizeof(uint32_t);
	}

	// Parse set.
	field_id = *(uint8_t*)(p + sizeof(uint32_t));

	if (field_id != AS_FIELD_SETNAME) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid set field id: %u", field_id);
	}

	uint32_t len = cf_swap_from_be32(*(uint32_t*)p) - 1;

	if (len >= AS_SET_MAX_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid set len: %u", len);
	}

	p += AS_FIELD_HEADER_SIZE;

	memcpy(set, p, len);
	set[len] = 0;
	p += len;

	// Parse digest.
	field_id = *(uint8_t*)(p + sizeof(uint32_t));

	if (field_id != AS_FIELD_DIGEST) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid digest field id: %u", field_id);
	}

	len = cf_swap_from_be32(*(uint32_t*)p) - 1;

	if (len != AS_DIGEST_VALUE_SIZE) {
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid digest len: %u", len);
	}

	p += AS_FIELD_HEADER_SIZE;

	memcpy(digest, p, len);
	return AEROSPIKE_OK;
}

static void
as_event_check_in_doubt(as_event_command* cmd, as_error* err) {
	if (err->in_doubt && cmd->txn) {
		// It's important that this logic is only executed for commands in a transaction,
		// but not transaction operations (add transaction key, commit, abort). Add transaction key
		// does not call this function and commit/abort do not set cmd->txn.
		as_set set;
		as_digest_value digest;

		as_status status = as_event_command_parse_set_digest(cmd, err, set, digest);

		if (status != AEROSPIKE_OK) {
			// Better to return original error and log message here.
			as_log_error("Send buffer is corrupt");
			return;
		}

		as_txn_on_write_in_doubt(cmd->txn, digest, set);
	}
}

void as_async_batch_error(as_event_command* cmd, as_error* err);

void
as_event_notify_error(as_event_command* cmd, as_error* err)
{
	as_error_set_in_doubt(err, cmd->flags & AS_ASYNC_FLAGS_READ, cmd->command_sent_counter);

	switch (cmd->type) {
		case AS_ASYNC_TYPE_WRITE:
			as_event_check_in_doubt(cmd, err);
			((as_async_write_command*)cmd)->listener(err, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_RECORD:
			as_event_check_in_doubt(cmd, err);
			((as_async_record_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_VALUE:
			as_event_check_in_doubt(cmd, err);
			((as_async_value_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_TXN_MONITOR:
			((as_async_record_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_INFO:
			((as_async_info_command*)cmd)->listener(err, 0, cmd->udata, cmd->event_loop);
			break;
		case AS_ASYNC_TYPE_CONNECTOR:
			connector_error(cmd, err);
			break;
		case AS_ASYNC_TYPE_BATCH:
			as_async_batch_error(cmd, err);
			as_event_executor_error(cmd->udata, err, 1);
			break;
		default:
			// Handle command that is part of a group (scan, query).
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
	as_event_timer_stop(cmd);
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
	as_event_timer_stop(cmd);
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
	as_event_timer_stop(cmd);
	as_event_stop_watcher(cmd, cmd->conn);
	
	as_async_conn_pool* pool = &cmd->node->async_conn_pools[cmd->event_loop->index];

	// Close socket on errors that can leave unread data in socket.
	switch (err->code) {
		case AEROSPIKE_ERR_CLUSTER:
		case AEROSPIKE_ERR_DEVICE_OVERLOAD:
			as_node_add_error(cmd->node);
			as_node_incr_error_rate(cmd->node);
			as_event_put_connection(cmd, pool);
			break;

		case AEROSPIKE_ERR_QUERY_ABORTED:
		case AEROSPIKE_ERR_SCAN_ABORTED:
		case AEROSPIKE_ERR_ASYNC_CONNECTION:
		case AEROSPIKE_ERR_TLS_ERROR:
		case AEROSPIKE_ERR_CLIENT_ABORT:
		case AEROSPIKE_ERR_CLIENT:
		case AEROSPIKE_NOT_AUTHENTICATED:
			as_node_add_error(cmd->node);
			as_node_incr_error_rate(cmd->node);
			as_event_release_connection(cmd->conn, pool);
			break;
		
		case AEROSPIKE_ERR_TIMEOUT:
			as_node_add_timeout(cmd->node);
			as_event_put_connection(cmd, pool);
			break;
			
		case AEROSPIKE_ERR_RECORD_NOT_FOUND:
			// Do not increment error count on record not found.
			// Add latency metrics instead.
			if (cmd->metrics_enabled && cmd->latency_type != AS_LATENCY_TYPE_NONE) {
				as_event_add_latency(cmd, cmd->latency_type);
			}
			as_event_put_connection(cmd, pool);
			break;

		default:
			as_node_add_error(cmd->node);
			as_event_put_connection(cmd, pool);
			break;
	}
	as_event_error_callback(cmd, err);
}

static as_status
as_event_command_parse_fields(as_event_command* cmd, as_error* err, as_msg* msg, uint8_t** pp)
{
	as_set set;
	as_digest_value digest;

	as_status status = as_event_command_parse_set_digest(cmd, err, set, digest);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	return as_command_parse_fields_txn(pp, err, msg, cmd->txn, digest, set, (cmd->flags & AS_ASYNC_FLAGS_READ) == 0);
}

bool
as_event_command_parse_header(as_event_command* cmd)
{
	uint8_t* p = cmd->buf + cmd->pos;
	as_msg* msg = (as_msg*)p;
	as_msg_swap_header_from_be(msg);
	p += sizeof(as_msg);

	if (cmd->txn) {
		as_error err;
		as_status status = as_event_command_parse_fields(cmd, &err, msg, &p);

		if (status != AEROSPIKE_OK) {
			as_event_response_error(cmd, &err);
			return true;
		}
	}

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
	as_status status;
	uint8_t* p = cmd->buf + cmd->pos;
	as_msg* msg = (as_msg*)p;
	as_msg_swap_header_from_be(msg);
	p += sizeof(as_msg);

	if (cmd->txn) {
		status = as_event_command_parse_fields(cmd, &err, msg, &p);

		if (status != AEROSPIKE_OK) {
			as_event_response_error(cmd, &err);
			return true;
		}
	}
	else {
		p = as_command_ignore_fields(p, msg->n_fields);
	}

	status = msg->result_code;

	switch (status) {
		case AEROSPIKE_OK: {
			if (cmd->flags & AS_ASYNC_FLAGS_HEAP_REC) {
				// Create record on heap and let user call as_record_destroy() on success.
				as_record* rec = as_record_new(msg->n_ops);

				rec->gen = msg->generation;
				rec->ttl = cf_server_void_time_to_ttl(msg->record_ttl);

				status = as_command_parse_bins(&p, &err, rec, msg->n_ops,
											   cmd->flags & AS_ASYNC_FLAGS_DESERIALIZE);

				if (status == AEROSPIKE_OK) {
					as_event_response_complete(cmd);
					((as_async_record_command*)cmd)->listener(0, rec, cmd->udata, cmd->event_loop);
					as_event_command_release(cmd);
				}
				else {
					as_record_destroy(rec);
					as_event_response_error(cmd, &err);
				}
			}
			else {
				// Create record on stack and call as_record_destroy() after listener completes.
				as_record rec;

				if (msg->n_ops < 1000) {
					as_record_inita(&rec, msg->n_ops);
				}
				else {
					as_record_init(&rec, msg->n_ops);
				}

				rec.gen = msg->generation;
				rec.ttl = cf_server_void_time_to_ttl(msg->record_ttl);
				
				status = as_command_parse_bins(&p, &err, &rec, msg->n_ops,
											   cmd->flags & AS_ASYNC_FLAGS_DESERIALIZE);

				if (status == AEROSPIKE_OK) {
					as_event_response_complete(cmd);
					((as_async_record_command*)cmd)->listener(0, &rec, cmd->udata, cmd->event_loop);
					as_event_command_release(cmd);
				}
				else {
					as_event_response_error(cmd, &err);
				}
				as_record_destroy(&rec);
			}
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
	as_error err;
	as_status status;
	uint8_t* p = cmd->buf + cmd->pos;
	as_msg* msg = (as_msg*)cmd->buf;
	as_msg_swap_header_from_be(msg);
	p += sizeof(as_msg);

	if (cmd->txn) {
		status = as_event_command_parse_fields(cmd, &err, msg, &p);

		if (status != AEROSPIKE_OK) {
			as_event_response_error(cmd, &err);
			return true;
		}
	}
	else {
		p = as_command_ignore_fields(p, msg->n_fields);
	}

	status = msg->result_code;

	switch (status) {
		case AEROSPIKE_OK: {
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
as_event_command_parse_deadline(as_event_command* cmd)
{
	as_error err;
	uint8_t* p = cmd->buf + cmd->pos;
	as_msg* msg = (as_msg*)p;
	as_msg_swap_header_from_be(msg);
	p += sizeof(as_msg);

	as_status status = as_command_parse_fields_deadline(&p, &err, msg, cmd->txn);

	if (status != AEROSPIKE_OK) {
		as_event_response_error(cmd, &err);
		return true;
	}

	status = msg->result_code;

	if (status != AEROSPIKE_OK) {
		as_error_update(&err, status, "%s %s", as_node_get_address_string(cmd->node), as_error_string(status));
		as_event_response_error(cmd, &err);
		return true;
	}

	as_event_response_complete(cmd);
	((as_async_record_command*)cmd)->listener(NULL, NULL, cmd->udata, cmd->event_loop);
	as_event_command_release(cmd);
	return true;
}

bool
as_event_command_parse_info(as_event_command* cmd)
{
	uint8_t* p = cmd->buf + cmd->pos;
	char* response = (char*)p;
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
		cmd->event_state->pending--;
	}

	if (cmd->node) {
		as_node_release(cmd->node);
	}

	if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
		cf_free(cmd->buf);
	}

	if (cmd->ubuf) {
		cf_free(cmd->ubuf);
	}

	cf_free(cmd);

	if (event_loop->max_commands_in_process > 0 && ! event_loop->using_delay_queue) {
		// Try executing commands from the delay queue.
		as_event_execute_from_delay_queue(event_loop);
	}
}

/******************************************************************************
 * CONNECTION CREATE
 *****************************************************************************/

typedef struct {
	as_monitor* monitor;
	uint32_t* loop_count;
	as_node* node;
	as_async_conn_pool* pool;
	uint32_t conn_start;
	uint32_t conn_count;
	uint32_t conn_max;
	uint32_t concur_max;
	uint32_t timeout_ms;
	bool error;
} connector_shared;

typedef struct {
	as_event_command command;
	uint8_t space[];
} connector_command;

static void
connector_execute_command(as_event_loop* event_loop, connector_shared* cs);

static inline void
connector_release(as_monitor* monitor, uint32_t* loop_count)
{
	if (as_aaf_uint32_rls(loop_count, -1) == 0) {
		as_monitor_notify(monitor);
	}
}

static void
connector_complete(connector_shared* cs)
{
	if (cs->monitor) {
		// Initial connector is allocated on stack.
		connector_release(cs->monitor, cs->loop_count);
	}
	else {
		// Balance connector is allocated on heap.
		cf_free(cs);
	}
}

static void
connector_command_complete(as_event_loop* event_loop, connector_shared* cs)
{
	if (++cs->conn_count == cs->conn_max) {
		connector_complete(cs);
		return;
	}

	if (cs->conn_start < cs->conn_max && !cs->error) {
		cs->conn_start++;
		connector_execute_command(event_loop, cs);
	}
}

static void
connector_abort(as_event_loop* event_loop, connector_shared* cs)
{
	if (!cs->error) {
		// Add connections not started yet to count.
		cs->conn_count += cs->conn_max - cs->conn_start;
		cs->error = true;
	}
	connector_command_complete(event_loop, cs);
}

static void
connector_error(as_event_command* cmd, as_error* err)
{
	// Connection failed.  Highly unlikely other connections will succeed.
	// Stop executing new commands. Command is released by calling function.
	as_log_debug("Async min connection failed: %d %s", err->code, err->message);
	connector_abort(cmd->event_loop, cmd->udata);
}

void
as_event_connector_success(as_event_command* cmd)
{
	as_event_loop* event_loop = cmd->event_loop;
	connector_shared* cs = cmd->udata;

	as_event_response_complete(cmd);
	as_event_command_release(cmd);

	connector_command_complete(event_loop, cs);
}

static void
connector_execute_command(as_event_loop* event_loop, connector_shared* cs)
{
	if (! as_async_conn_pool_incr_total(cs->pool)) {
		// We are already at max connections.
		connector_abort(event_loop, cs);
		return;
	}

	as_node* node = cs->node;
	as_node_reserve(node);

	as_cluster* cluster = node->cluster;

	size_t s = (sizeof(connector_command) + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	connector_command* cc = (connector_command*)cmd;

	memset(cc, 0, sizeof(connector_command));
	cmd->event_loop = event_loop;
	cmd->event_state = &cluster->event_state[event_loop->index];
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->udata = cs;
	cmd->buf = cc->space;
	cmd->write_offset = (uint32_t)(cmd->buf - (uint8_t*)cmd);
	cmd->read_capacity = (uint32_t)(s - sizeof(connector_command));
	cmd->type = AS_ASYNC_TYPE_CONNECTOR;
	cmd->proto_type = AS_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_CONNECT;
	cmd->flags = 0;
	cmd->replica_size = 1;
	cmd->replica_index = 0;

	event_loop->pending++;
	cmd->event_state->pending++;

	cmd->total_deadline = cf_getms() + cs->timeout_ms;
	as_event_timer_once(cmd, cs->timeout_ms);

	as_event_create_connection(cmd, cs->pool);
}

static void
connector_create_commands(as_event_loop* event_loop, connector_shared* cs)
{
	cs->conn_start = cs->concur_max;

	for (uint32_t i = 0; i < cs->concur_max; i++) {
		connector_execute_command(event_loop, cs);
	}
}

static void
create_connections_wait(as_node* node, as_async_conn_pool* pools)
{
	as_monitor monitor;
	as_monitor_init(&monitor);

	uint32_t loop_max = as_event_loop_size;
	uint32_t loop_count = loop_max;
	uint32_t max_concurrent = 20 / loop_max + 1;
	uint32_t timeout_ms = node->cluster->conn_timeout_ms;

	connector_shared* list = alloca(sizeof(connector_shared) * loop_max);

	for (uint32_t i = 0; i < loop_max; i++) {
		as_async_conn_pool* pool = &pools[i];
		uint32_t min_size = pool->min_size;

		if (min_size > 0) {
			connector_shared* cs = &list[i];
			cs->monitor = &monitor;
			cs->loop_count = &loop_count;
			cs->node = node;
			cs->pool = pool;
			cs->conn_count = 0;
			cs->conn_max = min_size;
			cs->concur_max = (min_size >= max_concurrent)? max_concurrent : min_size;
			cs->timeout_ms = timeout_ms;
			cs->error = false;

			if (!as_event_execute(&as_event_loops[i],
				(as_event_executable)connector_create_commands, cs)) {
				as_log_error("Failed to queue connector");
				connector_release(&monitor, &loop_count);
			}
		}
		else {
			connector_release(&monitor, &loop_count);
		}
	}
	as_monitor_wait(&monitor);
	as_monitor_destroy(&monitor);
}

static void
create_connections_nowait(as_node* node, as_async_conn_pool* pools)
{
	uint32_t loop_max = as_event_loop_size;
	uint32_t max_concurrent = 20 / loop_max + 1;
	uint32_t timeout_ms = node->cluster->conn_timeout_ms;

	connector_shared* list = cf_malloc(sizeof(connector_shared) * loop_max);

	for (uint32_t i = 0; i < loop_max; i++) {
		as_async_conn_pool* pool = &pools[i];
		uint32_t min_size = pool->min_size;

		if (min_size > 0) {
			connector_shared* cs = &list[i];
			cs->monitor = NULL;
			cs->loop_count = NULL;
			cs->node = node;
			cs->pool = pool;
			cs->conn_count = 0;
			cs->conn_max = min_size;
			cs->concur_max = (min_size >= max_concurrent)? max_concurrent : min_size;
			cs->timeout_ms = timeout_ms;
			cs->error = false;

			if (!as_event_execute(&as_event_loops[i],
				(as_event_executable)connector_create_commands, cs)) {
				as_log_error("Failed to queue connector");
			}
		}
	}
}

static bool
as_in_event_loops(void)
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
	return in_event_loop;
}

void
as_event_create_connections(as_node* node, as_async_conn_pool* pools)
{
	if (as_in_event_loops()) {
		create_connections_nowait(node, pools);
	}
	else {
		create_connections_wait(node, pools);
	}
}

static void
create_connections(as_event_loop* event_loop, as_node* node, as_async_conn_pool* pool, int count)
{
	connector_shared* cs = cf_malloc(sizeof(connector_shared));
	cs->monitor = NULL;
	cs->loop_count = NULL;
	cs->node = node;
	cs->pool = pool;
	cs->conn_count = 0;
	cs->conn_max = count;
	cs->concur_max = 1;
	cs->timeout_ms = node->cluster->conn_timeout_ms;
	cs->error = false;

	connector_create_commands(event_loop, cs);
}

/******************************************************************************
 * CONNECTION BALANCE
 *****************************************************************************/

typedef struct {
	as_cluster* cluster;
	as_monitor monitor;
	uint32_t loop_count;
} balancer_shared;

static inline void
balancer_release(balancer_shared* bs)
{
	if (as_aaf_uint32_rls(&bs->loop_count, -1) == 0) {
		as_monitor_notify(&bs->monitor);
	}
}

static void
close_idle_connections(as_async_conn_pool* pool, uint64_t max_socket_idle_ns, int count)
{
	as_event_connection* conn;

	while (count > 0) {
		if (! as_queue_pop_tail(&pool->queue, &conn)) {
			break;
		}

		if (as_event_conn_current_trim(conn, max_socket_idle_ns)) {
			if (! as_queue_push_limit(&pool->queue, &conn)) {
				as_event_release_connection(conn, pool);
			}
			break;
		}
		as_event_release_connection(conn, pool);
		count--;
	}
}

void
as_event_balance_connections_node(as_event_loop* event_loop, as_cluster* cluster, as_node* node)
{
	as_async_conn_pool* pool = &node->async_conn_pools[event_loop->index];
	int excess = pool->queue.total - pool->min_size;

	if (excess > 0) {
		close_idle_connections(pool, cluster->max_socket_idle_ns_trim, excess);
		// Do not close idle pipeline connections because pipelines work better with a stable
		// number of connections.
	}
	else if (excess < 0 && as_node_valid_error_rate(node)) {
		create_connections(event_loop, node, pool, -excess);
	}
}

void
as_event_balance_connections_cluster(as_event_loop* event_loop, as_cluster* cluster)
{
	as_nodes* nodes = as_nodes_reserve(cluster);

	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		as_event_balance_connections_node(event_loop, cluster, node);
	}

	as_nodes_release(nodes);
}

static void
balancer_in_loop_cluster(as_event_loop* event_loop, balancer_shared* bs)
{
	as_event_balance_connections_cluster(event_loop, bs->cluster);
	balancer_release(bs);
}

void
as_event_balance_connections(as_cluster* cluster)
{
	uint32_t loop_max = as_event_loop_size;

	if (loop_max == 0) {
		return;
	}

	balancer_shared bs;
	bs.cluster = cluster;
	as_monitor_init(&bs.monitor);
	bs.loop_count = loop_max;

	for (uint32_t i = 0; i < loop_max; i++) {
		if (! as_event_execute(&as_event_loops[i], (as_event_executable)balancer_in_loop_cluster, &bs)) {
			as_log_error("Failed to queue connection balancer");
			balancer_release(&bs);
		}
	}

	// Wait for all eventloops to finish balancing connections in the cluster tend.
	// This avoids the scenario where the cluster tend thread is shutdown and the
	// cluster is destroyed before the balancer’s eventloop callbacks are processed.
	// The cluster tend thread can't be shutdown until this cluster tend function
	// completes.
	as_monitor_wait(&bs.monitor);
	as_monitor_destroy(&bs.monitor);
}

typedef struct {
	as_cluster* cluster;
	as_node* node;
	as_monitor monitor;
	uint32_t loop_count;
} balancer_shared_node;

static inline void
balancer_release_node(balancer_shared_node* bs)
{
	if (as_aaf_uint32_rls(&bs->loop_count, -1) == 0) {
		as_node_release(bs->node);
		as_monitor_notify(&bs->monitor);
	}
}

static void
balancer_in_loop_node(as_event_loop* event_loop, balancer_shared_node* bs)
{
	as_event_balance_connections_node(event_loop, bs->cluster, bs->node);
	balancer_release_node(bs);
}

void
as_event_node_balance_connections(as_cluster* cluster, as_node* node)
{
	uint32_t loop_max = as_event_loop_size;

	if (loop_max == 0) {
		return;
	}

	balancer_shared_node bs;
	bs.cluster = cluster;
	bs.node = node;
	as_monitor_init(&bs.monitor);
	bs.loop_count = loop_max;

	as_node_reserve(node);

	for (uint32_t i = 0; i < loop_max; i++) {
		if (! as_event_execute(&as_event_loops[i], (as_event_executable)balancer_in_loop_node, &bs)) {
			as_log_error("Failed to queue node connection balancer");
			balancer_release_node(&bs);
		}
	}

	// Wait for all eventloops to finish balancing connections in the cluster tend.
	// This avoids the scenario where the cluster tend thread is shutdown and the
	// cluster is destroyed before the balancer’s eventloop callbacks are processed.
	// The cluster tend thread can't be shutdown until this cluster tend function
	// completes.
	as_monitor_wait(&bs.monitor);
	as_monitor_destroy(&bs.monitor);
}

/******************************************************************************
 * CLUSTER CLOSE
 *****************************************************************************/

typedef struct {
	as_monitor* monitor;
	as_cluster* cluster;
	uint32_t event_loop_count;
} as_event_close_state;

static void
as_event_close_cluster_event_loop(
	as_event_loop* event_loop, as_event_close_state* state, as_event_state* event_state
	)
{
	event_state->closed = true;

	if (as_aaf_uint32_rls(&state->event_loop_count, -1) == 0) {
		as_fence_acq();
		as_cluster_destroy(state->cluster);

		if (state->monitor) {
			as_monitor_notify(state->monitor);
		}
		cf_free(state);
	}
}

static void
as_event_close_cluster_cb(as_event_loop* event_loop, as_event_close_state* state)
{
	as_event_state* event_state = &state->cluster->event_state[event_loop->index];

	if (event_state->closed) {
		// Cluster's event loop connections are already closed.
		return;
	}

	if (event_state->pending > 0) {
		// Cluster has pending commands.
		// Check again after all other commands run.
		if (as_event_execute(event_loop, (as_event_executable)as_event_close_cluster_cb, state)) {
			return;
		}
		as_log_error("Failed to queue cluster close command");
	}

	as_event_close_cluster_event_loop(event_loop, state, event_state);
}

void
as_event_close_cluster(as_cluster* cluster)
{
	if (as_event_loop_size == 0) {
		return;
	}

	as_monitor* monitor = NULL;

	if (! as_in_event_loops()) {
		monitor = cf_malloc(sizeof(as_monitor));
		as_monitor_init(monitor);
	}

	as_event_close_state* state = cf_malloc(sizeof(as_event_close_state));
	state->monitor = monitor;
	state->cluster = cluster;
	state->event_loop_count = as_event_loop_size;

	// Send cluster close notification to async event loops.
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_loop* event_loop = &as_event_loops[i];

		if (! as_event_execute(event_loop, (as_event_executable)as_event_close_cluster_cb, state)) {
			as_log_error("Failed to queue cluster close command");
			as_event_close_cluster_event_loop(event_loop, state,
				&state->cluster->event_state[event_loop->index]);
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
