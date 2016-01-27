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
#include <aerospike/as_event.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_async.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_pipe.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <errno.h>

/******************************************************************************
 * GLOBALS
 *****************************************************************************/

extern int as_event_send_buffer_size;
extern int as_event_recv_buffer_size;
extern bool as_event_threads_created;

/******************************************************************************
 * LIBEV FUNCTIONS
 *****************************************************************************/

#if defined(AS_USE_LIBEV)

static void
as_ev_wakeup(struct ev_loop* loop, ev_async* wakeup, int revents)
{
	// Read command pointers from queue.
	as_event_loop* event_loop = wakeup->data;
	void* cmd;
	
	pthread_mutex_lock(&event_loop->lock);
	
	while (as_queue_pop(&event_loop->queue, &cmd)) {
		if (cmd) {
			// Process new command.
			as_event_command_execute_in_loop(cmd);
		}
		else {
			// Received stop signal.
			as_event_close_loop(event_loop);
			return;
		}
	}
	pthread_mutex_unlock(&event_loop->lock);
}

static void*
as_ev_worker(void* udata)
{
	struct ev_loop* loop = udata;
	ev_loop(loop, 0);
	ev_loop_destroy(loop);
	return NULL;
}

static inline void
as_ev_init_loop(as_event_loop* event_loop)
{
	as_queue_init(&event_loop->queue, sizeof(void*), AS_EVENT_QUEUE_INITIAL_CAPACITY);
	
	ev_async_init(&event_loop->wakeup, as_ev_wakeup);
	event_loop->wakeup.data = event_loop;
	ev_async_start(event_loop->loop, &event_loop->wakeup);
	
	event_loop->initialized = true;
}

bool
as_event_create_loop(as_event_loop* event_loop)
{
	event_loop->loop = ev_loop_new(EVFLAG_AUTO);
	
	if (! event_loop->loop) {
		as_log_error("Failed to create event loop");
		return false;
	}
	as_ev_init_loop(event_loop);
	
	return pthread_create(&event_loop->thread, NULL, as_ev_worker, event_loop->loop) == 0;
}

void
as_event_register_external_loop(as_event_loop* event_loop)
{
	// This method is only called when user sets an external event loop.
	as_ev_init_loop(event_loop);
}

bool
as_event_send(as_event_command* cmd)
{
	// Notify other event loop thread that queue needs to be processed.
	as_event_loop* event_loop = cmd->event_loop;
	
	pthread_mutex_lock(&event_loop->lock);
	bool queued = as_queue_push(&event_loop->queue, &cmd);
	pthread_mutex_unlock(&event_loop->lock);
	
	if (queued) {
		ev_async_send(event_loop->loop, &event_loop->wakeup);
	}
	return queued;
}

static inline void
as_ev_watch_write(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	ev_io_stop(cmd->event_loop->loop, &conn->watcher);
	ev_io_set(&conn->watcher, conn->fd, cmd->pipeline? EV_WRITE | EV_READ : EV_WRITE);
	ev_io_start(cmd->event_loop->loop, &conn->watcher);
}

static inline void
as_ev_watch_read(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	ev_io_stop(cmd->event_loop->loop, &conn->watcher);
	ev_io_set(&conn->watcher, conn->fd, EV_READ);
	ev_io_start(cmd->event_loop->loop, &conn->watcher);
}

#define AS_EVENT_WRITE_COMPLETE 0
#define AS_EVENT_WRITE_INCOMPLETE 1
#define AS_EVENT_WRITE_ERROR 2

static int
as_ev_write(as_event_command* cmd)
{
	int fd = cmd->conn->fd;
	ssize_t bytes;
	
	do {
#if defined(__linux__)
		bytes = send(fd, cmd->buf + cmd->pos, cmd->len - cmd->pos, MSG_NOSIGNAL);
#else
		bytes = write(fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
#endif
		
		if (bytes > 0) {
			cmd->pos += bytes;
			continue;
		}
		
		if (bytes < 0) {
			if (errno == EWOULDBLOCK) {
				return AS_EVENT_WRITE_INCOMPLETE;
			}
			
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket %d write failed: %d", fd, errno);
			as_event_socket_error(cmd, &err);
			return AS_EVENT_WRITE_ERROR;
		}
		else {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket %d write closed by peer", fd);
			as_event_socket_error(cmd, &err);
			return AS_EVENT_WRITE_ERROR;
		}
	} while (cmd->pos < cmd->len);
	
	return AS_EVENT_WRITE_COMPLETE;
}

static bool
as_ev_read(as_event_command* cmd)
{
	int fd = cmd->conn->fd;
	ssize_t bytes;
	
	do {
		bytes = read(fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
		
		if (bytes > 0) {
			cmd->pos += bytes;
			continue;
		}
		
		if (bytes < 0) {
			if (errno != EWOULDBLOCK) {
				as_error err;
				as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket %d read failed: %d", fd, errno);
				as_event_socket_error(cmd, &err);
			}
			return false;
		}
		else {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket %d read closed by peer", fd);
			as_event_socket_error(cmd, &err);
			return false;
		}
	} while (cmd->pos < cmd->len);
	
	return true;
}

static inline void
as_ev_command_read_start(as_event_command* cmd)
{
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_READ_HEADER;
	
	if (cmd->pipeline) {
		as_pipe_read_start(cmd);
	}
	as_ev_watch_read(cmd);
}

static inline void
as_ev_command_write_start(as_event_command* cmd)
{
	cmd->state = AS_ASYNC_STATE_WRITE;
	
	int ret = as_ev_write(cmd);
	
	if (ret == AS_EVENT_WRITE_COMPLETE) {
		// Done with write. Register for read.
		as_ev_command_read_start(cmd);
		return;
	}
	
	if (ret == AS_EVENT_WRITE_INCOMPLETE) {
		// Got would-block. Register for write.
		as_ev_watch_write(cmd);
	}
}

static void
as_ev_command_peek_block(as_event_command* cmd)
{
	// Batch, scan, query may be waiting on end block.
	// Prepare for next message block.
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_READ_HEADER;
	
	if (! as_ev_read(cmd)) {
		return;
	}
	
	as_proto* proto = (as_proto*)cmd->buf;
	as_proto_swap_from_be(proto);
	size_t size = proto->sz;
	
	cmd->len = (uint32_t)size;
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_READ_BODY;
	
	// Check for end block size.
	if (cmd->len == sizeof(as_msg)) {
		// Look like we received end block.  Read and parse to make sure.
		if (! as_ev_read(cmd)) {
			return;
		}
		
		if (! cmd->parse_results(cmd)) {
			// We did not finish after all. Prepare to read next header.
			cmd->len = sizeof(as_proto);
			cmd->pos = 0;
			cmd->state = AS_ASYNC_STATE_READ_HEADER;
		}
	}
	else {
		// Received normal data block.  Stop reading for fairness reasons and wait
		// till next iteration.
		if (cmd->len > cmd->capacity) {
			if (cmd->free_buf) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->capacity = cmd->len;
			cmd->free_buf = true;
		}
	}
}

static void
as_ev_parse_authentication(as_event_command* cmd)
{
	if (cmd->state == AS_ASYNC_STATE_AUTH_READ_HEADER) {
		// Read response length
		if (! as_ev_read(cmd)) {
			return;
		}
		as_event_set_auth_parse_header(cmd);

		if (cmd->len > cmd->capacity) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Authenticate response size is corrupt: %u", cmd->auth_len);
			as_event_socket_error(cmd, &err);
			return;
		}
	}
	
	if (! as_ev_read(cmd)) {
		return;
	}
	
	// Parse authentication response.
	cmd->len -= cmd->auth_len;
	uint8_t code = cmd->buf[cmd->len + AS_ASYNC_AUTH_RETURN_CODE];
	
	if (code) {
		// Can't authenticate socket, so must close it.
		as_error err;
		as_error_update(&err, code, "Authentication failed: %s", as_error_string(code));
		as_event_socket_error(cmd, &err);
		return;
	}
	
	cmd->pos = 0;
	as_ev_command_write_start(cmd);
}

static void
as_ev_command_read(as_event_command* cmd)
{
	// Check for authenticate read-header or read-body.
	if (cmd->state & (AS_ASYNC_STATE_AUTH_READ_HEADER | AS_ASYNC_STATE_AUTH_READ_BODY)) {
		as_ev_parse_authentication(cmd);
		return;
	}
	
	if (cmd->state == AS_ASYNC_STATE_READ_HEADER) {
		// Read response length
		if (! as_ev_read(cmd)) {
			return;
		}
		
		as_proto* proto = (as_proto*)cmd->buf;
		as_proto_swap_from_be(proto);
		size_t size = proto->sz;
		
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_READ_BODY;
		
		if (cmd->len > cmd->capacity) {
			if (cmd->free_buf) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->capacity = cmd->len;
			cmd->free_buf = true;
		}
	}
	
	// Read response body
	if (! as_ev_read(cmd)) {
		return;
	}
	
	if (! cmd->parse_results(cmd)) {
		// Batch, scan, query is not finished.
		as_ev_command_peek_block(cmd);
	}
}

static void
as_ev_callback(struct ev_loop* loop, ev_io* watcher, int revents)
{
	if (revents & EV_READ) {
		as_event_connection* conn = watcher->data;
		as_event_command* cmd;
		
		if (conn->pipeline) {
			as_pipe_connection* pipe = (as_pipe_connection*)conn;
			
			if (pipe->writer && cf_ll_size(&pipe->readers) == 0) {
				// Authentication response will only have a writer.
				cmd = pipe->writer;
			}
			else {
				// Next response is at head of reader linked list.
				cf_ll_element* link = cf_ll_get_head(&pipe->readers);
				
				if (link) {
					cmd = as_pipe_link_to_command(link);
				}
				else {
					as_log_debug("Pipeline read event ignored");
					return;
				}
			}
		}
		else {
			cmd = ((as_async_connection*)conn)->cmd;
		}
		as_ev_command_read(cmd);
	}
	else if (revents & EV_WRITE) {
		as_event_connection* conn = watcher->data;
		
		as_event_command* cmd = conn->pipeline ?
			((as_pipe_connection*)conn)->writer :
			((as_async_connection*)conn)->cmd;
		
		int ret = as_ev_write(cmd);
		
		if (ret == AS_EVENT_WRITE_COMPLETE) {
			// Done with write. Register for read.
			if (cmd->state == AS_ASYNC_STATE_AUTH_WRITE) {
				as_event_set_auth_read_header(cmd);
				as_ev_watch_read(cmd);
			}
			else {
				as_ev_command_read_start(cmd);
			}
		}
	}
	else if (revents & EV_ERROR) {
		as_log_error("Async error occurred: %d", revents);
	}
	else {
		as_log_warn("Unknown event received: %d", revents);
	}
}

static void
as_ev_watcher_init(as_event_command* cmd, int fd)
{
	if (cmd->cluster->user) {
		as_event_set_auth_write(cmd);
		cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
	}
	else {
		cmd->state = AS_ASYNC_STATE_WRITE;
	}
	
	as_event_connection* conn = cmd->conn;
	conn->fd = fd;
	ev_io_init(&conn->watcher, as_ev_callback, fd, cmd->pipeline? EV_WRITE | EV_READ : EV_WRITE);
	conn->watcher.data = conn;
	ev_io_start(cmd->event_loop->loop, &conn->watcher);
}

static void
as_ev_connect(as_event_command* cmd)
{
	int fd = as_event_create_socket(cmd);
	
	if (fd < 0) {
		return;
	}
		
	// Try primary address.
	as_node* node = cmd->node;
	as_address* primary = as_vector_get(&node->addresses, node->address_index);
	
	// Attempt non-blocking connection.
	if (connect(fd, (struct sockaddr*)&primary->addr, sizeof(struct sockaddr)) == 0) {
		as_ev_watcher_init(cmd, fd);
		return;
	}
	
	// Check if connection is in progress.
	if (errno == EINPROGRESS) {
		as_ev_watcher_init(cmd, fd);
		return;
	}
	
	// Try other addresses.
	as_vector* addresses = &node->addresses;
	for (uint32_t i = 0; i < addresses->size; i++) {
		as_address* address = as_vector_get(addresses, i);
		
		// Address points into alias array, so pointer comparison is sufficient.
		if (address != primary) {
			if (connect(fd, (struct sockaddr*)&address->addr, sizeof(struct sockaddr)) == 0) {
				// Replace invalid primary address with valid alias.
				// Other threads may not see this change immediately.
				// It's just a hint, not a requirement to try this new address first.
				as_log_debug("Change node address %s %s:%d", node->name, address->name, (int)cf_swap_from_be16(address->addr.sin_port));
				ck_pr_store_32(&node->address_index, i);
				as_ev_watcher_init(cmd, fd);
				return;
			}
			
			// Check if connection is in progress.
			if (errno == EINPROGRESS) {
				// Replace invalid primary address with valid alias.
				// Other threads may not see this change immediately.
				// It's just a hint, not a requirement to try this new address first.
				as_log_debug("Change node address %s %s:%d", node->name, address->name, (int)cf_swap_from_be16(address->addr.sin_port));
				ck_pr_store_32(&node->address_index, i);
				
				// Connection hasn't finished.
				as_ev_watcher_init(cmd, fd);
				return;
			}
		}
	}
	
	// Failed to start a connection on any socket address.
	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Failed to connect: %s %s:%d",
					node->name, primary->name, (int)cf_swap_from_be16(primary->addr.sin_port));
	as_event_connect_error(cmd, &err, fd);
}

static void
as_ev_timeout(struct ev_loop* loop, ev_timer* timer, int revents)
{
	// One-off timers are automatically stopped by libev.
	as_event_timeout(timer->data);
}

void
as_event_command_begin(as_event_command* cmd)
{
	// Always initialize timer first when timeouts are specified.
	if (cmd->timeout_ms) {
		ev_timer_init(&cmd->timer, as_ev_timeout, (double)cmd->timeout_ms / 1000.0, 0.0);
		cmd->timer.data = cmd;
		ev_timer_start(cmd->event_loop->loop, &cmd->timer);
	}
	
	as_connection_status status = cmd->pipeline ? as_pipe_get_connection(cmd) : as_event_get_connection(cmd);
	
	if (status == AS_CONNECTION_FROM_POOL) {
		as_ev_command_write_start(cmd);
	}
	else if (status == AS_CONNECTION_NEW) {
		as_ev_connect(cmd);
	}
}

void
as_event_close_connection(as_event_connection* conn)
{
	close(conn->fd);
	cf_free(conn);
}

static void
as_ev_close_connections(as_node* node, as_queue* conn_queue)
{
	as_event_connection* conn;
	
	// Queue connection commands to event loops.
	while (as_queue_pop(conn_queue, &conn)) {
		close(conn->fd);
		cf_free(conn);
		as_event_decr_connection(node->cluster, conn_queue);
		ck_pr_dec_32(&node->cluster->async_conn_pool);
	}
	as_queue_destroy(conn_queue);
}

void
as_event_node_destroy(as_node* node)
{
	// Close connections.
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_ev_close_connections(node, &node->async_conn_qs[i]);
		as_ev_close_connections(node, &node->pipe_conn_qs[i]);
	}
	cf_free(node->async_conn_qs);
	cf_free(node->pipe_conn_qs);
}

bool
as_event_send_close_loop(as_event_loop* event_loop)
{
	// Send stop command through queue so it can be executed in event loop thread.
	void* ptr = 0;
	pthread_mutex_lock(&event_loop->lock);
	bool queued = as_queue_push(&event_loop->queue, &ptr);
	pthread_mutex_unlock(&event_loop->lock);
	
	if (queued) {
		ev_async_send(event_loop->loop, &event_loop->wakeup);
	}
	return queued;
}

void
as_event_close_loop(as_event_loop* event_loop)
{
	ev_async_stop(event_loop->loop, &event_loop->wakeup);
	
	// Only stop event loop if client created event loop.
	if (as_event_threads_created) {
		ev_unloop(event_loop->loop, EVUNLOOP_ALL);
	}
	
	// Cleanup event loop resources.
	as_queue_destroy(&event_loop->queue);
	pthread_mutex_unlock(&event_loop->lock);
	pthread_mutex_destroy(&event_loop->lock);
}

#endif
