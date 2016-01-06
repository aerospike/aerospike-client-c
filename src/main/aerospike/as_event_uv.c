/*
 * Copyright 2008-2015 Aerospike, Inc.
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
#include <aerospike/as_async.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_pipe.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <errno.h>

/******************************************************************************
 * GLOBALS
 *****************************************************************************/

extern uint32_t as_event_loop_capacity;
extern int as_event_send_buffer_size;
extern int as_event_recv_buffer_size;
extern bool as_event_threads_created;

/******************************************************************************
 * LIBUV FUNCTIONS
 *****************************************************************************/

#if defined(AS_USE_LIBUV)

#define AS_UV_PROCESS_COMMAND 0
#define AS_UV_CLOSE_CONNECTION 1
#define AS_UV_EXIT_LOOP 2

typedef struct {
	uint64_t type;
	void* ptr;
} as_uv_command;

void
as_uv_timer_closed(uv_handle_t* handle)
{
	as_event_command_free(handle->data);
}

static void
as_uv_wakeup_closed(uv_handle_t* handle)
{
	cf_free(handle);
}

static void
as_uv_connection_closed(uv_handle_t* socket)
{
	as_async_connection* conn = (as_async_connection*)socket;
	ck_pr_dec_32(&conn->cmd->cluster->async_conn);
	ck_pr_dec_32(&conn->cmd->node->async_conn);

	// socket->data has as_event_command ptr but that may have already been freed,
	// so free as_event_connection ptr by socket which is first field in as_event_connection.
	cf_free(socket);
}

static void
as_uv_wakeup(uv_async_t* wakeup)
{
	// Read command pointers from queue.
	as_event_loop* event_loop = wakeup->data;
	as_uv_command cmd;
	
	pthread_mutex_lock(&event_loop->lock);
	
	while (as_queue_pop(&event_loop->queue, &cmd)) {
		switch (cmd.type) {
			case AS_UV_PROCESS_COMMAND:
				as_event_command_execute_in_loop(cmd.ptr);
				break;
				
			case AS_UV_CLOSE_CONNECTION:
				uv_close((uv_handle_t*)cmd.ptr, as_uv_connection_closed);
				break;
				
			case AS_UV_EXIT_LOOP:
				// Close handles.
				uv_close((uv_handle_t*)wakeup, as_uv_wakeup_closed);
				
				// Only stop event loop if client created event loop.
				if (as_event_threads_created) {
					uv_stop(event_loop->loop);
				}
				
				// Cleanup event loop resources.
				as_queue_destroy(&event_loop->queue);
				pthread_mutex_unlock(&event_loop->lock);
				pthread_mutex_destroy(&event_loop->lock);
				return;
		}
	}
	pthread_mutex_unlock(&event_loop->lock);
}

static void*
as_uv_worker(void* udata)
{
	as_event_loop* event_loop = udata;
	
	event_loop->loop = cf_malloc(sizeof(uv_loop_t));
	
	if (! event_loop->loop) {
		as_log_error("Failed to create event loop");
		return 0;
	}
	
	event_loop->wakeup = cf_malloc(sizeof(uv_async_t));
	
	if (! event_loop->wakeup) {
		as_log_error("Failed to create wakeup");
		return 0;
	}

	event_loop->wakeup->data = event_loop;

	uv_loop_init(event_loop->loop);
	uv_async_init(event_loop->loop, event_loop->wakeup, as_uv_wakeup);
	
	pthread_mutex_lock(&event_loop->lock);
	event_loop->initialized = true;
	pthread_mutex_unlock(&event_loop->lock);
	
	uv_run(event_loop->loop, UV_RUN_DEFAULT);
	
	int status = uv_loop_close(event_loop->loop);

	if (status) {
		as_log_warn("uv_loop_close failed: %s", uv_strerror(status));
	}
	cf_free(event_loop->loop);
	return NULL;
}

bool
as_event_create_loop(as_event_loop* event_loop)
{
	event_loop->wakeup = 0;
	as_queue_init(&event_loop->queue, sizeof(as_uv_command), AS_EVENT_QUEUE_INITIAL_CAPACITY);
	return pthread_create(&event_loop->thread, NULL, as_uv_worker, event_loop) == 0;
}

void
as_event_register_external_loop(as_event_loop* event_loop)
{
	// This method is only called when user sets an external event loop.
	event_loop->wakeup = cf_malloc(sizeof(uv_async_t));
	as_queue_init(&event_loop->queue, sizeof(as_uv_command), AS_EVENT_QUEUE_INITIAL_CAPACITY);
	
	// Assume uv_async_init is called on the same thread as the event loop.
	uv_async_init(event_loop->loop, event_loop->wakeup, as_uv_wakeup);
	event_loop->initialized = true;
}

bool
as_event_send(as_event_command* cmd)
{
	// Send command through queue so it can be executed in event loop thread.
	as_event_loop* event_loop = cmd->event_loop;
	
	pthread_mutex_lock(&event_loop->lock);
	as_uv_command qcmd = {.type = AS_UV_PROCESS_COMMAND, .ptr = cmd};
	bool queued = as_queue_push(&event_loop->queue, &qcmd);
	bool initialized = event_loop->initialized;
	pthread_mutex_unlock(&event_loop->lock);
	
	if (queued && initialized) {
		uv_async_send(event_loop->wakeup);
	}
	return queued;
}

static inline as_event_command*
as_uv_get_command(as_event_connection* conn)
{
	if (! conn->pipeline) {
		return ((as_async_connection*)conn)->cmd;
	}
	
	// Next response is at head of reader linked list.
	as_pipe_connection* pipe = (as_pipe_connection*)conn;
	cf_ll_element* link = cf_ll_get_head(&pipe->readers);
	return link ? as_pipe_link_to_command(link) : NULL;
}

static void
as_uv_command_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf)
{
	as_event_command* cmd = as_uv_get_command(stream->data);
			
	if (nread < 0) {
		uv_read_stop(stream);
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket read failed: %zd", nread);
		as_event_socket_error(cmd, &err);
		return;
	}

	cmd->pos += nread;
	
	if (cmd->pos < cmd->len) {
		// Read not finished.
		return;
	}

	if (cmd->state == AS_ASYNC_STATE_READ_HEADER) {
		as_proto* proto = (as_proto*)cmd->buf;
		as_proto_swap_from_be(proto);
		size_t size = proto->sz;
		
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_READ_BODY;
		
		if (cmd->len < sizeof(as_msg)) {
			uv_read_stop(stream);
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Invalid record header size: %u", cmd->len);
			as_event_socket_error(cmd, &err);
			return;
		}
		
		if (cmd->len > cmd->capacity) {
			if (cmd->free_buf) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->capacity = cmd->len;
			cmd->free_buf = true;
		}
		return;
	}

	if (cmd->parse_results(cmd)) {
		uv_read_stop(stream);
	}
	else {
		// Batch, scan, query is not finished.
		cmd->len = sizeof(as_proto);
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_READ_HEADER;
	}
}

static void
as_uv_command_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
	as_event_command* cmd = as_uv_get_command(handle->data);
	*buf = uv_buf_init((char*)cmd->buf + cmd->pos, cmd->len - cmd->pos);
}

static void
as_uv_command_write_complete(uv_write_t* req, int status)
{
	as_event_command* cmd = req->data;
	
	if (status == 0) {
		cmd->len = sizeof(as_proto);
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_READ_HEADER;
		
		if (cmd->pipeline) {
			as_pipe_read_start(cmd);
		}
		
		status = uv_read_start(req->handle, as_uv_command_buffer, as_uv_command_read);
		
		if (status) {
			if (uv_is_closing((uv_handle_t*)req->handle)) {
				return;
			}
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "uv_read_start failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
	}
	else if (status != UV_ECANCELED) {
		if (uv_is_closing((uv_handle_t*)req->handle)) {
			return;
		}
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket write failed: %s", uv_strerror(status));
		as_event_socket_error(cmd, &err);
	}
}

static inline void
as_uv_command_write_start(as_event_command* cmd, uv_stream_t* stream)
{
	cmd->state = AS_ASYNC_STATE_WRITE;
	
	uv_write_t* write_req = &cmd->conn->req.write;
	write_req->data = cmd;
	uv_buf_t buf = uv_buf_init((char*)cmd->buf, cmd->len);

	int status = uv_write(write_req, stream, &buf, 1, as_uv_command_write_complete);

	if (status) {
		if (uv_is_closing((uv_handle_t*)stream)) {
			return;
		}
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "uv_write failed: %s", uv_strerror(status));
		as_event_socket_error(cmd, &err);
	}
}

static inline as_event_command*
as_uv_auth_get_command(as_event_connection* conn)
{
	return conn->pipeline ? ((as_pipe_connection*)conn)->writer : ((as_async_connection*)conn)->cmd;
}

static void
as_uv_auth_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf)
{
	as_event_command* cmd = as_uv_auth_get_command(stream->data);
		
	if (nread < 0) {
		if (uv_is_closing((uv_handle_t*)stream)) {
			return;
		}
		uv_read_stop(stream);
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Authenticate socket read failed: %zd", nread);
		as_event_socket_error(cmd, &err);
		return;
	}
	
	cmd->pos += nread;
	
	if (cmd->pos < cmd->len) {
		// Read not finished.
		return;
	}
	
	if (cmd->state == AS_ASYNC_STATE_AUTH_READ_HEADER) {
		as_event_set_auth_parse_header(cmd);
		
		if (cmd->len > cmd->capacity) {
			if (uv_is_closing((uv_handle_t*)stream)) {
				return;
			}
			uv_read_stop(stream);
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Authenticate response size is corrupt: %u", cmd->auth_len);
			as_event_socket_error(cmd, &err);
			return;
		}
		return;
	}
	
	// Done reading authentication data.
	uv_read_stop(stream);
	
	// Parse authentication response.
	cmd->len -= cmd->auth_len;
	uint8_t code = cmd->buf[cmd->len + AS_ASYNC_AUTH_RETURN_CODE];
	
	if (code) {
		// Can't authenticate socket, so must close it.
		if (uv_is_closing((uv_handle_t*)stream)) {
			return;
		}
		as_error err;
		as_error_update(&err, code, "Authentication failed: %s", as_error_string(code));
		as_event_socket_error(cmd, &err);
		return;
	}
	
	cmd->pos = 0;
	as_uv_command_write_start(cmd, stream);
}

static void
as_uv_auth_command_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
	as_event_command* cmd = as_uv_auth_get_command(handle->data);
	*buf = uv_buf_init((char*)cmd->buf + cmd->pos, cmd->len - cmd->pos);
}

static void
as_uv_auth_write_complete(uv_write_t* req, int status)
{
	as_event_command* cmd = req->data;
	
	if (status == 0) {
		as_event_set_auth_read_header(cmd);
		status = uv_read_start(req->handle, as_uv_auth_command_buffer, as_uv_auth_read);
		
		if (status) {
			if (uv_is_closing((uv_handle_t*)req->handle)) {
				return;
			}
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Authenticate uv_read_start failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
	}
	else if (status != UV_ECANCELED) {
		if (uv_is_closing((uv_handle_t*)req->handle)) {
			return;
		}
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Authenticate socket write failed: %s", uv_strerror(status));
		as_event_socket_error(cmd, &err);
	}
}

static inline void
as_uv_auth_write_start(as_event_command* cmd, uv_stream_t* stream)
{
	as_event_set_auth_write(cmd);
	cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
	
	uv_write_t* write_req = &cmd->conn->req.write;
	write_req->data = cmd;
	uv_buf_t buf = uv_buf_init((char*)cmd->buf + cmd->pos, cmd->auth_len);
	
	int status = uv_write(write_req, stream, &buf, 1, as_uv_auth_write_complete);
	
	if (status) {
		if (uv_is_closing((uv_handle_t*)stream)) {
			return;
		}
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Authenticate uv_write failed: %s", uv_strerror(status));
		as_event_socket_error(cmd, &err);
	}
}

static void
as_uv_connect_error(as_event_command* cmd, as_error* err)
{
	// Timer will be stopped in as_event_command_release().
	// Watcher has not been registered yet.
	
	// libuv requires uv_close if socket released after uv_tcp_init succeeds.
	// The socket is the first field in as_event_connection, so just use connection.
	// The close callback will also free as_event_connection memory.
	uv_close((uv_handle_t*)cmd->conn, as_uv_connection_closed);
	as_event_error_callback(cmd, err);
}

static void
as_uv_connected(uv_connect_t* req, int status)
{
	as_event_command* cmd = req->data;

	if (status == 0) {
		if (cmd->cluster->user) {
			as_uv_auth_write_start(cmd, req->handle);
		}
		else {
			as_uv_command_write_start(cmd, req->handle);
		}
	}
	else if (status != UV_ECANCELED) {
		as_node* node = cmd->node;
		as_address* primary = as_vector_get(&node->addresses, node->address_index);
		
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Failed to connect: %s %s:%d",
						node->name, primary->name, (int)cf_swap_from_be16(primary->addr.sin_port));
		as_uv_connect_error(cmd, &err);
	}
}

static void
as_uv_connect(as_event_command* cmd)
{
	ck_pr_inc_32(&cmd->cluster->async_conn);
	ck_pr_inc_32(&cmd->node->async_conn);

	int fd = as_event_create_socket(cmd);
	
	if (fd < 0) {
		ck_pr_dec_32(&cmd->cluster->async_conn);
		ck_pr_dec_32(&cmd->node->async_conn);
		return;
	}
	
	as_event_connection* conn = cmd->conn;
	uv_tcp_t* socket = &conn->socket;
	int status = uv_tcp_init(cmd->event_loop->loop, socket);
	
	if (status) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "uv_tcp_init failed: %s", uv_strerror(status));
		// Call standard event connection error handler because as_uv_connect_error() requires that
		// uv_tcp_init() has already succeeded.
		as_event_connect_error(cmd, &err, fd);
		ck_pr_dec_32(&cmd->cluster->async_conn);
		ck_pr_dec_32(&cmd->node->async_conn);
		return;
	}
	
	// Define externally created fd to uv_tcp_t.
	status = uv_tcp_open(socket, fd);
	
	if (status) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "uv_tcp_open failed: %s", uv_strerror(status));
		// Close fd directly because we created it outside of libuv and uv_tcp_t does not know about it here.
		close(fd);
		as_uv_connect_error(cmd, &err);
		return;
	}
	
	socket->data = conn;
	conn->req.connect.data = cmd;
	
	as_node* node = cmd->node;
	as_address* primary = as_vector_get(&node->addresses, node->address_index);
	
	status = uv_tcp_connect(&conn->req.connect, socket, (struct sockaddr*)&primary->addr, as_uv_connected);
	
	if (status) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "uv_tcp_connect failed: %s", uv_strerror(status));
		as_uv_connect_error(cmd, &err);
	}
}

static void
as_uv_timeout(uv_timer_t* timer)
{
	// One-off timers are automatically stopped by libuv.
	as_event_timeout(timer->data);
}

void
as_event_command_begin(as_event_command* cmd)
{
	// Always initialize timer first when timeouts are specified.
	if (cmd->timeout_ms) {
		uv_timer_init(cmd->event_loop->loop, &cmd->timer);
		cmd->timer.data = cmd;
		uv_timer_start(&cmd->timer, as_uv_timeout, cmd->timeout_ms, 0);
	}
	
	bool found = cmd->pipeline ? as_pipe_get_connection(cmd) : as_event_get_connection(cmd);
	
	if (found) {
		as_uv_command_write_start(cmd, (uv_stream_t*)&cmd->conn->socket);
	}
	else {
		as_uv_connect(cmd);
	}
}

void
as_event_close_connection(as_event_connection* conn, as_node* node)
{
	uv_close((uv_handle_t*)&conn->socket, as_uv_connection_closed);
}

static bool
as_uv_queue_close_connections(as_node* node, as_queue* conn_queue, as_queue* cmd_queue)
{
	as_uv_command qcmd;
	qcmd.type = AS_UV_CLOSE_CONNECTION;
	
	as_event_connection* conn;
	
	// Queue connection commands to event loops.
	while (as_queue_pop(conn_queue, &conn)) {
		qcmd.ptr = conn;
		
		ck_pr_dec_32(&node->cluster->async_conn_pool);

		if (! as_queue_push(cmd_queue, &qcmd)) {
			as_log_error("Failed to queue connection close");
			return false;
		}
	}
	return true;
}

void
as_event_node_destroy(as_node* node)
{
	// Send close connection commands to event loops.
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_loop* event_loop = &as_event_loops[i];
		
		pthread_mutex_lock(&event_loop->lock);
		as_uv_queue_close_connections(node, &node->async_conn_qs[i], &event_loop->queue);
		as_uv_queue_close_connections(node, &node->pipe_conn_qs[i], &event_loop->queue);
		pthread_mutex_unlock(&event_loop->lock);
		
		uv_async_send(event_loop->wakeup);
	}
		
	// Destroy all queues.
	for (uint32_t i = 0; i < as_event_loop_capacity; i++) {
		as_queue_destroy(&node->async_conn_qs[i]);
		as_queue_destroy(&node->pipe_conn_qs[i]);
	}
	cf_free(node->async_conn_qs);
	cf_free(node->pipe_conn_qs);
}

bool
as_event_close_loop(as_event_loop* event_loop)
{
	// Send stop command through queue so it can be executed in event loop thread.
	pthread_mutex_lock(&event_loop->lock);
	as_uv_command qcmd = {.type = AS_UV_EXIT_LOOP, .ptr = 0};
	bool queued = as_queue_push(&event_loop->queue, &qcmd);
	pthread_mutex_unlock(&event_loop->lock);
	
	if (queued) {
		uv_async_send(event_loop->wakeup);
	}
	return queued;
}

#endif
