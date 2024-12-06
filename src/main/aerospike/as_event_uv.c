/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_monitor.h>
#include <aerospike/as_pipe.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_thread.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>
#include <errno.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

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

static void as_uv_tls_command_write_start(as_event_command* cmd);

typedef void (*as_uv_tls_fn) (as_event_command* cmd);

typedef struct as_uv_tls {
	as_tls_context* ctx;
	struct ssl_st* ssl;
	struct bio_st* ibio;  // internal bio.
	struct bio_st* nbio;  // network bio.
	as_uv_tls_fn callback;
	char* buf;
	int capacity;
	int len;
	int error;
} as_uv_tls;

typedef struct {
	as_event_loop* event_loop;
	as_monitor monitor;
} as_uv_thread_data;

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
	// socket->data has as_event_command ptr but that may have already been freed,
	// so free as_event_connection ptr by socket which is first field in as_event_connection.
	as_event_connection* conn = (as_event_connection*)socket;

	if (conn->tls) {
		as_uv_tls* tls = conn->tls;
		SSL_free(tls->ssl);
		BIO_free(tls->nbio);
		cf_free(tls->buf);
		cf_free(tls);
	}
	cf_free(conn);
}

void
as_event_close_loop(as_event_loop* event_loop)
{
	uv_close((uv_handle_t*)event_loop->wakeup, as_uv_wakeup_closed);
	
	// Only stop event loop if client created event loop.
	if (as_event_threads_created) {
		uv_stop(event_loop->loop);
	}
	
	// Cleanup event loop resources.
	as_event_loop_destroy(event_loop);
}

static void
as_uv_wakeup(uv_async_t* wakeup)
{
	// Read command pointers from queue.
	as_event_loop* event_loop = wakeup->data;
	as_event_commander cmd;
	uint32_t i = 0;

	// Only process original size of queue.  Recursive pre-registration errors can
	// result in new commands being added while the loop is in process.  If we process
	// them, we could end up in an infinite loop.
	pthread_mutex_lock(&event_loop->lock);
	uint32_t size = as_queue_size(&event_loop->queue);
	bool status = as_queue_pop(&event_loop->queue, &cmd);
	pthread_mutex_unlock(&event_loop->lock);

	while (status) {
		if (! cmd.executable) {
			// Received stop signal.
			as_event_close_loop(event_loop);
			return;
		}
		cmd.executable(event_loop, cmd.udata);

		if (++i < size) {
			pthread_mutex_lock(&event_loop->lock);
			status = as_queue_pop(&event_loop->queue, &cmd);
			pthread_mutex_unlock(&event_loop->lock);
		}
		else {
			break;
		}
	}
}

static void
as_uv_close_walk(uv_handle_t* handle, void* arg)
{
	if (! uv_is_closing(handle)) {
		// as_log_debug("Close handle %p %d", handle, handle->type);
		if (handle->type == UV_TCP) {
			// Give callback for known connection handles.
			uv_close(handle, as_uv_connection_closed);
		}
		else {
			// Received unexpected handle.
			// Close handle, but do not provide callback that might free unallocated data.
			uv_close(handle, NULL);
		}
	}
}

static void*
as_uv_worker(void* udata)
{
	as_uv_thread_data* data = udata;
	as_event_loop* event_loop = data->event_loop;

	as_thread_set_name_index("uv", event_loop->index);
	
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
	as_monitor_notify(&data->monitor);
	
	uv_run(event_loop->loop, UV_RUN_DEFAULT);
	
	uv_walk(event_loop->loop, as_uv_close_walk, NULL);
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
	
	as_uv_thread_data thread_data;
	thread_data.event_loop = event_loop;
	as_monitor_init(&thread_data.monitor);
	
	if (pthread_create(&event_loop->thread, NULL, as_uv_worker, &thread_data) != 0) {
		return false;
	}
	
	// Must wait until uv_async_init() is called in event loop thread.
	as_monitor_wait(&thread_data.monitor);
	as_monitor_destroy(&thread_data.monitor);
	return true;
}

void
as_event_register_external_loop(as_event_loop* event_loop)
{
	// This method is only called when user sets an external event loop.
	event_loop->wakeup = cf_malloc(sizeof(uv_async_t));
	event_loop->wakeup->data = event_loop;

	// Assume uv_async_init is called on the same thread as the event loop.
	uv_async_init(event_loop->loop, event_loop->wakeup, as_uv_wakeup);
}

bool
as_event_execute(as_event_loop* event_loop, as_event_executable executable, void* udata)
{
	// Send command through queue so it can be executed in event loop thread.
	pthread_mutex_lock(&event_loop->lock);
	as_event_commander qcmd = {.executable = executable, .udata = udata};
	bool queued = as_queue_push(&event_loop->queue, &qcmd);
	pthread_mutex_unlock(&event_loop->lock);

	if (queued) {
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

// With libuv, as_event_stop_watcher() is a no-op. So, after cancel_connection()
// freed all commands, we might still get read or write callbacks. This function
// tests, whether we're dealing with a canceled pipelined connection.
static inline bool
as_uv_connection_alive(uv_handle_t* handle)
{
	if (uv_is_closing(handle)) {
		return false;
	}

	if (!((as_event_connection*)handle)->pipeline) {
		return true;
	}

	return !((as_pipe_connection*)handle)->canceled;
}

static void
as_uv_command_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
	if (as_uv_connection_alive(handle)) {
		as_event_command* cmd = as_uv_get_command(handle->data);
		*buf = uv_buf_init((char*)cmd->buf + cmd->pos, cmd->len - cmd->pos);
	}
	else {
		*buf = uv_buf_init(NULL, 0);
	}
}

static void
as_uv_command_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf)
{
	if (!as_uv_connection_alive((uv_handle_t*)stream)) {
		return;
	}

	as_event_command* cmd = as_uv_get_command(stream->data);
			
	if (nread < 0) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket read failed: %zd", nread);
			as_event_socket_error(cmd, &err);
		}
		return;
	}

	cmd->flags |= AS_ASYNC_FLAGS_EVENT_RECEIVED;
	cmd->pos += (uint32_t)nread;
	
	if (cmd->pos < cmd->len) {
		// Read not finished.
		return;
	}

	if (cmd->state == AS_ASYNC_STATE_COMMAND_READ_HEADER) {
		as_proto* proto = (as_proto*)cmd->buf;

		if (! as_event_proto_parse(cmd, proto)) {
			return;
		}

		size_t size = proto->sz;
		
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_COMMAND_READ_BODY;
		
		if (cmd->len < sizeof(as_msg)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Invalid record header size: %u", cmd->len);
			as_event_parse_error(cmd, &err);
			return;
		}
		
		if (cmd->len > cmd->read_capacity) {
			if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->read_capacity = cmd->len;
			cmd->flags |= AS_ASYNC_FLAGS_FREE_BUF;
		}
		return;
	}
	cmd->pos = 0;

	if (cmd->proto_type_rcv == AS_COMPRESSED_MESSAGE_TYPE) {
		if (! as_event_decompress(cmd)) {
			return;
		}
	}

	if (! cmd->parse_results(cmd)) {
		// Batch, scan, query is not finished.
		cmd->len = sizeof(as_proto);
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;
	}
}

static void
as_uv_command_write_complete(uv_write_t* req, int status)
{
	if (!as_uv_connection_alive((uv_handle_t*)req->handle)) {
		return;
	}

	as_event_command* cmd = req->data;
	
	if (status == 0) {
		cmd->command_sent_counter++;
		cmd->len = sizeof(as_proto);
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;

		if (cmd->pipe_listener != NULL) {
			as_pipe_read_start(cmd);
			as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;

			// There already was an active reader for a previous command.
			if (cf_ll_size(&conn->readers) > 1) {
				return;
			}
		}
		
		status = uv_read_start(req->handle, as_uv_command_buffer, as_uv_command_read);
		
		if (status) {
			if (! as_event_socket_retry(cmd)) {
				as_error err;
				as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
								"uv_read_start failed: %s", uv_strerror(status));
				as_event_socket_error(cmd, &err);
			}
		}
	}
	else if (status != UV_ECANCELED) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"Socket write failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
	}
}

static void
as_uv_command_write_start(as_event_command* cmd, uv_stream_t* stream)
{
	as_event_set_write(cmd);
	cmd->state = AS_ASYNC_STATE_COMMAND_WRITE;
	cmd->flags &= ~AS_ASYNC_FLAGS_EVENT_RECEIVED;

	uv_write_t* write_req = &cmd->conn->req.write;
	write_req->data = cmd;
	uv_buf_t buf = uv_buf_init((char*)cmd + cmd->write_offset, cmd->len);

	int status = uv_write(write_req, stream, &buf, 1, as_uv_command_write_complete);

	if (status) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"uv_write failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
	}
}

static inline void
as_uv_command_start(as_event_command* cmd, uv_stream_t* stream)
{
	as_event_connection_complete(cmd);
	
	if (cmd->type == AS_ASYNC_TYPE_CONNECTOR) {
		as_event_connector_success(cmd);
		return;
	}

	as_uv_command_write_start(cmd, stream);
}

void
as_event_command_write_start(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;

	if (!conn->tls) {
		as_uv_command_write_start(cmd, (uv_stream_t*)conn);
	}
	else {
		as_uv_tls_command_write_start(cmd);
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
	if (uv_is_closing((uv_handle_t*)stream)) {
		return;
	}

	as_event_command* cmd = as_uv_auth_get_command(stream->data);
		
	if (nread < 0) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"Authenticate socket read failed: %zd", nread);
			as_event_socket_error(cmd, &err);
		}
		return;
	}
	
	cmd->pos += (uint32_t)nread;
	
	if (cmd->pos < cmd->len) {
		// Read not finished.
		return;
	}
	
	if (cmd->state == AS_ASYNC_STATE_AUTH_READ_HEADER) {
		if (! as_event_set_auth_parse_header(cmd)) {
			return;
		}
		
		if (cmd->len > cmd->read_capacity) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
							"Authenticate response size is corrupt: %u", cmd->len);
			as_event_parse_error(cmd, &err);
			return;
		}
		return;
	}
	
	// Done reading authentication data.
	uv_read_stop(stream);
	
	// Parse authentication response.
	uint8_t code = cmd->buf[AS_ASYNC_AUTH_RETURN_CODE];
	
	if (code && code != AEROSPIKE_SECURITY_NOT_ENABLED) {
		// Can't authenticate socket, so must close it.
		as_node_signal_login(cmd->node);
		as_error err;
		as_error_update(&err, code, "Authentication failed: %s", as_error_string(code));
		as_event_parse_error(cmd, &err);
		return;
	}

	as_uv_command_start(cmd, stream);
}

static void
as_uv_auth_command_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
	if (as_uv_connection_alive(handle)) {
		as_event_command* cmd = as_uv_auth_get_command(handle->data);
		*buf = uv_buf_init((char*)cmd->buf + cmd->pos, cmd->len - cmd->pos);
	}
	else {
		*buf = uv_buf_init(NULL, 0);
	}
}

static void
as_uv_auth_write_complete(uv_write_t* req, int status)
{
	if (uv_is_closing((uv_handle_t*)req->handle)) {
		return;
	}

	as_event_command* cmd = req->data;
	
	if (status == 0) {
		as_event_set_auth_read_header(cmd);
		status = uv_read_start(req->handle, as_uv_auth_command_buffer, as_uv_auth_read);
		
		if (status) {
			if (! as_event_socket_retry(cmd)) {
				as_error err;
				as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
								"Authenticate uv_read_start failed: %s", uv_strerror(status));
				as_event_socket_error(cmd, &err);
			}
		}
	}
	else if (status != UV_ECANCELED) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"Authenticate socket write failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
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
	as_event_decr_conn(cmd);
	cmd->event_loop->errors++;

	if (! as_event_command_retry(cmd, true)) {
		as_event_timer_stop(cmd);
		as_event_error_callback(cmd, err);
	}
}

/******************************************************************************
 * BEGIN LIBUV TLS FUNCTIONS
 *****************************************************************************/

static void
as_uv_tls_write(as_event_command* cmd);

static void
as_uv_tls_read(as_event_command* cmd);

static void
as_uv_tls_handshake_send_complete(uv_write_t* req, int status);

static int
as_uv_tls_try_send_pending(as_event_connection* conn)
{
	// Try quick writes from the stack.
	as_uv_tls* tls = conn->tls;
	int pending = BIO_pending(tls->nbio);

	if (pending <= 0) {
		return 0;
	}

	int max = 16 * 1024;
	uv_buf_t buf;
	buf.len = (pending <= max) ? pending : max;
	buf.base = alloca(buf.len);

	while (pending > 0) {
		int rv = BIO_read(tls->nbio, buf.base, (int)buf.len);

		if (rv != buf.len) {
			return -2;
		}

		rv = uv_try_write((uv_stream_t*)conn, &buf, 1);

		if (rv == buf.len) {
			// Quick write succeeded.  Try another block.
			pending -= rv;
			buf.len = (pending <= max) ? pending : max;
			continue;
		}

		// Quick write failed.
		if (rv < 0) {
			if (rv == UV_EAGAIN) {
				rv = 0;
			}
			else {
				return -3;
			}
		}

		// Put remaining buffer on heap.
		as_uv_tls* tls = conn->tls;
		tls->len = pending - rv;
		
		if (tls->len > tls->capacity) {
			tls->buf = cf_realloc(tls->buf, tls->len);
			tls->capacity = tls->len;
		}
		
		int unsent_len = buf.len - rv;
		memcpy(tls->buf, buf.base + rv, unsent_len);
		
		int read_len = pending - buf.len;
		rv = BIO_read(tls->nbio, tls->buf + unsent_len, read_len);
		
		if (rv != read_len) {
			return -2;
		}
		return 1;
	}
	return 0;
}

static void
as_uv_tls_send_pending_slow(as_event_connection* conn, uv_write_cb cb)
{
	// Try slow write with callback.
	uv_write_t* write_req = &conn->req.write;
	write_req->data = conn;

	as_uv_tls* tls = conn->tls;
	uv_buf_t buf;
	buf.base = tls->buf;
	buf.len = tls->len;

	int rv = uv_write(write_req, (uv_stream_t*)conn, &buf, 1, cb);

	if (rv) {
		cb(write_req, rv);
	}
}

static void
as_uv_tls_send_pending(as_event_connection* conn, uv_write_cb cb)
{
	int rv = as_uv_tls_try_send_pending(conn);

	if (rv <= 0) {
		uv_write_t* write_req = &conn->req.write;
		write_req->data = conn;
		write_req->handle = (uv_stream_t*)conn;
		cb(write_req, rv);
		return;
	}

	as_uv_tls_send_pending_slow(conn, cb);
}

static bool
as_uv_tls_fill_buffer(as_event_command* cmd, ssize_t nread)
{
	as_event_connection* conn = cmd->conn;

	if (nread < 0) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket read failed: %zd", nread);
			as_event_socket_error(cmd, &err);
		}
		return false;
	}

	as_uv_tls* tls = conn->tls;
	int len = (int)nread;
	int rv = 0;

	for (int pos = 0; pos < len; pos += rv) {
		rv = BIO_write(tls->nbio, tls->buf + pos, len - pos);

		if (rv <= 0) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "BIO_write failed: %zd %d",
							nread, rv);
			as_event_parse_error(cmd, &err);
			return false;
		}
	}
	return true;
}

static void
as_uv_tls_command_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf)
{
	if (!as_uv_connection_alive((uv_handle_t*)stream)) {
		return;
	}

	as_event_command* cmd = as_uv_get_command(stream->data);
	cmd->flags |= AS_ASYNC_FLAGS_EVENT_RECEIVED;

	if (as_uv_tls_fill_buffer(cmd, nread)) {
		as_uv_tls_read(cmd);
	}
}

static void
as_uv_tls_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
	if (as_uv_connection_alive(handle)) {
		as_uv_tls* tls = ((as_event_connection*)handle->data)->tls;
		tls->len = 0;
		*buf = uv_buf_init(tls->buf, tls->capacity);
	}
	else {
		*buf = uv_buf_init(NULL, 0);
	}
}

static void
as_uv_tls_handle_error(as_event_command* cmd, int rv, int e)
{
	unsigned long errcode = ERR_get_error();
	char errbuf[1024];

	if (errcode != 0) {
		ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
	}
	else {
		errbuf[0] = 0;
	}

	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "TLS failed: %d %d %d %s",
					rv, e, errcode, errbuf);
	as_event_parse_error(cmd, &err);
}

static void
as_uv_tls_write_read_complete(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf)
{
	if (!as_uv_connection_alive((uv_handle_t*)stream)) {
		return;
	}

	as_event_command* cmd = as_uv_get_command(stream->data);
	cmd->flags |= AS_ASYNC_FLAGS_EVENT_RECEIVED;

	if (as_uv_tls_fill_buffer(cmd, nread)) {
		uv_read_stop(stream);
		as_uv_tls_write(cmd); // Recursive.
	}
}

static void
as_uv_tls_write_pending_complete(uv_write_t* req, int status)
{
	if (!as_uv_connection_alive((uv_handle_t*)req->handle)) {
		return;
	}

	as_event_connection* conn = req->data;
	as_uv_tls* tls = conn->tls;
	as_event_command* cmd = as_uv_get_command(conn);

	if (status == 0) {
		if (cmd->pos < cmd->len) {
			if (tls->error == SSL_ERROR_WANT_READ) {
				// Start reading.
				tls->error = 0;

				int rv = uv_read_start((uv_stream_t*)conn, as_uv_tls_buffer,
								   as_uv_tls_write_read_complete);

				if (rv) {
					if (! as_event_socket_retry(cmd)) {
						as_error err;
						as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
										"uv_read_start failed: %s", uv_strerror(rv));
						as_event_socket_error(cmd, &err);
					}
				}
			}
			else {
				// Resume write. Recursive.
				as_uv_tls_write(cmd);
			}
		}
		else {
			// Write complete
			tls->callback(cmd);
		}
		return;
	}
	else if (status != UV_ECANCELED) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "TLS write failed: %d %s %s",
							status, cmd->node->name, as_node_get_address_string(cmd->node));
			as_event_socket_error(cmd, &err);
		}
	}
}

static void
as_uv_tls_write(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	uint8_t* buf = (uint8_t*)cmd + cmd->write_offset;

	as_uv_tls* tls = conn->tls;
	tls->error = 0;

	while (cmd->pos < cmd->len) {
		int rv = SSL_write(tls->ssl, buf + cmd->pos, cmd->len - cmd->pos);

		if (rv <= 0) {
			int e = SSL_get_error(tls->ssl, rv);

			if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
				tls->error = e;
				as_uv_tls_send_pending(conn, as_uv_tls_write_pending_complete);
			}
			else {
				as_uv_tls_handle_error(cmd, rv, e);
			}
			return;
		}

		cmd->pos += (uint32_t)rv;
		rv = as_uv_tls_try_send_pending(conn);

		if (rv == 0) {
			continue;
		}

		if (rv > 0) {
			as_uv_tls_send_pending_slow(conn, as_uv_tls_write_pending_complete);
			return;
		}

		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"TLS socket write failed: %d %s %s",
							rv, cmd->node->name, as_node_get_address_string(cmd->node));
			as_event_socket_error(cmd, &err);
		}
		return;
	}

	// Write complete
	tls->callback(cmd);
}

static void
as_uv_tls_command_write_complete(as_event_command* cmd)
{
	cmd->command_sent_counter++;
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;

	if (cmd->pipe_listener != NULL) {
		as_pipe_read_start(cmd);
		as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;

		// There already was an active reader for a previous command.
		if (cf_ll_size(&conn->readers) > 1) {
			return;
		}
	}

	int status = uv_read_start((uv_stream_t*)cmd->conn, as_uv_tls_buffer,
							   as_uv_tls_command_read);

	if (status) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"uv_read_start failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
	}
}

static void
as_uv_tls_command_write_start(as_event_command* cmd)
{
	as_event_set_write(cmd);
	cmd->state = AS_ASYNC_STATE_COMMAND_WRITE;
	cmd->flags &= ~AS_ASYNC_FLAGS_EVENT_RECEIVED;
	cmd->conn->tls->callback = as_uv_tls_command_write_complete;
	as_uv_tls_write(cmd);
}

static inline void
as_uv_tls_command_start(as_event_command* cmd)
{
	as_event_connection_complete(cmd);

	if (cmd->type == AS_ASYNC_TYPE_CONNECTOR) {
		as_event_connector_success(cmd);
		return;
	}

	as_uv_tls_command_write_start(cmd);
}

static void
as_uv_tls_read_want_write_complete(uv_write_t* req, int status)
{
	if (!as_uv_connection_alive((uv_handle_t*)req->handle)) {
		return;
	}

	as_event_command* cmd = as_uv_get_command(req->data);

	if (status == 0) {
		// Resume reading.
		as_uv_tls_read(cmd);
	}
	else if (status != UV_ECANCELED) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "TLS write failed: %d %s %s",
							status, cmd->node->name, as_node_get_address_string(cmd->node));
			as_event_socket_error(cmd, &err);
		}
	}
}

static void
as_uv_tls_read(as_event_command* cmd)
{
	as_event_connection* conn = cmd->conn;
	as_uv_tls* tls = conn->tls;

	while (true) {
		int rv = SSL_read(tls->ssl, (char*)cmd->buf + cmd->pos, cmd->len - cmd->pos);

		if (rv <= 0) {
			int e = SSL_get_error(tls->ssl, rv);

			if (e == SSL_ERROR_WANT_READ) {
				return;
			}

			if (e == SSL_ERROR_WANT_WRITE) {
				as_uv_tls_send_pending(conn, as_uv_tls_read_want_write_complete);
				return;
			}

			as_uv_tls_handle_error(cmd, rv, e);
			return;
		}

		cmd->pos += (uint32_t)rv;

		if (cmd->pos < cmd->len) {
			// Read not finished.
			continue;
		}

		switch (cmd->state) {
			case AS_ASYNC_STATE_AUTH_READ_HEADER:
				// Done reading authentication header.
				if (! as_event_set_auth_parse_header(cmd)) {
					return;
				}

				if (cmd->len > cmd->read_capacity) {
					as_error err;
					as_error_update(&err, AEROSPIKE_ERR_CLIENT,
									"Authenticate response size is corrupt: %u", cmd->len);
					as_event_parse_error(cmd, &err);
					return;
				}
				break;

			case AS_ASYNC_STATE_AUTH_READ_BODY: {
				// Parse authentication response.
				uint8_t code = cmd->buf[AS_ASYNC_AUTH_RETURN_CODE];

				if (code && code != AEROSPIKE_SECURITY_NOT_ENABLED) {
					// Can't authenticate socket, so must close it.
					as_node_signal_login(cmd->node);
					as_error err;
					as_error_update(&err, code, "Authentication failed: %s", as_error_string(code));
					as_event_parse_error(cmd, &err);
					return;
				}
				// Done reading authentication data.
				uv_read_stop((uv_stream_t*)&conn->socket);
				as_uv_tls_command_start(cmd);
				return;
			}

			case AS_ASYNC_STATE_COMMAND_READ_HEADER: {
				// Done reading command header.
				as_proto* proto = (as_proto*)cmd->buf;

				if (! as_event_proto_parse(cmd, proto)) {
					return;
				}

				size_t size = proto->sz;

				cmd->len = (uint32_t)size;
				cmd->pos = 0;
				cmd->state = AS_ASYNC_STATE_COMMAND_READ_BODY;

				if (cmd->len < sizeof(as_msg)) {
					as_error err;
					as_error_update(&err, AEROSPIKE_ERR_CLIENT,
									"Invalid record header size: %u", cmd->len);
					as_event_parse_error(cmd, &err);
					return;
				}

				if (cmd->len > cmd->read_capacity) {
					if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
						cf_free(cmd->buf);
					}
					cmd->buf = cf_malloc(size);
					cmd->read_capacity = cmd->len;
					cmd->flags |= AS_ASYNC_FLAGS_FREE_BUF;
				}
				break;
			}

			case AS_ASYNC_STATE_COMMAND_READ_BODY: {
				// Done reading command block.
				cmd->pos = 0;

				if (cmd->proto_type_rcv == AS_COMPRESSED_MESSAGE_TYPE) {
					if (! as_event_decompress(cmd)) {
						return;
					}
				}

				if (cmd->parse_results(cmd)) {
					// Done with command.
					return;
				}

				// Batch, scan, query is not finished.
				cmd->len = sizeof(as_proto);
				cmd->pos = 0;
				cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;
				break;
			}
		}
	}
}

static void
as_uv_tls_auth_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf)
{
	if (uv_is_closing((uv_handle_t*)stream)) {
		return;
	}

	as_event_command* cmd = as_uv_auth_get_command(stream->data);

	if (as_uv_tls_fill_buffer(cmd, nread)) {
		as_uv_tls_read(cmd);
	}
}

static void
as_uv_tls_auth_write_complete(as_event_command* cmd)
{
	as_event_set_auth_read_header(cmd);
	int status = uv_read_start((uv_stream_t*)cmd->conn, as_uv_tls_buffer, as_uv_tls_auth_read);

	if (status) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"uv_read_start failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
	}
}

static void
as_uv_tls_handshake_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf)
{
	if (uv_is_closing((uv_handle_t*)stream)) {
		return;
	}

	as_event_connection* conn = stream->data;
	as_event_command* cmd = as_uv_auth_get_command(conn);

	if (! as_uv_tls_fill_buffer(cmd, nread)) {
		return;
	}

	as_uv_tls* tls = conn->tls;

	int rv = SSL_do_handshake(tls->ssl);

	if (rv == 1) {
		// Handshake complete.
		uv_read_stop(stream);

		if (cmd->cluster->auth_enabled) {
			as_session* session = as_session_load(&cmd->node->session);

			if (session) {
				as_incr_uint32(&session->ref_count);
				as_event_set_auth_write(cmd, session);
				as_session_release(session);

				cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
				cmd->conn->tls->callback = as_uv_tls_auth_write_complete;
				as_uv_tls_write(cmd);
			}
			else {
				as_uv_tls_command_start(cmd);
			}
		}
		else {
			as_uv_tls_command_start(cmd);
		}
		return;
	}

	int e = SSL_get_error(tls->ssl, rv);

	if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
		// Per OpenSSL docs, flush pending data even if OpenSSL wants read.
		as_uv_tls_send_pending(conn, as_uv_tls_handshake_send_complete);
		return;
	}

	as_uv_tls_handle_error(cmd, rv, e);
}

static void
as_uv_tls_handshake_send_complete(uv_write_t* req, int status)
{
	if (uv_is_closing((uv_handle_t*)req->handle)) {
		return;
	}

	as_event_command* cmd = as_uv_auth_get_command(req->data);

	if (status == 0) {
		if (cmd->state == AS_ASYNC_STATE_CONNECT) {
			// Initiate read once.
			cmd->state = AS_ASYNC_STATE_TLS_CONNECT;

			status = uv_read_start(req->handle, as_uv_tls_buffer,
								   as_uv_tls_handshake_read);

			if (status) {
				as_error err;
				as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
								"uv_read_start failed: %s", uv_strerror(status));
				as_uv_connect_error(cmd, &err);
			}
		}
	}
	else if (status != UV_ECANCELED) {
		if (cmd->state == AS_ASYNC_STATE_TLS_CONNECT) {
			uv_read_stop(req->handle);
		}

		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
						"uv_read_start failed: %s", uv_strerror(status));
		as_uv_connect_error(cmd, &err);
	}
}

static void
as_uv_tls_connect_fatal_error(as_event_command* cmd, as_error* err)
{
	// libuv requires uv_close if socket released after uv_tcp_init succeeds.
	// The socket is the first field in as_event_connection, so just use connection.
	// The close callback will also free as_event_connection memory.
	uv_close((uv_handle_t*)cmd->conn, as_uv_connection_closed);
	as_event_decr_conn(cmd);
	cmd->event_loop->errors++;
	as_event_timer_stop(cmd);
	as_event_error_callback(cmd, err);
}

static void
as_uv_tls_init_connection(as_event_command* cmd, uv_stream_t* stream, as_tls_context* ctx)
{
	as_uv_tls* tls = cf_malloc(sizeof(as_uv_tls));
	tls->ctx = ctx;
	tls->ssl = NULL;
	tls->ibio = NULL;
	tls->nbio = NULL;
	tls->callback = NULL;
	tls->capacity = 8 * 1024;
	tls->buf = cf_malloc(tls->capacity);
	tls->len = 0;
	cmd->conn->tls = tls;

	pthread_mutex_lock(&ctx->lock);
	tls->ssl = SSL_new(ctx->ssl_ctx);
	pthread_mutex_unlock(&ctx->lock);

	if (! tls->ssl) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "SSL_new failed: %s %s",
						cmd->node->name, as_node_get_address_string(cmd->node));
		as_uv_tls_connect_fatal_error(cmd, &err);
		return;
	}

	as_tls_set_context_name(tls->ssl, ctx, cmd->node->tls_name);

	int rv = BIO_new_bio_pair(&tls->ibio, 0, &tls->nbio, 0);

	if (rv != 1) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "BIO_new_bio_pair failed: %d %s %s",
						rv, cmd->node->name, as_node_get_address_string(cmd->node));
		as_uv_tls_connect_fatal_error(cmd, &err);
		return;
	}

	SSL_set_bio(tls->ssl, tls->ibio, tls->ibio);
	SSL_set_connect_state(tls->ssl);

	// Handshake always fails the first time.
	SSL_do_handshake(tls->ssl);

	// Send bytes created by handshake.
	as_uv_tls_send_pending(cmd->conn, as_uv_tls_handshake_send_complete);
}

static void
as_uv_tls_shutdown_complete(uv_write_t* req, int status)
{
	if (uv_is_closing((uv_handle_t*)req->handle)) {
		return;
	}

	// Close socket regardless of shutdown status.
	uv_close((uv_handle_t*)req->handle, as_uv_connection_closed);
}

/******************************************************************************
 * END LIBUV TLS FUNCTIONS
 *****************************************************************************/

void
as_event_close_connection(as_event_connection* conn)
{
	if (conn->tls) {
		SSL_shutdown(conn->tls->ssl);
		as_uv_tls_send_pending(conn, as_uv_tls_shutdown_complete);
		return;
	}
	uv_close((uv_handle_t*)&conn->socket, as_uv_connection_closed);
}

static void
as_uv_auth_write_start(as_event_command* cmd, uv_stream_t* stream, as_session* session)
{
	as_event_set_auth_write(cmd, session);
	cmd->state = AS_ASYNC_STATE_AUTH_WRITE;

	uv_write_t* write_req = &cmd->conn->req.write;
	write_req->data = cmd;
	// Authentication buffer is located after the write buffer.
	uv_buf_t buf = uv_buf_init((char*)cmd + cmd->write_offset + cmd->write_len, cmd->len - cmd->pos);

	int status = uv_write(write_req, stream, &buf, 1, as_uv_auth_write_complete);
	
	if (status) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							"Authenticate uv_write failed: %s", uv_strerror(status));
			as_event_socket_error(cmd, &err);
		}
	}
}

static void
as_uv_fd_error(as_event_command* cmd, as_error* err)
{
	cmd->event_loop->errors++;

	// Only timer needs to be released on socket connection failure.
	// Watcher has not been registered yet.
	as_event_timer_stop(cmd);

	// Socket has already been closed.
	cf_free(cmd->conn);
	as_event_decr_conn(cmd);
	as_event_error_callback(cmd, err);
}

static void
as_uv_connected(uv_connect_t* req, int status)
{
	if (uv_is_closing((uv_handle_t*)req->handle)) {
		return;
	}

	as_event_command* cmd = req->data;

	if (status == 0) {
		cmd->event_loop->errors = 0; // Reset errors on valid connection.

		as_async_conn_pool* pool = cmd->pipe_listener != NULL ?
			&cmd->node->pipe_conn_pools[cmd->event_loop->index] :
			&cmd->node->async_conn_pools[cmd->event_loop->index];
		pool->opened++;

		as_tls_context* ctx = as_socket_get_tls_context(cmd->cluster->tls_ctx);

		if (!ctx) {
			if (cmd->cluster->auth_enabled) {
				as_session* session = as_session_load(&cmd->node->session);

				if (session) {
					as_incr_uint32(&session->ref_count);
					as_uv_auth_write_start(cmd, req->handle, session);
					as_session_release(session);
				}
				else {
					as_uv_command_start(cmd, req->handle);
				}
			}
			else {
				as_uv_command_start(cmd, req->handle);
			}
		}
		else {
			as_uv_tls_init_connection(cmd, req->handle, ctx);
		}
	}
	else if (status != UV_ECANCELED) {
		as_node* node = cmd->node;
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Failed to connect: %s %s",
						node->name, as_node_get_address_string(node));
		as_uv_connect_error(cmd, &err);
	}
}

void
as_event_connect(as_event_command* cmd, as_async_conn_pool* pool)
{
	// Create a non-blocking socket.
	as_address* address = as_node_get_address(cmd->node);
	as_socket_fd fd;
	int rv = as_socket_create_fd(address->addr.ss_family, &fd);

	if (rv) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
						"Socket create failed: %d %s %s", rv, cmd->node->name, address->name);
		as_uv_fd_error(cmd, &err);
		return;
	}

	if (cmd->pipe_listener && ! as_pipe_modify_fd(fd)) {
		// as_pipe_modify_fd() will close fd on error.
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
							 "Failed to modify fd for pipeline");
		as_uv_fd_error(cmd, &err);
		return;
	}

	as_event_connection* conn = cmd->conn;
	uv_tcp_t* socket = &conn->socket;
	int status = uv_tcp_init(cmd->event_loop->loop, socket);
	
	if (status) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
						"uv_tcp_init failed: %s", uv_strerror(status));
		as_close(fd);
		as_uv_fd_error(cmd, &err);
		return;
	}

	// Indicate that watcher has been initialized.
	conn->watching = 1;
	conn->tls = NULL;

	// Define externally created fd to uv_tcp_t.
	status = uv_tcp_open(socket, fd);
	
	if (status) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
						"uv_tcp_open failed: %s", uv_strerror(status));
		// Close fd directly because we created it outside of libuv
		// and uv_tcp_t does not know about it here.
		as_close(fd);
		as_uv_connect_error(cmd, &err);
		return;
	}
	
	socket->data = conn;
	conn->req.connect.data = cmd;
		
	status = uv_tcp_connect(&conn->req.connect, socket, (struct sockaddr*)&address->addr,
							as_uv_connected);
	
	if (status) {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION,
						"uv_tcp_connect failed: %s", uv_strerror(status));
		as_uv_connect_error(cmd, &err);
		return;
	}
}

void
as_uv_timer_cb(uv_timer_t* timer)
{
	as_event_process_timer(timer->data);
}

void
as_uv_repeat_cb(uv_timer_t* timer)
{
	as_event_socket_timeout(timer->data);
}

static void
as_event_close_connection_cb(as_event_loop* event_loop, as_event_connection* conn)
{
	as_event_close_connection(conn);
}

static bool
as_uv_queue_close_connections(as_node* node, as_async_conn_pool* pool, as_queue* cmd_queue)
{
	as_event_commander qcmd;
	qcmd.executable = (as_event_executable)as_event_close_connection_cb;
	
	as_event_connection* conn;
	
	// Queue connection commands to event loops.
	while (as_queue_pop(&pool->queue, &conn)) {
		qcmd.udata = conn;
		
		if (! as_queue_push(cmd_queue, &qcmd)) {
			as_log_error("Failed to queue connection close");
			return false;
		}
		
		// In this case, connection counts are decremented before the connection is closed.
		// This is done because the node will be invalid when the deferred connection close occurs.
		// Since node destroy always waits till there are no node references, all commands that
		// referenced this node should be completed by the time this code is executed.
		as_queue_decr_total(&pool->queue);
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
		as_uv_queue_close_connections(node, &node->async_conn_pools[i], &event_loop->queue);
		as_uv_queue_close_connections(node, &node->pipe_conn_pools[i], &event_loop->queue);
		pthread_mutex_unlock(&event_loop->lock);
		
		uv_async_send(event_loop->wakeup);
	}
		
	// Destroy all queues.
	for (uint32_t i = 0; i < as_event_loop_capacity; i++) {
		as_queue_destroy(&node->async_conn_pools[i].queue);
		as_queue_destroy(&node->pipe_conn_pools[i].queue);
	}
	cf_free(node->async_conn_pools);
	cf_free(node->pipe_conn_pools);
}

#endif
