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
#include <aerospike/as_admin.h>
#include <aerospike/as_async.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_pipe.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_status.h>
#include <aerospike/as_thread.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_byte_order.h>

#if defined(AS_USE_LIBEVENT)

#include <event.h>
#include <event2/thread.h>

/******************************************************************************
 * GLOBALS
 *****************************************************************************/

extern int as_event_send_buffer_size;
extern int as_event_recv_buffer_size;
extern bool as_event_threads_created;

static struct timeval as_immediate_tv;

static void
as_event_callback(evutil_socket_t sock, short revents, void* udata);

/******************************************************************************
 * LIBEVENT FUNCTIONS
 *****************************************************************************/

void
as_event_close_loop(as_event_loop* event_loop)
{
	event_del(&event_loop->wakeup);

	if (event_loop->clusters.capacity > 0) {
		event_del(&event_loop->trim);
		as_vector_destroy(&event_loop->clusters);
	}

	// Only stop event loop if client created event loop.
	if (as_event_threads_created) {
		event_base_loopbreak(event_loop->loop);
	}
	
	// Cleanup event loop resources.
	as_event_loop_destroy(event_loop);
}

static void
as_event_wakeup(evutil_socket_t socket, short revents, void* udata)
{
	// Read command pointers from queue.
	as_event_loop* event_loop = udata;
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

static void*
as_event_worker(void* udata)
{
#if defined(_MSC_VER)
	// event_base_dispatch() requires that WSAStartup() be called 
	// in this thread on windows.
	WORD version = MAKEWORD(2, 2);
	WSADATA data;
	if (WSAStartup(version, &data) != 0) {
		as_log_error("WSAStartup() failed");
		return NULL;
	}
#endif

	as_event_loop* event_loop = udata;

	as_thread_set_name_index("event", event_loop->index);

	struct event_base* loop = event_loop->loop;

#if LIBEVENT_VERSION_NUMBER < 0x02010000
	int status = event_base_dispatch(loop);
#else
	int status = event_base_loop(loop, EVLOOP_NO_EXIT_ON_EMPTY);
#endif

	if (status) {
		as_log_error("event_base_dispatch failed: %d", status);
	}
	event_base_free(loop);
	as_tls_thread_cleanup();

#if defined(_MSC_VER)
	// Call WSACleanup() when event loop thread ends.
	WSACleanup();
#endif
	return NULL;
}

static inline void
as_event_init_loop(as_event_loop* event_loop)
{
	memset(&event_loop->clusters, 0, sizeof(as_vector));

	if (evthread_make_base_notifiable(event_loop->loop) == -1) {
        as_log_error("evthread_make_base_notifiable failed");
        return;
    }

	evtimer_assign(&event_loop->wakeup, event_loop->loop, as_event_wakeup, event_loop);
	/*
	event_assign(&event_loop->wakeup, event_loop->loop, -1, EV_PERSIST | EV_READ, as_event_wakeup, event_loop);

	if (event_add(&event_loop->wakeup, NULL) == -1) {
        as_log_error("as_event_init_loop: event_add failed");
		return;
	}
	*/
}

#if LIBEVENT_VERSION_NUMBER < 0x02010000
void event_base_add_virtual(struct event_base*);
#endif

bool
as_event_create_loop(as_event_loop* event_loop)
{
#if !defined(_MSC_VER)
	event_loop->loop = event_base_new();
#else
	struct event_config* config = event_config_new();
	event_config_set_flag(config, EVENT_BASE_FLAG_STARTUP_IOCP);
	event_loop->loop = event_base_new_with_config(config);
	event_config_free(config);
#endif

	if (! event_loop->loop) {
		return false;
	}

	// Add a virtual event to prevent event_base_dispatch() from returning prematurely.
#if LIBEVENT_VERSION_NUMBER < 0x02010000
	event_base_add_virtual(event_loop->loop);
#endif

	as_event_init_loop(event_loop);

	return pthread_create(&event_loop->thread, NULL, as_event_worker, event_loop) == 0;
}

void
as_event_register_external_loop(as_event_loop* event_loop)
{
	// This method is only called when user sets an external event loop.
	as_event_init_loop(event_loop);
}

bool
as_event_execute(as_event_loop* event_loop, as_event_executable executable, void* udata)
{
	// Cross thread command queueing is not allowed in libevent single thread mode.
	if (as_event_single_thread) {
		as_log_error("Cross thread command queueing not allowed in single thread mode");
		return false;
	}

	// Send command through queue so it can be executed in event loop thread.
	pthread_mutex_lock(&event_loop->lock);
	as_event_commander qcmd = {.executable = executable, .udata = udata};
	bool queued = as_queue_push(&event_loop->queue, &qcmd);
	pthread_mutex_unlock(&event_loop->lock);

	if (queued) {
		if (! evtimer_pending(&event_loop->wakeup, NULL)) {
			event_del(&event_loop->wakeup);
			evtimer_add(&event_loop->wakeup, &as_immediate_tv);
		}
		//event_active(&event_loop->wakeup, 0, 0);
	}
	return queued;
}

static inline void
as_event_watch(as_event_command* cmd, int watch)
{
	as_event_connection* conn = cmd->conn;

	// Skip if we're already watching the right stuff.
	if (watch == conn->watching) {
		return;
	}
	conn->watching = watch;

	event_del(&conn->watcher);
	event_assign(&conn->watcher, cmd->event_loop->loop, conn->socket.fd, watch | EV_PERSIST, as_event_callback, conn);

	if (event_add(&conn->watcher, NULL) == -1) {
		as_log_error("as_event_watch: event_add failed");
	}
}

static inline void
as_event_watch_write(as_event_command* cmd)
{
	int watch = cmd->pipe_listener != NULL ? EV_WRITE | EV_READ : EV_WRITE;
	return as_event_watch(cmd, watch);
}

static inline void
as_event_watch_read(as_event_command* cmd)
{
	return as_event_watch(cmd, EV_READ);
}

#define AS_EVENT_WRITE_COMPLETE 0
#define AS_EVENT_WRITE_INCOMPLETE 1
#define AS_EVENT_WRITE_ERROR 2

#define AS_EVENT_READ_COMPLETE 3
#define AS_EVENT_READ_INCOMPLETE 4
#define AS_EVENT_READ_ERROR 5

#define AS_EVENT_TLS_NEED_READ 6
#define AS_EVENT_TLS_NEED_WRITE 7

#define AS_EVENT_COMMAND_DONE 8

static int
as_event_write(as_event_command* cmd)
{
	uint8_t* buf = (uint8_t*)cmd + cmd->write_offset;

	if (cmd->conn->socket.ctx) {
		do {
			int rv = as_tls_write_once(&cmd->conn->socket, buf + cmd->pos, cmd->len - cmd->pos);
			if (rv > 0) {
				as_event_watch_write(cmd);
				cmd->pos += rv;
				continue;
			}
			else if (rv == -1) {
				// TLS sometimes need to read even when we are writing.
				as_event_watch_read(cmd);
				return AS_EVENT_TLS_NEED_READ;
			}
			else if (rv == -2) {
				// TLS wants a write, we're all set for that.
				as_event_watch_write(cmd);
				return AS_EVENT_WRITE_INCOMPLETE;
			}
			else if (rv < -2) {
				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(cmd->conn->socket.fd, cmd->node, &err, AEROSPIKE_ERR_TLS_ERROR, "TLS write failed", rv);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_WRITE_ERROR;
			}
			// as_tls_write_once can't return 0
		} while (cmd->pos < cmd->len);
	}
	else {
		as_socket_fd fd = cmd->conn->socket.fd;
	
		do {
#if defined(__linux__)
			int bytes = (int)send(fd, buf + cmd->pos, cmd->len - cmd->pos, MSG_NOSIGNAL);
#elif defined(_MSC_VER)
			int bytes = send(fd, buf + cmd->pos, cmd->len - cmd->pos, 0);
#else
			int bytes = (int)write(fd, buf + cmd->pos, cmd->len - cmd->pos);
#endif
			if (bytes > 0) {
				cmd->pos += bytes;
				continue;
			}
		
			if (bytes < 0) {
				int e = as_last_error();

				if (e == AS_WOULDBLOCK) {
					as_event_watch_write(cmd);
					return AS_EVENT_WRITE_INCOMPLETE;
				}
			
				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket write failed", e);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_WRITE_ERROR;
			}
			else {
				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket write closed by peer", 0);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_WRITE_ERROR;
			}
		} while (cmd->pos < cmd->len);
	}
	
	// Socket timeout applies only to read events.
	// Reset event received because we are switching from a write to a read state.
	// This handles case where write succeeds and read event does not occur.  If we didn't reset,
	// the socket timeout would go through two iterations (double the timeout) because a write
	// event occurred in the first timeout period.
	cmd->flags &= ~AS_ASYNC_FLAGS_EVENT_RECEIVED;
	return AS_EVENT_WRITE_COMPLETE;
}

static int
as_event_read(as_event_command* cmd)
{
	cmd->flags |= AS_ASYNC_FLAGS_EVENT_RECEIVED;

	if (cmd->conn->socket.ctx) {
		do {
			int rv = as_tls_read_once(&cmd->conn->socket, cmd->buf + cmd->pos, cmd->len - cmd->pos);
			if (rv > 0) {
				as_event_watch_read(cmd);
				cmd->pos += rv;
				continue;
			}
			else if (rv == -1) {
				// TLS wants a read
				as_event_watch_read(cmd);
				return AS_EVENT_READ_INCOMPLETE;
			}
			else if (rv == -2) {
				// TLS sometimes needs to write, even when the app is reading.
				as_event_watch_write(cmd);
				return AS_EVENT_TLS_NEED_WRITE;
			}
			else if (rv < -2) {
				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(cmd->conn->socket.fd, cmd->node, &err, AEROSPIKE_ERR_TLS_ERROR, "TLS read failed", rv);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_READ_ERROR;
			}
			// as_tls_read_once doesn't return 0
		} while (cmd->pos < cmd->len);
	}
	else {
		as_socket_fd fd = cmd->conn->socket.fd;
	
		do {
#if !defined(_MSC_VER)
			int bytes = (int)read(fd, cmd->buf + cmd->pos, cmd->len - cmd->pos);
#else
			int bytes = (int)recv(fd, cmd->buf + cmd->pos, cmd->len - cmd->pos, 0);
#endif

			if (bytes > 0) {
				cmd->pos += bytes;
				continue;
			}
		
			if (bytes < 0) {
				int e = as_last_error();

				if (e == AS_WOULDBLOCK) {
					as_event_watch_read(cmd);
					return AS_EVENT_READ_INCOMPLETE;
				}

				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket read failed", e);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_READ_ERROR;
			}
			else {
				if (! as_event_socket_retry(cmd)) {
					as_error err;
					as_socket_error(fd, cmd->node, &err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Socket read closed by peer", 0);
					as_event_socket_error(cmd, &err);
				}
				return AS_EVENT_READ_ERROR;
			}
		} while (cmd->pos < cmd->len);
	}
	
	return AS_EVENT_READ_COMPLETE;
}

static inline void
as_event_command_read_start(as_event_command* cmd)
{
	cmd->command_sent_counter++;
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;

	as_event_watch_read(cmd);
	
	if (cmd->pipe_listener != NULL) {
		as_pipe_read_start(cmd);
	}
}

static inline void
as_event_command_write(as_event_command* cmd)
{
	as_event_watch_write(cmd);

	if (as_event_write(cmd) == AS_EVENT_WRITE_COMPLETE) {
		// Done with write. Register for read.
		as_event_command_read_start(cmd);
	}
}

void
as_event_command_write_start(as_event_command* cmd)
{
	cmd->state = AS_ASYNC_STATE_COMMAND_WRITE;
	as_event_set_write(cmd);
	as_event_command_write(cmd);
}

static int
as_event_command_start(as_event_command* cmd)
{
	as_event_connection_complete(cmd);
	
	if (cmd->type == AS_ASYNC_TYPE_CONNECTOR) {
		as_event_connector_success(cmd);
		return AS_EVENT_COMMAND_DONE;
	}
	else {
		as_event_command_write_start(cmd);
		return AS_EVENT_READ_COMPLETE;
	}
}

static inline void
as_event_command_auth_write(as_event_command* cmd)
{
	as_event_watch_write(cmd);
		
	if (as_event_write(cmd) == AS_EVENT_WRITE_COMPLETE) {
		// Done with auth write. Register for auth read.
		as_event_set_auth_read_header(cmd);
		as_event_watch_read(cmd);
	}
}

static inline void
as_event_connect_complete(as_event_command* cmd)
{
	if (cmd->cluster->auth_enabled) {
		as_session* session = as_session_load(&cmd->node->session);

		if (session) {
			as_incr_uint32(&session->ref_count);
			as_event_set_auth_write(cmd, session);
			as_session_release(session);

			cmd->state = AS_ASYNC_STATE_AUTH_WRITE;
			as_event_command_auth_write(cmd);
		}
		else {
			as_event_command_start(cmd);
		}
	}
	else {
		as_event_command_start(cmd);
	}
}

static int
as_event_command_peek_block(as_event_command* cmd)
{
	// Batch, scan, query may be waiting on end block.
	// Prepare for next message block.
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;

	int rv = as_event_read(cmd);
	if (rv != AS_EVENT_READ_COMPLETE) {
		return rv;
	}
	
	as_proto* proto = (as_proto*)cmd->buf;

	if (! as_event_proto_parse(cmd, proto)) {
		return AS_EVENT_READ_ERROR;
	}

	size_t size = proto->sz;
	
	cmd->len = (uint32_t)size;
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_COMMAND_READ_BODY;
	
	// Check for end block size.
	if (cmd->len == sizeof(as_msg) && cmd->proto_type_rcv != AS_COMPRESSED_MESSAGE_TYPE) {
		// Look like we received end block.  Read and parse to make sure.
		rv = as_event_read(cmd);
		if (rv != AS_EVENT_READ_COMPLETE) {
			return rv;
		}
		cmd->pos = 0;

		if (! cmd->parse_results(cmd)) {
			// We did not finish after all. Prepare to read next header.
			cmd->len = sizeof(as_proto);
			cmd->pos = 0;
			cmd->state = AS_ASYNC_STATE_COMMAND_READ_HEADER;
		}
		else {
			return AS_EVENT_COMMAND_DONE;
		}
	}
	else {
		// Received normal data block.  Stop reading for fairness reasons and wait
		// till next iteration.
		if (cmd->len > cmd->read_capacity) {
			if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->read_capacity = cmd->len;
			cmd->flags |= AS_ASYNC_FLAGS_FREE_BUF;
		}
	}

	return AS_EVENT_READ_COMPLETE;
}

static int
as_event_parse_authentication(as_event_command* cmd)
{
	int rv;
	if (cmd->state == AS_ASYNC_STATE_AUTH_READ_HEADER) {
		// Read response length
		rv = as_event_read(cmd);
		if (rv != AS_EVENT_READ_COMPLETE) {
			return rv;
		}

		if (! as_event_set_auth_parse_header(cmd)) {
			return AS_EVENT_READ_ERROR;
		}

		if (cmd->len > cmd->read_capacity) {
			as_error err;
			as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Authenticate response size is corrupt: %u", cmd->len);
			as_event_parse_error(cmd, &err);
			return AS_EVENT_READ_ERROR;
		}
	}

	rv = as_event_read(cmd);
	if (rv != AS_EVENT_READ_COMPLETE) {
		return rv;
	}
	
	// Parse authentication response.
	uint8_t code = cmd->buf[AS_ASYNC_AUTH_RETURN_CODE];
	
	if (code && code != AEROSPIKE_SECURITY_NOT_ENABLED) {
		// Can't authenticate socket, so must close it.
		as_node_signal_login(cmd->node);
		as_error err;
		as_error_update(&err, code, "Authentication failed: %s", as_error_string(code));
		as_event_parse_error(cmd, &err);
		return AS_EVENT_READ_ERROR;
	}
	
	return as_event_command_start(cmd);
}

static int
as_event_command_read(as_event_command* cmd)
{
	int rv;
	
	if (cmd->state == AS_ASYNC_STATE_COMMAND_READ_HEADER) {
		// Read response length
		rv = as_event_read(cmd);
		if (rv != AS_EVENT_READ_COMPLETE) {
			return rv;
		}
		
		as_proto* proto = (as_proto*)cmd->buf;

		if (! as_event_proto_parse(cmd, proto)) {
			return AS_EVENT_READ_ERROR;
		}

		size_t size = proto->sz;
		
		cmd->len = (uint32_t)size;
		cmd->pos = 0;
		cmd->state = AS_ASYNC_STATE_COMMAND_READ_BODY;
		
		if (cmd->len > cmd->read_capacity) {
			if (cmd->flags & AS_ASYNC_FLAGS_FREE_BUF) {
				cf_free(cmd->buf);
			}
			cmd->buf = cf_malloc(size);
			cmd->read_capacity = cmd->len;
			cmd->flags |= AS_ASYNC_FLAGS_FREE_BUF;
		}
	}
	
	// Read response body
	rv = as_event_read(cmd);
	if (rv != AS_EVENT_READ_COMPLETE) {
		return rv;
	}
	cmd->pos = 0;

	if (cmd->proto_type_rcv == AS_COMPRESSED_MESSAGE_TYPE) {
		if (! as_event_decompress(cmd)) {
			return AS_EVENT_READ_ERROR;
		}
	}

	if (! cmd->parse_results(cmd)) {
		// Batch, scan, query is not finished.
		return as_event_command_peek_block(cmd);
	}

	return AS_EVENT_COMMAND_DONE;		
}

bool
as_event_tls_connect(as_event_command* cmd, as_event_connection* conn)
{
	int rv = as_tls_connect_once(&conn->socket);

	if (rv < -2) {
		if (! as_event_socket_retry(cmd)) {
			// Failed, error has been logged.
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_TLS_ERROR, "TLS connection failed");
			as_event_socket_error(cmd, &err);
		}
		return false;
	}

	if (rv == -1) {
		// TLS needs a read.
		as_event_watch_read(cmd);
		return true;
	}

	if (rv == -2) {
		// TLS needs a write.
		as_event_watch_write(cmd);
		return true;
	}

	if (rv == 0) {
		if (! as_event_socket_retry(cmd)) {
			as_error err;
			as_error_set_message(&err, AEROSPIKE_ERR_TLS_ERROR, "TLS connection shutdown");
			as_event_socket_error(cmd, &err);
		}
		return false;
	}

	// TLS connection established.
	as_event_connect_complete(cmd);
	return false;
}

static void
as_event_callback_common(as_event_command* cmd, as_event_connection* conn) {
	switch (cmd->state) {
	case AS_ASYNC_STATE_CONNECT:
		as_event_connect_complete(cmd);
		break;

	case AS_ASYNC_STATE_TLS_CONNECT:
		do {
			if (! as_event_tls_connect(cmd, conn)) {
				return;
			}
		} while (as_tls_read_pending(&cmd->conn->socket) > 0);
		break;

	case AS_ASYNC_STATE_AUTH_WRITE:
		as_event_command_auth_write(cmd);
		break;

	case AS_ASYNC_STATE_AUTH_READ_HEADER:
	case AS_ASYNC_STATE_AUTH_READ_BODY:
		// If we're using TLS we must loop until there are no bytes
		// left in the encryption buffer because we won't get another
		// read event.
		do {
			switch (as_event_parse_authentication(cmd)) {
				case AS_EVENT_COMMAND_DONE:
				case AS_EVENT_READ_ERROR:
					// Do not touch cmd again because it's been deallocated.
					return;

				case AS_EVENT_READ_COMPLETE:
					as_event_watch_read(cmd);
					break;

				default:
					break;
			}
		} while (as_tls_read_pending(&cmd->conn->socket) > 0);
		break;

	case AS_ASYNC_STATE_COMMAND_WRITE:
		as_event_command_write(cmd);
		break;

	case AS_ASYNC_STATE_COMMAND_READ_HEADER:
	case AS_ASYNC_STATE_COMMAND_READ_BODY:
		// If we're using TLS we must loop until there are no bytes
		// left in the encryption buffer because we won't get another
		// read event.
		do {
			switch (as_event_command_read(cmd)) {
			case AS_EVENT_COMMAND_DONE:
			case AS_EVENT_READ_ERROR:
				// Do not touch cmd again because it's been deallocated.
				return;
			
			case AS_EVENT_READ_COMPLETE:
				as_event_watch_read(cmd);
				break;
				
			default:
				break;
			}
		} while (as_tls_read_pending(&cmd->conn->socket) > 0);
		break;

	default:
		as_log_error("unexpected cmd state %d", cmd->state);
		break;
	}
}

static void
as_event_callback(evutil_socket_t sock, short revents, void* udata)
{
	if (revents & EV_READ) {
		as_event_connection* conn = udata;
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

		as_event_callback_common(cmd, conn);
	}
	else if (revents & EV_WRITE) {
		as_event_connection* conn = udata;
		
		as_event_command* cmd = conn->pipeline ?
			((as_pipe_connection*)conn)->writer :
			((as_async_connection*)conn)->cmd;

		as_event_callback_common(cmd, conn);
	}
	else {
		as_log_warn("Unknown event received: %d", revents);
	}
}

static void
as_event_watcher_init(as_event_command* cmd, as_socket* sock)
{
	as_event_connection* conn = cmd->conn;
	memcpy(&conn->socket, sock, sizeof(as_socket));

	// Change state if using TLS.
	if (as_socket_use_tls(cmd->cluster->tls_ctx)) {
		cmd->state = AS_ASYNC_STATE_TLS_CONNECT;
	}

	int watch = cmd->pipe_listener != NULL ? EV_WRITE | EV_READ : EV_WRITE;
	conn->watching = watch;

	event_assign(&conn->watcher, cmd->event_loop->loop, conn->socket.fd, watch | EV_PERSIST, as_event_callback, conn);

	if (event_add(&conn->watcher, NULL) == -1) {
        as_log_error("as_event_watcher_init: event_add failed");
	}
}

static int
as_event_try_connections(as_socket_fd fd, as_address* addresses, socklen_t size, int i, int max)
{
	while (i < max) {
		if (as_socket_connect_fd(fd, (struct sockaddr*)&addresses[i].addr, size)) {
			return i;
		}
		i++;
	}
	return -1;
}

static int
as_event_try_family_connections(as_event_command* cmd, int family, int begin, int end, int index, as_address* primary, as_socket* sock)
{
	// Create a non-blocking socket.
	as_socket_fd fd;
	int rv = as_socket_create_fd(family, &fd);

	if (rv < 0) {
		return rv;
	}

	if (cmd->pipe_listener && ! as_pipe_modify_fd(fd)) {
		return -1000;
	}

	as_tls_context* ctx = as_socket_get_tls_context(cmd->cluster->tls_ctx);

	if (! as_socket_wrap(sock, family, fd, ctx, cmd->node->tls_name)) {
		return -1001;
	}

	// Try addresses.
	as_address* addresses = cmd->node->addresses;
	socklen_t size = (family == AF_INET)? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);

	if (index >= 0) {
		// Try primary address.
		if (as_socket_connect_fd(fd, (struct sockaddr*)&primary->addr, size)) {
			return index;
		}
		
		// Start from current index + 1 to end.
		rv = as_event_try_connections(fd, addresses, size, index + 1, end);
		
		if (rv < 0) {
			// Start from begin to index.
			rv = as_event_try_connections(fd, addresses, size, begin, index);
		}
	}
	else {
		rv = as_event_try_connections(fd, addresses, size, begin, end);
	}
	
	if (rv < 0) {
		// Couldn't start a connection on any socket address - close the socket.
		as_socket_close(sock);
		return -1002;
	}
	return rv;
}

static void
as_event_connect_error(as_event_command* cmd, as_address* primary, int rv)
{
	// Socket has already been closed. Release connection.
	cf_free(cmd->conn);
	as_event_decr_conn(cmd);
	cmd->event_loop->errors++;

	if (as_event_command_retry(cmd, false)) {
		return;
	}

	as_error err;
	as_error_update(&err, AEROSPIKE_ERR_ASYNC_CONNECTION, "Connect failed: %d %s %s", rv, cmd->node->name, primary->name);

	// Only timer needs to be released on socket connection failure.
	// Watcher has not been registered yet.
	as_event_timer_stop(cmd);
	as_event_error_callback(cmd, &err);
}

void
as_event_connect(as_event_command* cmd, as_async_conn_pool* pool)
{
	// Try addresses.
	as_socket sock;
	as_node* node = cmd->node;
	uint32_t index = node->address_index;
	as_address* primary = &node->addresses[index];
	int rv;
	int first_rv;

	if (primary->addr.ss_family == AF_INET) {
		// Try IPv4 addresses first.
		rv = as_event_try_family_connections(cmd, AF_INET, 0, node->address4_size, index, primary, &sock);
		
		if (rv < 0) {
			// Try IPv6 addresses.
			first_rv = rv;
			rv = as_event_try_family_connections(cmd, AF_INET6, AS_ADDRESS4_MAX, AS_ADDRESS4_MAX + node->address6_size, -1, NULL, &sock);
		}
	}
	else {
		// Try IPv6 addresses first.
		rv = as_event_try_family_connections(cmd, AF_INET6, AS_ADDRESS4_MAX, AS_ADDRESS4_MAX + node->address6_size, index, primary, &sock);
		
		if (rv < 0) {
			// Try IPv4 addresses.
			first_rv = rv;
			rv = as_event_try_family_connections(cmd, AF_INET, 0, node->address4_size, -1, NULL, &sock);
		}
	}
	
	if (rv < 0) {
		as_event_connect_error(cmd, primary, first_rv);
		return;
	}
	
	if (rv != index) {
		// Replace invalid primary address with valid alias.
		// Other threads may not see this change immediately.
		// It's just a hint, not a requirement to try this new address first.
		as_store_uint32(&node->address_index, rv);
		as_log_debug("Change node address %s %s", node->name, as_node_get_address_string(node));
	}

	pool->opened++;
	as_event_watcher_init(cmd, &sock);
	cmd->event_loop->errors = 0; // Reset errors on valid connection.
}

void
as_libevent_timer_cb(evutil_socket_t sock, short events, void* udata)
{
	as_event_process_timer(udata);
}

void
as_libevent_repeat_cb(evutil_socket_t sock, short events, void* udata)
{
	as_event_socket_timeout(udata);
}

static void
as_event_close_connections(as_node* node, as_async_conn_pool* pool)
{
	as_event_connection* conn;
	
	while (as_queue_pop(&pool->queue, &conn)) {
		as_event_release_connection(conn, pool);
	}
	as_queue_destroy(&pool->queue);
}

void
as_event_node_destroy(as_node* node)
{
	// Close connections.
	for (uint32_t i = 0; i < as_event_loop_size; i++) {
		as_event_close_connections(node, &node->async_conn_pools[i]);
		as_event_close_connections(node, &node->pipe_conn_pools[i]);
	}
	cf_free(node->async_conn_pools);
	cf_free(node->pipe_conn_pools);
}

/******************************************************************************
 * AEROSPIKE REGISTER/CLOSE FUNCTIONS
 *****************************************************************************/

void as_event_balance_connections_cluster(as_event_loop* event_loop, as_cluster* cluster);

static int
as_event_find_cluster(as_vector* clusters, as_cluster* cluster)
{
	for (uint32_t i = 0; i < clusters->size; i++) {
		as_cluster* c = as_vector_get_ptr(clusters, i);

		if (c == cluster) {
			return (int)i;
		}
	}
	return -1;
}

static void
as_libevent_balance_conn(evutil_socket_t sock, short events, void* udata)
{
	as_event_loop* event_loop = udata;
	as_vector* clusters = &event_loop->clusters;

	for (uint32_t i = 0; i < clusters->size; i++) {
		as_cluster* cluster = as_vector_get_ptr(clusters, i);
		as_event_balance_connections_cluster(event_loop, cluster);
	}
}

static void
as_libevent_append_cluster(as_event_loop* event_loop, as_vector* clusters, as_cluster* cluster)
{
	as_vector_append(clusters, &cluster);

	if (cluster->async_min_conns_per_node > 0) {
		as_event_balance_connections_cluster(event_loop, cluster);
	}
}

void
as_event_loop_register_aerospike(as_event_loop* event_loop, aerospike* as)
{
	as_vector* clusters = &event_loop->clusters;
	as_cluster* cluster = as->cluster;

	if (clusters->capacity == 0) {
		// Create cluster vector.
		as_vector_init(clusters, sizeof(as_cluster*), 4);
		as_libevent_append_cluster(event_loop, clusters, cluster);

		// Create trim connections timer to run every 30 seconds.
		event_assign(&event_loop->trim, event_loop->loop, -1, EV_PERSIST, as_libevent_balance_conn,
			event_loop);
		struct timeval tv = {30,0};
		event_add(&event_loop->trim, &tv);
	}
	else {
		int index = as_event_find_cluster(clusters, cluster);

		if (index < 0) {
			as_libevent_append_cluster(event_loop, clusters, cluster);
		}
	}
}

typedef struct {
	struct event timer;
	as_event_loop* event_loop;
	aerospike* as;
	as_event_close_listener listener;
	void* udata;
} as_close_state;

static void
as_event_loop_close_aerospike_cb(evutil_socket_t sock, short events, void* udata)
{
	as_close_state* state = udata;
	as_event_state* event_state = &state->as->cluster->event_state[state->event_loop->index];

	if (event_state->closed) {
		state->listener(state->udata);
		cf_free(state);
		return;
	}

	if (event_state->pending <= 0) {
		event_state->closed = true;
		state->listener(state->udata);
		cf_free(state);
		return;
	}

	// Cluster has pending commands.
	// Check again in 1 second.
	evtimer_assign(&state->timer, state->event_loop->loop, as_event_loop_close_aerospike_cb, state);
	struct timeval tv = {1,0};
	evtimer_add(&state->timer, &tv);
}

void
as_event_loop_close_aerospike(
	as_event_loop* event_loop, aerospike* as, as_event_close_listener listener, void* udata
	)
{
	// Remove cluster from registered clusters.
	as_vector* clusters = &event_loop->clusters;
	int index = as_event_find_cluster(clusters, as->cluster);

	if (index >= 0) {
		as_vector_remove(clusters, index);
	}

	as_event_state* event_state = &as->cluster->event_state[event_loop->index];

	if (event_state->closed) {
		listener(udata);
		return;
	}

	if (event_state->pending <= 0) {
		event_state->closed = true;
		listener(udata);
		return;
	}

	// Cluster has pending commands.
	as_close_state* state = cf_malloc(sizeof(as_close_state));
	state->event_loop = event_loop;
	state->as = as;
	state->listener = listener;
	state->udata = udata;

	evtimer_assign(&state->timer, event_loop->loop, as_event_loop_close_aerospike_cb, state);
	// If only one pending command, this function was probably called from last listener
	// callback which has not decremented pending yet. In this case, set timer
	// to next event loop iteration.  Otherwise, wait 1 second before checking again.
	struct timeval tv = {(event_state->pending == 1)? 0 : 1, 0};
	evtimer_add(&state->timer, &tv);
}

#endif
