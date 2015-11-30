/*
 * Copyright 2015 Aerospike, Inc.
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

#include <aerospike/as_pipe.h>

#define PIPE_WRITE_BUFFER_SIZE (5 * 1024 * 1024)
#define PIPE_READ_BUFFER_SIZE (15 * 1024 * 1024)

static as_async_command*
link_to_command(cf_ll_element* link)
{
	return (as_async_command*)((uint8_t*)link - offsetof(as_async_command, pipe_link));
}

static void
next_reader(as_async_command* reader)
{
	as_pipe_connection* conn = reader->pipe_conn;
	as_log_trace("Selecting next reader for command %p, pipeline connection %p, FD %d", reader, conn, conn->fd);
	assert(cf_ll_get_head(&conn->readers) == &reader->pipe_link);

	cf_ll_delete(&conn->readers, &reader->pipe_link);
	as_async_unregister(reader);

	if (cf_ll_size(&conn->readers) == 0) {
		as_log_trace("No reader left");

		if (conn->in_pool) {
			as_log_trace("Pipeline connection still in pool");
			return;
		}

		as_log_trace("Closing non-pooled pipeline connection %p, FD %d", conn, conn->fd);
		close(conn->fd);
		ck_pr_dec_32(&reader->node->async_conn);
		cf_free(conn);
		return;
	}

	as_log_trace("Pipeline connection %p has %d reader(s)", conn, cf_ll_size(&conn->readers));
	cf_ll_element* link = cf_ll_get_head(&conn->readers);
	as_async_command* next = link_to_command(link);
	as_log_trace("Next reader after %p is %p", reader, next);
	as_event_register_read(&next->event);
}

#define CANCEL_COMMAND_TIMER 1
#define CANCEL_COMMAND_EVENT 2

static void
cancel_command(as_async_command* cmd, as_error* err, uint32_t what)
{
	as_log_trace("Canceling command %p, error code %d, mask 0x%x", cmd, err->code, what);

	if ((what & CANCEL_COMMAND_EVENT) != 0 && cmd->state != AS_ASYNC_STATE_UNREGISTERED) {
		as_log_trace("Unregistering event");
		as_event_unregister(&cmd->event);
	}

	if ((what & CANCEL_COMMAND_TIMER) != 0 && cmd->event.timeout_ms != 0) {
		as_log_trace("Canceling timeout");
		as_event_stop_timer(&cmd->event);
	}

	as_log_trace("Invoking callback function");
	as_async_error_callback(cmd, err);
}

#define CANCEL_CONNECTION_SOCKET 1
#define CANCEL_CONNECTION_RESPONSE 2
#define CANCEL_CONNECTION_TIMEOUT 3

static void
cancel_connection(as_async_command* cmd, as_error* err, int32_t source)
{
	as_pipe_connection* conn = cmd->pipe_conn;
	as_log_trace("Canceling pipeline connection for command %p, error code %d, connection %p, FD %d", cmd, err->code, conn, conn->fd);

	if (source != CANCEL_CONNECTION_TIMEOUT) {
		assert(cmd == conn->writer || cf_ll_get_head(&conn->readers) == &cmd->pipe_link);
	}

	as_event_close(&cmd->event);
	ck_pr_dec_32(&cmd->node->async_conn);

	// Don't count connection as being in the pool, if we're going to cancel it.
	if (conn->in_pool) {
		ck_pr_dec_32(&cmd->node->async_conn_pool);
	}

	uint32_t what = CANCEL_COMMAND_EVENT | CANCEL_COMMAND_TIMER;

	if (conn->writer != NULL) {
		as_log_trace("Canceling writer %p", conn->writer);
		cancel_command(conn->writer, err, what);
	}

	bool is_reader = false;

	while (cf_ll_size(&conn->readers) > 0) {
		cf_ll_element* link = cf_ll_get_head(&conn->readers);
		as_async_command* walker = link_to_command(link);

		if (cmd == walker) {
			is_reader = true;
		}

		as_log_trace("Canceling reader %p", walker);
		cf_ll_delete(&conn->readers, link);
		cancel_command(walker, err, what);

		// Only the first reader has an event registered.
		what &= ~CANCEL_COMMAND_EVENT;
	}

	if (source == CANCEL_CONNECTION_TIMEOUT) {
		assert(cmd == conn->writer || is_reader);
	}

	if (! conn->in_pool) {
		cf_free(conn);
		return;
	}

	conn->writer = NULL;
	conn->fd = -1;
	conn->canceled = true;
}

static void
release_connection(as_pipe_connection* conn, as_node* node)
{
	as_log_trace("Releasing pipeline connection %p, FD %d", conn, conn->fd);

	if (conn->writer != NULL || cf_ll_size(&conn->readers) > 0) {
		as_log_trace("Pipeline connection %p is still draining", conn);
		return;
	}

	as_log_trace("Closing pipeline connection %p, FD %d", conn, conn->fd);
	close(conn->fd);
	ck_pr_dec_32(&node->async_conn);
	cf_free(conn);
}

static void
put_connection(as_async_command* cmd)
{
	as_pipe_connection* conn = cmd->pipe_conn;
	as_log_trace("Returning pipeline connection for command %p, pipeline connection %p, FD %d", cmd, conn, conn->fd);
	as_queue* q = &cmd->node->pipe_conn_qs[cmd->event.event_loop->index];

	if (as_queue_push_limit(q, &conn)) {
		ck_pr_inc_32(&cmd->node->async_conn_pool);
		conn->in_pool = true;
		return;
	}

	release_connection(conn, cmd->node);
}

static bool
read_file(const char* path, char* buffer, size_t size)
{
	bool res = false;
	int fd = open(path, O_RDONLY);

	if (fd < 0) {
		as_log_warn("Failed to open %s for reading", path);
		goto cleanup0;
	}

	size_t len = 0;

	while (len < size - 1) {
		ssize_t n = read(fd, buffer + len, size - len - 1);

		if (n < 0) {
			as_log_warn("Failed to read from %s", path);
			goto cleanup1;
		}

		if (n == 0) {
			buffer[len] = 0;
			res = true;
			goto cleanup1;
		}

		len += n;
	}

	as_log_warn("%s is too large", path);

cleanup1:
	close(fd);

cleanup0:
	return res;
}

static bool
read_integer(const char* path, int* value)
{
	char buffer[21];

	if (! read_file(path, buffer, sizeof buffer)) {
		return false;
	}

	char *end;
	uint64_t x = strtoul(buffer, &end, 10);

	if (*end != '\n' || x > INT_MAX) {
		as_log_warn("Invalid integer value in %s", path);
		return false;
	}

	*value = (int)x;
	return true;
}

static bool
set_buffer(int fd, int option, int size)
{
	const char* proc = option == SO_RCVBUF ?
			"/proc/sys/net/core/rmem_max" :
			"/proc/sys/net/core/wmem_max";
	int max;

	if (! read_integer(proc, &max)) {
		as_log_error("Failed to read %s", proc);
		return false;
	}

	if (max < size) {
		as_log_warn("Buffer limit is %d, should be at least %d; please set %s accordingly",
				max, size, proc);
		return false;
	}

	if (setsockopt(fd, SOL_SOCKET, option, &size, sizeof size) < 0) {
		as_log_error("Failed to set socket buffer, size %d, error %d (%s)",
				size, errno, strerror(errno));
		return false;
	}

	return true;
}

bool
as_pipe_connection_setup(int32_t fd, as_error* err)
{
	if (! set_buffer(fd, SO_RCVBUF, PIPE_READ_BUFFER_SIZE)) {
		as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to configure pipeline read buffer.");
		return false;
	}

	if (! set_buffer(fd, SO_SNDBUF, PIPE_WRITE_BUFFER_SIZE)) {
		as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to configure pipeline write buffer.");
		return false;
	}

	int arg;
	
#if defined(__linux__)
	arg = PIPE_READ_BUFFER_SIZE;

	if (setsockopt(fd, SOL_TCP, TCP_WINDOW_CLAMP, &arg, sizeof arg) < 0) {
		as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to configure pipeline TCP window.");
		return false;
	}
#endif
	
	arg = 0;

	if (setsockopt(fd, SOL_TCP, TCP_NODELAY, &arg, sizeof arg) < 0) {
		as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to configure pipeline Nagle algorithm.");
		return false;
	}

	return true;
}

int32_t
as_pipe_get_connection(as_async_command* cmd)
{
	as_log_trace("Getting pipeline connection for command %p", cmd);
	as_queue* q = &cmd->node->pipe_conn_qs[cmd->event.event_loop->index];
	as_pipe_connection* conn;

	while (as_queue_pop(q, &conn)) {
		as_log_trace("Checking pipeline connection %p, FD %d", conn, conn->fd);

		if (conn->canceled) {
			as_log_trace("Pipeline connection %p was canceled earlier", conn);
			cf_free(conn);
			continue;
		}

		ck_pr_dec_32(&cmd->node->async_conn_pool);
		conn->in_pool = false;

		if (as_socket_validate(conn->fd, true)) {
			as_log_trace("Validation OK");
			cmd->pipe_conn = conn;
			cmd->event.fd = conn->fd;
			return AS_ASYNC_CONNECTION_COMPLETE;
		}

		as_log_trace("Validation failed");
		release_connection(conn, cmd->node);
	}

	as_log_trace("Creating new pipeline connection for command %p", cmd);
	int32_t res = as_async_create_connection(cmd);

	if (res == AS_ASYNC_CONNECTION_ERROR) {
		as_log_trace("Failed to create pipeline connection FD");
		return res;
	}

	conn = cf_malloc(sizeof(as_pipe_connection));
	assert(conn != NULL);

	conn->writer = NULL;
	cf_ll_init(&conn->readers, NULL, false);
	conn->fd = cmd->event.fd;
	conn->in_pool = false;
	conn->canceled = false;

	as_log_trace("New pipeline connection %p, FD %d", conn, conn->fd);
	cmd->pipe_conn = conn;
	return res;
}

void
as_pipe_socket_error(as_async_command* cmd, as_error* err)
{
	as_log_trace("Socket error for command %p", cmd);
	cancel_connection(cmd, err, CANCEL_CONNECTION_SOCKET);
}

void
as_pipe_timeout(as_async_command* cmd)
{
	as_log_trace("Timeout for command %p", cmd);
	as_error err;
	as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
	cancel_connection(cmd, &err, CANCEL_CONNECTION_TIMEOUT);
}

void
as_pipe_response_error(as_async_command* cmd, as_error* err)
{
	as_log_trace("Error response for command %p, code %d", cmd, err->code);

	switch (err->code) {
		case AEROSPIKE_ERR_QUERY_ABORTED:
		case AEROSPIKE_ERR_SCAN_ABORTED:
		case AEROSPIKE_ERR_ASYNC_CONNECTION:
		case AEROSPIKE_ERR_CLIENT_ABORT:
		case AEROSPIKE_ERR_CLIENT:
			as_log_trace("Error is fatal");
			cancel_connection(cmd, err, CANCEL_CONNECTION_RESPONSE);
			break;

		default:
			as_log_trace("Error is non-fatal");
			next_reader(cmd);
			as_async_error_callback(cmd, err);
			break;
	}
}

void
as_pipe_response_complete(as_async_command* cmd)
{
	as_log_trace("Response for command %p", cmd);
	next_reader(cmd);
	ck_pr_dec_32(&cmd->node->async_pending);
	as_node_release(cmd->node);
}

void
as_pipe_write_start(as_async_command* cmd)
{
	as_log_trace("Setting writer %p", cmd);
	as_pipe_connection* conn = cmd->pipe_conn;
	as_log_trace("Pipeline connection %p, FD %d", conn, conn->fd);
	assert(conn != NULL);
	assert(conn->writer == NULL);

	conn->writer = cmd;
}

void
as_pipe_read_start(as_async_command* cmd, bool has_event)
{
	as_log_trace("Writer %p becomes reader", cmd);
	as_pipe_connection* conn = cmd->pipe_conn;
	as_log_trace("Pipeline connection %p, FD %d", conn, conn->fd);
	assert(conn != NULL);
	assert(conn->writer == cmd);

	conn->writer = NULL;
	cf_ll_append(&conn->readers, &cmd->pipe_link);
	as_log_trace("Pipeline connection %p has %d reader(s)", conn, cf_ll_size(&conn->readers));

	put_connection(cmd);

	if (cf_ll_size(&conn->readers) == 1) {
		if (! has_event) {
			as_log_trace("Registering read event for command %p", cmd);
			as_event_register_read(&cmd->event);
		}
		else {
			as_log_trace("Changing write event to read event for command %p", cmd);
			as_event_set_read(&cmd->event);
		}
	}
	else {
		if (has_event) {
			as_log_trace("Removing write event for command %p", cmd);
			as_event_unregister(&cmd->event);
		}
	}
}

extern uint32_t as_event_loop_capacity;

void
as_pipe_node_destroy(as_node* node)
{
	for (uint32_t i = 0; i < as_event_loop_capacity; i++) {
		as_pipe_connection* conn;

		while (as_queue_pop(&node->pipe_conn_qs[i], &conn)) {
			ck_pr_dec_32(&node->async_conn_pool);
			as_log_trace("Closing pipeline connection %p, FD %d, %s", conn, conn->fd, conn->canceled ? "canceled" : "not canceled");

			if (! conn->canceled) {
				close(conn->fd);
				ck_pr_dec_32(&node->async_conn);
			}

			cf_free(conn);
		}

		as_queue_destroy(&node->pipe_conn_qs[i]);
	}

	cf_free(node->pipe_conn_qs);
}
