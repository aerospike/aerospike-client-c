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

static void
next_reader(as_event_command* reader)
{
	as_pipe_connection* conn = (as_pipe_connection*)reader->conn;
	as_log_trace("Selecting next reader for command %p, pipeline connection %p", reader, conn);
	assert(cf_ll_get_head(&conn->readers) == &reader->pipe_link);

	cf_ll_delete(&conn->readers, &reader->pipe_link);
	as_event_stop_timer(reader);

	if (cf_ll_size(&conn->readers) == 0) {
		as_log_trace("No reader left");
		as_event_stop_watcher(reader, reader->conn);

		if (conn->in_pool) {
			as_log_trace("Pipeline connection still in pool");
			return;
		}

		as_log_trace("Closing non-pooled pipeline connection %p", conn);
		as_event_close_connection(reader->conn, reader->node);
		return;
	}

	as_log_trace("Pipeline connection %p has %d reader(s)", conn, cf_ll_size(&conn->readers));
}

#define CANCEL_COMMAND_TIMER 1
#define CANCEL_COMMAND_EVENT 2

static void
cancel_command(as_event_command* cmd, as_error* err)
{
	as_log_trace("Canceling command %p, error code %d", cmd, err->code);

	as_log_trace("Canceling timeout");
	as_event_stop_timer(cmd);

	as_log_trace("Invoking callback function");
	as_event_error_callback(cmd, err);
}

#define CANCEL_CONNECTION_SOCKET 1
#define CANCEL_CONNECTION_RESPONSE 2
#define CANCEL_CONNECTION_TIMEOUT 3

static void
cancel_connection(as_event_command* cmd, as_error* err, int32_t source)
{
	as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;
	as_node* node = cmd->node;
	
	as_log_trace("Canceling pipeline connection for command %p, error code %d, connection %p", cmd, err->code, conn);

	if (source != CANCEL_CONNECTION_TIMEOUT) {
		assert(cmd == conn->writer || cf_ll_get_head(&conn->readers) == &cmd->pipe_link);
	}

	as_log_trace("Stop watcher on connection");
	as_event_stop_watcher(cmd, &conn->base);

	// To eliminate libuv close race condition, do not close.  Just cancel and leave open.
	//as_event_close(&cmd->event);
	//ck_pr_dec_32(&cmd->node->async_conn);

	if (conn->writer != NULL) {
		as_log_trace("Canceling writer %p", conn->writer);
		cancel_command(conn->writer, err);
	}

	bool is_reader = false;

	while (cf_ll_size(&conn->readers) > 0) {
		cf_ll_element* link = cf_ll_get_head(&conn->readers);
		as_event_command* walker = as_pipe_link_to_command(link);

		if (cmd == walker) {
			is_reader = true;
		}

		as_log_trace("Canceling reader %p", walker);
		cf_ll_delete(&conn->readers, link);
		cancel_command(walker, err);
	}

	if (source == CANCEL_CONNECTION_TIMEOUT) {
		assert(cmd == conn->writer || is_reader);
	}

	if (! conn->in_pool) {
		as_event_close_connection(&conn->base, node);
		return;
	}

	conn->writer = NULL;
	conn->canceled = true;
}

static void
release_connection(as_event_command* cmd, as_pipe_connection* conn, as_node* node)
{
	as_log_trace("Releasing pipeline connection %p", conn);

	if (conn->writer != NULL || cf_ll_size(&conn->readers) > 0) {
		as_log_trace("Pipeline connection %p is still draining", conn);
		return;
	}

	as_log_trace("Closing pipeline connection %p", conn);
	as_event_stop_watcher(cmd, &conn->base);
	as_event_close_connection(&conn->base, node);
}

static void
put_connection(as_event_command* cmd)
{
	as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;
	as_log_trace("Returning pipeline connection for command %p, pipeline connection %p", cmd, conn);
	as_queue* q = &cmd->node->pipe_conn_qs[cmd->event_loop->index];

	if (as_queue_push_limit(q, &conn)) {
		conn->in_pool = true;
		return;
	}

	release_connection(cmd, conn, cmd->node);
}

#if defined(__linux__)

static bool
read_file(const char* path, char* buffer, size_t size)
{
	bool res = false;
	int fd = open(path, O_RDONLY);

	if (fd < 0) {
		as_log_error("Failed to open %s for reading", path);
		goto cleanup0;
	}

	size_t len = 0;

	while (len < size - 1) {
		ssize_t n = read(fd, buffer + len, size - len - 1);

		if (n < 0) {
			as_log_error("Failed to read from %s", path);
			goto cleanup1;
		}

		if (n == 0) {
			buffer[len] = 0;
			res = true;
			goto cleanup1;
		}

		len += n;
	}

	as_log_error("%s is too large", path);

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
		as_log_error("Invalid integer value in %s", path);
		return false;
	}

	*value = (int)x;
	return true;
}

static int
get_buffer_size(const char* proc, int size)
{
	int max;
	
	if (! read_integer(proc, &max)) {
		as_log_error("Failed to read %s", proc);
		return 0;
	}
	
	if (max < size) {
		as_log_error("Buffer limit is %d, should be at least %d; please set %s accordingly",
					 max, size, proc);
		return 0;
	}
	return size;
}

#endif

int
as_pipe_get_send_buffer_size()
{
#if defined(__linux__)
	return get_buffer_size("/proc/sys/net/core/wmem_max", PIPE_WRITE_BUFFER_SIZE);
#else
	return PIPE_WRITE_BUFFER_SIZE;  // TODO: Try to find max for __APPLE__.
#endif
}

int
as_pipe_get_recv_buffer_size()
{
#if defined(__linux__)
	return get_buffer_size("/proc/sys/net/core/rmem_max", PIPE_READ_BUFFER_SIZE);
#else
	return 0;  // TODO: Try to find max for __APPLE__.
#endif
}

bool
as_pipe_get_connection(as_event_command* cmd)
{
	as_log_trace("Getting pipeline connection for command %p", cmd);
	as_queue* q = &cmd->node->pipe_conn_qs[cmd->event_loop->index];
	as_pipe_connection* conn;
	
	while (as_queue_pop(q, &conn)) {
		as_log_trace("Checking pipeline connection %p", conn);

		if (conn->canceled) {
			as_log_trace("Pipeline connection %p was canceled earlier", conn);
			// Do not need to stop watcher because it was stopped in cancel_connection().
			as_event_close_connection(cmd->conn, cmd->node);
			continue;
		}
		
		conn->in_pool = false;

		if (as_event_validate_connection(&conn->base, true)) {
			as_log_trace("Validation OK");
			cmd->conn = (as_event_connection*)conn;
			as_pipe_write_start(cmd);
			return true;
		}
		
		as_log_trace("Validation failed");
		release_connection(cmd, conn, cmd->node);
	}
	
	as_log_trace("Creating new pipeline connection");
	conn = cf_malloc(sizeof(as_pipe_connection));
	assert(conn != NULL);
	
	conn->base.pipeline = true;
	conn->writer = NULL;
	cf_ll_init(&conn->readers, NULL, false);
	conn->canceled = false;
	conn->in_pool = false;

	cmd->conn = (as_event_connection*)conn;
	as_pipe_write_start(cmd);
	return false;
}

void
as_pipe_socket_error(as_event_command* cmd, as_error* err)
{
	as_log_trace("Socket error for command %p", cmd);
	cancel_connection(cmd, err, CANCEL_CONNECTION_SOCKET);
}

void
as_pipe_timeout(as_event_command* cmd)
{
	as_log_trace("Timeout for command %p", cmd);
	as_error err;
	as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
	cancel_connection(cmd, &err, CANCEL_CONNECTION_TIMEOUT);
}

void
as_pipe_response_error(as_event_command* cmd, as_error* err)
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
			as_event_error_callback(cmd, err);
			break;
	}
}

void
as_pipe_response_complete(as_event_command* cmd)
{
	as_log_trace("Response for command %p", cmd);
	next_reader(cmd);
	ck_pr_dec_32(&cmd->node->async_pending);
	as_node_release(cmd->node);
}

void
as_pipe_write_start(as_event_command* cmd)
{
	as_log_trace("Setting writer %p", cmd);
	as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;
	as_log_trace("Pipeline connection %p", conn);
	assert(conn != NULL);
	assert(conn->writer == NULL);

	conn->writer = cmd;
}

void
as_pipe_read_start(as_event_command* cmd)
{
	as_log_trace("Writer %p becomes reader", cmd);
	as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;
	as_log_trace("Pipeline connection %p", conn);
	assert(conn != NULL);
	assert(conn->writer == cmd);

	conn->writer = NULL;
	cf_ll_append(&conn->readers, &cmd->pipe_link);
	as_log_trace("Pipeline connection %p has %d reader(s)", conn, cf_ll_size(&conn->readers));

	put_connection(cmd);
}
