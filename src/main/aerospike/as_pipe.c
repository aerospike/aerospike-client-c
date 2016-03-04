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

#if defined(__linux__)
#define PIPE_WRITE_BUFFER_SIZE (5 * 1024 * 1024)
#define PIPE_READ_BUFFER_SIZE (15 * 1024 * 1024)
#else
#define PIPE_WRITE_BUFFER_SIZE (2 * 1024 * 1024)
#define PIPE_READ_BUFFER_SIZE  (4 * 1024 * 1024)
#endif

extern uint32_t as_event_loop_capacity;

static void
write_start(as_event_command* cmd)
{
	as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;
	as_log_trace("Setting writer %p, pipeline connection %p", cmd, conn);
	assert(conn != NULL);
	assert(conn->writer == NULL);

	conn->writer = cmd;
}

static void
next_reader(as_event_command* reader)
{
	as_pipe_connection* conn = (as_pipe_connection*)reader->conn;
	as_log_trace("Selecting successor to reader %p, pipeline connection %p", reader, conn);
	assert(cf_ll_get_head(&conn->readers) == &reader->pipe_link);

	cf_ll_delete(&conn->readers, &reader->pipe_link);
	as_event_stop_timer(reader);

	if (conn->writer == NULL && cf_ll_size(&conn->readers) == 0) {
		as_log_trace("No writer and no reader left");
		as_event_stop_watcher(reader, reader->conn);

		if (conn->in_pool) {
			as_log_trace("Pipeline connection still in pool");
			return;
		}

		as_log_trace("Closing non-pooled pipeline connection %p", conn);
		as_queue* queue = &reader->node->pipe_conn_qs[reader->event_loop->index];
		as_event_release_connection(reader->cluster, reader->conn, queue);
		return;
	}

	as_log_trace("Pipeline connection %p has %d reader(s)", conn, cf_ll_size(&conn->readers));
}

static void
cancel_command(as_event_command* cmd, as_error* err)
{
	as_log_trace("Canceling command %p, error code %d", cmd, err->code);
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
	as_event_loop* loop = cmd->event_loop;
	// So that cancel_command() doesn't free the node.
	as_node_reserve(node);
	
	as_log_trace("Canceling pipeline connection for command %p, error code %d, connection %p", cmd, err->code, conn);

	if (source != CANCEL_CONNECTION_TIMEOUT) {
		assert(cmd == conn->writer || cf_ll_get_head(&conn->readers) == &cmd->pipe_link);
	}

	as_log_trace("Stopping watcher");
	as_event_stop_watcher(cmd, &conn->base);

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
		as_log_trace("Closing canceled non-pooled pipeline connection %p", conn);
		// For as_uv_connection_alive().
		conn->canceled = true;
		as_queue* queue = &node->pipe_conn_qs[loop->index];
		as_event_release_connection(node->cluster, (as_event_connection*)conn, queue);
		as_node_release(node);
		return;
	}

	as_log_trace("Marking pooled pipeline connection %p as canceled", conn);
	conn->writer = NULL;
	conn->canceled = true;

	as_node_release(node);
}

static void
release_connection(as_event_command* cmd, as_pipe_connection* conn, as_queue* queue)
{
	as_log_trace("Releasing pipeline connection %p", conn);

	if (conn->writer != NULL || cf_ll_size(&conn->readers) > 0) {
		as_log_trace("Pipeline connection %p is still draining", conn);
		return;
	}

	as_log_trace("Closing pipeline connection %p", conn);
	as_event_stop_watcher(cmd, &conn->base);
	as_event_release_connection(cmd->cluster, &conn->base, queue);
}

static void
put_connection(as_event_command* cmd)
{
	as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;
	as_log_trace("Returning pipeline connection for writer %p, pipeline connection %p", cmd, conn);
	as_queue* q = &cmd->node->pipe_conn_qs[cmd->event_loop->index];

	if (as_queue_push(q, &conn)) {
		ck_pr_inc_32(&cmd->cluster->async_conn_pool);
		conn->in_pool = true;
		return;
	}

	release_connection(cmd, conn, q);
}

#if defined(__linux__)
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

static int
get_buffer_size(const char* proc, int size)
{
	int max;
	
	if (! read_integer(proc, &max)) {
		as_log_warn("Failed to read %s; should be at least %d. Please verify.", proc, size);
		return size;
	}
	
	if (max < size) {
#if defined(USE_XDR)
		as_log_warn("Buffer limit is %d, should be at least %d for async pipelining. Please set %s accordingly.",
				max, size, proc);
#else
		as_log_debug("Buffer limit is %d, should be at least %d if async pipelining is used. Please set %s accordingly.",
					max, size, proc);
#endif

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
	return PIPE_WRITE_BUFFER_SIZE;
#endif
}

int
as_pipe_get_recv_buffer_size()
{
#if defined(__linux__)
	return get_buffer_size("/proc/sys/net/core/rmem_max", PIPE_READ_BUFFER_SIZE);
#else
	return PIPE_READ_BUFFER_SIZE;
#endif
}

as_connection_status
as_pipe_get_connection(as_event_command* cmd)
{
	as_log_trace("Getting pipeline connection for command %p", cmd);
	as_queue* q = &cmd->node->pipe_conn_qs[cmd->event_loop->index];
	as_pipe_connection* conn;

	// Prefer to open new connections, as long as we are below pool capacity. This is to
	// make sure that we fully use the allowed number of connections. Pipelining otherwise
	// tends to open very few connections, which isn't good for write parallelism on the
	// server. The server processes all commands from the same connection sequentially.
	// More connections thus mean more parallelism.
	if (q->total >= q->capacity) {
		while (as_queue_pop(q, &conn)) {
			as_log_trace("Checking pipeline connection %p", conn);
			ck_pr_dec_32(&cmd->cluster->async_conn_pool);

			if (conn->canceled) {
				as_log_trace("Pipeline connection %p was canceled earlier", conn);
				// Do not need to stop watcher because it was stopped in cancel_connection().
				as_event_release_connection(cmd->cluster, (as_event_connection*)conn, q);
				continue;
			}

			conn->in_pool = false;
			
			// Verify that socket is active.  Socket receive buffer may already have data.
			int len = as_event_validate_connection(&conn->base);

			if (len >= 0) {
				as_log_trace("Validation OK");
				cmd->conn = (as_event_connection*)conn;
				write_start(cmd);
				return AS_CONNECTION_FROM_POOL;
			}

			as_log_debug("Invalid pipeline socket from pool: %d", len);
			release_connection(cmd, conn, q);
		}
	}
	
	// Create connection structure only when node connection count within limit.
	as_log_trace("Creating new pipeline connection");

	if (as_queue_incr_total(q)) {
		ck_pr_inc_32(&cmd->cluster->async_conn_count);
		conn = cf_malloc(sizeof(as_pipe_connection));
		assert(conn != NULL);
		
		conn->base.pipeline = true;
		conn->writer = NULL;
		cf_ll_init(&conn->readers, NULL, false);
		conn->canceled = false;
		conn->in_pool = false;
		
		cmd->conn = (as_event_connection*)conn;
		write_start(cmd);
		return AS_CONNECTION_NEW;
	}
	else {
		as_error err;
		as_error_update(&err, AEROSPIKE_ERR_NO_MORE_CONNECTIONS,
						"Max node/event loop %s pipeline connections would be exceeded: %u",
						cmd->node->name, q->capacity);
		as_event_stop_timer(cmd);
		as_event_error_callback(cmd, &err);
		return AS_CONNECTION_TOO_MANY;
	}
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
}

void
as_pipe_read_start(as_event_command* cmd)
{
	as_pipe_connection* conn = (as_pipe_connection*)cmd->conn;
	as_log_trace("Writer %p becomes reader, pipeline connection %p", cmd, conn);
	assert(conn != NULL);
	assert(conn->writer == cmd);

	conn->writer = NULL;
	cf_ll_append(&conn->readers, &cmd->pipe_link);
	as_log_trace("Pipeline connection %p has %d reader(s)", conn, cf_ll_size(&conn->readers));

	put_connection(cmd);

	as_event_loop* loop = cmd->event_loop;
	as_queue* q = &loop->pipe_cb_queue;

	if (cmd->pipe_listener != NULL) {
		as_queue_push(q, &(as_queued_pipe_cb){ cmd->pipe_listener, cmd->udata });
	}

	if (loop->pipe_cb_calling) {
		return;
	}

	loop->pipe_cb_calling = true;
	as_queued_pipe_cb cb;

	while (as_queue_pop(q, &cb)) {
		cb.listener(cb.udata, loop);
	}

	loop->pipe_cb_calling = false;
}
