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
#pragma once

#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_listener.h>
#include <aerospike/as_queue.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/cf_ll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>

#if defined(AS_USE_LIBEV)
#include <ev.h>
#elif defined(AS_USE_LIBUV)
#include <uv.h>
#else
#endif

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/
	
#define AS_ASYNC_STATE_UNREGISTERED 0
#define AS_ASYNC_STATE_AUTH_WRITE 1
#define AS_ASYNC_STATE_AUTH_READ_HEADER 2
#define AS_ASYNC_STATE_AUTH_READ_BODY 4
#define AS_ASYNC_STATE_WRITE 8
#define AS_ASYNC_STATE_READ_HEADER 16
#define AS_ASYNC_STATE_READ_BODY 32
	
#define AS_ASYNC_AUTH_RETURN_CODE 1

#define AS_EVENT_CONNECTION_COMPLETE 0
#define AS_EVENT_CONNECTION_PENDING 1
#define AS_EVENT_CONNECTION_ERROR 2

#define AS_EVENT_QUEUE_INITIAL_CAPACITY 256
	
struct as_event_command;
struct as_event_executor;
	
typedef struct {
#if defined(AS_USE_LIBEV)
	struct ev_io watcher;
	int fd;
#elif defined(AS_USE_LIBUV)
	uv_tcp_t socket;
	
	// Reuse memory for requests, because only one request is active at a time.
	union {
		uv_connect_t connect;
		uv_write_t write;
	} req;
#else
#endif
	bool pipeline;
} as_event_connection;

typedef struct {
	as_event_connection base;
	struct as_event_command* cmd;
} as_async_connection;

typedef struct {
	as_pipe_listener listener;
	void* udata;
} as_queued_pipe_cb;

typedef bool (*as_event_parse_results_fn) (struct as_event_command* cmd);
typedef void (*as_event_executor_complete_fn) (struct as_event_executor* executor, as_error* err);
typedef void (*as_event_executor_destroy_fn) (struct as_event_executor* executor);

typedef struct as_event_command {
#if defined(AS_USE_LIBEV)
	struct ev_timer timer;
#elif defined(AS_USE_LIBUV)
	uv_timer_t timer;
#else
#endif
	as_event_loop* event_loop;
	as_event_connection* conn;
	as_cluster* cluster;
	as_node* node;
	void* udata;
	as_event_parse_results_fn parse_results;
	as_pipe_listener pipe_listener;
	cf_ll_element pipe_link;
	
	uint8_t* buf;
	uint32_t capacity;
	uint32_t len;
	uint32_t pos;
	uint32_t auth_len;
	uint32_t timeout_ms;
	
	uint8_t type;
	uint8_t state;
	bool deserialize;
	bool free_buf;
} as_event_command;
		
typedef struct as_event_executor {
	pthread_mutex_t lock;
	struct as_event_command** commands;
	as_event_loop* event_loop;
	as_event_executor_complete_fn complete_fn;
	void* udata;
	uint32_t max_concurrent;
	uint32_t max;
	uint32_t count;
	bool valid;
} as_event_executor;
	
typedef enum as_connection_status_e {
	AS_CONNECTION_FROM_POOL = 0,
	AS_CONNECTION_NEW = 1,
	AS_CONNECTION_TOO_MANY = 2
} as_connection_status;
	
/******************************************************************************
 * GLOBAL VARIABLES
 *****************************************************************************/

extern as_event_loop* as_event_loops;
extern uint32_t as_event_loop_size;
extern uint32_t as_event_loop_current;
	
/******************************************************************************
 * COMMON FUNCTIONS
 *****************************************************************************/

as_status
as_event_command_execute(as_event_command* cmd, as_error* err);

void
as_event_executor_complete(as_event_command* cmd);

void
as_event_executor_cancel(as_event_executor* executor, int queued_count);

as_connection_status
as_event_get_connection(as_event_command* cmd);
	
int
as_event_create_socket(as_event_command* cmd);
	
void
as_event_connect_error(as_event_command* cmd, as_error* err, int fd);

void
as_event_error_callback(as_event_command* cmd, as_error* err);
	
void
as_event_socket_error(as_event_command* cmd, as_error* err);

void
as_event_response_error(as_event_command* cmd, as_error* err);

void
as_event_timeout(as_event_command* cmd);

bool
as_event_command_parse_result(as_event_command* cmd);
	
bool
as_event_command_parse_header(as_event_command* cmd);

bool
as_event_command_parse_success_failure(as_event_command* cmd);

void
as_event_command_free(as_event_command* cmd);

/******************************************************************************
 * IMPLEMENTATION SPECIFIC FUNCTIONS
 *****************************************************************************/

bool
as_event_create_loop(as_event_loop* event_loop);

void
as_event_register_external_loop(as_event_loop* event_loop);

bool
as_event_send(as_event_command* cmd);

void
as_event_command_begin(as_event_command* cmd);

void
as_event_close_connection(as_event_connection* conn);

void
as_event_node_destroy(as_node* node);
	
bool
as_event_send_close_loop(as_event_loop* event_loop);

void
as_event_close_loop(as_event_loop* event_loop);

/******************************************************************************
 * LIBEV INLINE FUNCTIONS
 *****************************************************************************/

#if defined(AS_USE_LIBEV)

static inline int
as_event_validate_connection(as_event_connection* conn)
{
	return as_socket_validate(conn->fd);
}

static inline void
as_event_stop_timer(as_event_command* cmd)
{
	if (cmd->timeout_ms) {
		ev_timer_stop(cmd->event_loop->loop, &cmd->timer);
	}
}

static inline void
as_event_stop_watcher(as_event_command* cmd, as_event_connection* conn)
{
	ev_io_stop(cmd->event_loop->loop, &conn->watcher);
}

static inline void
as_event_command_release(as_event_command* cmd)
{
	as_event_command_free(cmd);
}

/******************************************************************************
 * LIBUV INLINE FUNCTIONS
 *****************************************************************************/

#elif defined(AS_USE_LIBUV)

static inline int
as_event_validate_connection(as_event_connection* conn)
{
	// Libuv does not have a peek function, so use fd directly.
	uv_os_fd_t fd;
	
	if (uv_fileno((uv_handle_t*)&conn->socket, &fd) == 0) {
		return as_socket_validate(fd);
	}
	return false;
}
	
static inline void
as_event_stop_timer(as_event_command* cmd)
{
	// Timer is stopped in libuv by uv_close which occurs later in as_event_command_release().
}

static inline void
as_event_stop_watcher(as_event_command* cmd, as_event_connection* conn)
{
	// Watcher already stopped by design in libuv.
}

void
as_uv_timer_closed(uv_handle_t* handle);

static inline void
as_event_command_release(as_event_command* cmd)
{
	if (cmd->timeout_ms) {
		// libuv requires that cmd can't be freed until timer is closed.
		uv_close((uv_handle_t*)&cmd->timer, as_uv_timer_closed);
	}
	else {
		as_event_command_free(cmd);
	}
}

/******************************************************************************
 * EVENT_LIB NOT DEFINED INLINE FUNCTIONS
 *****************************************************************************/

#else

static inline int
as_event_validate_connection(as_event_connection* conn)
{
	return -1;
}

static inline void
as_event_stop_timer(as_event_command* cmd)
{
}

static inline void
as_event_stop_watcher(as_event_command* cmd, as_event_connection* conn)
{
}

static inline void
as_event_command_release(as_event_command* cmd)
{
}

#endif
	
/******************************************************************************
 * COMMON INLINE FUNCTIONS
 *****************************************************************************/

static inline void
as_event_command_execute_in_loop(as_event_command* cmd)
{
	// Check if command timed out after coming off queue.
	if (cmd->timeout_ms && (cf_getms() - *(uint64_t*)cmd) > cmd->timeout_ms) {
		as_error err;
		as_error_set_message(&err, AEROSPIKE_ERR_TIMEOUT, as_error_string(AEROSPIKE_ERR_TIMEOUT));
		// Tell the libuv version of as_event_command_release() to not try to close the uv_timer_t.
		cmd->timeout_ms = 0;
		as_event_error_callback(cmd, &err);
		return;
	}
	
	// Start processing.
	as_event_command_begin(cmd);
}

static inline as_event_loop*
as_event_assign(as_event_loop* event_loop)
{
	if (! event_loop) {
		// Assign event loop using round robin distribution.
		// Not atomic because doesn't need to be exactly accurate.
		uint32_t current = as_event_loop_current++;
		event_loop = &as_event_loops[current % as_event_loop_size];
	}
	return event_loop;
}

static inline void
as_event_set_auth_write(as_event_command* cmd)
{
	// The command buffer was already allocated with enough space for max authentication size,
	// so just use the end of the write buffer for authentication bytes.
	cmd->pos = cmd->len;
	cmd->auth_len = as_authenticate_set(cmd->cluster->user, cmd->cluster->password, &cmd->buf[cmd->pos]);
	cmd->len = cmd->pos + cmd->auth_len;
}

static inline void
as_event_set_auth_read_header(as_event_command* cmd)
{
	// Authenticate response buffer is at end of write buffer.
	cmd->pos = cmd->len - cmd->auth_len;
	cmd->auth_len = sizeof(as_proto);
	cmd->len = cmd->pos + cmd->auth_len;
	cmd->state = AS_ASYNC_STATE_AUTH_READ_HEADER;
}
	
static inline void
as_event_set_auth_parse_header(as_event_command* cmd)
{
	// Authenticate response buffer is at end of write buffer.
	cmd->pos = cmd->len - cmd->auth_len;
	as_proto* proto = (as_proto*)&cmd->buf[cmd->pos];
	as_proto_swap_from_be(proto);
	cmd->auth_len = (uint32_t)proto->sz;
	cmd->len = cmd->pos + cmd->auth_len;
	cmd->state = AS_ASYNC_STATE_AUTH_READ_BODY;
}

static inline void
as_event_release_connection(as_cluster* cluster, as_event_connection* conn, as_queue* queue)
{
	as_event_close_connection(conn);
	ck_pr_dec_32(&cluster->async_conn_count);
	as_queue_decr_total(queue);
}

static inline void
as_event_decr_connection(as_cluster* cluster, as_queue* queue)
{
	ck_pr_dec_32(&cluster->async_conn_count);
	as_queue_decr_total(queue);
}

static inline void
as_event_decr_conn(as_event_command* cmd)
{
	as_queue* queue = cmd->pipe_listener != NULL ?
		&cmd->node->pipe_conn_qs[cmd->event_loop->index] :
		&cmd->node->async_conn_qs[cmd->event_loop->index];
	
	as_event_decr_connection(cmd->cluster, queue);
}

#ifdef __cplusplus
} // end extern "C"
#endif
