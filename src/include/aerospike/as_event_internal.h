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
#pragma once

#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_listener.h>
#include <aerospike/as_queue.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/cf_ll.h>
#include <pthread.h>

#if defined(AS_USE_LIBEV)
#include <ev.h>
#elif defined(AS_USE_LIBUV)
#include <uv.h>
struct as_uv_tls;
#elif defined(AS_USE_LIBEVENT)
#include <event2/event.h>
#else
#endif

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/
	
#define AS_ASYNC_STATE_UNREGISTERED 0
#define AS_ASYNC_STATE_REGISTERED 1
#define AS_ASYNC_STATE_DELAY_QUEUE 2
#define AS_ASYNC_STATE_CONNECT 3
#define AS_ASYNC_STATE_TLS_CONNECT 4
#define AS_ASYNC_STATE_AUTH_WRITE 5
#define AS_ASYNC_STATE_AUTH_READ_HEADER 6
#define AS_ASYNC_STATE_AUTH_READ_BODY 7
#define AS_ASYNC_STATE_COMMAND_WRITE 8
#define AS_ASYNC_STATE_COMMAND_READ_HEADER 9
#define AS_ASYNC_STATE_COMMAND_READ_BODY 10
#define AS_ASYNC_STATE_QUEUE_ERROR 11
#define AS_ASYNC_STATE_RETRY 12

#define AS_ASYNC_FLAGS_DESERIALIZE 1
#define AS_ASYNC_FLAGS_READ 2
#define AS_ASYNC_FLAGS_HAS_TIMER 4
#define AS_ASYNC_FLAGS_USING_SOCKET_TIMER 8
#define AS_ASYNC_FLAGS_EVENT_RECEIVED 16
#define AS_ASYNC_FLAGS_FREE_BUF 32
#define AS_ASYNC_FLAGS_LINEARIZE 64
#define AS_ASYNC_FLAGS_HEAP_REC 128

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
	as_socket socket;
#elif defined(AS_USE_LIBUV)
	uv_tcp_t socket;
	struct as_uv_tls* tls;
	// Reuse memory for requests, because only one request is active at a time.
	union {
		uv_connect_t connect;
		uv_write_t write;
	} req;
	uint64_t last_used;
#elif defined(AS_USE_LIBEVENT)
	struct event watcher;
	as_socket socket;
#else
#endif
	int watching;
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

typedef void (*as_event_executable) (as_event_loop* event_loop, void* udata);
typedef bool (*as_event_parse_results_fn) (struct as_event_command* cmd);
typedef void (*as_event_executor_complete_fn) (struct as_event_executor* executor);

typedef struct as_event_command {
#if defined(AS_USE_LIBEV)
	struct ev_timer timer;
#elif defined(AS_USE_LIBUV)
	uv_timer_t timer;
#elif defined(AS_USE_LIBEVENT)
	struct event timer;
#else
#endif
	uint64_t total_deadline;
	uint32_t socket_timeout;
	uint32_t max_retries;
	uint32_t iteration;
	as_policy_replica replica;
	as_event_loop* event_loop;
	as_event_state* event_state;
	as_event_connection* conn;
	as_cluster* cluster;
	as_node* node;
	const char* ns;
	void* partition;  // as_partition* or as_partition_shm*
	void* udata;
	as_event_parse_results_fn parse_results;
	as_pipe_listener pipe_listener;
	cf_ll_element pipe_link;
	
	uint8_t* buf;
	uint64_t begin; // Used for metrics
	uint32_t command_sent_counter;
	uint32_t write_offset;
	uint32_t write_len;
	uint32_t read_capacity;
	uint32_t len;
	uint32_t pos;

	uint8_t type;
	uint8_t proto_type;
	uint8_t proto_type_rcv;
	uint8_t state;
	uint8_t flags;
	uint8_t replica_size;
	uint8_t replica_index;
	uint8_t replica_index_sc; // Used in batch only.

	struct as_txn* txn;
	uint8_t* ubuf; // Uncompressed send buffer. Used when compression is enabled.
	uint32_t ubuf_size;
	as_latency_type latency_type;
	bool metrics_enabled;
} as_event_command;

typedef struct {
	as_event_executable executable;
	void* udata;
} as_event_commander;

typedef struct as_event_executor {
	pthread_mutex_t lock;
	struct as_event_command** commands;
	as_event_loop* event_loop;
	as_event_executor_complete_fn complete_fn;
	void* udata;
	as_error* err;
	char* ns;
	uint64_t cluster_key;
	uint32_t max_concurrent;
	uint32_t max;
	uint32_t count;
	uint32_t queued;
	bool notify;
	bool valid;
} as_event_executor;

/******************************************************************************
 * COMMON FUNCTIONS
 *****************************************************************************/

as_status
as_event_command_execute(as_event_command* cmd, as_error* err);

void
as_event_command_schedule(as_event_command* cmd);

void
as_event_connection_complete(as_event_command* cmd);

bool
as_event_proto_parse(as_event_command* cmd, as_proto* proto);

bool
as_event_proto_parse_auth(as_event_command* cmd, as_proto* proto);

bool
as_event_decompress(as_event_command* cmd);

void
as_event_process_timer(as_event_command* cmd);

void
as_event_socket_timeout(as_event_command* cmd);

void
as_event_total_timeout(as_event_command* cmd);

bool
as_event_command_retry(as_event_command* cmd, bool timeout);
	
void
as_event_execute_retry(as_event_command* cmd);

void
as_event_query_complete(as_event_command* cmd);

void
as_event_batch_complete(as_event_command* cmd);

void
as_event_response_complete(as_event_command* cmd);

void
as_event_executor_error(as_event_executor* executor, as_error* err, uint32_t command_count);

void
as_event_executor_cancel(as_event_executor* executor, uint32_t queued_count);

void
as_event_executor_complete(as_event_executor* executor);

void
as_event_error_callback(as_event_command* cmd, as_error* err);

void
as_event_notify_error(as_event_command* cmd, as_error* err);

void
as_event_parse_error(as_event_command* cmd, as_error* err);

void
as_event_socket_error(as_event_command* cmd, as_error* err);

void
as_event_response_error(as_event_command* cmd, as_error* err);

bool
as_event_command_parse_result(as_event_command* cmd);
	
bool
as_event_command_parse_header(as_event_command* cmd);

bool
as_event_command_parse_success_failure(as_event_command* cmd);

bool
as_event_command_parse_deadline(as_event_command* cmd);

bool
as_event_command_parse_info(as_event_command* cmd);

void
as_event_command_free(as_event_command* cmd);

void
as_event_connector_success(as_event_command* cmd);

void
as_event_create_connections(as_node* node, as_async_conn_pool* pools);

void
as_event_close_cluster(as_cluster* cluster);

/******************************************************************************
 * IMPLEMENTATION SPECIFIC FUNCTIONS
 *****************************************************************************/

bool
as_event_create_loop(as_event_loop* event_loop);

void
as_event_register_external_loop(as_event_loop* event_loop);

/**
 * Schedule execution of function on specified event loop.
 * Command is placed on event loop queue and is never executed directly.
 */
bool
as_event_execute(as_event_loop* event_loop, as_event_executable executable, void* udata);

void
as_event_command_write_start(as_event_command* cmd);

void
as_event_connect(as_event_command* cmd, as_async_conn_pool* pool);

void
as_event_node_destroy(as_node* node);

/******************************************************************************
 * LIBEV INLINE FUNCTIONS
 *****************************************************************************/

#if defined(AS_USE_LIBEV)

void as_ev_timer_cb(struct ev_loop* loop, ev_timer* timer, int revents);
void as_ev_repeat_cb(struct ev_loop* loop, ev_timer* timer, int revents);

static inline bool
as_event_conn_current_trim(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return as_socket_current_trim(conn->socket.last_used, max_socket_idle_ns);
}

static inline bool
as_event_conn_current_tran(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return as_socket_current_tran(conn->socket.last_used, max_socket_idle_ns);
}

static inline int
as_event_conn_validate(as_event_connection* conn)
{
	return as_socket_validate_fd(conn->socket.fd);
}

static inline void
as_event_close_connection(as_event_connection* conn)
{
	as_socket_close(&conn->socket);
	cf_free(conn);
}

static inline void
as_event_set_conn_last_used(as_event_connection* conn)
{
	conn->socket.last_used = cf_getns();
}

static inline void
as_event_timer_once(as_event_command* cmd, uint64_t timeout)
{
	ev_timer_init(&cmd->timer, as_ev_timer_cb, (double)timeout / 1000.0, 0.0);
	cmd->timer.data = cmd;
	ev_timer_start(cmd->event_loop->loop, &cmd->timer);
	cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER;
}

static inline void
as_event_timer_repeat(as_event_command* cmd, uint64_t repeat)
{
	ev_init(&cmd->timer, as_ev_repeat_cb);
	cmd->timer.repeat = (double)repeat / 1000.0;
	cmd->timer.data = cmd;
	ev_timer_again(cmd->event_loop->loop, &cmd->timer);
	cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER | AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
}

static inline void
as_event_timer_again(as_event_command* cmd)
{
	ev_timer_again(cmd->event_loop->loop, &cmd->timer);
}

static inline void
as_event_timer_stop(as_event_command* cmd)
{
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		ev_timer_stop(cmd->event_loop->loop, &cmd->timer);
	}
}

static inline void
as_event_stop_watcher(as_event_command* cmd, as_event_connection* conn)
{
	ev_io_stop(cmd->event_loop->loop, &conn->watcher);
	conn->watching = 0;
}

static inline void
as_event_stop_read(as_event_connection* conn)
{
	// This method only needed for libuv pipelined connections.
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

void as_uv_timer_cb(uv_timer_t* timer);
void as_uv_repeat_cb(uv_timer_t* timer);
void as_event_close_connection(as_event_connection* conn);

static inline bool
as_event_conn_current_trim(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return as_socket_current_trim(conn->last_used, max_socket_idle_ns);
}

static inline bool
as_event_conn_current_tran(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return as_socket_current_tran(conn->last_used, max_socket_idle_ns);
}

static inline int
as_event_conn_validate(as_event_connection* conn)
{
	// Libuv does not have a peek function, so use fd directly.
	uv_os_fd_t fd;
	
	if (uv_fileno((uv_handle_t*)&conn->socket, &fd) == 0) {
		return as_socket_validate_fd((as_socket_fd)fd);
	}
	return -1;
}

static inline void
as_event_set_conn_last_used(as_event_connection* conn)
{
	conn->last_used = cf_getns();
}

static inline void
as_event_timer_once(as_event_command* cmd, uint64_t timeout)
{
	if (!(cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER)) {
		uv_timer_init(cmd->event_loop->loop, &cmd->timer);
		cmd->timer.data = cmd;
	}
	uv_timer_start(&cmd->timer, as_uv_timer_cb, timeout, 0);
	cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER;
}

static inline void
as_event_timer_repeat(as_event_command* cmd, uint64_t repeat)
{
	if (!(cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER)) {
		uv_timer_init(cmd->event_loop->loop, &cmd->timer);
		cmd->timer.data = cmd;
	}
	uv_timer_start(&cmd->timer, as_uv_repeat_cb, repeat, repeat);
	cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER | AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
}

static inline void
as_event_timer_again(as_event_command* cmd)
{
	// libuv socket timers automatically repeat.
}

static inline void
as_event_timer_stop(as_event_command* cmd)
{
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		uv_timer_stop(&cmd->timer);
	}
}

static inline void
as_event_stop_watcher(as_event_command* cmd, as_event_connection* conn)
{
	// uv_read_stop() will handle case where read is already stopped.
	// Do not set watching to zero because conn is still initialized and active.
	// libuv works differently here.
	uv_read_stop((uv_stream_t*)conn);
}

static inline void
as_event_stop_read(as_event_connection* conn)
{
	uv_read_stop((uv_stream_t*)conn);
}

void
as_uv_timer_closed(uv_handle_t* handle);

static inline void
as_event_command_release(as_event_command* cmd)
{
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		// libuv requires that cmd can't be freed until timer is closed.
		uv_close((uv_handle_t*)&cmd->timer, as_uv_timer_closed);
	}
	else {
		as_event_command_free(cmd);
	}
}

/******************************************************************************
 * LIBEVENT INLINE FUNCTIONS
 *****************************************************************************/

#elif defined(AS_USE_LIBEVENT)

void as_libevent_timer_cb(evutil_socket_t sock, short events, void* udata);
void as_libevent_repeat_cb(evutil_socket_t sock, short events, void* udata);

static inline bool
as_event_conn_current_trim(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return as_socket_current_trim(conn->socket.last_used, max_socket_idle_ns);
}

static inline bool
as_event_conn_current_tran(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return as_socket_current_tran(conn->socket.last_used, max_socket_idle_ns);
}

static inline int
as_event_conn_validate(as_event_connection* conn)
{
	return as_socket_validate_fd(conn->socket.fd);
}

static inline void
as_event_close_connection(as_event_connection* conn)
{
	as_socket_close(&conn->socket);
	cf_free(conn);
}

static inline void
as_event_set_conn_last_used(as_event_connection* conn)
{
	conn->socket.last_used = cf_getns();
}

static inline void
as_event_timer_once(as_event_command* cmd, uint64_t timeout)
{
	evtimer_assign(&cmd->timer, cmd->event_loop->loop, as_libevent_timer_cb, cmd);
	struct timeval tv;
	tv.tv_sec = (uint32_t)timeout / 1000;
	tv.tv_usec = ((uint32_t)timeout % 1000) * 1000;
	evtimer_add(&cmd->timer, &tv);
	cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER;
}

static inline void
as_event_timer_repeat(as_event_command* cmd, uint64_t repeat)
{
	event_assign(&cmd->timer, cmd->event_loop->loop, -1, EV_PERSIST, as_libevent_repeat_cb, cmd);
	struct timeval tv;
	tv.tv_sec = (uint32_t)repeat / 1000;
	tv.tv_usec = ((uint32_t)repeat % 1000) * 1000;
	evtimer_add(&cmd->timer, &tv);
	cmd->flags |= AS_ASYNC_FLAGS_HAS_TIMER | AS_ASYNC_FLAGS_USING_SOCKET_TIMER;
}

static inline void
as_event_timer_again(as_event_command* cmd)
{
	// libevent socket timers automatically repeat.
}

static inline void
as_event_timer_stop(as_event_command* cmd)
{
	if (cmd->flags & AS_ASYNC_FLAGS_HAS_TIMER) {
		evtimer_del(&cmd->timer);
	}
}

static inline void
as_event_stop_watcher(as_event_command* cmd, as_event_connection* conn)
{
	event_del(&conn->watcher);
	conn->watching = 0;
}

static inline void
as_event_stop_read(as_event_connection* conn)
{
	// This method only needed for libuv pipelined connections.
}

static inline void
as_event_command_release(as_event_command* cmd)
{
	as_event_command_free(cmd);
}

/******************************************************************************
 * EVENT_LIB NOT DEFINED INLINE FUNCTIONS
 *****************************************************************************/

#else

static inline bool
as_event_conn_current_trim(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return false;
}

static inline bool
as_event_conn_current_tran(as_event_connection* conn, uint64_t max_socket_idle_ns)
{
	return false;
}

static inline int
as_event_conn_validate(as_event_connection* conn)
{
	return -1;
}

static inline void
as_event_close_connection(as_event_connection* conn)
{
}

static inline void
as_event_set_conn_last_used(as_event_connection* conn)
{
}

static inline void
as_event_timer_once(as_event_command* cmd, uint64_t timeout)
{
}

static inline void
as_event_timer_repeat(as_event_command* cmd, uint64_t repeat)
{
}

static inline void
as_event_timer_again(as_event_command* cmd)
{
}

static inline void
as_event_timer_stop(as_event_command* cmd)
{
}

static inline void
as_event_stop_watcher(as_event_command* cmd, as_event_connection* conn)
{
}

static inline void
as_event_stop_read(as_event_connection* conn)
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

static inline as_event_loop*
as_event_assign(as_event_loop* event_loop)
{
	// Assign event loop using round robin distribution if not specified.
	return event_loop ? event_loop : as_event_loop_get();
}

static inline void
as_event_set_auth_write(as_event_command* cmd, as_session* session)
{
	// Authentication write buffer is always located after command write buffer.
	uint8_t* buf = (uint8_t*)cmd + cmd->write_offset + cmd->write_len;
	uint32_t len = as_authenticate_set(cmd->cluster, session, buf);
	cmd->len = cmd->write_len + len;
	cmd->pos = cmd->write_len;
}

static inline void
as_event_set_auth_read_header(as_event_command* cmd)
{
	// Authenticate read buffer uses the standard read buffer (buf).
	cmd->len = sizeof(as_proto);
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_AUTH_READ_HEADER;
}
	
static inline bool
as_event_set_auth_parse_header(as_event_command* cmd)
{
	// Authenticate read buffer uses the standard read buffer (buf).
	as_proto* proto = (as_proto*)cmd->buf;

	if (! as_event_proto_parse_auth(cmd, proto)) {
		return false;
	}

	cmd->len = (uint32_t)proto->sz;
	cmd->pos = 0;
	cmd->state = AS_ASYNC_STATE_AUTH_READ_BODY;
	return true;
}

static inline void
as_event_set_write(as_event_command* cmd)
{
	cmd->len = cmd->write_len;
	cmd->pos = 0;
}

static inline void
as_async_conn_pool_init(as_async_conn_pool* pool, uint32_t min_size, uint32_t max_size)
{
	as_queue_init(&pool->queue, sizeof(void*), max_size);
	pool->min_size = min_size;
	pool->limit = max_size;
	pool->opened = 0;
	pool->closed = 0;
}

static inline bool
as_async_conn_pool_incr_total(as_async_conn_pool* pool)
{
	if (pool->queue.total >= pool->limit) {
		return false;
	}
	pool->queue.total++;
	return true;
}

static inline bool
as_async_conn_pool_push_head(as_async_conn_pool* pool, as_event_connection* conn)
{
	if (pool->queue.total > pool->limit) {
		return false;
	}
	return as_queue_push_head(&pool->queue, &conn);
}

static inline bool
as_async_conn_pool_push(as_async_conn_pool* pool, as_event_connection* conn)
{
	if (pool->queue.total > pool->limit) {
		return false;
	}
	return as_queue_push(&pool->queue, &conn);
}

static inline void
as_event_release_connection(as_event_connection* conn, as_async_conn_pool* pool)
{
	as_event_close_connection(conn);
	as_queue_decr_total(&pool->queue);
	pool->closed++;
}

static inline void
as_event_release_async_connection(as_event_command* cmd)
{
	as_async_conn_pool* pool = &cmd->node->async_conn_pools[cmd->event_loop->index];
	as_event_release_connection(cmd->conn, pool);
	as_node_incr_error_rate(cmd->node);
}

static inline void
as_event_decr_conn(as_event_command* cmd)
{
	as_async_conn_pool* pool = cmd->pipe_listener != NULL ?
		&cmd->node->pipe_conn_pools[cmd->event_loop->index] :
		&cmd->node->async_conn_pools[cmd->event_loop->index];

	as_queue_decr_total(&pool->queue);
}

static inline void
as_event_connection_timeout(as_event_command* cmd, as_async_conn_pool* pool)
{
	as_event_connection* conn = cmd->conn;

	if (conn) {
		if (conn->watching > 0) {
			as_event_stop_watcher(cmd, conn);
			as_event_release_connection(conn, pool);
			as_node_incr_error_rate(cmd->node);
		}
		else {
			cf_free(conn);
			as_queue_decr_total(&pool->queue);
			pool->closed++;
		}
	}
}

static inline bool
as_event_socket_retry(as_event_command* cmd)
{
	if (cmd->pipe_listener) {
		return false;
	}

	as_event_stop_watcher(cmd, cmd->conn);
	as_event_release_async_connection(cmd);
	return as_event_command_retry(cmd, false);
}

static inline uint8_t*
as_event_get_ubuf(as_event_command* cmd)
{
	// Return saved uncompressed buffer when compression is enabled.
	// Return command buffer when compression is not enabled.
	return cmd->ubuf ? cmd->ubuf : (uint8_t*)cmd + cmd->write_offset;
}

static inline void
as_event_command_destroy(as_event_command* cmd)
{
	// Use this function to free async commands that were never started.
	if (cmd->node) {
		as_node_release(cmd->node);
	}

	if (cmd->ubuf) {
		cf_free(cmd->ubuf);
	}

	cf_free(cmd);
}

static inline void
as_event_loop_destroy(as_event_loop* event_loop)
{
	as_queue_destroy(&event_loop->queue);
	as_queue_destroy(&event_loop->delay_queue);
	as_queue_destroy(&event_loop->pipe_cb_queue);
	pthread_mutex_destroy(&event_loop->lock);
}

#ifdef __cplusplus
} // end extern "C"
#endif
