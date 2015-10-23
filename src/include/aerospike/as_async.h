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
#pragma once

#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_record.h>
#include <aerospike/as_queue.h>
#include <citrusleaf/alloc.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * PUBLIC TYPES
 *****************************************************************************/

/**
 *	User callback when an asynchronous write completes.
 *
 *	@param err			This error structure is only populated when the command fails. Null on success.
 *	@param udata 		User data that is forwarded from asynchronous command function.
 *	@param event_loop 	Event loop that this command was executed on.  Use this event loop when running
 *						nested asynchronous commands when single threaded behavior is desired for the
 *						group of commands.
 */
typedef void (*as_async_write_listener) (as_error* err, void* udata, as_event_loop* event_loop);
	
/**
 *	User callback when an asynchronous read completes with a record result.
 *
 *	@param err			This error structure is only populated when the command fails. Null on success.
 *	@param record 		The return value from the asynchronous command. This value will need to be cast
 *						to the structure that corresponds to the asynchronous command.  Null on error.
 *	@param udata 		User data that is forwarded from asynchronous command function.
 *	@param event_loop 	Event loop that this command was executed on.  Use this event loop when running
 *						nested asynchronous commands when single threaded behavior is desired for the
 *						group of commands.
 */
typedef void (*as_async_record_listener) (as_error* err, as_record* record, void* udata, as_event_loop* event_loop);

/**
 *	User callback when asynchronous read completes with an as_val result.
 *
 *	@param err			This error structure is only populated when the command fails. Null on success.
 *	@param val			The return value from the asynchronous command. This value will need to be cast
 *						to the structure that corresponds to the asynchronous command.  Null on error.
 *	@param udata 		User data that is forwarded from asynchronous command function.
 *	@param event_loop 	Event loop that this command was executed on.  Use this event loop when running
 *						nested asynchronous commands when single threaded behavior is desired for the
 *						group of commands.
 */
typedef void (*as_async_value_listener) (as_error* err, as_val* val, void* udata, as_event_loop* event_loop);

/******************************************************************************
 * PRIVATE TYPES
 *****************************************************************************/

#define AS_ASYNC_TYPE_WRITE 0
#define AS_ASYNC_TYPE_RECORD 1
#define AS_ASYNC_TYPE_VALUE 2
#define AS_ASYNC_TYPE_BATCH 3
#define AS_ASYNC_TYPE_SCAN 4
#define AS_ASYNC_TYPE_QUERY 5
	
#define AS_ASYNC_STATE_UNREGISTERED 0
#define AS_ASYNC_STATE_AUTH_WRITE 1
#define AS_ASYNC_STATE_AUTH_READ_HEADER 2
#define AS_ASYNC_STATE_AUTH_READ_BODY 3
#define AS_ASYNC_STATE_WRITE 4
#define AS_ASYNC_STATE_READ_HEADER 8
#define AS_ASYNC_STATE_READ_BODY 16

#define AS_AUTHENTICATION_MAX_SIZE 158

struct as_async_executor;
struct as_async_command;
	
typedef bool (*as_async_parse_results_fn) (struct as_async_command* cmd);
typedef void (*as_async_executor_complete_fn) (struct as_async_executor* executor, as_error* err);

typedef struct as_async_executor {
	struct as_async_command** commands;
	as_event_loop* event_loop;
	as_async_executor_complete_fn complete_fn;
	void* udata;
	uint32_t max_concurrent;
	uint32_t max;
	uint32_t count;
	bool valid;
} as_async_executor;
	
typedef struct as_async_command {
	as_event_command event;
	
	as_cluster* cluster;
	as_node* node;
	void* udata;
	as_async_parse_results_fn parse_results;

	uint8_t* buf;
	uint32_t capacity;
	uint32_t len;
	uint32_t pos;
	uint32_t auth_len;

	uint8_t type;
	uint8_t state;
	bool pipeline;
	bool deserialize;
	bool free_buf;
} as_async_command;

typedef struct as_async_write_command {
	as_async_command command;
	as_async_write_listener listener;
	uint8_t space[];
} as_async_write_command;
	
typedef struct as_async_record_command {
	as_async_command command;
	as_async_record_listener listener;
	uint8_t space[];
} as_async_record_command;

typedef struct as_async_value_command {
	as_async_command command;
	as_async_value_listener listener;
	uint8_t space[];
} as_async_value_command;

/******************************************************************************
 * PRIVATE FUNCTIONS
 *****************************************************************************/

static inline as_async_command*
as_async_write_command_create(
	as_cluster* cluster, as_node* node, uint32_t timeout_ms, bool deserialize,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop, bool pipeline,
	size_t size, as_async_parse_results_fn parse_results
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments.
	size_t s = (sizeof(as_async_write_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_async_command* cmd = cf_malloc(s);
	as_async_write_command* wcmd = (as_async_write_command*)cmd;
	cmd->event.event_loop = pipeline? node->pipeline_loop : as_event_assign(event_loop);
	cmd->event.fd = -1;
	cmd->event.timeout_ms = timeout_ms;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->buf = wcmd->space;
	cmd->capacity = (uint32_t)(s - sizeof(as_async_command));
	cmd->len = 0;
	cmd->pos = 0;
	cmd->auth_len = 0;
	cmd->type = AS_ASYNC_TYPE_WRITE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->pipeline = pipeline;
	cmd->deserialize = deserialize;
	cmd->free_buf = false;
	wcmd->listener = listener;
	return cmd;
}
	
static inline as_async_command*
as_async_record_command_create(
	as_cluster* cluster, as_node* node, uint32_t timeout_ms, bool deserialize,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop, bool pipeline,
	size_t size, as_async_parse_results_fn parse_results
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments to reduce fragmentation and to allow socket
	// read to reuse buffer for small socket write sizes.
	size_t s = (sizeof(as_async_record_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_async_command* cmd = cf_malloc(s);
	as_async_record_command* rcmd = (as_async_record_command*)cmd;
	cmd->event.event_loop = pipeline? node->pipeline_loop : as_event_assign(event_loop);
	cmd->event.fd = -1;
	cmd->event.timeout_ms = timeout_ms;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->buf = rcmd->space;
	cmd->capacity = (uint32_t)(s - sizeof(as_async_command));
	cmd->len = 0;
	cmd->pos = 0;
	cmd->auth_len = 0;
	cmd->type = AS_ASYNC_TYPE_RECORD;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->pipeline = pipeline;
	cmd->deserialize = deserialize;
	cmd->free_buf = false;
	rcmd->listener = listener;
	return cmd;
}

static inline as_async_command*
as_async_value_command_create(
   as_cluster* cluster, as_node* node, uint32_t timeout_ms, bool deserialize,
   as_async_value_listener listener, void* udata, as_event_loop* event_loop, bool pipeline,
   size_t size, as_async_parse_results_fn parse_results
   )
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments to reduce fragmentation and to allow socket
	// read to reuse buffer for small socket write sizes.
	size_t s = (sizeof(as_async_value_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_async_command* cmd = cf_malloc(s);
	as_async_value_command* vcmd = (as_async_value_command*)cmd;
	cmd->event.event_loop = pipeline? node->pipeline_loop : as_event_assign(event_loop);
	cmd->event.fd = -1;
	cmd->event.timeout_ms = timeout_ms;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->buf = vcmd->space;
	cmd->capacity = (uint32_t)(s - sizeof(as_async_command));
	cmd->len = 0;
	cmd->pos = 0;
	cmd->auth_len = 0;
	cmd->type = AS_ASYNC_TYPE_VALUE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->pipeline = pipeline;
	cmd->deserialize = deserialize;
	cmd->free_buf = false;
	vcmd->listener = listener;
	return cmd;
}

void
as_async_command_execute(as_async_command* cmd);

void
as_async_command_thread_execute(as_async_command* cmd);

bool
as_async_command_parse_header(as_async_command* cmd);

bool
as_async_command_parse_result(as_async_command* cmd);

bool
as_async_command_parse_success_failure(as_async_command* cmd);

void
as_async_command_send(as_async_command* cmd);
	
void
as_async_command_receive(as_async_command* cmd);
	
void
as_async_response_error(as_async_command* cmd, as_error* err);

void
as_async_timeout(as_async_command* cmd);
	
void
as_async_executor_complete(as_async_command* cmd);

#ifdef __cplusplus
} // end extern "C"
#endif
