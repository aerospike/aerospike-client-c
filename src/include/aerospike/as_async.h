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
#include <aerospike/as_queue.h>
#include <citrusleaf/alloc.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * PUBLIC TYPES
 *****************************************************************************/

/**
 *	User callback when asynchronous command completes.
 *
 *	@param err			This error structure is only populated when the command fails. Null on success.
 *	@param result 		The return value from the asynchronous command. This value will need to be cast
 *						to the structure that corresponds to the asynchronous command.  Null on error.
 *	@param udata 		User data that is forwarded from asynchronous command function.
 *	@param event_loop 	Event loop that this command was executed on.  Use this event loop when running
 *						nested asynchronous commands when single threaded behavior is desired for the
 *						group of commands.
 */
typedef void (*as_async_callback_fn) (as_error* err, void* result, void* udata, as_event_loop* event_loop);
	
/******************************************************************************
 * PRIVATE TYPES
 *****************************************************************************/

#define AS_ASYNC_TYPE_RECORD 0
#define AS_ASYNC_TYPE_BATCH 1
#define AS_ASYNC_TYPE_SCAN 2
#define AS_ASYNC_TYPE_QUERY 3
	
#define AS_ASYNC_STATE_UNREGISTERED 0
#define AS_ASYNC_STATE_AUTH_WRITE 1
#define AS_ASYNC_STATE_AUTH_READ_HEADER 2
#define AS_ASYNC_STATE_AUTH_READ_BODY 3
#define AS_ASYNC_STATE_WRITE 4
#define AS_ASYNC_STATE_READ_HEADER 8
#define AS_ASYNC_STATE_READ_BODY 16

#define AS_AUTHENTICATION_MAX_SIZE 158
	
struct as_async_command;
	
typedef void (*as_async_parse_results_fn) (struct as_async_command* cmd);

typedef struct as_async_command {
	as_event_command event;
	
	as_cluster* cluster;
	as_node* node;
	as_async_callback_fn ucb;
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
	bool release_node;
	bool in_authenticate;
	bool free_buf;
} as_async_command;
	
typedef struct {
	as_async_command cmd;
	uint8_t space[];
} as_async_record_command;

typedef struct {
	as_async_command cmd;
	// TODO add batch args.
	uint8_t space[];
} as_async_batch;

/******************************************************************************
 * PRIVATE FUNCTIONS
 *****************************************************************************/

static inline as_async_command*
as_async_record_command_create(size_t size, as_cluster* cluster, as_node* node,
	uint32_t timeout_ms, bool deserialize, as_event_loop* event_loop, bool pipeline,
	as_async_callback_fn ucb, void* udata, as_async_parse_results_fn parse_results)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments to reduce fragmentation and to allow socket
	// read to reuse buffer for small socket write sizes.
	size_t s = (sizeof(as_async_record_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_async_command* cmd = cf_malloc(s);
	cmd->event.event_loop = event_loop;
	cmd->event.fd = -1;
	cmd->event.timeout_ms = timeout_ms;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->ucb = ucb;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->buf = ((as_async_record_command*)cmd)->space;
	cmd->capacity = (uint32_t)(s - sizeof(as_async_command));
	cmd->len = 0;
	cmd->pos = 0;
	cmd->auth_len = 0;
	cmd->type = AS_ASYNC_TYPE_RECORD;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->pipeline = pipeline;
	cmd->deserialize = deserialize;
	cmd->release_node = true;
	cmd->in_authenticate = false;
	cmd->free_buf = false;
	return cmd;
}

void
as_async_command_assign(as_async_command* cmd, size_t size);

void
as_async_command_execute(as_async_command* cmd);

void
as_async_command_parse_header(as_async_command* cmd);

void
as_async_command_parse_result(as_async_command* cmd);

void
as_async_command_parse_success_failure(as_async_command* cmd);

void
as_async_command_send(as_async_command* cmd);
	
void
as_async_command_receive(as_async_command* cmd);

void
as_async_timeout(as_async_command* cmd);
	
void
as_async_error(as_async_command* cmd, as_error* err);

#ifdef __cplusplus
} // end extern "C"
#endif
