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

#include <aerospike/as_async_proto.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_listener.h>
#include <citrusleaf/alloc.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

#define AS_ASYNC_TYPE_WRITE 0
#define AS_ASYNC_TYPE_RECORD 1
#define AS_ASYNC_TYPE_VALUE 2
#define AS_ASYNC_TYPE_BATCH 3
#define AS_ASYNC_TYPE_SCAN 4
#define AS_ASYNC_TYPE_QUERY 5
	
#define AS_AUTHENTICATION_MAX_SIZE 158

#define AS_ASYNC_CONNECTION_COMPLETE 0
#define AS_ASYNC_CONNECTION_PENDING 1
#define AS_ASYNC_CONNECTION_ERROR 2
	
typedef struct as_async_write_command {
	as_event_command command;
	as_async_write_listener listener;
	uint8_t space[];
} as_async_write_command;
	
typedef struct as_async_record_command {
	as_event_command command;
	as_async_record_listener listener;
	uint8_t space[];
} as_async_record_command;

typedef struct as_async_value_command {
	as_event_command command;
	as_async_value_listener listener;
	uint8_t space[];
} as_async_value_command;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static inline as_event_command*
as_async_write_command_create(
	as_cluster* cluster, as_node* node, uint32_t timeout_ms, bool deserialize,
	as_async_write_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener, size_t size, as_event_parse_results_fn parse_results
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments.
	size_t s = (sizeof(as_async_write_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	as_async_write_command* wcmd = (as_async_write_command*)cmd;
	cmd->event_loop = as_event_assign(event_loop);
	cmd->conn = 0;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->buf = wcmd->space;
	cmd->capacity = (uint32_t)(s - sizeof(as_async_write_command));
	cmd->len = 0;
	cmd->pos = 0;
	cmd->auth_len = 0;
	cmd->timeout_ms = timeout_ms;
	cmd->type = AS_ASYNC_TYPE_WRITE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->pipe_listener = pipe_listener;
	cmd->deserialize = deserialize;
	cmd->free_buf = false;
	wcmd->listener = listener;
	return cmd;
}
	
static inline as_event_command*
as_async_record_command_create(
	as_cluster* cluster, as_node* node, uint32_t timeout_ms, bool deserialize,
	as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener, size_t size, as_event_parse_results_fn parse_results
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments to reduce fragmentation and to allow socket
	// read to reuse buffer for small socket write sizes.
	size_t s = (sizeof(as_async_record_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	as_async_record_command* rcmd = (as_async_record_command*)cmd;
	cmd->event_loop = as_event_assign(event_loop);
	cmd->conn = 0;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->buf = rcmd->space;
	cmd->capacity = (uint32_t)(s - sizeof(as_async_record_command));
	cmd->len = 0;
	cmd->pos = 0;
	cmd->auth_len = 0;
	cmd->timeout_ms = timeout_ms;
	cmd->type = AS_ASYNC_TYPE_RECORD;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->pipe_listener = pipe_listener;
	cmd->deserialize = deserialize;
	cmd->free_buf = false;
	rcmd->listener = listener;
	return cmd;
}

static inline as_event_command*
as_async_value_command_create(
   as_cluster* cluster, as_node* node, uint32_t timeout_ms, bool deserialize,
   as_async_value_listener listener, void* udata, as_event_loop* event_loop,
   as_pipe_listener pipe_listener, size_t size, as_event_parse_results_fn parse_results
   )
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments to reduce fragmentation and to allow socket
	// read to reuse buffer for small socket write sizes.
	size_t s = (sizeof(as_async_value_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	as_async_value_command* vcmd = (as_async_value_command*)cmd;
	cmd->event_loop = as_event_assign(event_loop);
	cmd->conn = 0;
	cmd->cluster = cluster;
	cmd->node = node;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->buf = vcmd->space;
	cmd->capacity = (uint32_t)(s - sizeof(as_async_value_command));
	cmd->len = 0;
	cmd->pos = 0;
	cmd->auth_len = 0;
	cmd->timeout_ms = timeout_ms;
	cmd->type = AS_ASYNC_TYPE_VALUE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->pipe_listener = pipe_listener;
	cmd->deserialize = deserialize;
	cmd->free_buf = false;
	vcmd->listener = listener;
	return cmd;
}
	
#ifdef __cplusplus
} // end extern "C"
#endif
