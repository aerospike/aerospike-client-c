/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_command.h>
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
#define AS_ASYNC_TYPE_INFO 6
#define AS_ASYNC_TYPE_SCAN_PARTITION 7
#define AS_ASYNC_TYPE_QUERY_PARTITION 8
#define AS_ASYNC_TYPE_CONNECTOR 9
#define AS_ASYNC_TYPE_TXN_MONITOR 10

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

typedef struct as_async_info_command {
	as_event_command command;
	as_async_info_listener listener;
	uint8_t space[];
} as_async_info_command;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

static inline as_event_command*
as_async_write_command_create(
	as_cluster* cluster, const as_policy_base* policy, as_partition_info* pi,
	as_policy_replica replica, as_async_write_listener listener, void* udata,
	as_event_loop* event_loop, as_pipe_listener pipe_listener, size_t size,
	as_event_parse_results_fn parse_results, uint8_t* ubuf, uint32_t ubuf_size
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments.
	size_t s = (sizeof(as_async_write_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	as_async_write_command* wcmd = (as_async_write_command*)cmd;
	cmd->total_deadline = policy->total_timeout;
	cmd->socket_timeout = policy->socket_timeout;
	cmd->max_retries = policy->max_retries;
	cmd->iteration = 0;
	cmd->replica = as_command_write_replica(replica);
	cmd->event_loop = as_event_assign(event_loop);
	cmd->cluster = cluster;
	cmd->node = NULL;
	cmd->ns = pi->ns;
	cmd->partition = pi->partition;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->pipe_listener = pipe_listener;
	cmd->buf = wcmd->space;
	cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_write_command));
	cmd->type = AS_ASYNC_TYPE_WRITE;
	cmd->proto_type = AS_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->flags = 0;
	cmd->replica_size = pi->replica_size;
	cmd->replica_index = 0;
	cmd->txn = policy->txn;
	cmd->ubuf = ubuf;
	cmd->ubuf_size = ubuf_size;
	cmd->latency_type = AS_LATENCY_TYPE_WRITE;
	wcmd->listener = listener;
	as_cluster_add_command_count(cluster);
	return cmd;
}
	
static inline as_event_command*
as_async_record_command_create(
	as_cluster* cluster, const as_policy_base* policy, as_partition_info* pi,
	as_policy_replica replica, uint8_t replica_index, bool deserialize, bool heap_rec,
	uint8_t flags, as_async_record_listener listener, void* udata, as_event_loop* event_loop,
	as_pipe_listener pipe_listener, size_t size, as_event_parse_results_fn parse_results, 
	uint8_t type, as_latency_type latency_type, uint8_t* ubuf, uint32_t ubuf_size
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 4KB increments to reduce fragmentation and to allow socket
	// read to reuse buffer for small socket write sizes.
	size_t s = (sizeof(as_async_record_command) + size + AS_AUTHENTICATION_MAX_SIZE + 4095) & ~4095;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	as_async_record_command* rcmd = (as_async_record_command*)cmd;
	cmd->total_deadline = policy->total_timeout;
	cmd->socket_timeout = policy->socket_timeout;
	cmd->max_retries = policy->max_retries;
	cmd->iteration = 0;
	cmd->replica = replica;
	cmd->event_loop = as_event_assign(event_loop);
	cmd->cluster = cluster;
	cmd->node = NULL;
	cmd->ns = pi->ns;
	cmd->partition = pi->partition;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->pipe_listener = pipe_listener;
	cmd->buf = rcmd->space;
	cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_record_command));
	cmd->type = type;
	cmd->proto_type = AS_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->flags = flags;

	if (deserialize) {
		cmd->flags |= AS_ASYNC_FLAGS_DESERIALIZE;
	}

	if (heap_rec) {
		cmd->flags |= AS_ASYNC_FLAGS_HEAP_REC;
	}

	cmd->replica_size = pi->replica_size;
	cmd->replica_index = replica_index;
	cmd->txn = policy->txn;
	cmd->ubuf = ubuf;
	cmd->ubuf_size = ubuf_size;
	cmd->latency_type = latency_type;
	rcmd->listener = listener;
	as_cluster_add_command_count(cluster);
	return cmd;
}

static inline as_event_command*
as_async_value_command_create(
	as_cluster* cluster, const as_policy_base* policy, as_partition_info* pi,
	as_policy_replica replica, as_async_value_listener listener, void* udata,
	as_event_loop* event_loop, as_pipe_listener pipe_listener, size_t size,
	as_event_parse_results_fn parse_results, uint8_t* ubuf, uint32_t ubuf_size
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 4KB increments to reduce fragmentation and to allow socket
	// read to reuse buffer for small socket write sizes.
	size_t s = (sizeof(as_async_value_command) + size + AS_AUTHENTICATION_MAX_SIZE + 4095) & ~4095;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	as_async_value_command* vcmd = (as_async_value_command*)cmd;
	cmd->total_deadline = policy->total_timeout;
	cmd->socket_timeout = policy->socket_timeout;
	cmd->max_retries = policy->max_retries;
	cmd->iteration = 0;
	cmd->replica = as_command_write_replica(replica);
	cmd->event_loop = as_event_assign(event_loop);
	cmd->cluster = cluster;
	cmd->node = NULL;
	cmd->ns = pi->ns;
	cmd->partition = pi->partition;
	cmd->udata = udata;
	cmd->parse_results = parse_results;
	cmd->pipe_listener = pipe_listener;
	cmd->buf = vcmd->space;
	cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_value_command));
	cmd->type = AS_ASYNC_TYPE_VALUE;
	cmd->proto_type = AS_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->flags = 0;
	cmd->replica_size = pi->replica_size;
	cmd->replica_index = 0;
	cmd->txn = policy->txn;
	cmd->ubuf = ubuf;
	cmd->ubuf_size = ubuf_size;
	cmd->latency_type = AS_LATENCY_TYPE_WRITE;
	vcmd->listener = listener;
	as_cluster_add_command_count(cluster);
	return cmd;
}

static inline as_event_command*
as_async_info_command_create(
	as_node* node, const as_policy_info* policy, as_async_info_listener listener, void* udata,
	as_event_loop* event_loop, size_t size
	)
{
	// Allocate enough memory to cover: struct size + write buffer size + auth max buffer size
	// Then, round up memory size in 1KB increments.
	size_t s = (sizeof(as_async_info_command) + size + AS_AUTHENTICATION_MAX_SIZE + 1023) & ~1023;
	as_event_command* cmd = (as_event_command*)cf_malloc(s);
	as_async_info_command* icmd = (as_async_info_command*)cmd;
	cmd->total_deadline = policy->timeout;
	cmd->socket_timeout = policy->timeout;
	cmd->max_retries = 1;
	cmd->iteration = 0;
	cmd->replica = AS_POLICY_REPLICA_MASTER;
	cmd->event_loop = as_event_assign(event_loop);
	cmd->cluster = node->cluster;
	cmd->node = node;
	cmd->ns = NULL;
	cmd->partition = NULL;
	cmd->udata = udata;
	cmd->parse_results = as_event_command_parse_info;
	cmd->pipe_listener = NULL;
	cmd->buf = icmd->space;
	cmd->read_capacity = (uint32_t)(s - size - sizeof(as_async_info_command));
	cmd->type = AS_ASYNC_TYPE_INFO;
	cmd->proto_type = AS_INFO_MESSAGE_TYPE;
	cmd->state = AS_ASYNC_STATE_UNREGISTERED;
	cmd->flags = 0;
	cmd->replica_size = 1;
	cmd->replica_index = 0;
	cmd->txn = NULL;
	cmd->ubuf = NULL;
	cmd->ubuf_size = 0;
	cmd->latency_type = AS_LATENCY_TYPE_NONE;
	icmd->listener = listener;
	as_cluster_add_command_count(node->cluster);
	return cmd;
}

#ifdef __cplusplus
} // end extern "C"
#endif
