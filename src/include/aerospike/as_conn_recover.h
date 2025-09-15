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

#include <aerospike/as_command.h>
#include <aerospike/as_node.h>
#include <aerospike/as_socket.h>
#include <citrusleaf/alloc.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * @private
 * Record a connection which has gone into a timeout state, and which we should
 * attempt to recover. A recovered connection saves us the overhead of having
 * to close and re-open a TCP connection.
 *
 * As connections need recovery, they are queued onto an as_cluster structure.
 * See as_cluster for more information.
 */
typedef struct as_conn_recover_s {
	as_socket socket;
	as_node* node;
	uint8_t* header_buf;
	uint64_t deadline_ns;
	as_read_state state;
	uint32_t length;
	uint32_t offset;
	bool is_single;
	bool check_return_code;
	bool last_group;
} as_conn_recover;

//---------------------------------
// Functions
//---------------------------------

/**
 * @private
 * Allocate and initialize a connection recover record on the heap.
 * Use as_conn_recover_destroy() to dispose of the recover record.
 */
as_conn_recover*
as_conn_recover_create(
	as_socket* socket, as_socket_context* ctx, as_node* node, uint8_t* buf, size_t buf_len
	);

/**
 * @private
 * Dispose of a heap-allocated as_conn_recover instance.
 */
void
as_conn_recover_destroy(as_conn_recover* self);

/**
 * @private
 * Attempt to drain a connection.  Return true if successfully drained or the recover was aborted.
 * Return false if the draining has not completed.
 */
bool
as_conn_recover_drain(as_conn_recover* self);

#ifdef __cplusplus
} // end extern "C"
#endif
