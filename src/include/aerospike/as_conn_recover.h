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
#include <aerospike/as_timeout_ctx.h>
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
	uint8_t* buffer_rc;
	uint32_t capacity;
	uint32_t offset;
	as_read_state state;
	uint32_t timeout_delay;
	bool is_single;
	bool check_return_code;
	bool last_group;
	uint8_t* header_buf;
	uint32_t length;
	as_node* node;

	/**
	 * @private
	 * The socket to be recovered. This structure is considered read-only,
	 * side-effects of using the socket notwithstanding.
	 */
	as_socket socket;

	uint32_t socket_timeout;
	uint64_t deadline;

	/**
	 * @private
	 * Records the most recent time when the socket was used. Although the socket
	 * has a last_used field within it, it overlaps with a pool pointer, which we
	 * need to recover the socket. This field is used while the socket is being
	 * recovered instead.
	 */
	uint64_t socket_last_used;
} as_conn_recover;

//---------------------------------
// Functions
//---------------------------------

/**
 * @private
 * Initialize a connection recover record on the stack or on the heap.
 */
as_conn_recover*
as_conn_recover_init(
	as_conn_recover* self, as_timeout_ctx* timeout_ctx, uint32_t timeout_delay, bool is_single,
	as_node* node, as_socket* socket, uint32_t socket_timeout, uint64_t deadline_ns
	);

/**
 * @private
 * Allocate and initialize a connection recover record on the heap.
 * Use as_conn_recover_release() to dispose of the recover record.
 */
static inline as_conn_recover*
as_conn_recover_new(
	as_timeout_ctx* timeout_ctx, uint32_t timeout_delay, bool is_single, as_node* node,
	as_socket* socket, uint32_t socket_timeout, uint64_t deadline_ns
	)
{
	return as_conn_recover_init(
		(as_conn_recover*)cf_rc_alloc(sizeof(as_conn_recover)),
		timeout_ctx, timeout_delay, is_single, node, socket,
		socket_timeout, deadline_ns);
}

/**
 * @private
 * Dispose of a heap-allocated as_conn_recover instance.
 */
static inline void
as_conn_recover_release(as_conn_recover* self)
{
	if (! cf_rc_release(self)) {
		if (self->buffer_rc) {
			cf_rc_releaseandfree(self->buffer_rc);
		}

		if (self->header_buf) {
			cf_rc_releaseandfree(self->header_buf);
		}

		if (self->node) {
			as_node_release(self->node);
			self->node = NULL;
		}
	}
}

/**
 * @private
 * Attempt to drain a connection.  Answer true if successfully drained;
 * false otherwise.
 */
bool
as_conn_recover_drain(as_conn_recover* self);

/**
 * @private
 * Copy a header buffer into a locally managed working buffer.
 */
static inline void
as_conn_recover_copy_header_buffer(as_conn_recover* self)
{
	if (! self->header_buf ) {
		self->header_buf = cf_rc_alloc(self->length);
	}
	memcpy(self->header_buf, self->buffer_rc, self->offset);
}

/**
 * @private
 * Abort a connection recovery process.
 */
static inline void
as_conn_recover_abort(as_conn_recover* self) {
	self->state = AS_READ_STATE_COMPLETE;
	as_node_incr_sync_conns_aborted(self->node);
	as_node_close_conn_error(self->node, &self->socket, self->socket.pool);
}

/**
 * @private
 * If we treat the buffer contents as an as_proto structure,
 * return the size field of that structure.
 */
static inline uint64_t
as_conn_recover_get_proto_size(as_conn_recover* self) {
	return ((as_proto*)self->buffer_rc)->sz;
}

/**
 * @private
 * "Parse" an as_proto object (in the buffer_rc buffer) to further initialize
 * the as_conn_recover instance.  Answers false if an error occurred.
 */
static inline bool
as_conn_recover_parse_proto(as_conn_recover* self) {
	as_proto* proto = (as_proto*)self->buffer_rc;

	if (! self->is_single) {
		if (proto->type == AS_COMPRESSED_MESSAGE_TYPE) {
			// Do not recover connections with compressed data b/c
			// that would require saving large buffers with associated
			// state and performing decompression just to drain the
			// connection.

			return false;
		}

		// WARNING: The following code assumes multi-record responses
		// always end with a separate proto that only contains one header
		// with the info3 last group bit.  This is always true for batch
		// and scan, but query does not conform.  Therefore, connection
		// recovery for queries will likely fail.

		uint8_t info3 = self->buffer_rc[self->length - 1];
		if (info3 & AS_MSG_INFO3_LAST) {
			self->last_group = true;
		}
	}

	self->length = (uint32_t)proto->sz - (self->offset - 8);
	self->offset = 0;
	self->state = AS_READ_STATE_DETAIL;

	return true;
}

/**
 * @private
 * Mark a connection as fully recovered and put it back into rotation.
 */
static inline void
as_conn_recover_recover(as_conn_recover* self)
{
	// as_node_put_connection() updates the last_used field of the socket for us.
	as_node_put_connection(self->node, &self->socket);
	as_node_incr_sync_conns_recovered(self->node);
	self->state = AS_READ_STATE_COMPLETE;
}

#ifdef __cplusplus
} // end extern "C"
#endif
