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

#include <aerospike/as_conn_recover.h>

//---------------------------------
// Functions
//---------------------------------

as_conn_recover*
as_conn_recover_init(
		as_conn_recover* self, as_timeout_ctx* timeout_ctx,
		uint32_t timeout_delay, bool is_single, as_node* node,
		as_socket* socket, uint32_t socket_timeout, uint64_t deadline_ns)
{
	if (! self) {
		return self;
	}

	memset(self, 0, sizeof(as_conn_recover));

	cf_rc_reserve(timeout_ctx->buffer_rc);
	self->buffer_rc = timeout_ctx->buffer_rc;
	self->capacity = timeout_ctx->capacity;
	self->offset = timeout_ctx->offset;
	self->state = timeout_ctx->state;
	self->timeout_delay = timeout_delay;
	self->is_single = is_single;
	self->check_return_code = false;
	as_node_reserve(node);
	self->node = node;
	self->socket = *socket;
	self->socket_timeout = socket_timeout;
	self->deadline = deadline_ns;

	switch(self->state) {
	case AS_READ_STATE_AUTH_HEADER:
		self->length = 10;
		self->is_single = true;
		self->check_return_code = true;
		self->state = AS_READ_STATE_PROTO;

		if (self->offset >= self->length) {
			if (timeout_ctx->buffer_rc[self->length - 1] != 0) {
				// Auth failed.
				as_conn_recover_abort(self);
				return self;
			}
			self->length =
					as_conn_recover_get_proto_size(self) - (self->offset - 8);
			self->offset = 0;
			self->state = AS_READ_STATE_DETAIL;
		}
		else if (self->offset > 0) {
			as_conn_recover_copy_header_buffer(self);
		}
		break;

	case AS_READ_STATE_PROTO:
		// For multi-record responses, adjust length to cover last group info3
		// bit at offset 11.
		self->length = is_single ? 8 : 12;

		if (self->offset >= self->length) {
			if (! as_conn_recover_parse_proto(self)) {
				as_conn_recover_abort(self);
				return self;
			}
		}
		else if (self->offset > 0) {
			as_conn_recover_copy_header_buffer(self);
		}
		break;

	case AS_READ_STATE_DETAIL:
	default:
		self->length = timeout_ctx->capacity;
		break;
	}

	self->socket_last_used = cf_getns();
	self->socket_timeout = 1; // millisecond

	return self;
}


static void
as_conn_recover_drain_detail(as_conn_recover* self, bool* must_abort,
		bool* timeout_exception)
{
	uint32_t remainder = self->length - self->offset;
	uint32_t length = (remainder <= self->length) ? remainder : self->length;

	// The as_socket_read_deadline() function includes a while loop that will
	// ensure we read as much as we can.
	as_error err;
	as_status status = as_socket_read_deadline(
			&err, &self->socket, self->node, self->buffer_rc, length,
			self->socket_timeout, self->deadline);
	if (status != AEROSPIKE_OK) {
		if (status == AEROSPIKE_ERR_TIMEOUT) {
			*timeout_exception = true;
		}
		else {
			*must_abort = true;
		}
		return;
	}
	self->offset += self->socket.offset;
}

static void
as_conn_recover_drain_header(as_conn_recover* self, bool* must_abort, bool* timeout_exception)
{
	bool started_with_buffer_rc = (self->offset == 0);
	uint8_t* b = (started_with_buffer_rc) ? self->buffer_rc : self->header_buf;

	while (true) {
		as_error err;

		as_status status = as_socket_read_deadline(
				&err, &self->socket, self->node, b, self->length,
				self->socket_timeout, self->deadline);
		if (status != AEROSPIKE_OK) {
			if (status == AEROSPIKE_ERR_TIMEOUT) {
				*timeout_exception = true;
			}
			else {
				*must_abort = true;
			}

			return;
		}
		// socket offset set to 0 in read_deadline, and is incremented as the read completes.
		// Thus, at end of call, socket offset = total number of bytes read.
		self->offset += self->socket.offset;

		if (self->offset >= self->length) {
			break;
		}

		if (started_with_buffer_rc) {
			as_conn_recover_copy_header_buffer(self);
			b = self->header_buf;
		}
	}

	if (self->check_return_code) {
		if (b[self->length - 1] != 0) {
			*must_abort = true;
			return;
		}
	}

	if (! as_conn_recover_parse_proto(self)) {
		*must_abort = true;
		return;
	}
}


static bool
as_conn_recover_try_drain(as_conn_recover* self, bool* must_abort,
		bool* timeout_exception)
{
	bool connection_drained = false;

	if (self->is_single) {
		if (self->state == AS_READ_STATE_PROTO) {
			as_conn_recover_drain_header(self, must_abort, timeout_exception);
			if (*must_abort || *timeout_exception) {
				goto exception;
			}
		}

		as_conn_recover_drain_detail(self, must_abort, timeout_exception);
		if (*must_abort || *timeout_exception) {
			goto exception;
		}

		as_conn_recover_recover(self);

		connection_drained = true;
	}
	else {
		while (true) {
			if (self->state == AS_READ_STATE_PROTO) {
				as_conn_recover_drain_header(self, must_abort, timeout_exception);
				if (*must_abort || *timeout_exception) {
					goto exception;
				}
			}
			as_conn_recover_drain_detail(self, must_abort, timeout_exception);
			if (*must_abort || *timeout_exception) {
				goto exception;
			}

			if (self->last_group) {
				break;
			}

			self->length = 12;
			self->offset = 0;
			self->state = AS_READ_STATE_PROTO;
		}
		as_conn_recover_recover(self);
		connection_drained = true;
	}

exception:
	return connection_drained;
}


bool
as_conn_recover_drain(as_conn_recover* self)
{
	bool must_abort = false;
	bool timeout_exception = false;

	bool connection_drained =
			as_conn_recover_try_drain(self, &must_abort, &timeout_exception);

	if (timeout_exception) {
		uint64_t current_time_ns = cf_getns();

		if (current_time_ns - self->socket_last_used >= self->timeout_delay * 1000) {
			// Forcibly close the connection.
			must_abort = true;
		}
		else {
			// Put back on queue for later draining.
			return false;
		}
	}

	if (must_abort) {
		as_conn_recover_abort(self);
		return true;
	}

	return connection_drained;
}

