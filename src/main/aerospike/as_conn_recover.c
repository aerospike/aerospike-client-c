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
// Static Functions
//---------------------------------

static void
log_ts(const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	char fmtbuf[1024];
	struct timespec now;
	clock_gettime(CLOCK_REALTIME, &now);

	struct tm* t = localtime(&now.tv_sec);
	uint64_t msecs = now.tv_nsec / 1000000;
	int len = sprintf(fmtbuf, "%d-%02d-%02d %02d:%02d:%02d.%03" PRIu64 " ",
		t->tm_year+1900, t->tm_mon+1, t->tm_mday, t->tm_hour, t->tm_min,
		t->tm_sec, msecs);

	size_t len2 = strlen(fmt);
	char* p = fmtbuf + len;
	memcpy(p, fmt, len2);
	p += len2;
	*p = 0;

    vprintf(fmtbuf, ap);
    printf("\n");
	va_end(ap);
}


static inline void
as_conn_recover_copy_header_buffer(as_conn_recover* self, uint8_t* buf)
{
	if (! self->header_buf) {
		self->header_buf = cf_malloc(self->length);
	}
	memcpy(self->header_buf, buf, self->offset);
}

static inline size_t
as_conn_recover_get_proto_size(uint8_t* buf)
{
	return (size_t)((as_proto*)buf)->sz;
}

/**
 * @private
 * "Parse" an as_proto object (in the buffer_rc buffer) to further initialize
 * the as_conn_recover instance.  Answers false if an error occurred.
 */
static bool
as_conn_recover_parse_proto(as_conn_recover* self, uint8_t* buf)
{
	as_proto* proto = (as_proto*)buf;

	if (! self->is_single) {
		// The last group trailer will never be compressed.
		if (proto->type == AS_COMPRESSED_MESSAGE_TYPE) {
			// Do not recover connections with compressed data because that would
			// require saving large buffers with associated state and performing decompression
			// just to drain the connection.
			return false;
		}

		// Warning: The following code assumes multi-record responses always end with a separate proto
		// that only contains one header with the info3 last group bit. This is always true for batch
		// and scan, but query does not conform. Therefore, connection recovery for queries will
		// likely fail.
		uint8_t info3 = buf[self->length - 1];

		if (info3 & AS_MSG_INFO3_LAST) {
			self->last_group = true;
		}
	}

	as_proto_swap_from_be(proto);
	self->length = proto->sz - (self->offset - 8);
	self->offset = 0;
	self->state = AS_READ_STATE_DETAIL;
	log_ts("DETAIL SIZE %zd", self->length);
	return true;
}

static void
as_conn_recover_abort(as_conn_recover* self)
{
	log_ts("CONN ABORTED");
	as_node_incr_sync_conns_aborted(self->node);
	as_node_close_conn_error(self->node, &self->socket, self->socket.pool);
	self->state = AS_READ_STATE_COMPLETE;
}

/**
 * @private
 * Mark a connection as fully recovered and put it back into rotation.
 */
static void
as_conn_recover_recover(as_conn_recover* self)
{
	log_ts("CONN RECOVERED");
	// as_node_put_connection() updates the last_used field of the socket for us.
	as_node_put_connection(self->node, &self->socket);
	as_node_incr_sync_conns_recovered(self->node);
	self->state = AS_READ_STATE_COMPLETE;
}

static int
as_conn_recover_drain_header(as_conn_recover* self)
{
	uint8_t* buf;
	bool on_heap;

	if (self->offset == 0) {
		buf = alloca(self->length);
		on_heap = false;
	}
	else {
		buf = self->header_buf;
		on_heap = true;
	}

	int len = as_socket_read_non_blocking(&self->socket, buf, self->length - self->offset);

	log_ts("DRAIN HEADER %d", len);

	if (len < 0) {
		return -1;
	}

	self->offset += len;

	if (self->offset < self->length) {
		// Drain not complete.
		if (! on_heap) {
			// Convert to heap allocated header buf.
			as_conn_recover_copy_header_buffer(self, buf);
		}
		return 0; // Drain header not complete.
	}

	// Header drained.
	if (self->check_return_code) {
		if (buf[self->length - 1] != 0) {
			return -1;
		}
	}

	if (! as_conn_recover_parse_proto(self, buf)) {
		return -1;
	}

	return 1; // Drain header complete.
}

static int
as_conn_recover_drain_detail(as_conn_recover* self)
{
	size_t rem = self->length - self->offset;
	size_t max = 16 * 1024;
	uint8_t* buf = alloca(max);

	while (rem > 0) {
		size_t req_size = (rem < max) ? rem : max;
		log_ts("DRAIN REQ %zd", req_size);
		int len = as_socket_read_non_blocking(&self->socket, buf, req_size);

		log_ts("DRAIN DETAIL %d", len);

		if (len < 0) {
			return -1;
		}

		if (len == 0) {
			return 0; // Drain detail not complete.
		}

		self->offset += len;
		rem = self->length - self->offset;
	}
	return 1; // Drain detail complete.
}

static bool
as_conn_recover_error(as_conn_recover* self, int rv)
{
	if (rv < 0) {
		as_conn_recover_abort(self);
		return true;
	}

	if (rv == 0) {
		if (cf_getns() < self->deadline_ns) {
			return false;
		}
		else {
			as_conn_recover_abort(self);
			return true;
		}
	}
	return true;
}

static inline uint64_t
as_clock_ms_to_ns(uint64_t ms)
{
	return (uint64_t)ms * 1000 * 1000;
}

//---------------------------------
// Functions
//---------------------------------

as_conn_recover*
as_conn_recover_create(
	as_socket* socket, as_socket_context* ctx, as_node* node, uint8_t* buf, size_t buf_len
	)
{
	log_ts("ATTEMPT CONN RECOVER");
	as_conn_recover* self = cf_malloc(sizeof(as_conn_recover));

	memset(self, 0, sizeof(as_conn_recover));
	self->socket = *socket;
	as_node_reserve(node);
	self->node = node;
	self->offset = ctx->offset;
	self->state = ctx->state;
	self->is_single = ctx->is_single;
	self->check_return_code = false;

	switch(self->state) {
	case AS_READ_STATE_AUTH_HEADER:
		self->length = 10;
		self->is_single = true;
		self->check_return_code = true;
		self->state = AS_READ_STATE_PROTO;

		if (self->offset >= self->length) {
			if (buf[self->length - 1] != 0) {
				// Authentication failed.
				as_conn_recover_destroy(self);
				return NULL;
			}
			self->length = as_conn_recover_get_proto_size(buf) - (self->offset - 8);
			self->offset = 0;
			self->state = AS_READ_STATE_DETAIL;
		}
		else if (self->offset > 0) {
			as_conn_recover_copy_header_buffer(self, buf);
		}
		break;

	case AS_READ_STATE_PROTO:
		// Extend header length to 12 for multi-record responses to include
		// last group info3 bit at offset 11.
		self->length = self->is_single ? 8 : 12;

		if (self->offset >= self->length) {
			if (! as_conn_recover_parse_proto(self, buf)) {
				as_conn_recover_destroy(self);
				return NULL;
			}
		}
		else if (self->offset > 0) {
			as_conn_recover_copy_header_buffer(self, buf);
		}
		break;

	case AS_READ_STATE_DETAIL:
	default:
		self->length = buf_len;
		break;
	}

	uint64_t now = cf_getns();
	self->deadline_ns = now + as_clock_ms_to_ns(ctx->timeout_delay);
	log_ts("TIMEOUTDELAY=%u NOW=%" PRIu64 " DEADLINENS=%" PRIu64, ctx->timeout_delay, now, self->deadline_ns);

	return self;
}

void
as_conn_recover_destroy(as_conn_recover* self)
{
	if (self->header_buf) {
		cf_free(self->header_buf);
	}
	as_node_release(self->node);
	cf_free(self);
}

bool
as_conn_recover_drain(as_conn_recover* self)
{
	log_ts("DRAIN CONN");
	int rv;

	if (self->is_single) {
		if (self->state == AS_READ_STATE_PROTO) {
			rv = as_conn_recover_drain_header(self);

			if (rv <= 0) {
				return as_conn_recover_error(self, rv);
			}
		}

		rv = as_conn_recover_drain_detail(self);

		if (rv <= 0) {
			return as_conn_recover_error(self, rv);
		}
		as_conn_recover_recover(self);
	}
	else {
		while (true) {
			if (self->state == AS_READ_STATE_PROTO) {
				rv = as_conn_recover_drain_header(self);

				if (rv <= 0) {
					return as_conn_recover_error(self, rv);
				}
			}

			rv = as_conn_recover_drain_detail(self);

			if (rv <= 0) {
				return as_conn_recover_error(self, rv);
			}

			if (self->last_group) {
				break;
			}

			self->length = 12;
			self->offset = 0;
			self->state = AS_READ_STATE_PROTO;
		}
		as_conn_recover_recover(self);
	}
	return true;
}
