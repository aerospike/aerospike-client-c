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
as_conn_recover_init(as_conn_recover* self, as_timeout_ctx* timeout_ctx, uint32_t timeout_delay, bool is_single,
                     as_node* node, as_socket* socket, uint32_t socket_timeout, uint64_t deadline_ns)
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
                        self->length = as_conn_recover_get_proto_size(self) - (self->offset - 8);
                        self->offset = 0;
                        self->state = AS_READ_STATE_DETAIL;
                }
                else if (self->offset > 0) {
                        as_conn_recover_copy_header_buffer(self);
                }
                break;

        case AS_READ_STATE_PROTO:
                // For multi-record responses, adjust length to cover last group info3 bit at offset 11.
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
as_conn_recover_drain_detail(as_conn_recover* self, bool* must_abort, bool* timeout_exception) {
        fprintf(stderr, "ASDDT001 as_conn_recover_drain_detail entered\n");
        uint32_t remainder = self->length - self->offset;
        uint32_t length = (remainder <= self->length) ? remainder : self->length;

        // The as_socket_read_deadline() function includes a while loop that will ensure we read as much as we can.
        as_error err;
        fprintf(stderr, "ASDDT003 Attempting to call as_socket_read_deadline(%p, %d, %p, %p, %lu, %lu, %u)\n", &err, &self->socket, self->node, self->buffer_rc, length, self->socket_timeout, self->deadline);
        as_status status = as_socket_read_deadline(&err, &self->socket, self->node, self->buffer_rc, length, self->socket_timeout, self->deadline);
        if (status != AEROSPIKE_OK) {
                if (status == AEROSPIKE_ERR_TIMEOUT) {
                        fprintf(stderr, "ASDDT004 Timeout detected\n");
                        *timeout_exception = true;
                }
                else {
                        fprintf(stderr, "ASDDT005 Other error detected: %d\n", status);
                        *must_abort = true;
                }
                fprintf(stderr, "ASDDT006 as_conn_recover_drain_detail left abnormally\n");
                return;
        }
        self->offset += self->socket.offset;
        fprintf(stderr, "ASDDT002 as_conn_recover_drain_detail left normally\n");
}

static void
as_conn_recover_drain_header(as_conn_recover* self, bool* must_abort, bool* timeout_exception) {
        fprintf(stderr, "ASDHD001 as_conn_recover_drain_header entered\n");
        bool started_with_buffer_rc = (self->offset == 0);
        uint8_t* b = (started_with_buffer_rc) ? self->buffer_rc : self->header_buf;

        while (true) {
                as_error err;

                fprintf(stderr, "ASDHD010 as_socket_read_deadline(%p, %p, %p, %p, %lu, %lu, %lu\n", &err, &self->socket, self->node, b, self->length, self->socket_timeout, self->deadline);
                as_status status = as_socket_read_deadline(&err, &self->socket, self->node, b, self->length, self->socket_timeout, self->deadline);
                if (status != AEROSPIKE_OK) {
                        if (status == AEROSPIKE_ERR_TIMEOUT) {
                                fprintf(stderr, "ASDHD011 Timeout detected\n");
                                *timeout_exception = true;
                        }
                        else {
                                fprintf(stderr, "ASDHD012 Other error detected: %d\n", status);
                                *must_abort = true;
                        }

                        fprintf(stderr, "ASDHD013 as_conn_recover_drain_header() left abnormally\n");
                        return;
                }
                // socket offset set to 0 in read_deadline, and is incremented as the read completes.
                // Thus, at end of call, socket offset = total number of bytes read.
                self->offset += self->socket.offset;

                if (self->offset >= self->length) {
                        fprintf(stderr, "ASDHD014 offset >= length\n");
                        break;
                }

                if (started_with_buffer_rc) {
                        as_conn_recover_copy_header_buffer(self);
                        b = self->header_buf;
                }
        }

        if (self->check_return_code) {
                fprintf(stderr, "ASDHD005 check_return_code is true\n");
                if (b[self->length - 1] != 0) {
                        fprintf(stderr, "ASDHD006 INFO3 byte found to be nonzero; must abort\n");
                        *must_abort = true;
                        return;
                }
        }

        fprintf(stderr, "ASDHD004 Attempt to process proto\n");
        if (! as_conn_recover_parse_proto(self)) {
                fprintf(stderr, "ASDHD003 Parsing proto failed; must abort.\n");
                *must_abort = true;
                return;
        }
        fprintf(stderr, "ASDHD002 as_conn_recover_drain_header left fallthrough\n");
}


static bool
as_conn_recover_try_drain(as_conn_recover* self, bool* must_abort, bool* timeout_exception) {
        fprintf(stderr, "ASTDR001 as_conn_recover_try_drain() entered\n");
        bool connection_drained = false;

        if (self->is_single) {
                fprintf(stderr, "ASTDR003 is_single is true\n");
                if (self->state == AS_READ_STATE_PROTO) {
                        fprintf(stderr, "ASTDR004 attempting to drain proto header\n");
                        as_conn_recover_drain_header(self, must_abort, timeout_exception);
                        if (*must_abort || *timeout_exception) {
                                fprintf(stderr, "ASTDR005 Exception detected: abort=%d timeout=%d\n", *must_abort, *timeout_exception);
                                goto exception;
                        }
                }

                fprintf(stderr, "ASTDR006 Attempting to drain \"detail\"\n");
                as_conn_recover_drain_detail(self, must_abort, timeout_exception);
                if (*must_abort || *timeout_exception) {
                        fprintf(stderr, "ASTDR007 Exception detected: abort=%d timeout=%d\n", *must_abort, *timeout_exception);
                        goto exception;
                }

                fprintf(stderr, "ASTDR008 Attempting to mark connection as recovered\n");
                as_conn_recover_recover(self);

                connection_drained = true;
        }
        else {
                fprintf(stderr, "ASTDR010 is_single is false\n");
                while (true) {
                        if (self->state == AS_READ_STATE_PROTO) {
                                fprintf(stderr, "ASTDR011 Attempting to drain proto header\n");
                                as_conn_recover_drain_header(self, must_abort, timeout_exception);
                                if (*must_abort || *timeout_exception) {
                                        fprintf(stderr, "ASTDR012 Exception detected: abort=%d timeout=%d\n", *must_abort, *timeout_exception);
                                        goto exception;
                                }
                        }
                        fprintf(stderr, "ASTDR013 Attempting to drain detail\n");
                        as_conn_recover_drain_detail(self, must_abort, timeout_exception);
                        if (*must_abort || *timeout_exception) {
                                goto exception;
                                fprintf(stderr, "ASTDR014 Exception detected: abort=%d timeout=%d\n", *must_abort, *timeout_exception);
                        }

                        if (self->last_group) {
                                fprintf(stderr, "ASTDR015 Last in the group; exiting loop.\n");
                                break;
                        }

                        self->length = 12;
                        self->offset = 0;
                        self->state = AS_READ_STATE_PROTO;
                }
                fprintf(stderr, "ASTDR016 Attempting to mark connection as recovered\n");
                as_conn_recover_recover(self);
                connection_drained = true;
        }

exception:
        fprintf(stderr, "ASTDR002 as_conn_recover_try_drain() left\n");
        return connection_drained;
}


bool
as_conn_recover_drain(as_conn_recover* self)
{
        fprintf(stderr, "ASRCV001 as_conn_recover_drain() called\n");

        bool must_abort = false;
        bool timeout_exception = false;

        bool connection_drained = as_conn_recover_try_drain(self, &must_abort, &timeout_exception);

        if (timeout_exception) {
                fprintf(stderr, "ASRCV006 Timeout exception detected from try_drain()\n");
                uint64_t current_time_ns = cf_getns();

                if (current_time_ns - self->socket_last_used >= self->timeout_delay * 1000) {
                        // Forcibly close the connection.
                        fprintf(stderr, "ASRCV005 We are forcibly closing connection due to timeout expiry\n");
                        must_abort = true;
                }
                else {
                        // Put back on queue for later draining.
                        fprintf(stderr, "ASRCV004 as_conn_recover_drain() left\n");
                        return false;
                }
        }

        if (must_abort) {
                as_conn_recover_abort(self);
                fprintf(stderr, "ASRCV003 as_conn_recover_drain() left\n");
                return true;
        }

        fprintf(stderr, "ASRCV002 as_conn_recover_drain() left\n");
        return connection_drained;
}

