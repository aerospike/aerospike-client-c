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
                     as_node* node, as_socket* socket)
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

        // TODO: conn.updateLastUsed(); // Updates connection timestamp.
        // TODO: conn.setTimeout(1); // I think this sets a future timeout on this connection to 1ms.

        return self;
}


static void
as_conn_recover_drain_header(as_conn_recover* self, bool* must_abort, bool* timeout_exception) {
}

static void
as_conn_recover_drain_detail(as_conn_recover* self, bool* must_abort, bool* timeout_exception) {
}


static bool
as_conn_recover_try_drain(as_conn_recover* self, bool *must_abort, bool *timeout_exception) {
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

        bool connection_drained = as_conn_recover_try_drain(self, &must_abort, &timeout_exception);

        if (timeout_exception) {
                uint64_t current_time_ns = cf_getns();

                if (current_time_ns - self->socket.last_used >= self->timeout_delay * 1000) {
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

