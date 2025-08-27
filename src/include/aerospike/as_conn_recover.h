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
#include <citrusleaf/alloc.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * @private
 * Record a connection which has gone into a timeout state, and which we should
 * attempt to recover.  A recovered connection saves us the overhead of having
 * to close and re-open a TCP connection.
 *
 * As connections need recovery, they are queued onto an as_cluster structure.
 * See as_cluster for more information.
 */
typedef struct as_conn_recover_s {
        uint8_t*      buffer_rc;
        uint32_t      capacity;
        uint32_t      offset;
        as_read_state state;
        uint32_t      timeout_delay;
        bool          is_single;
        uint8_t*      header_buf;
        uint32_t      length;
} as_conn_recover;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * @private
 * Initialize a connection recover record on the stack or on the heap.
 */
static inline as_conn_recover*
as_conn_recover_init(as_conn_recover* self, as_timeout_ctx* timeout_ctx, uint32_t timeout_delay, bool is_single)
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
                                return;
                        }
                        self->length = as_conn_recover_get_buffer_size(self) - (self->offset - 8);
                        self->offset = 0;
                        self->state = AS_READ_STATE_DETAIL;
                }
                else if (self->offset > 0) {
                        as_conn_copy_header_buffer(self);
                }
                break;

        case AS_READ_STATE_PROTO:
                // For multi-record responses, adjust length to cover last group info3 bit at offset 11.
                self->length = is_single ? 8 : 12;

                if (self->offset >= self->length) {
                        as_conn_recover_parse_proto(self);
                }
                else if (self->offset > 0) {
                        as_conn_copy_header_buffer(self);
                }
                break;

        case AS_READ_STATE_DETAIL:
        default:
                self->length = timeout_ctx->length;
                break;
        }

        // TODO: conn.updateLastUsed(); // Updates connection timestamp.
        // TODO: conn.setTimeout(1); // I think this sets a future timeout on this connection to 1ms.

        return self;
}

/**
 * @private
 * Allocate and initialize a connection recover record on the heap.
 * Use as_conn_recover_release() to dispose of the recover record.
 */
static inline as_conn_recover*
as_conn_recover_new(as_timeout_ctx* timeout_ctx)
{
        return as_conn_recover_init(
                (as_conn_recover*)cf_rc_alloc(sizeof(as_conn_recover)),
                timeout_ctx);
}

/**
 * @private
 * Dispose of a heap-allocated as_conn_recover instance.
 */
static inline void
as_conn_recover_release(as_conn_recover* cr)
{
        if (! cf_rc_release(cr)) {
                if (cr->buffer_rc) {
                        cf_rc_releaseandfree(cr->buffer_rc);
                }

                if (cr->header_buf) {
                        cf_rc_releaseandfree(cr->header_buf);
                }
        }
}

/**
 * @private
 * Attempt to drain a connection.  Answer true if there is no further
 * need to re-queue the connection recovery record; false otherwise.
 */
static inline bool
as_conn_recover_try_drain(as_conn_recover* cr)
{
        // TODO: fill this out from Java reference; move to as_conn_recover.c
        return false;
}

/**
 * @private
 * Copy a header buffer into a locally managed working buffer.
 */
static inline void
as_conn_recover_copy_header_buffer(as_conn_recover* cr, uint8_t* buf)
{
        if (! cr->header_buf ) {
                cr->header_buf = cf_rc_alloc(self->length);
        }
        memcpy(self->header_buf, buf, self->offset);
}

#ifdef __cplusplus
} // end extern "C"
#endif
