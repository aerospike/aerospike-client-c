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
} as_conn_recover;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * @private
 * Initialize a connection recover record on the stack or on the heap.
 */
static inline as_conn_recover*
as_conn_recover_init(as_conn_recover* self, as_timeout_ctx* timeout_ctx) {
        if (self) {
                cf_rc_reserve(timeout_ctx->buffer_rc);
                self->buffer_rc = timeout_ctx->buffer_rc;
                self->capacity = timeout_ctx->capacity;
                self->offset = timeout_ctx->offset;
                self->state = timeout_ctx->state;
        }

        return self;
}

/**
 * @private
 * Allocate and initialize a connection recover record on the heap.
 */
static inline as_conn_recover*
as_conn_recover_new(as_timeout_ctx* timeout_ctx) {
        return as_conn_recover_init(
                (as_conn_recover*)cf_rc_alloc(sizeof(as_conn_recover)),
                timeout_ctx);
        return cr;
}

/**
 * @private
 * Dispose of a heap-allocated as_conn_recover instance.
 */
static void
as_conn_recover_release(as_conn_recover *cr) {
        if (cr->buffer_rc) {
                cf_rc_releaseandfree(cr->buffer_rc);
        }
        cf_rc_releaseandfree(cr);
}


#ifdef __cplusplus
} // end extern "C"
#endif
