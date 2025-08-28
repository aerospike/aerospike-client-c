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
        bool          check_return_code;
        bool          last_group;
        uint8_t*      header_buf;
        uint32_t      length;
        as_node*      node;
} as_conn_recover;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * @private
 * Initialize a connection recover record on the stack or on the heap.
 */
as_conn_recover*
as_conn_recover_init(as_conn_recover* self, as_timeout_ctx* timeout_ctx, uint32_t timeout_delay, bool is_single,
                     as_node* node);

/**
 * @private
 * Allocate and initialize a connection recover record on the heap.
 * Use as_conn_recover_release() to dispose of the recover record.
 */
static inline as_conn_recover*
as_conn_recover_new(as_timeout_ctx* timeout_ctx, uint32_t timeout_delay, bool is_single,
                    as_node* node)
{
        return as_conn_recover_init(
                (as_conn_recover*)cf_rc_alloc(sizeof(as_conn_recover)),
                timeout_ctx, timeout_delay, is_single, node);
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
 * Attempt to drain a connection.  Answer true if there is no further
 * need to re-queue the connection recovery record; false otherwise.
 */
static inline bool
as_conn_recover_try_drain(as_conn_recover* self)
{
        // TODO: fill this out from Java reference; move to as_conn_recover.c
        return true;
}

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
        // TODO: node.closeConnection(conn); // close the socket and update abort metrics
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
 * "Parse" an as_proto object (in the buffer_rc buffer) to further initialize the as_conn_recover instance.
 * Answers false if an error occurred.
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

        self->length = proto->sz - (self->offset - 8);
        self->offset = 0;
        self->state = AS_READ_STATE_DETAIL;

        return true;
}


#ifdef __cplusplus
} // end extern "C"
#endif
