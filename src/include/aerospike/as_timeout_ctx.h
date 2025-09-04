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
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * @private
 * The socket state when a read timeout occurs.
 */
typedef enum as_read_state_e {
        AS_READ_STATE_NONE,
	AS_READ_STATE_PROTO,
	AS_READ_STATE_DETAIL,
        AS_READ_STATE_AUTH_HEADER,
        AS_READ_STATE_COMPLETE,
} as_read_state;

/**
 * @private
 * When a socket read timeout occurs, this structure records
 * the context in which it happened.
 */

typedef struct as_timeout_ctx_s {
	/**
	 * Points to a reference counted buffer of length `capacity`.
	 * Dispose of the buffer using cf_rc_releaseandfree().
	 */
	uint8_t* buffer_rc;

	/**
	 * The number of bytes total in the buffer above.
	 */
	uint32_t capacity;

	/**
	 * When draining a socket, the received data will be placed
	 * starting at this byte offset.
	 */
	uint32_t offset;

	/**
	 * What state the socket was in when the timeout happened.
	 */
	as_read_state state;
} as_timeout_ctx;

//---------------------------------
// Functions
//---------------------------------

/**
 * @private
 * Initializes an as_timeout_ctx instance with relevant data.  The supplied buffer, if any, must be
 * allocated with cf_rc_alloc().  The buffer will already be reserved when cf_rc_alloc() returns the
 * pointer, and does not need to be reserved again unless you create additional references to it.
 *
 * If the supplied context pointer is NULL, then nothing happens, and true is returned.
 *
 * @return true if the allocation succeeded; false otherwise.
 */
static inline void
as_timeout_ctx_set(as_timeout_ctx* context, uint8_t* buffer, uint32_t cap, uint32_t offset, uint8_t state) {
	if (context) {
                context->buffer_rc = buffer;
                context->capacity = cap;
                context->offset = offset;
                context->state = state;
        }
}

#ifdef __cplusplus
} // end extern "C"
#endif

