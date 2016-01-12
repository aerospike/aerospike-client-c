/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include "producer_stream.h"

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_val * producer_stream_read(const as_stream *);

/*****************************************************************************
 * CONSTANTS
 *****************************************************************************/

static const as_stream_hooks producer_stream_hooks = {
    .destroy    = NULL,
    .read       = producer_stream_read,
    .write      = NULL
};

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_stream * producer_stream_new(producer_callback f) {
    return as_stream_new(f, &producer_stream_hooks);
}

as_stream * producer_stream_init(as_stream * s, producer_callback f) {
    return as_stream_init(s, f, &producer_stream_hooks);
}

static as_val * producer_stream_read(const as_stream * s) {
    producer_callback f = (producer_callback) as_stream_source(s);
    return f();
}
