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
#include "consumer_stream.h"

/**
 * A stream which consumes values via a callback.
 */

/*****************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static as_stream_status consumer_stream_write(const as_stream *, as_val *);

/*****************************************************************************
 * CONSTANTS
 *****************************************************************************/

static const as_stream_hooks consumer_stream_hooks = {
    .destroy    = NULL,
    .read       = NULL,
    .write      = consumer_stream_write
};

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_stream * consumer_stream_new(consumer_callback f) {
    return as_stream_new(f, &consumer_stream_hooks);
}

as_stream * consumer_stream_init(as_stream * s, consumer_callback f) {
    return as_stream_init(s, f, &consumer_stream_hooks);
}

static as_stream_status consumer_stream_write(const as_stream * s, as_val * v) {
    consumer_callback f = (consumer_callback) as_stream_source(s);
    return f(v);
}
