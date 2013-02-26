/**
 * A stream which consumes values via a callback.
 */

#include "consumer_stream.h"

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
