/**
 * A stream which produces values via a callback.
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
