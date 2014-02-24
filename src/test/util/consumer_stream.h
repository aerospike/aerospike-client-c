#pragma once

/**
 * A stream which consumes values via a callback.
 */

#include <aerospike/as_stream.h>
#include <aerospike/as_val.h>

/*****************************************************************************
 * TYPES
 *****************************************************************************/

typedef as_stream_status (* consumer_callback)(as_val *);

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_stream * consumer_stream_new(consumer_callback);
as_stream * consumer_stream_init(as_stream *, consumer_callback);