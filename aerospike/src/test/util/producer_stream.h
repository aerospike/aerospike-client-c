#pragma once

/**
 * A stream which produces values via a callback.
 */

#include <aerospike/as_stream.h>
#include <aerospike/as_val.h>

/*****************************************************************************
 * TYPES
 *****************************************************************************/

typedef as_val * (* producer_callback)(void);

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_stream * producer_stream_new(producer_callback);
as_stream * producer_stream_init(as_stream *, producer_callback);
