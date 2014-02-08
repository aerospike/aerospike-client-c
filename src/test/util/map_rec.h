#pragma once

/**
 * An as_rec backed by a map.
 */

#include <aerospike/as_rec.h>
#include <aerospike/as_map.h>

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_rec * map_rec_new();
as_rec * map_rec_init(as_rec *);
