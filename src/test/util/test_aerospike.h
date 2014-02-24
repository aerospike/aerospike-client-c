#pragma once

/**
 * An as_aerospike for tests.
 */

#include <aerospike/as_aerospike.h>

/*****************************************************************************
 * FUNCTIONS
 *****************************************************************************/

as_aerospike * test_aerospike_new();
as_aerospike * test_aerospike_init(as_aerospike *);