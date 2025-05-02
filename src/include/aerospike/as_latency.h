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

#include <aerospike/as_atomic.h>
#include <citrusleaf/alloc.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

typedef uint8_t as_latency_type;

#define AS_LATENCY_TYPE_CONN 0
#define AS_LATENCY_TYPE_WRITE 1
#define AS_LATENCY_TYPE_READ 2
#define AS_LATENCY_TYPE_BATCH 3
#define AS_LATENCY_TYPE_QUERY 4
#define AS_LATENCY_TYPE_NONE 5
#define AS_LATENCY_TYPE_MAX 5

/**
 * Latency histogram for a command group.
 * Latency histogram counts are cumulative and not reset on each metrics snapshot interval
 */
typedef struct as_latency_s {
	uint32_t ref_count;
	uint8_t shift;
	uint8_t size;
	uint64_t buckets[];
} as_latency;

//---------------------------------
// Functions
//---------------------------------

/**
 * Reserver latency histogram.
 */
static inline as_latency*
as_latency_reserve(as_latency* latency)
{
	as_latency* lat = (as_latency*)as_load_ptr((void* const*)&latency);
	as_incr_uint32(&lat->ref_count);
	return lat;
}

/**
 * Release latency histogram.
 */
static inline void
as_latency_release(as_latency* latency)
{
	if (as_aaf_uint32_rls(&latency->ref_count, -1) == 0) {
		as_fence_acq();
		cf_free(latency);
	}
}

/**
 * Retrieve specified bucket using atomics.
 */
static inline uint64_t
as_latency_get_bucket(as_latency* latency, uint32_t index)
{
	return as_load_uint64(&latency->buckets[index]);
}

/**
 * Convert latency_type to string version for printing to the output file
 */
AS_EXTERN char*
as_latency_type_to_string(as_latency_type type);

#ifdef __cplusplus
} // end extern "C"
#endif
