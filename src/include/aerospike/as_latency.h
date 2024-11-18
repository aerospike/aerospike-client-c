/*
 * Copyright 2008-2024 Aerospike, Inc.
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

/**
 * Latency buckets for a command group.
 * Latency bucket counts are cumulative and not reset on each metrics snapshot interval
 */
typedef struct as_latency_buckets_s {
	uint64_t* buckets;
	uint32_t latency_shift;
	uint32_t latency_columns;
} as_latency_buckets;

//---------------------------------
// Functions
//---------------------------------

static inline uint64_t
as_latency_get_bucket(as_latency_buckets* buckets, uint32_t i)
{
	return as_load_uint64(&buckets->buckets[i]);
}

/**
 * Convert latency_type to string version for printing to the output file
 */
AS_EXTERN char*
as_latency_type_to_string(as_latency_type type);

#ifdef __cplusplus
} // end extern "C"
#endif
