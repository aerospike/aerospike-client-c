/*
 * Copyright 2008-2014 Aerospike, Inc.
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
 
#include <stdint.h>

#include <citrusleaf/cf_types.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

struct cf_average_s {
	int			flags;
	uint32_t	n_points;
	uint64_t	points_sum;
};

typedef struct cf_average_s cf_average;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

cf_average * cf_average_create(uint32_t initial_size, uint32_t flags);
void cf_average_destroy( cf_average * avg);
void cf_average_clear(cf_average * avg);
int cf_average_add(cf_average * avgp, uint64_t value);   // warning! this fails if too many samples
double cf_average_calculate(cf_average * avg, bool clear);

// maybe it would be nice to have a floating point version?

/*****************************************************************************/

#ifdef __cplusplus
} // end extern "C"
#endif
