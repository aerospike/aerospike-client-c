/*
 * Copyright 2008-2022 Aerospike, Inc.
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

#include <aerospike/as_key.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

struct as_node_s;
struct as_event_executor;
struct as_event_command;
struct as_event_loop;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * @private
 * Verify migrations are not occurring and obtain cluster key.
 */
as_status
as_query_validate_begin(
	as_error* err, struct as_node_s* node, const char* ns, uint32_t timeout,
	uint64_t* cluster_key
	);

/**
 * @private
 * Verify migrations are not occurring and expected cluster key has not changed.
 */
as_status
as_query_validate(
	as_error* err, struct as_node_s* node, const char* ns, uint32_t timeout,
	uint64_t expected_key
	);

/**
 * @private
 * Verify migrations are not occurring and obtain cluster key in async mode.
 */
as_status
as_query_validate_begin_async(
	struct as_event_executor* executor, const char* ns, as_error* err
	);

/**
 * @private
 * Verify migrations are not occurring and expected cluster key has not changed in async mode.
 * Then execute query.
 */
as_status
as_query_validate_next_async(struct as_event_executor* executor, uint32_t index);

/**
 * @private
 * Verify migrations are not occurring and expected cluster key has not changed in async mode.
 * Then complete query.
 */
void
as_query_validate_end_async(
	struct as_event_executor* executor, struct as_node_s* node, struct as_event_loop* event_loop
	);

#ifdef __cplusplus
} // end extern "C"
#endif
