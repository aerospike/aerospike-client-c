/*
 * Copyright 2008-2026 Aerospike, Inc.
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

#include <aerospike/aerospike_query.h>
#include <aerospike/as_query.h>

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * @private
 * Buffers records from every targeted node (each node's response is already
 * a fully sorted, deduplicated, <= k batch) and, once every node has reported,
 * delivers the merged/truncated global top-k in final rank order to the
 * caller's real callback.
 *
 * A collector is single-use: create it, drive one query through
 * as_query_topk_collect() (used as the query's callback/udata in place of the
 * caller's own callback/udata), then destroy it with
 * as_query_topk_collector_destroy().
 */
typedef struct as_query_topk_collector_s as_query_topk_collector;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * @private
 * Whether any order-by or top-k setting was made, so the query is checked as a
 * Top-K query.
 */
static inline bool
as_query_has_topk_settings(const as_query* query)
{
	return query->order_by.defined || query->top_k_defined || query->top_k > 0;
}

/**
 * @private
 * Whether the client merges this query's results as a Top-K query. An order
 * key without k is sent to the server, which rejects it, so it is not merged.
 */
static inline bool
as_query_is_topk(const as_query* query)
{
	return query->order_by.defined && query->top_k > 0;
}

/**
 * @private
 * Create a new collector for a synchronous (aerospike_query_foreach()/
 * aerospike_query_partitions()) Top-K query. The query's order keys and k
 * must already be validated (as_query_validate_topk()) by the caller.
 *
 * Pass as_query_topk_collect() (with the returned collector as udata) to the
 * underlying as_query_partitions() call in place of the caller's own
 * callback/udata.
 *
 * The caller remains responsible for destroying the collector via
 * as_query_topk_collector_destroy() once the underlying call returns,
 * regardless of the returned status (destroy is safe to call whether or not
 * a flush already happened internally).
 */
AS_EXTERN as_query_topk_collector*
as_query_topk_collector_create(
	const as_query* query, aerospike_query_foreach_callback callback, void* udata
	);

/**
 * @private
 * Trampoline callback with the same signature as
 * aerospike_query_foreach_callback. Deep-copies val (assumed to be an
 * as_record) so it survives past the lifetime of the caller's
 * stack-allocated record. val == NULL signals that the underlying query
 * completed successfully across every targeted node; at that point, this
 * function synchronously sorts/dedups/truncates the buffered records and
 * replays them to the real callback, followed by one final NULL callback.
 *
 * Always returns true for non-NULL val (never aborts the underlying query
 * early) - Top-K requires a full scan of the candidate set on every
 * targeted node, so there is nothing to gain by stopping early once a
 * single node's batch has arrived.
 */
AS_EXTERN bool
as_query_topk_collect(const as_val* val, void* udata);

/**
 * @private
 * Destroy the collector and any buffered records it still owns (e.g.
 * because the underlying query failed before completion, so
 * as_query_topk_collect() never observed the val == NULL completion
 * signal).
 */
AS_EXTERN void
as_query_topk_collector_destroy(as_query_topk_collector* collector);

#ifdef __cplusplus
} // end extern "C"
#endif
