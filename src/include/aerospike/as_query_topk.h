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
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_status.h>

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
 * caller's real callback/listener.
 *
 * A collector is single-use: create it, drive one query through
 * as_query_topk_collect()/as_query_topk_collect_async() (used as the
 * query's callback/udata in place of the caller's own callback/udata), and
 * it flushes and (for the async variant) destroys itself automatically when
 * the underlying query signals completion. Callers of the sync variant must
 * still call as_query_topk_collector_destroy() themselves; see below.
 */
typedef struct as_query_topk_collector_s as_query_topk_collector;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * @private
 * Create a new collector for a synchronous (aerospike_query_foreach()/
 * aerospike_query_partitions()) Top-K query. query->order_by/query->top_k
 * must already be validated (as_query_validate_topk()) by the caller.
 *
 * Pass as_query_topk_collect() (with the returned collector as udata) to the
 * underlying as_query_partitions()/as_query_execute() call in place of the
 * caller's own callback/udata.
 *
 * The caller remains responsible for destroying the collector via
 * as_query_topk_collector_destroy() once the underlying call returns,
 * regardless of the returned status (destroy is safe to call whether or not
 * a flush already happened internally).
 */
as_query_topk_collector*
as_query_topk_collector_create_sync(
	const as_query_order_by_field* order_by, uint32_t k, aerospike_query_foreach_callback callback,
	void* udata
	);

/**
 * @private
 * Create a new collector for an asynchronous (aerospike_query_async()/
 * aerospike_query_partitions_async()) Top-K query. query->order_by/
 * query->top_k must already be validated (as_query_validate_topk()) by the
 * caller.
 *
 * Pass as_query_topk_collect_async() (with the returned collector as udata)
 * to the underlying as_query_partition_async() call in place of the
 * caller's own listener/udata.
 *
 * Unlike the sync collector, this collector destroys itself: the
 * asynchronous query may still be executing (on the event loop) after the
 * call that started it returns, so there is no synchronous point at which
 * the caller could safely destroy it. as_query_topk_collect_async()
 * destroys the collector exactly once, when the underlying query reports
 * final completion or a fatal error.
 *
 * If as_query_partition_async() (or equivalent) fails synchronously before
 * any command is queued (so as_query_topk_collect_async() will never be
 * invoked), the caller must instead call as_query_topk_collector_destroy()
 * itself.
 */
as_query_topk_collector*
as_query_topk_collector_create_async(
	const as_query_order_by_field* order_by, uint32_t k, as_async_query_record_listener listener,
	void* udata
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
bool
as_query_topk_collect(const as_val* val, void* udata);

/**
 * @private
 * Trampoline listener with the same signature as
 * as_async_query_record_listener. err set signals a fatal error (forwarded
 * to the real listener once, without flushing); record == NULL (and err
 * NULL) signals that the underlying query completed successfully across
 * every targeted node, triggering a flush to the real listener followed by
 * one final NULL-record callback. In both terminal cases, the collector
 * destroys itself before returning.
 */
bool
as_query_topk_collect_async(
	as_error* err, as_record* record, void* udata, as_event_loop* event_loop
	);

/**
 * @private
 * Destroy the collector and any buffered records it still owns (e.g.
 * because the underlying synchronous query failed before completion, so
 * as_query_topk_collect() never observed the val == NULL completion
 * signal). Only used by the synchronous collector; the async collector
 * destroys itself, see as_query_topk_collector_create_async().
 */
void
as_query_topk_collector_destroy(as_query_topk_collector* collector);

#ifdef __cplusplus
} // end extern "C"
#endif
