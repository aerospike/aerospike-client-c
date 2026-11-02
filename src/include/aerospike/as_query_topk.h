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
 * Merges records from every targeted node (each node's response is already
 * a fully sorted, deduplicated, <= k batch), keeping only the best k seen so
 * far, so it holds about k records whatever the cluster size. Once every node
 * has reported, it delivers the global top-k in final rank order to the
 * caller's real callback or listener.
 *
 * A collector is single-use: create it, then drive one query through
 * as_query_topk_collect() or as_query_topk_collect_async() (used as the
 * query's callback/udata in place of the caller's own callback/udata). A
 * synchronous collector is destroyed by its caller with
 * as_query_topk_collector_destroy(). An asynchronous collector destroys
 * itself; see as_query_topk_collector_create_async().
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
 * When the underlying call returns AEROSPIKE_OK, the caller must also check
 * as_query_topk_collector_status(), because a candidate that could not be
 * buffered stops the query the same way a user abort does.
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
 * Create a new collector for an asynchronous (aerospike_query_async()/
 * aerospike_query_partitions_async()) Top-K query. The query's order keys and
 * k must already be validated (as_query_validate_topk()) by the caller.
 *
 * Pass as_query_topk_collect_async() (with the returned collector as udata)
 * to the underlying as_query_partition_async() call in place of the caller's
 * own listener/udata.
 *
 * The collector destroys itself when the query reports completion or an
 * error, because the query is still running on its event loop after the
 * call that started it returns. If that call fails, the listener is never
 * notified, so the caller must destroy the collector with
 * as_query_topk_collector_destroy() instead.
 */
AS_EXTERN as_query_topk_collector*
as_query_topk_collector_create_async(
	const as_query* query, as_async_query_record_listener listener, void* udata
	);

/**
 * @private
 * Trampoline callback with the same signature as
 * aerospike_query_foreach_callback. If val (assumed to be an as_record) ranks
 * among the best k records seen so far, and no copy of the same record is
 * already kept, deep-copies it so it survives past the lifetime of the
 * caller's stack-allocated record, and drops the worst kept record if there
 * are now more than k. val == NULL signals that the underlying query
 * completed successfully across every targeted node; at that point, this
 * function synchronously sorts the kept records and replays them to the real
 * callback, followed by one final NULL callback.
 *
 * Returns true for non-NULL val unless the record could not be buffered (out
 * of memory). Top-K requires a full scan of the candidate set on every
 * targeted node, so the query is not stopped early otherwise. A record that
 * could not be buffered might belong in the top k, so this returns false to
 * stop the query, and nothing is delivered to the real callback, not even the
 * final NULL.
 *
 * A foreach callback can only return a bool, so this function cannot return
 * the error itself. It records the failure in the collector instead. The
 * underlying query treats the false return as a user abort and returns
 * AEROSPIKE_OK, so the caller must then call as_query_topk_collector_status()
 * to get the error.
 */
AS_EXTERN bool
as_query_topk_collect(const as_val* val, void* udata);

/**
 * @private
 * Return AEROSPIKE_OK, or set err and return AEROSPIKE_ERR_CLIENT when
 * as_query_topk_collect() could not buffer a candidate record. Call after the
 * underlying synchronous query returns AEROSPIKE_OK.
 */
AS_EXTERN as_status
as_query_topk_collector_status(const as_query_topk_collector* collector, as_error* err);

/**
 * @private
 * Trampoline listener with the same signature as
 * as_async_query_record_listener. Deep-copies record, like
 * as_query_topk_collect(). record == NULL (with err NULL) signals that the
 * underlying query completed successfully across every targeted node: the
 * buffered records are merged and replayed to the real listener, followed by
 * one final NULL record unless the listener returned false. err set signals a
 * fatal error, which is forwarded to the real listener without delivering
 * the buffered records. A record that could not be buffered is reported the
 * same way, with AEROSPIKE_ERR_CLIENT, and stops the query. In each case the
 * collector then destroys itself.
 */
AS_EXTERN bool
as_query_topk_collect_async(
	as_error* err, as_record* record, void* udata, as_event_loop* event_loop
	);

/**
 * @private
 * Destroy the collector and any buffered records it still owns (e.g.
 * because the underlying synchronous query failed before completion, so
 * as_query_topk_collect() never observed the val == NULL completion
 * signal, or because an asynchronous query failed to start).
 */
AS_EXTERN void
as_query_topk_collector_destroy(as_query_topk_collector* collector);

#ifdef __cplusplus
} // end extern "C"
#endif
