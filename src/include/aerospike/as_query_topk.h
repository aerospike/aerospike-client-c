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
 * CONSTANTS
 *****************************************************************************/

/**
 * @private
 * Maximum allowed as_query.top_k. Shared between as_query_validate_topk()
 * (aerospike_query.c), which rejects a top_k outside [1, AS_QUERY_TOPK_MAX],
 * and as_query_topk_collector_new() (as_query_topk.c), which must size its
 * heap/index from a capped k since it runs before that validation.
 */
#define AS_QUERY_TOPK_MAX 1000

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
 * A collector is single-use: create it, then run one query with
 * as_query_topk_complete() or as_query_topk_complete_async() (and the
 * collector as udata) in place of the caller's own callback/udata. The query
 * parser sends each record to as_query_topk_collect_record(), and the
 * completion callback delivers the merged top k once every targeted node has
 * reported. A synchronous collector is destroyed by its caller with
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
 * Pass as_query_topk_complete() (with the returned collector as udata) to the
 * underlying as_query_partitions() call in place of the caller's own
 * callback/udata. The query parser then sends each record to
 * as_query_topk_collect_record(), and as_query_partitions() returns any
 * error it reports.
 *
 * The caller remains responsible for destroying the collector via
 * as_query_topk_collector_destroy() once as_query_partitions() returns,
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
 * Pass as_query_topk_complete_async() (with the returned collector as udata)
 * to the underlying as_query_partition_async() call in place of the caller's
 * own listener/udata. The query parser then sends each record to
 * as_query_topk_collect_record(), and any error it reports goes to the
 * listener through as_query_topk_complete_async().
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
 * aerospike_query_foreach_callback. The query parser recognizes this callback
 * and passes each record to as_query_topk_collect_record() instead, so val is
 * always NULL. as_query_partitions() makes that call only once the query
 * completed successfully across every targeted node: this function
 * synchronously sorts the kept records and replays them to the real
 * callback, followed by one final NULL callback.
 */
AS_EXTERN bool
as_query_topk_complete(const as_val* val, void* udata);

/**
 * @private
 * Collect a record parsed from a synchronous or asynchronous Top-K query
 * response, along with the record's last-update time from the response (0 if
 * the server did not send it). If the record ranks among the best k records seen so far,
 * deep-copies it so it survives past the lifetime of the parser's
 * stack-allocated record, and drops the worst kept record if there are now
 * more than k. A record can reach the client from two nodes while its
 * partition migrates, and the copy with the later last-update time is kept.
 * On a tie, the copy already kept stays.
 *
 * Returns AEROSPIKE_OK unless the record could not be buffered (out of
 * memory). Top-K requires a full scan of the candidate set on every targeted
 * node, so the query is not stopped early otherwise. A record that could not
 * be buffered might belong in the top k, so this sets err and returns
 * AEROSPIKE_ERR_CLIENT, which the parser returns to fail the query.
 */
AS_EXTERN as_status
as_query_topk_collect_record(
	as_query_topk_collector* collector, as_error* err, const as_record* record,
	uint64_t last_update_time
	);

/**
 * @private
 * Trampoline listener with the same signature as
 * as_async_query_record_listener. The query parser recognizes this listener
 * and passes each record to as_query_topk_collect_record() instead, so record
 * is always NULL. err NULL signals that as_query_partition_async() completed
 * successfully across every targeted node: the buffered records are merged
 * and replayed to the real listener, followed by one final NULL record unless
 * the listener returned false. err set signals a fatal error, including one
 * from as_query_topk_collect_record(), which is forwarded to the real
 * listener without delivering the buffered records. In each case the
 * collector then destroys itself.
 */
AS_EXTERN bool
as_query_topk_complete_async(
	as_error* err, as_record* record, void* udata, as_event_loop* event_loop
	);

/**
 * @private
 * Destroy the collector and any buffered records it still owns (e.g.
 * because as_query_partitions() failed before completion, so
 * as_query_topk_complete() never observed the val == NULL completion
 * signal, or because an asynchronous query failed to start).
 */
AS_EXTERN void
as_query_topk_collector_destroy(as_query_topk_collector* collector);

#ifdef __cplusplus
} // end extern "C"
#endif
