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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/aerospike_query.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @private
 * Dynamic configuration metrics.exporter. Stored in as_metrics_policy.metrics_exporter.
 * Not part of the application policy.
 */
typedef enum as_metrics_builtin_exporter_e {
	AS_METRICS_BUILTIN_EXPORTER_FILE = 0,
	AS_METRICS_BUILTIN_EXPORTER_NONE = 1,
	AS_METRICS_BUILTIN_EXPORTER_CUSTOM = 2
} as_metrics_builtin_exporter;

/**
 * Synchronous commands the Node.js client calls. These do not increment
 * feature.api. The public functions still increment blocking. Node calls the
 * internal function and then as_metrics_add_api_deferred().
 *
 * Async commands are not listed. Their public functions already increment
 * feature.api.deferred, and that is what Node calls. *_internal and
 * *_async_internal elsewhere skip the count so an internal caller, such as a
 * batch single-record fallback, does not count the command again.
 *
 * Background query and scan stay on the public functions, which increment
 * feature.api.background.
 */

/**
 * @private
 * aerospike_query_foreach() without incrementing feature.api.blocking.
 */
AS_EXTERN as_status
aerospike_query_foreach_internal(
	aerospike* as, as_error* err, const as_policy_query* policy, as_query* query,
	aerospike_query_foreach_callback callback, void* udata);

/**
 * @private
 * aerospike_batch_get() without incrementing feature.api.blocking.
 */
AS_EXTERN as_status
aerospike_batch_get_internal(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_listener listener, void* udata
	);

/**
 * @private
 * aerospike_batch_get_bins() without incrementing feature.api.blocking.
 */
AS_EXTERN as_status
aerospike_batch_get_bins_internal(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	const char** bins, uint32_t n_bins, as_batch_listener listener, void* udata
	);

/**
 * @private
 * aerospike_batch_exists() without incrementing feature.api.blocking.
 */
AS_EXTERN as_status
aerospike_batch_exists_internal(
	aerospike* as, as_error* err, const as_policy_batch* policy, const as_batch* batch,
	as_batch_listener listener, void* udata
	);

/**
 * @private
 * aerospike_batch_apply() without incrementing feature.api.blocking.
 */
AS_EXTERN as_status
aerospike_batch_apply_internal(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_apply* policy_apply, const as_batch* batch,
	const char* module, const char* function, as_list* arglist,
	as_batch_listener listener, void* udata
	);

/**
 * @private
 * aerospike_batch_remove() without incrementing feature.api.blocking.
 */
AS_EXTERN as_status
aerospike_batch_remove_internal(
	aerospike* as, as_error* err, const as_policy_batch* policy,
	const as_policy_batch_remove* policy_remove, const as_batch* batch,
	as_batch_listener listener, void* udata
	);

/**
 * @private
 * Convert an export duration to milliseconds.
 * NULL or "" sets *ms to 0 and returns true. A bare number is seconds.
 * Suffixes are ms, s, m, and h. Zero, overflow, and unknown suffixes return false.
 */
bool
as_metrics_export_interval_to_ms(const char* interval, uint64_t* ms);

/**
 * @private
 * Count one public synchronous API call toward feature.api.blocking.
 */
void
as_metrics_add_api_blocking(struct aerospike_s* as);

/**
 * @private
 * Count one public asynchronous API call toward feature.api.deferred.
 * Exported for the Node.js client.
 */
AS_EXTERN void
as_metrics_add_api_deferred(struct aerospike_s* as);

/**
 * @private
 * Count one public background scan or query toward feature.api.background.
 */
void
as_metrics_add_api_background(struct aerospike_s* as);

#ifdef __cplusplus
}
#endif
