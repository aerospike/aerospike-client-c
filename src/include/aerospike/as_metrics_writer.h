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

#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_metrics.h>
#include <aerospike/as_status.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * Built-in learn-metrics file exporter. Embeds as_metrics_exporter as its first field.
 * The log is header(3). CamelCase segment names are unchanged. Cpu, memory,
 * and namespace histograms are written whenever metrics are enabled, including
 * when operational_enabled is false.
 * blockingCount, deferredCount, and backgroundCount follow commandCount on
 * the cluster line. They are feature.api.blocking, feature.api.deferred, and
 * feature.api.background. The columns are always present. They increment only
 * while as_metrics_snapshot.usage_metrics_enabled is true.
 */
typedef struct as_metrics_file_exporter_s as_metrics_file_exporter;

/**
 * @deprecated
 * Listener-based file writer. Use as_metrics_file_exporter with
 * as_metrics_policy_add_exporter(), or set report_dir and let enable install it.
 */
typedef as_metrics_file_exporter as_metrics_writer;

//---------------------------------
// Functions
//---------------------------------

/**
 * Create the learn-metrics file exporter.
 *
 * The caller owns the exporter when it is passed to as_metrics_policy_add_exporter().
 * Destroy it after metrics are disabled. Do not destroy an exporter that enable
 * installed itself from report_dir; disable destroys that instance.
 *
 * The first Export, and the first Export after each re-enable, creates
 * report_dir/metrics-YYYYMMDDHHMMSS.log and writes the header. Enable does not
 * open the log. Disable closes it and does not append to that file later.
 * See as_metrics_policy.report_dir.
 * Later exports append a cluster line and one node line per nodes_departed entry.
 */
AS_EXTERN as_status
as_metrics_file_exporter_create(
	as_error* err, const as_metrics_policy* policy, as_metrics_exporter** exporter
	);

/**
 * Close the metrics file and free a file exporter created by
 * as_metrics_file_exporter_create() or as_metrics_writer_create().
 */
AS_EXTERN void
as_metrics_file_exporter_destroy(as_metrics_exporter* exporter);

/**
 * @deprecated
 * Create the file writer and install it as a four-callback listener.
 * Prefer as_metrics_file_exporter_create() and as_metrics_policy_add_exporter().
 * A non-empty report_dir already installs this exporter when no listeners or
 * exporters are set.
 */
AS_EXTERN as_status
as_metrics_writer_create(as_error* err, const as_metrics_policy* policy, as_metrics_listeners* listeners);

/**
 * @deprecated
 * Listener enable callback used by as_metrics_writer_create().
 * The log stays closed until the next snapshot or disable export.
 * See as_metrics_file_exporter_create().
 */
AS_EXTERN as_status
as_metrics_writer_enable(as_error* err, void* udata);

/**
 * @deprecated
 * Listener snapshot callback used by as_metrics_writer_create().
 */
AS_EXTERN as_status
as_metrics_writer_snapshot(as_error* err, as_cluster* cluster, void* udata);

/**
 * @deprecated
 * Listener node-close callback used by as_metrics_writer_create().
 */
AS_EXTERN as_status
as_metrics_writer_node_close(as_error* err, struct as_node_s* node, void* udata);

/**
 * @deprecated
 * Listener disable callback used by as_metrics_writer_create(). Writes a final
 * cluster line, opening the log first when it is closed, and destroys the writer.
 * See as_metrics_file_exporter_create().
 */
AS_EXTERN as_status
as_metrics_writer_disable(as_error* err, struct as_cluster_s* cluster, void* udata);

/**
 * @private
 * True when exporter is the built-in learn-metrics file exporter.
 */
bool
as_metrics_exporter_is_file(const as_metrics_exporter* exporter);

#ifdef __cplusplus
} // end extern "C"
#endif
