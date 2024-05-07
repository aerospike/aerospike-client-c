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

#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_metrics.h>
#include <aerospike/as_status.h>
#include <stdio.h>

#if defined(_MSC_VER)
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

/**
 * Default metrics listener. This implementation writes periodic metrics snapshots to a file which
 * will later be read and forwarded to OpenTelemetry by a separate offline application.
 */
typedef struct as_metrics_writer_s {
	char report_dir[256];
	FILE* file;
	uint64_t max_size;
	uint64_t size;
	uint32_t latency_columns;
	uint32_t latency_shift;
#ifdef _MSC_VER
	FILETIME prev_process_times_kernel;
	FILETIME prev_system_times_kernel;
	FILETIME prev_process_times_user;
	FILETIME prev_system_times_user;
	HANDLE process;
	DWORD pid;
#endif
	bool enable;
} as_metrics_writer;

//---------------------------------
// Functions
//---------------------------------

AS_EXTERN as_status
as_metrics_writer_create(as_error* err, const as_metrics_policy* policy, as_metrics_listeners* listeners);

AS_EXTERN as_status
as_metrics_writer_enable(as_error* err, void* udata);

AS_EXTERN as_status
as_metrics_writer_snapshot(as_error* err, as_cluster* cluster, void* udata);

AS_EXTERN as_status
as_metrics_writer_node_close(as_error* err, struct as_node_s* node, void* udata);

AS_EXTERN as_status
as_metrics_writer_disable(as_error* err, struct as_cluster_s* cluster, void* udata);

#ifdef __cplusplus
} // end extern "C"
#endif
