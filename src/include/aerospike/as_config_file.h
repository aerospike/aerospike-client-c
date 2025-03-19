/*
 * Copyright 2008-2025 Aerospike, Inc.
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

#include <aerospike/as_config.h>

#ifdef __cplusplus
extern "C" {
#endif

struct as_cluster_s;

/**
 * @private
 * Read dynamic configuration file and populate config. Call this function before creating the cluster.
 */
AS_EXTERN as_status
as_config_file_init(as_config* config, as_error* err);

/**
 * @private
 * Read dynamic configuration file and update cluster with new values.
 */
AS_EXTERN as_status
as_config_file_update(struct as_cluster_s* cluster, as_config* orig, as_error* err);

#ifdef __cplusplus
} // end extern "C"
#endif
