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
