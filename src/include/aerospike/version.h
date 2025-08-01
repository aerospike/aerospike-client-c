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

#include <aerospike/as_std.h>

#ifdef __cplusplus
extern "C" {
#endif

// Version format: MNNPPBBBB
// M: major
// N: minor
// P: patch
// B: build id
#define AEROSPIKE_CLIENT_VERSION 701000000L

AS_EXTERN extern char* aerospike_client_version;

#ifdef __cplusplus
} // end extern "C"
#endif
