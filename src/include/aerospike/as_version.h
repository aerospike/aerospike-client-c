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

//---------------------------------
// Types
//---------------------------------

/**
 * Aerospike server version components
 */
typedef struct {
	uint16_t major;
	uint16_t minor;
	uint16_t patch;
	uint16_t build;
} as_version;

//---------------------------------
// Functions
//---------------------------------

/**
 * Convert version string into it's components. Any suffix is ignored.
 *
 * @param ver	Version components.
 * @param str 	Version string.
 */
AS_EXTERN bool
as_version_from_string(as_version* ver, const char* str);

/**
 * Convert version components to a string.
 *
 * @param ver	Version components.
 * @param str 	Version string.
 * @param size	Size of the version string.
 */
AS_EXTERN void
as_version_to_string(const as_version* ver, char* str, size_t size);

/**
 * Compare versions.
 *
 * @param ver1	First version components.
 * @param ver2 	Second version components.
 * @returns less than zero for (ver1 < ver2). zero for (ver1 == ver2). greater than zero for (ver1 > ver2).
 */
AS_EXTERN int
as_version_compare(const as_version* ver1, const as_version* ver2);

#ifdef __cplusplus
} // end extern "C"
#endif
