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
#include <aerospike/as_version.h>
#include <stdio.h>

//---------------------------------
// Globals
//---------------------------------

as_version as_server_version_8_1 = {8, 1, 0, 0};

//---------------------------------
// Functions
//---------------------------------

bool
as_version_from_string(as_version* ver, const char* str)
{
	int rv = sscanf(str, "%hu.%hu.%hu.%hu", &ver->major, &ver->minor, &ver->patch, &ver->build);

	// 3 components are required.
	if (rv < 3) {
		return false;
	}

	// If build not provided, initialize to zero.
	if (rv == 3) {
		ver->build = 0;
	}
	return true;
}

void
as_version_to_string(const as_version* ver, char* str, size_t size)
{
	snprintf(str, size, "%hu.%hu.%hu.%hu", ver->major, ver->minor, ver->patch, ver->build);
}

int
as_version_compare(const as_version* ver1, const as_version* ver2)
{
	if (ver1->major != ver2->major) {
		return ver1->major - ver2->major;
	}

	if (ver1->minor != ver2->minor) {
		return ver1->minor - ver2->minor;
	}

	if (ver1->patch != ver2->patch) {
		return ver1->patch - ver2->patch;
	}

	if (ver1->build != ver2->build) {
		return ver1->build - ver2->build;
	}
	return 0;
}
