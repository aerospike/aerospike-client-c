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

#include <sys/stat.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

typedef struct {
	struct timespec timestamp;
} as_file_status;

//---------------------------------
// Functions
//---------------------------------

static inline bool
as_file_get_status(const char* path, as_file_status* fs)
{
	struct stat stats;
	int rv = stat(path, &stats);

	if (rv != 0) {
		return false;
	}

	fs->timestamp = stats.st_mtimespec;
	return true;
}

static inline bool
as_file_has_changed(const char* path, as_file_status* fs)
{
	struct stat stats;
	int rv = stat(path, &stats);

	if (rv != 0) {
		return false;
	}

	if (stats.st_mtimespec.tv_sec > fs->timestamp.tv_sec ||
		(stats.st_mtimespec.tv_sec == fs->timestamp.tv_sec &&
		stats.st_mtimespec.tv_nsec > fs->timestamp.tv_nsec)) {
		fs->timestamp = stats.st_mtimespec;
		return true;
	}
	return false;
}

#ifdef __cplusplus
} // end extern "C"
#endif
