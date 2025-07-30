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

#if defined(_MSC_VER)
#include <time.h>
#endif

#include <sys/stat.h>

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(_MSC_VER)

typedef struct {
	struct timespec timestamp;
} as_file_status;

static inline struct timespec*
as_file_get_timestamp(struct stat* stats)
{
#if defined(__APPLE__)
	return &stats->st_mtimespec;
#else
	return &stats->st_mtim;
#endif
}

static inline bool
as_file_get_status(const char* path, as_file_status* fs)
{
	struct stat stats;
	int rv = stat(path, &stats);

	if (rv != 0) {
		return false;
	}

	fs->timestamp = *as_file_get_timestamp(&stats);
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

	struct timespec* ts = as_file_get_timestamp(&stats);

	if (ts->tv_sec > fs->timestamp.tv_sec ||
		(ts->tv_sec == fs->timestamp.tv_sec &&
		ts->tv_nsec > fs->timestamp.tv_nsec)) {
		fs->timestamp = *ts;
		return true;
	}
	return false;
}

#else

typedef struct {
	time_t timestamp;
} as_file_status;

static inline bool
as_file_get_status(const char* path, as_file_status* fs)
{
	struct _stat stats;
	int rv = _stat(path, &stats);

	if (rv != 0) {
		return false;
	}

	fs->timestamp = stats.st_mtime;
	return true;
}

static inline bool
as_file_has_changed(const char* path, as_file_status* fs)
{
	struct _stat stats;
	int rv = _stat(path, &stats);

	if (rv != 0) {
		return false;
	}

	time_t ts = stats.st_mtime;

	if (ts > fs->timestamp) {
		fs->timestamp = ts;
		return true;
	}
	return false;
}

#endif

#ifdef __cplusplus
} // end extern "C"
#endif
