/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#include <aerospike/as_log.h>

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

/******************************************************************************
 * CONSTANTS
 *****************************************************************************/

static const char * as_log_level_strings[5] = {
	[AS_LOG_LEVEL_ERROR]	= "ERROR",
	[AS_LOG_LEVEL_WARN]		= "WARN",
	[AS_LOG_LEVEL_INFO]		= "INFO",
	[AS_LOG_LEVEL_DEBUG]	= "DEBUG",
	[AS_LOG_LEVEL_TRACE] 	= "TRACE"
};

#define MAX_LOG_MSG_SIZE 2048
const size_t MAX_LOG_MSG_LEN = MAX_LOG_MSG_SIZE - 1;

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool as_log_stderr(
	as_log_level level, const char * func, const char * file, uint32_t line,
	const char * fmt, ...)
{
	char msg[MAX_LOG_MSG_SIZE] = {0};

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, MAX_LOG_MSG_LEN, fmt, ap);
	msg[MAX_LOG_MSG_LEN] = '\0';
	va_end(ap);

	const char* base_name = strrchr(file, '/');

	if (base_name) {
		base_name++;
	}
	else {
		base_name = file;
	}

	fprintf(stderr, "[%s:%d][%s] %s - %s\n",
			base_name, line, func, as_log_level_strings[level], msg);

	return true;
}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize Log Context 
 */
as_log * as_log_init(as_log * log) 
{
	if ( !log ) return NULL;
	cf_atomic32_set(&log->level, (cf_atomic32) AS_LOG_LEVEL_INFO);
	cf_atomic_p_set(&log->callback, (cf_atomic_p) as_log_stderr);
	return log;
}

/**
 * Set the level for the given log
 */
bool as_log_set_level(as_log * log, as_log_level level) 
{
	if ( !log ) return false;
	cf_atomic32_set(&log->level, (cf_atomic32) level);
	return true;
}

/**
 * Set the callback for the given log
 */
bool as_log_set_callback(as_log * log, as_log_callback callback)
{
	if ( !log ) return false;
	cf_atomic_p_set(&log->callback, (cf_atomic_p) callback);
	return true;
}



