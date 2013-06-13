/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <aerospike/as_log.h>

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define AS_LOG_MSG_LEN 1024

/******************************************************************************
 * STATIC VARIABLES
 *****************************************************************************/

static const char * as_log_level_strings[5] = {
	[AS_LOG_LEVEL_ERROR]	= "ERROR",
	[AS_LOG_LEVEL_WARN]		= "WARN",
	[AS_LOG_LEVEL_INFO]		= "INFO",
	[AS_LOG_LEVEL_DEBUG]	= "DEBUG",
	[AS_LOG_LEVEL_TRACE] 	= "TRACE"
};

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

 static bool as_log_stderr(
	as_log_level level, const char * func, const char * file, uint32_t line,
	const char * fmt, ...)
{
	char msg[AS_LOG_MSG_LEN] = {0};

	va_list ap;
	va_start(ap, fmt);
	vsnprintf(msg, AS_LOG_MSG_LEN, fmt, ap);
	va_end(ap);

	fprintf(stderr, "[%s:%d][%s] %s - %s\n", 
		file, line, func, as_log_level_strings[level], msg);
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

	log->level = AS_LOG_LEVEL_INFO;
	log->callback = as_log_stderr;
	return log;
}

/**
 * Set the level for the given log
 */
int as_log_set_level(as_log * log, as_log_level level) 
{
	if ( !log ) return 1;
	log->level = level;
	return 0;
}

/**
 * Set the callback for the given log
 */
int as_log_set_callback(as_log * log, as_log_callback callback)
{
	if ( !log ) return 1;
	log->callback = callback;
	return 0;
}



