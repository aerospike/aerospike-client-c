/******************************************************************************
 *	Copyright 2008-2013 by Aerospike.
 *
 *	Permission is hereby granted, free of charge, to any person obtaining a copy 
 *	of this software and associated documentation files (the "Software"), to 
 *	deal in the Software without restriction, including without limitation the 
 *	rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 *	sell copies of the Software, and to permit persons to whom the Software is 
 *	furnished to do so, subject to the following conditions:
 *	
 *	The above copyright notice and this permission notice shall be included in 
 *	all copies or substantial portions of the Software.
 *	
 *	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 *	FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 *	IN THE SOFTWARE.
 *****************************************************************************/

#pragma once 

#include <aerospike/as_status.h>

#include <citrusleaf/cf_atomic.h>

#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/******************************************************************************
 *	TYPES
 *****************************************************************************/

/**
 *	Log Level
 */
typedef enum as_log_level_e {
	AS_LOG_LEVEL_OFF 	= -1,
	AS_LOG_LEVEL_ERROR	= 0,
	AS_LOG_LEVEL_WARN	= 1,
	AS_LOG_LEVEL_INFO	= 2,
	AS_LOG_LEVEL_DEBUG	= 3,
	AS_LOG_LEVEL_TRACE 	= 4
} as_log_level;

/**
 *	Callback function for as_log related logging calls.
 *
 *	The following is a simple log callback:
 *	~~~~~~~~~~{.c}
 *	bool my_log_callback(
 *		as_log_level level, const char * func, const char * file, uint32_t line,
 *		const char * fmt, ...)
 *	{
 *		char msg[1024] = {0};
 *	
 *		va_list ap;
 *		va_start(ap, fmt);
 *		vsnprintf(msg, 1024, fmt, ap);
 *		msg[1023] = '\0';
 *		va_end(ap);
 *		
 *		fprintf(stderr, "[%s:%d][%s] %d - %s\n", file, line, func, level, msg);
 *	
 *		return true;
 *	}
 *	~~~~~~~~~~
 *
 *	The function should return true on success.
 *
 *
 *	@param level 		The log level of the message.
 *	@param func 		The function where the message was logged.
 *	@param file 		The file where the message was logged.
 *	@param line 		The line where the message was logged.
 *	@param fmt 			The format string used.
 *	@param ... 			The format argument.
 *
 *	@return true if the message was logged. Otherwise false.
 *
 *	@ingroup as_log_object
 */
typedef bool (* as_log_callback)(
	as_log_level level, const char * func, const char * file, uint32_t line,
	const char * fmt, ...);

/**
 *	Aerospike Client exposed logging functionality including:
 *	- Ability to control the verbosity of log messages.
 *	- Direct where log messages are sent to.
 *
 *	Each @ref aerospike contains its own as_log instance: aerospike.log.
 *
 *	## Setting Log Level
 *
 *	To set the log level for the aerospike client, simply use 
 *	as_log_set_level() and pass in the client log to set.
 *
 *	~~~~~~~~~~{.c}
 *	as_log_set_level(&as->log, AS_LOG_LEVEL_INFO);
 *	~~~~~~~~~~
 *
 *	## Redirecting Log Output
 *
 *	By default, the logger sends log messages to STDERR. 
 *
 *	To change where log messages are sent, simply define a new @ref as_log_callback,
 *	and set it for the client using as_log_set_callback():
 *
 *	~~~~~~~~~~{.c}
 *	as_log_set_callback(&as->log, my_log_callback);
 *	~~~~~~~~~~
 *
 *	Where the `my_log_callback` could be defined as 
 *
 *	~~~~~~~~~~{.c}
 *	bool my_log_callback(
 *	    as_log_level level, const char * func, const char * file, uint32_t line,
 *	    const char * fmt, ...)
 *	{
 *	    char msg[1024] = {0};
 *	    va_list ap;
 *	    va_start(ap, fmt);
 *	    vsnprintf(msg, 1024, fmt, ap);
 *	    msg[1023] = '\0';
 *	    va_end(ap);
 *	    fprintf(stderr, "[%s:%d][%s] %d - %s\n", file, line, func, level, msg);
 *	    return true;
 *	}
 *	~~~~~~~~~~
 *
 *	@ingroup client_objects
 */
typedef struct as_log_s {

	/**
	 *	Log Level
	 */
	cf_atomic32 level;

	/**
	 *	Logging Callback
	 */
	cf_atomic_p callback;

} as_log;

/******************************************************************************
 *	FUNCTIONS
 *****************************************************************************/

/**
 *	Initialize Log Context 
 *
 *	@relates as_log
 */
as_log * as_log_init(as_log * log);

/**
 *	Set the level for the given log.
 *
 *	@param log 		The log context.
 *	@param level 	The log level.
 *
 *	@return true on success. Otherwise false.
 *
 *	@relates as_log
 */
bool as_log_set_level(as_log * log, as_log_level level);

/**
 *	Set the callback for the given log
 *
 *	@param log 		The log context.
 *	@param callback 	The log callback.
 *
 *	@return true on success. Otherwise false.
 *
 *	@relates as_log
 */
bool as_log_set_callback(as_log * log, as_log_callback callback);
