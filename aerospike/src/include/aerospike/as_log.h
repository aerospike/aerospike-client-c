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

#pragma once 

#include <aerospike/as_status.h>

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Log Level
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
 * Callback function for as_log related logging calls.
 */
typedef bool (* as_log_callback)(
	as_log_level level, const char * func, const char * file, uint32_t line,
	const char * fmt, ...);

/**
 * Logging Context
 */
typedef struct as_log_s {

	/**
	 * Log Level
	 */
	as_log_level level;

	/**
	 * Logging Callback
	 */
	as_log_callback callback;

} as_log;

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define as_error(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->level && (__ctx)->callback && AS_LOG_LEVEL_ERROR <= (__ctx)->level ) {\
		(__ctx)->callback(AS_LOG_LEVEL_ERROR, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_warn(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->level && (__ctx)->callback && AS_LOG_LEVEL_WARN <= (__ctx)->level ) {\
		(__ctx)->callback(AS_LOG_LEVEL_WARN, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_info(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->level && (__ctx)->callback && AS_LOG_LEVEL_INFO <= (__ctx)->level ) {\
		(__ctx)->callback(AS_LOG_LEVEL_INFO, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_debug(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->level && (__ctx)->callback && AS_LOG_LEVEL_DEBUG <= (__ctx)->level ) {\
		(__ctx)->callback(AS_LOG_LEVEL_DEBUG, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_trace(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->level && (__ctx)->callback && AS_LOG_LEVEL_TRACE <= (__ctx)->level ) {\
		(__ctx)->callback(AS_LOG_LEVEL_TRACE, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initialize Log Context 
 */
as_log * as_log_init(as_log * log);

/**
 * Set the level for the given log
 */
int as_log_set_level(as_log * log, as_log_level level);

/**
 * Set the callback for the given log
 */
int as_log_set_callback(as_log * log, as_log_callback callback);
