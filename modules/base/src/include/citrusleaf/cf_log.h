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

#include <stdarg.h>
#include <citrusleaf/cf_atomic.h>

extern cf_atomic32 g_log_level;
extern cf_atomic_p g_log_callback;

#define G_LOG_LEVEL ((int)cf_atomic32_get(g_log_level))
#define G_LOG_CB ((cf_log_callback)cf_atomic_p_get(g_log_callback))

//====================================================================
// Public API
//

/**
 * Log escalation level.
 */
typedef enum {
	/**
	 * Pass this in cf_set_log_level() to suppress all logging.
	 */
	CF_NO_LOGGING = -1,

	/**
	 * Error condition has occurred.
	 */
	CF_ERROR,

	/**
	 * Unusual non-error condition has occurred.
	 */
	CF_WARN,

	/**
	 * Normal information message.
	 */
	CF_INFO,

	/**
	 * Message used for debugging purposes.
	 */
	CF_DEBUG
} cf_log_level;

/**
 * A callback function of this signature may be passed in cf_set_log_callback(),
 * so the caller can channel Aerospike client logs as desired.
 *
 * @param level				log level for this log statement
 * @param fmt				format string for this log statement (does not end
 * 							with '\n')
 * @param ...				arguments corresponding to conversion characters in
 * 							format string
 */
typedef void (*cf_log_callback)(cf_log_level level, const char* fmt, ...);

/**
 * Set logging level filter.
 * <p>
 * Thread-safe - may be called at any time.
 * <p>
 * To suppress logs, either set log level to CF_NO_LOGGING or ignore callbacks.
 *
 * @param level				only show logs at this or more urgent level
 */
static inline void cf_set_log_level(cf_log_level level)
{
	cf_atomic32_set(&g_log_level, (cf_atomic32)level);
}

/**
 * Set optional log callback.
 * <p>
 * Thread-safe - may be called at any time.
 * <p>
 * If no callback is registered, the Aerospike client writes logs to stderr.
 * <p>
 * To suppress logs, either set log level to CF_NO_LOGGING or ignore callbacks.
 *
 * @param callback			cf_log_callback implementation
 */
static inline void cf_set_log_callback(cf_log_callback callback)
{
	if (callback) {
		cf_atomic_p_set(&g_log_callback, (cf_atomic_p)callback);
	}
}

static inline int cf_info_enabled()
{
	return CF_INFO <= G_LOG_LEVEL;
}

static inline int cf_debug_enabled()
{
	return CF_DEBUG <= G_LOG_LEVEL;
}
