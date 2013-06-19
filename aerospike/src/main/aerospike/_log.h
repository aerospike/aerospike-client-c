#pragma once

#include <aerospike/as_log.h>
#include <citrusleaf/cf_log_internal.h>

/******************************************************************************
 * as_log.h MACROS
 *****************************************************************************/

#define LOGGER &as->log

#define as_error(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->callback && AS_LOG_LEVEL_ERROR <= (__ctx)->level ) {\
		((as_log_callback) (__ctx)->callback)(AS_LOG_LEVEL_ERROR, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_warn(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->callback && AS_LOG_LEVEL_WARN <= (__ctx)->level ) {\
		((as_log_callback) (__ctx)->callback)(AS_LOG_LEVEL_WARN, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_info(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->callback && AS_LOG_LEVEL_INFO <= (__ctx)->level ) {\
		((as_log_callback) (__ctx)->callback)(AS_LOG_LEVEL_INFO, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_debug(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->callback && AS_LOG_LEVEL_DEBUG <= (__ctx)->level ) {\
		((as_log_callback) (__ctx)->callback)(AS_LOG_LEVEL_DEBUG, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define as_trace(__ctx, __fmt, ... ) \
	if ( (__ctx) && (__ctx)->callback && AS_LOG_LEVEL_TRACE <= (__ctx)->level ) {\
		((as_log_callback) (__ctx)->callback)(AS_LOG_LEVEL_TRACE, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define _log_debug(__fmt, ... ) \
	cf_debug("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);

#define _log_info(__fmt, ... ) \
	cf_info("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);

#define _log_warn(__fmt, ... ) \
	cf_warn("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);

#define _log_error(__fmt, ... ) \
	cf_error("@%s[%s:%d] - "__fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__);
