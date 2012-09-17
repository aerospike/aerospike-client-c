/*
 * Copyright 2012 Aerospike. All rights reserved.
 */
#pragma once

#include <stdarg.h>

void cf_error(const char* fmt, ...);

typedef enum {
	CF_NO_LOGGING = -1,
	CF_ERROR,
	CF_WARN,
	CF_INFO,
	CF_DEBUG
} cf_log_level;

typedef void (*cf_log_callback)(cf_log_level level, const char* fmt, ...);

extern cf_log_level g_log_level;
extern cf_log_callback g_log_callback;

static inline void cf_set_log_level(cf_log_level level)
{
	g_log_level = level;
}

static inline void cf_set_log_callback(cf_log_callback callback)
{
	if (callback) {
		g_log_callback = callback;
	}
}

#define cf_error(__fmt, __args...) if (CF_ERROR <= g_log_level) {(*g_log_callback)(CF_ERROR, __fmt, ## __args);}
#define cf_warn(__fmt, __args...) if (CF_WARN <= g_log_level) {(*g_log_callback)(CF_WARN, __fmt, ## __args);}
#define cf_info(__fmt, __args...) if (CF_INFO <= g_log_level) {(*g_log_callback)(CF_INFO, __fmt, ## __args);}
#define cf_debug(__fmt, __args...) if (CF_DEBUG <= g_log_level) {(*g_log_callback)(CF_DEBUG, __fmt, ## __args);}
