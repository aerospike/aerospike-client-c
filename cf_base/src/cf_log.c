/*
 * Copyright 2012 Aerospike. All rights reserved.
 */
#include <stdio.h>
#include "citrusleaf/cf_log.h"

static void cf_default_log(cf_log_level level, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	vfprintf(stderr, fmt, ap);
	fprintf(stderr, "\n");

	va_end(ap);
}

cf_log_level g_log_level = CF_INFO;
cf_log_callback g_log_callback = cf_default_log;
