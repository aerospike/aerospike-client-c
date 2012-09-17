/*
 * Copyright 2012 Aerospike. All rights reserved.
 */
#include <stdio.h>
#include "citrusleaf/cf_log.h"
#include "citrusleaf/cf_atomic.h"

static void cf_default_log(cf_log_level level, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	vfprintf(stderr, fmt, ap);
	fprintf(stderr, "\n");

	va_end(ap);
}

cf_atomic32 g_log_level = (cf_atomic32)CF_INFO;
cf_atomic_p g_log_callback = (cf_atomic_p)cf_default_log;
