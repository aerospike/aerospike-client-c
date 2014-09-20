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
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_log.h>
#include <citrusleaf/cf_types.h>

static void
cf_default_log(cf_log_level level, const char* fmt_no_newline, ...)
{
	(void)level; // Suppress not used warning.
	(void)fmt_no_newline; // Suppress not used warning.
	/* Do not log by default.
	(void)level; // Suppress level not used warning.
	size_t fmt_size = strlen(fmt_no_newline) + 2;
	char* fmt = (char*)alloca(fmt_size);

	strncpy(fmt, fmt_no_newline, fmt_size);
	fmt[fmt_size - 2] = '\n';

	va_list ap;

	va_start(ap, fmt_no_newline);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	*/
}

cf_atomic32 g_log_level = (cf_atomic32)CF_INFO;
cf_atomic_p g_log_callback = (cf_atomic_p)cf_default_log;
