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
	(void)level; // Suppress level not used warning.
	size_t fmt_size = strlen(fmt_no_newline) + 2;
	char* fmt = (char*)alloca(fmt_size);

	strncpy(fmt, fmt_no_newline, fmt_size);
	fmt[fmt_size - 2] = '\n';

	va_list ap;

	va_start(ap, fmt_no_newline);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
}

cf_atomic32 g_log_level = (cf_atomic32)CF_INFO;
cf_atomic_p g_log_callback = (cf_atomic_p)cf_default_log;
