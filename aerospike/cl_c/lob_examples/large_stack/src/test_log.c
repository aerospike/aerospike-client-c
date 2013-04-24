/*  Citrusleaf Large Object Stack Test Program
 *  Logging/Tracing
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <stdarg.h>
#include "test_log.h"


// NOTE: INFO(), ERROR() and LOG() defined in log.h
void __log_append(FILE * f, const char * prefix, const char * fmt, ...)
{
    char msg[128] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 128, fmt, ap);
    va_end(ap);
    fprintf(f, "%s%s\n",prefix,msg);
}

