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

#include <stdio.h>

// Comment out this line to turn OFF debugging.
// #define DEBUG

#ifdef DEBUG
// With debugging defined, we'll have these mechanisms for printing
#define INFO(fmt, args...)  __log_append(stderr,"", fmt, ## args);
#define ERROR(fmt, args...) __log_append(stderr,"    ", fmt, ## args);
#define LOG(fmt, args...)   __log_append(stderr,"    ", fmt, ## args);
#else
#define INFO(fmt, args...) 
#define ERROR(fmt, args...) 
#define LOG(fmt, args...)  
#endif


// Forward declare the log call.
extern void __log_append(FILE * f, const char * prefix, const char * fmt, ... );

