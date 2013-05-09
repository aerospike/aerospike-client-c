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

#include <stdint.h>

#include <citrusleaf/cf_atomic.h>

/* SYNOPSIS
 * For timing things, you want to know this histogram of what took how much time.
 * So have an interface where you create a histogram object, can dump a histogram object,
 * and can "start" / "stop" a given timer and add it to the histogram - multithread safe,
 * of course.
 */

#define CF_N_HIST_COUNTS 64

//
// The counts are powers of two. count[0]
// count[0] is 1024 * 1024 a second
// count[13] is about a millisecond (1/1024 second)
// count[25] is a second


typedef struct cf_histogram_counts_s {
	uint64_t count[CF_N_HIST_COUNTS];
} cf_histogram_counts;

typedef struct cf_histogram_s {
	char name[64];
	cf_atomic_int n_counts;
	cf_atomic_int count[CF_N_HIST_COUNTS];
} cf_histogram;

extern cf_histogram * cf_histogram_create(char *name);
extern void cf_histogram_dump( cf_histogram *h );  // for debugging
extern void cf_histogram_get_counts(cf_histogram *h, cf_histogram_counts *hc);
extern void cf_histogram_insert_data_point(cf_histogram *h, uint64_t start);
