/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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
 ******************************************************************************/
#include "latency.h"
#include <aerospike/ck/ck_pr.h>
#include <citrusleaf/alloc.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void
latency_init(latency* l, int columns, int shift)
{
	l->last_bucket = columns - 1;
	l->bit_shift = shift;
	l->buckets = cf_calloc(columns, sizeof(uint32_t));
}

void
latency_free(latency* l)
{
	cf_free((void*)l->buckets);
}

static int
latency_getindex(latency* l, uint64_t elapsed_ms)
{
	uint64_t limit = 1;
	int max = l->last_bucket;
	int shift = l->bit_shift;
	
	for (int i = 0; i < max; i++) {
		if (elapsed_ms <= limit) {
			return i;
		}
		limit <<= shift;
	}
	return max;
}

void
latency_add(latency* l, uint64_t elapsed_ms)
{
	int index = latency_getindex(l, elapsed_ms);
	ck_pr_inc_32(&l->buckets[index]);
}

void
latency_set_header(latency* l, char* header)
{
	char* p = header;
	p += sprintf(p, "      <=1ms >1ms");
	
	int max = l->last_bucket + 1;
	int shift = l->bit_shift;
	int limit = 1;

	for (int i = 2; i < max; i++) {
		limit <<= shift;
		p += sprintf(p, " >%dms", limit);
	}
	*p = 0;
}

static int
latency_print_column(latency* l, int limit, double sum, int value, char* out)
{
	int percent = 0;
	
	if (value > 0) {
		percent = (int)((double)value * 100.0 / sum + 0.5);
	}
	
	char fmt[32];
	int width = sprintf(fmt, "%d", limit) + 3;
	sprintf(fmt, "%%%dd%%%%", width);
	return sprintf(out, fmt, percent);
}

/**
 * Print latency percents for specified cumulative ranges.
 * This function is not absolutely accurate for a given time slice because this method
 * is not synchronized with the add() method.  Some values will slip into the next iteration.
 * It is not a good idea to add extra locks just to measure performance since that actually
 * affects performance.  Fortunately, the values will even out over time
 * (ie. no double counting).
 */
void
latency_print_results(latency* l, const char* prefix, char* out) {
	// Capture snapshot and make buckets cumulative.
	int max = l->last_bucket + 1;
	uint32_t* array = alloca(max * sizeof(uint32_t));
	uint32_t* buckets = l->buckets;
	int sum = 0;
	int count;
	
	for (int i = max - 1; i >= 1 ; i--) {
		count = ck_pr_fas_32(&buckets[i], 0);
		array[i] = count + sum;
		sum += count;
	}
	// The first bucket (<=1ms) does not need a cumulative adjustment.
	count = ck_pr_fas_32(&buckets[0], 0);
	array[0] = count;
	sum += count;
	
	// Print cumulative results.
	char* p = out;
	p += sprintf(p, "%-6s", prefix);
		
	double sumDouble = (double)sum;
	int limit = 1;
	
	p += latency_print_column(l, limit, sumDouble, array[0], p);
	p += latency_print_column(l, limit, sumDouble, array[1], p);

	int shift = l->bit_shift;

	for (int i = 2; i < max; i++) {
		limit <<= shift;
		p += latency_print_column(l, limit, sumDouble, array[i], p);
	}
	*p = 0;
}
