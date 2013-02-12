/*
 *  Citrusleaf Foundation
 *  include/hist.h - timer functionality
 *
 *  Copyright 2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#pragma once

#include <stdint.h>

#include "cf_atomic.h"

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

/* SYNOPSIS
 * Some bithacks are eternal and handy
 * http://graphics.stanford.edu/~seander/bithacks.html
 */

#define cf_bits_find_first_set(__x) ffs(__x)
#define cf_bits_find_first_set_64(__x) ffsll(__x)
 
extern int cf_bits_find_last_set(uint32_t c);
extern int cf_bits_find_last_set_64(uint64_t c);

static const char cf_LogTable256[] =
{
#define CF_LT(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n
	-1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
	CF_LT(4), CF_LT(5), CF_LT(5), CF_LT(6), CF_LT(6), CF_LT(6), CF_LT(6),
	CF_LT(7), CF_LT(7), CF_LT(7), CF_LT(7), CF_LT(7), CF_LT(7), CF_LT(7), CF_LT(7)
};

// round a value up to the nearest MODULUS

static inline uint32_t cf_roundup( uint32_t i, uint32_t modulus) {
	uint32_t t = i % modulus;
	if (t == 0)	return(i);
	return(  i + (modulus - t ) );
}

static inline uint64_t cf_roundup_64( uint64_t i, uint32_t modulus) {
	uint64_t t = i % modulus;
	if (t == 0)	return(i);
	return(  i + (modulus - t ) );
}

