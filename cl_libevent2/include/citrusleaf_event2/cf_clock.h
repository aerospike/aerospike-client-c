/*
 *  Citrusleaf Foundation
 *  src/clock.c - memory allocation framework
 *
 *  Copyright 2008-2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once

#include <time.h>
#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif


inline static uint64_t 
TIMESPEC_TO_MS( struct timespec *ts )
{
	uint64_t r1 = ts->tv_nsec;
	r1 /= 1000000;
	uint64_t r2 = ts->tv_sec;
	r2 *= 1000;
	return( r1 + r2 );
}


// FIXME ought to be cf_clock_getvolatile 
inline static uint64_t
cf_getms() {
	struct timespec ts;
	clock_gettime( CLOCK_MONOTONIC, &ts);
	return ( TIMESPEC_TO_MS( &ts ) );
}	


static inline uint64_t 
cf_getmicros()
{
	struct timespec ts = { 0, 0};
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
	uint64_t micro = (ts.tv_sec * 1000000) + (ts.tv_nsec / 1000);
	return(micro);
}



inline static uint64_t
cf_clock_getabsolute() {
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return(TIMESPEC_TO_MS(&ts));
}


#ifdef __cplusplus
} // end extern "C"
#endif

