/*
 *  Citrusleaf Foundation
 *  src/clock.c - memory allocation framework
 *
 *  Copyright 2008-2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once

#include <stdint.h>


#ifndef CF_WINDOWS
//====================================================================
// Linux
//

#include <time.h>
#include <bits/time.h>


#else // CF_WINDOWS
//====================================================================
// Windows
//

#include <WinSock2.h>	// for struct timeval
#include <pthread.h>	// for struct timespec

#define CLOCK_REALTIME				0
#define CLOCK_MONOTONIC				1
#define CLOCK_PROCESS_CPUTIME_ID	2


inline static LARGE_INTEGER
getFILETIMEoffset()
{
	SYSTEMTIME s;
	FILETIME f;
	LARGE_INTEGER t;

	s.wYear = 1970;
	s.wMonth = 1;
	s.wDay = 1;
	s.wHour = 0;
	s.wMinute = 0;
	s.wSecond = 0;
	s.wMilliseconds = 0;
	SystemTimeToFileTime(&s, &f);
	t.QuadPart = f.dwHighDateTime;
	t.QuadPart <<= 32;
	t.QuadPart |= f.dwLowDateTime;

	return (t);
}

static inline int clock_gettime(int clock_type, struct timespec* p_ts)
{
    LARGE_INTEGER           t;
    FILETIME                f;
    double                  nanoseconds;
    static LARGE_INTEGER    offset;
    static double           frequencyToNanoseconds;
    static int              initialized = 0;
    static BOOL             usePerformanceCounter = 0;

    if (!initialized) {
        LARGE_INTEGER performanceFrequency;
        initialized = 1;
        usePerformanceCounter = QueryPerformanceFrequency(&performanceFrequency);
        if (usePerformanceCounter) {
            QueryPerformanceCounter(&offset);
            frequencyToNanoseconds = (double)performanceFrequency.QuadPart / 1000000000.;
        } else {
            offset = getFILETIMEoffset();
            frequencyToNanoseconds = 10000.;
        }
    }

    if (usePerformanceCounter) {
		QueryPerformanceCounter(&t);
	}
    else {
        GetSystemTimeAsFileTime(&f);
        t.QuadPart = f.dwHighDateTime;
        t.QuadPart <<= 32;
        t.QuadPart |= f.dwLowDateTime;
    }

    t.QuadPart -= offset.QuadPart;
    nanoseconds = (double)t.QuadPart / frequencyToNanoseconds;
    t.QuadPart = (LONGLONG)nanoseconds;
    p_ts->tv_sec = t.QuadPart / 1000000000;
    p_ts->tv_nsec = t.QuadPart % 1000000000;

	return 0;
}

#endif // CF_WINDOWS


#ifdef __cplusplus
extern "C" {
#endif

#ifdef OSX // Macs aren't posix compliant
#include <sys/time.h>
#include <mach/mach.h>
#include <mach/mach_time.h>
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
#ifdef OSX
	// XXX from what I can tell, mach_absolute_time is a monotonic clock. At least, it appears
	// to be used that way by game programmers. The literature is unclear about this.
	// However, to allow people to use mach_absolute_time across multiple platforms, the 
	// designers decided not to specify the units of time that it returned, but instead paired it
	// with a timebase function that can be used to determine the units.
	// While this ends up being very fast, it also requires an initialization which is slightly
	// outside of the current CL architecture. So, instead of changing CL, I'm going to just
	// use the fact that mach_absolute_time returns nanoseconds on modern macs, and accept
	// that at some point this implementation may break. Que sera sera. - csw
	// mach_timebase_info_data_t    sTimebaseInfo; 
	// mach_timebase_info(&sTimebaseInfo);
	// - or - 
	// pthread_once(&mtime_timebase_info_once, mtime_init_timebase);
	uint64_t date = mach_absolute_time();
	
	/* Convert to nanoseconds  - we're OSX on the Mac, it's already ns... see above */
	// date *= sTimebaseInfo.numer;
	// date /= sTimebaseInfo.denom;

	/* Convert to milliseconds */
	return date / 1000000;	

//	nanouptime(&ts);
#else
	struct timespec ts;
	clock_gettime( CLOCK_MONOTONIC, &ts);
	return ( TIMESPEC_TO_MS( &ts ) );
#endif
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
TIMESPEC_TO_US( struct timespec *ts )
{
	uint64_t r1 = ts->tv_nsec;
	r1 /= 1000;
	uint64_t r2 = ts->tv_sec;
	r2 *= 1000000;
	return( r1 + r2 );
}
// FIXME ought to be cf_clock_getvolatile 
inline static uint64_t
cf_getus() {
#ifdef OSX
	uint64_t date = mach_absolute_time();
	
	/* Convert to nanoseconds  - we're OSX on the Mac, it's already ns... see above */
	// date *= sTimebaseInfo.numer;
	// date /= sTimebaseInfo.denom;

	/* Convert to microseconds */
	return date / 1000;	

#else
	struct timespec ts;
	clock_gettime( CLOCK_MONOTONIC, &ts);
	return ( TIMESPEC_TO_US( &ts ) );
#endif
}	

inline static uint64_t
cf_clock_getabsolute() {
#ifdef OSX
	struct timeval tv;
	gettimeofday(&tv, NULL);
	
	uint64_t realtime = tv.tv_sec * 1000000 + tv.tv_usec;
	return realtime;
#else
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return(TIMESPEC_TO_MS(&ts));
#endif
}
#ifdef __cplusplus
} // end extern "C"
#endif

//
// Citrusleaf Epoch RTC API
//

#define CITRUSLEAF_EPOCH 1262304000

static inline uint32_t
cf_clepoch_seconds()
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return (uint32_t)(ts.tv_sec - CITRUSLEAF_EPOCH);
}

