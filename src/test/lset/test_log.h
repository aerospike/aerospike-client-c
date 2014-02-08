/*
 *  Aerospike Large SET (lset) Performance Test
 *  Logging support.
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 */
#pragma once

#include <stdio.h>


// Use this to turn on/off tracing/debugging prints and checks
// // Comment out this next line to quiet the output.
#define DEBUG  // Comment out to quiet the output
// #undef DEBUG  // comment out to ensure that no one else turned it on

#ifdef DEBUG
#define TRA_ENTER true   // show method ENTER values
#define TRA_EXIT true    // show method EXIT values
#define TRA_DEBUG true   // show various DEBUG prints
#define TRA_ERROR true   // show ERROR conditions
// Enable the logging output
#define INFO(fmt, args...)  __log_append(stderr,"", fmt, ## args);
#define ERROR(fmt, args...) __log_append(stderr,"    ", fmt, ## args);
#define LOG(fmt, args...)   __log_append(stderr,"    ", fmt, ## args);

#else

#define TRA_ENTER false
#define TRA_EXIT false
#define TRA_DEBUG false
#define TRA_ERROR true   // Best to leave this ON
// Disable the logging output
#define INFO(fmt, args...)   
#define ERROR(fmt, args...) 
#define LOG(fmt, args...)  

#endif


// Here's the log call that we'll be using
extern void __log_append(FILE * f, const char * prefix, const char * fmt, ... );

