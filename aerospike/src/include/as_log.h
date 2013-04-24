/*
 *  Aerospike Client-side Internal Tracing/Debugging Mechanism.
 *  This is for INTERNAL USE ONLY.
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 */
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

