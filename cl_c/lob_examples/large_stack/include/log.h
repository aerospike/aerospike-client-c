/*
 *  Aerospike Large Stack (lstack) Performance Test
 *  Logging support.
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 */
#pragma once


#define INFO(fmt, args...) \
//    __log_append(stderr,"", fmt, ## args);

#define ERROR(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

#define LOG(fmt, args...) \
//    __log_append(stderr,"    ", fmt, ## args);


// Here's the log call that we'll be using
extern void __log_append(FILE * f, const char * prefix, const char * fmt, ...);


