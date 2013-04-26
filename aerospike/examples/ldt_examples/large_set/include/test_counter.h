/*
 *  Aerospike Performance Test Counter
 *  Define the counter mechanism for general performance tests.
 *
 *  Copyright 2013 by Citrusleaf, Aerospike Inc.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 *
 */
#pragma once

#include <pthread.h>
#include <stdint.h>

#ifndef ATOMIC_STRUCT
typedef struct atomic_int_s {
	uint64_t		val;
	pthread_mutex_t	lock;
} atomic_int;

// We differentiate between a read call and the number of values
// that are passed back.  If all read calls return a single value,
// then these two values are the same.  If each read returns 100
// values, then read_vals will be 100x the read calls.
// This is more likely on Read calls than write/delete calls, but
// are ready for the more general case.
typedef struct {
    atomic_int      *read_ops;
    atomic_int      *read_vals;
    atomic_int      *write_ops;
    atomic_int      *write_vals;
    atomic_int      *delete_ops;
    atomic_int      *delete_vals;
    atomic_int      *keys;

    int             death;
    pthread_t       th;
} test_counter_thread_control;

#define ATOMIC_STRUCT
#endif


extern atomic_int * atomic_int_create(uint64_t val);
extern void			atomic_int_destroy(atomic_int *ai);
extern uint64_t		atomic_int_add(atomic_int *ai, int val);
extern uint64_t		atomic_int_get(atomic_int *ai);

void * counter_fn( void * arg );
void * start_test_counter_thread( atomic_int *read_ops, // Read calls
                                  atomic_int *read_vals,// Read values
                                  atomic_int *write_ops,
                                  atomic_int *write_vals,
                                  atomic_int *delete_ops,
                                  atomic_int *delete_vals,
                                  atomic_int *keys );
void stop_test_counter_thread( void * control );
