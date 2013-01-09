/*
 * A general purpose hashtable implementation
 * Uses locks, so only moderately fast
 * Just, hopefully, the last hash table you'll ever need
 * And you can keep adding cool things to it
 * Copywrite 2008 Brian Bulkowski
 * All rights reserved
 */

#pragma once
 
#include <inttypes.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>

// get the nice juicy SSL random bytes
#include <openssl/rand.h>

extern uint64_t rand_64();

typedef struct atomic_int_s {
	uint64_t		val;
	pthread_mutex_t	lock;
} atomic_int;

extern atomic_int	*atomic_int_create(uint64_t val);
extern void			atomic_int_destroy(atomic_int *ai);
extern uint64_t		atomic_int_add(atomic_int *ai, int val);
extern uint64_t		atomic_int_get(atomic_int *ai);



