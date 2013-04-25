/*
 *  Citrusleaf Tools
 *  src/backup.c - Uses the 'multi_get' function to get a large number of keys, and
 *     store them to a file
 *
 *  Copyright 2009 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>


// get the nice juicy SSL random bytes
#include <openssl/rand.h>
#include "utils.h"



// #define DEBUG 1
// TODO: With backup, we actually care about performance, so fix this shit
// and use the good atomic operations
//
atomic_int	*
atomic_int_create(uint64_t val)
{
	atomic_int *ai = malloc(sizeof(atomic_int));
	ai->val = val;
	pthread_mutex_init(&ai->lock,0);
	return(ai);
}

void			
atomic_int_destroy(atomic_int *ai)
{
	pthread_mutex_destroy(&ai->lock);
	free(ai);
}


uint64_t
atomic_int_add(atomic_int *ai, int val)
{
	uint32_t	rv;
	pthread_mutex_lock(&ai->lock);
	ai->val += val;
	rv = ai->val;
	pthread_mutex_unlock(&ai->lock);
	return(rv);
}

uint64_t		
atomic_int_get(atomic_int *ai)
{
	uint64_t	val;
	pthread_mutex_lock(&ai->lock);
	val = ai->val;
	pthread_mutex_unlock(&ai->lock);
	return(val);
}

//
// Buffer up the random numbers.
//

#define SEED_SZ 64
static uint8_t rand_buf[1024 * 8];
static uint rand_buf_off = 0;
static int	seeded = 0;
static pthread_mutex_t rand_buf_lock = PTHREAD_MUTEX_INITIALIZER;

uint64_t
rand_64()
{
	uint64_t r;
	pthread_mutex_lock(&rand_buf_lock);
	if (rand_buf_off < sizeof(uint64_t) ) {
		if (seeded == 0) {
			int rfd = open("/dev/urandom",	O_RDONLY);
			int rsz = read(rfd, rand_buf, SEED_SZ);
			if (rsz < SEED_SZ) {
				fprintf(stderr, "warning! can't seed random number generator");
				return(0);
			}
			close(rfd);
			RAND_seed(rand_buf, rsz);
			seeded = 1;
		}
		if (1 != RAND_bytes(rand_buf, sizeof(rand_buf))) {
			fprintf(stderr, "RAND_bytes not so happy.\n");
			pthread_mutex_unlock(&rand_buf_lock);
			return(0);
		}
		rand_buf_off = sizeof(rand_buf);
	}
	
	rand_buf_off -= sizeof(uint64_t);
	r = *(uint64_t *) (&rand_buf[rand_buf_off]);
	pthread_mutex_unlock(&rand_buf_lock);
	return(r);
}

/* SYNOPSIS */
/* this is a simple test to excersize the sort system.
   Especially good for telling how optimal the code is.
*/

// This is random 64 bit numbers with holes.
// might not fit your pattern of use....

uint64_t *
random_binary_array( uint nelems )
{
	uint64_t *a = malloc( nelems * sizeof(uint64_t) );
	
	RAND_bytes((void *) a, nelems * sizeof(uint64_t ) );
	
	return(a);
	
}


