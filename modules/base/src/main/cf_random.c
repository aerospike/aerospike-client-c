/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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
 *****************************************************************************/

#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_types.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <openssl/rand.h>

#define SEED_SZ 64
static uint8_t rand_buf[1024 * 8];
static uint rand_buf_off = 0;
static int  seeded = 0;
static pthread_mutex_t rand_buf_lock = PTHREAD_MUTEX_INITIALIZER;

#ifdef __APPLE__

int cf_rand_reload() {
    if (seeded == 0) {
        arc4random_stir();
        seeded = 1;
    }
    
    arc4random_buf(rand_buf, sizeof(rand_buf));
    rand_buf_off = sizeof(rand_buf);
    return(0);
}

#else

int cf_rand_reload() {
    if (seeded == 0) {
        int rfd = open("/dev/urandom",  O_RDONLY);
        int rsz = (int)read(rfd, rand_buf, SEED_SZ);
        if (rsz < SEED_SZ) {
            fprintf(stderr, "warning! can't seed random number generator");
            return(-1);
        }
        close(rfd);
        RAND_seed(rand_buf, rsz);
        seeded = 1;
    }
    if (1 != RAND_bytes(rand_buf, sizeof(rand_buf))) {
        fprintf(stderr, "RAND_bytes not so happy.\n");
        return(-1);
    }
    rand_buf_off = sizeof(rand_buf);
    return(0);
}

#endif


int cf_get_rand_buf(uint8_t *buf, int len) {
    if ((uint)len >= sizeof(rand_buf))  return(-1);
    
    pthread_mutex_lock(&rand_buf_lock);
    
    if (rand_buf_off < (uint)len ) {
        if (-1 == cf_rand_reload()) {
            pthread_mutex_unlock(&rand_buf_lock);
            return(-1);
        }
    }

    rand_buf_off -= len;
    memcpy(buf, &rand_buf[rand_buf_off] ,len);

    pthread_mutex_unlock(&rand_buf_lock);   

    return(0);  
}


uint64_t cf_get_rand64() {
    pthread_mutex_lock(&rand_buf_lock);
    if (rand_buf_off < sizeof(uint64_t) ) {
        if (-1 == cf_rand_reload()) {
            pthread_mutex_unlock(&rand_buf_lock);
            return(0);
        }
    }
    rand_buf_off -= sizeof(uint64_t);
    uint64_t r = *(uint64_t *) (&rand_buf[rand_buf_off]);
    pthread_mutex_unlock(&rand_buf_lock);
    return(r);
}

uint32_t cf_get_rand32() {
    pthread_mutex_lock(&rand_buf_lock);
    if (rand_buf_off < sizeof(uint64_t) ) {
        if (-1 == cf_rand_reload()) {
            pthread_mutex_unlock(&rand_buf_lock);
            return(0);
        }
    }
    
    rand_buf_off -= sizeof(uint64_t);
    uint64_t r = *(uint64_t *) (&rand_buf[rand_buf_off]);
    pthread_mutex_unlock(&rand_buf_lock);
    return((uint32_t)r);
}
