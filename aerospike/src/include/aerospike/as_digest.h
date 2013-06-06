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

#pragma once 

#include <stdbool.h>
#include <stdint.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define AS_DIGEST_VALUE_MAX 20

/******************************************************************************
 * TYPES
 *****************************************************************************/

/**
 * Contains information about an error
 */
struct as_digest_s {
	bool            _free;
	const char *    set;
	const char *    key;
	uint8_t         value[AS_DIGEST_VALUE_MAX];
};

typedef struct as_digest_s as_digest;

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

/**
 * Initializes a digest.
 */
as_digest * as_digest_init(as_digest * digest, const char * set, const char * key);

/**
 * Creates a new digest on the heap.
 */
as_digest * as_digest_new(const char * set, const char * key);

/**
 * Destory the digest, releasing resources.
 */
void as_digest_destroy(as_digest * digest);