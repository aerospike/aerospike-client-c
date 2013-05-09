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

#include <stddef.h>

#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_types.h>


/* SYNOPSIS
 * Reference counting allocation
 * This extends the traditional C memory allocation system to support
 * reference-counted garbage collection.  When a memory region is allocated
 * via cf_rc_alloc(), slightly more memory than was requested is actually
 * allocated.  A reference counter is inserted in the excess space at the
 * at the front of the region, and a pointer to the first byte of the data
 * allocation is returned.
 *
 * Two additional functions are supplied to support using a reference
 * counted region: cf_client_rc_reserve() reserves a memory region, and
 * cf_client_rc_release() releases an already-held reservation.  It is possible to
 * call cf_client_rc_release() on a region without first acquiring a reservation.
 * This will result in undefined behavior.
 */



/* cf_client_rc_counter
 * A reference counter */
typedef cf_atomic32 cf_client_rc_counter;

/* Function declarations */
extern cf_atomic_int_t cf_client_rc_count(void *addr);
extern void *cf_client_rc_alloc(size_t sz);
extern int cf_client_rc_reserve(void *addr);
extern cf_atomic_int_t cf_client_rc_release_x(void *addr, bool autofree);
extern void cf_client_rc_free(void *addr);
#define cf_client_rc_release(a) (cf_client_rc_release_x((a), false))
#define cf_client_rc_releaseandfree(a) (cf_client_rc_release_x((a), true))

