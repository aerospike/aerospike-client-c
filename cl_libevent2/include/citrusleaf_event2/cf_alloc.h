/*
 *  Citrusleaf Foundation
 *  include/alloc.h - memory allocation framework
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#pragma once
#include <stdbool.h>

#include "cf_atomic.h"


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
 * counted region: cf_rc_reserve() reserves a memory region, and
 * cf_rc_release() releases an already-held reservation.  It is possible to
 * call cf_rc_release() on a region without first acquiring a reservation.
 * This will result in undefined behavior.
 */



/* cf_rc_counter
 * A reference counter */
typedef cf_atomic32 cf_rc_counter;

/* Function declarations */
extern cf_atomic_int_t cf_rc_count(void *addr);
extern void *cf_rc_alloc(size_t sz);
extern int cf_rc_reserve(void *addr);
extern cf_atomic_int_t _cf_rc_release(void *addr, bool autofree);
#define cf_rc_release(a) (_cf_rc_release((a), false))
#define cf_rc_releaseandfree(a) (_cf_rc_release((a), true))
extern void cf_rc_free(void *addr);
