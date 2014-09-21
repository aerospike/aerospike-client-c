/*
 * Copyright 2008-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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

