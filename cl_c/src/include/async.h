/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef void (*cl_async_fail_cb) (void *udata, int rv, uint64_t);
typedef void (*cl_async_success_cb) (void *udata, int rv, uint64_t);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

int citrusleaf_async_init(int size_limit, int num_receiver_threads, cl_async_fail_cb fail_cb_fn, cl_async_success_cb success_cb_fn);
int citrusleaf_async_reinit(int size_limit, unsigned int num_receiver_threads);
void citrusleaf_async_getstats(uint64_t *retries, uint64_t *dropouts, int *workitems);
void citrusleaf_async_set_nw_timeout(int nw_timeout);