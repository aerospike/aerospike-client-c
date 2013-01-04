/*
 *      async.h
 *
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

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