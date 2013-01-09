/*
 *      cl_arglist.h
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

#include "as_arraylist.h"
#include "as_integer.h"
#include "as_list.h"
#include "as_map.h"
#include "as_string.h"

/******************************************************************************
 * INLINE FUNCTIONS
 ******************************************************************************/

static inline as_list * as_arglist_new(uint32_t size) {
    return as_arraylist_new(size, 1);
}

static inline int as_list_add_string(as_list * arglist, const char * s) {
    return as_list_append(arglist, (as_val *) as_string_new(cf_strdup(s)));
}

static inline int as_list_add_integer(as_list * arglist, uint64_t i) {
    return as_list_append(arglist, (as_val *) as_integer_new(i));
}

static inline int as_list_add_list(as_list * arglist, as_list * l) {
    return as_list_append(arglist, (as_val *) l);
}

static inline int as_list_add_map(as_list * arglist, as_map * m) {
    return as_list_append(arglist, (as_val *) m);
}
