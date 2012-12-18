/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

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
