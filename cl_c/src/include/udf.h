/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "citrusleaf.h"
#include "cluster.h"
#include "as_arraylist.h"
#include "as_types.h"

typedef struct cl_udf_call_s cl_udf_call;

struct cl_udf_call_s {
    const char *    filename;
    const char *    function;
    as_list *       arglist;
};



cl_rv citruslead_udf_set(cl_cluster * asc, const char * package, const char * content, char ** err_str, cl_script_lang_t lang);
cl_rv citruslead_udf_get(cl_cluster * asc, const char * package_name, char ** content, int * content_len, cl_script_lang_t lang);
cl_rv citruslead_udf_delete(cl_cluster * asc, const char * package, cl_script_lang_t lang);
cl_rv citruslead_udf_list(cl_cluster * asc, char *** package_names, int *n_packages, cl_script_lang_t lang);

cl_rv citruslead_udf_apply_record(cl_cluster *asc, const char * ns, const char * set, const cl_object * key, cl_udf_call * call);


static inline as_list * citrusleaf_arglist_new(uint32_t size) {
    return as_arraylist_new(size, 1);
}

static inline int citruslead_arglist_add(as_list * l, as_val * v) {
    return as_list_append(l, v);
}

static inline int citrusleaf_arglist_add_string(as_list * l, const char * s) {
    return as_list_append(l, (as_val *) as_string_new(cf_strdup(s)));
}

static inline int citruslead_arglist_add_integer(as_list * l, uint64_t i) {
    return as_list_append(l, (as_val *) as_integer_new(i));
}

static inline int citruslead_arglist_add_list(as_list * l, as_list * list) {
    return as_list_append(l, (as_val *) list);
}

static inline int citruslead_arglist_add_map(as_list * l, as_map * m) {
    return as_list_append(l, (as_val *) m);
}

static inline int citrusleaf_arglist_free(as_list * l) {
    return as_list_free(l);
}