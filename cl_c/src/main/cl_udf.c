/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include "udf.h"

cl_rv citruslead_udf_set(cl_cluster * asc, const char * package, const char * content, char ** err_str, cl_script_lang_t lang) {
    return 0;
}

cl_rv citruslead_udf_get(cl_cluster * asc, const char * package_name, char ** content, int * content_len, cl_script_lang_t lang) {
    return 0;
}

cl_rv citruslead_udf_delete(cl_cluster * asc, const char * package, cl_script_lang_t lang) {
    return 0;
}

cl_rv citruslead_udf_list(cl_cluster * asc, char *** package_names, int *n_packages, cl_script_lang_t lang) {
    return 0;
}

cl_rv citruslead_udf_apply_record(cl_cluster *asc, const char * ns, const char * set, const cl_object * key, cl_udf_call * call) {
    return 0;
}


