/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include "citrusleaf-internal.h"
#include "udf.h"
#include "write.h"

#include <as_msgpack.h>

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

// cl_rv citrusleaf_udf_set(cl_cluster * asc, const char * package, const char * content, char ** err_str, cl_script_lang_t lang) {
//     return 0;
// }

// cl_rv citrusleaf_udf_get(cl_cluster * asc, const char * package_name, char ** content, int * content_len, cl_script_lang_t lang) {
//     return 0;
// }

// cl_rv citrusleaf_udf_delete(cl_cluster * asc, const char * package, cl_script_lang_t lang) {
//     return 0;
// }

// cl_rv citrusleaf_udf_list(cl_cluster * asc, char *** package_names, int *n_packages, cl_script_lang_t lang) {
//     return 0;
// }

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }


int print_buffer(as_buffer * buff) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    sbuf.data = buff->data;
    sbuf.size = buff->size;
    sbuf.alloc = buff->capacity;

    msgpack_zone mempool;
    msgpack_zone_init(&mempool, 2048);

    msgpack_object deserialized;
    msgpack_unpack(sbuf.data, sbuf.size, NULL, &mempool, &deserialized);
    
    printf("b: ");
    msgpack_object_print(stdout, deserialized);
    puts("");

    msgpack_zone_destroy(&mempool);
    return 0;
}

cl_rv citrusleaf_udf_record_apply(cl_cluster * cl, const char * ns, const char * set, const cl_object * key, const char * filename, const char * function, as_list * arglist, int timeout_ms, as_result * res) {

    cl_rv rv = CITRUSLEAF_OK;

    as_serializer ser;
    as_msgpack_init(&ser);

    as_string file;
    as_string_init(&file, (char *) filename);

    as_string func;
    as_string_init(&func, (char *) function);
    
    as_buffer args;
    as_buffer_init(&args);

    as_serializer_serialize(&ser, (as_val *) arglist, &args);

    as_call call = {
        .file = &file,
        .func = &func,
        .args = &args
    };

    uint64_t trid = 0;

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = timeout_ms;

    cl_bin * bins = NULL;
    int nbins = 0;

    // print_buffer(&args);

    do_the_full_monte( 
        cl, 0, CL_MSG_INFO2_WRITE, 0, 
        ns, set, key, 0, &bins, CL_OP_WRITE, 0, &nbins, 
        NULL, &wp, &trid, NULL, &call
    );

    as_buffer_destroy(&args);

    if ( nbins == 1 && bins != NULL ) {
        cl_bin bin = *bins;

        as_val * val = NULL;

        switch( bin.object.type ) {
            case CL_INT : {
                val = (as_val *) as_integer_new(bin.object.u.i64);
                break;
            }
            case CL_STR : {
                val = (as_val *) as_string_new(bin.object.u.str);
                break;
            }
            case CL_LIST :
            case CL_MAP : {
                as_buffer buf = {
                    .capacity = (uint32_t) bin.object.sz,
                    .size = (uint32_t) bin.object.sz,
                    .data = (char *) bin.object.u.blob
                };
                // print_buffer(&buf);
                as_serializer_deserialize(&ser, &buf, &val);
                break;
            }
            default : {
                val = NULL;
                break;
            }
        }

        if ( val ) {
            if ( strcmp(bin.bin_name,"FAILURE") == 0 ) {
                as_result_tofailure(res, val);
            }
            else if ( strcmp(bin.bin_name,"SUCCESS") == 0 ) {
                as_result_tosuccess(res, val);
            }
            else {
                as_result_tofailure(res, as_string_new("Invalid response. (1)"));
                rv = CITRUSLEAF_FAIL_UDF_BAD_RESPONSE;
            }
        }
        else {
            as_result_tofailure(res, as_string_new("Invalid response. (2)"));
            rv = CITRUSLEAF_FAIL_UDF_BAD_RESPONSE;
        }
    }
    else {
        as_result_tofailure(res, as_string_new("Invalid response. (3)"));
        rv = CITRUSLEAF_FAIL_UDF_BAD_RESPONSE;
    }

    as_serializer_destroy(&ser);
    
    return rv;
}


