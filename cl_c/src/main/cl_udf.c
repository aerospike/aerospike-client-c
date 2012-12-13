/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include "citrusleaf-internal.h"
#include "udf.h"
#include "write.h"

#include <as_msgpack.h>
#include <citrusleaf/cf_b64.h>

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

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
                as_result_tofailure(res, (as_val *) as_string_new("Invalid response. (1)"));
                rv = CITRUSLEAF_FAIL_UDF_BAD_RESPONSE;
            }
        }
        else {
            as_result_tofailure(res, (as_val *) as_string_new("Invalid response. (2)"));
            rv = CITRUSLEAF_FAIL_UDF_BAD_RESPONSE;
        }
    }
    else {
        as_result_tofailure(res, (as_val *) as_string_new("Invalid response. (3)"));
        rv = CITRUSLEAF_FAIL_UDF_BAD_RESPONSE;
    }

    as_serializer_destroy(&ser);
    
    return rv;
}



cl_rv citrusleaf_udf_list(cl_cluster *asc, char *** filenames, int *nfilenames) {
//  fprintf(stderr, "citrusleaf list get \n");
    
    *filenames = NULL;
    *nfilenames = 0;

    char info_query[512];
    if (sizeof(info_query) <= (size_t) snprintf(info_query, sizeof(info_query), "packages;")) {
        fprintf(stderr, "string too long \n"); 
        return(-1);
    }

    char *values = 0;
    // shouldn't do this on a blocking thread --- todo, queue
    if (0 != citrusleaf_info_cluster(asc, info_query, &values, true/*asis*/, 100/*timeout*/)) {
        fprintf(stderr, "could not get package_list from cluster\n");
        return(-1);
    }
    if (0 == values) {
        fprintf(stderr, "info cluster success, but no response on server\n");
        return(-1);
    }
    
    // got response, 
    // format: request\tresponse
    // response is packages=p1,p2,p3;
    
    char *value = strchr(values, '\t') + 1; // skip request, parse response 
    
    int n_tok=0;
    char *brkb = 0;
    char *words[20];
    do {
        words[n_tok] = strtok_r(value,"=",&brkb);
        if (0 == words[n_tok]) break;
        value = 0;
        words[n_tok+1] = strtok_r(value,";",&brkb);
        if (0 == words[n_tok+1]) break;
        char *newline = strchr(words[n_tok+1],'\n');
        if (newline) *newline = 0;
        n_tok += 2;
        if (n_tok >= 20) {
            fprintf(stderr, "too many tokens\n");
            free(values);
            return(-1);
        }
    } while(true);
    
    char *packages_str = 0;
    char *error_str = 0;
    
    
    for (int i = 0; i < n_tok ; i += 2) {
        char *key = words[i];
        char *value = words[i+1];
        if (0 == strcmp(key,"packages")) {
            packages_str = value;
        }
        if (0 == strcmp(key,"error")) {
            error_str = value;
        }
    }
    
    // now break down all the package names
    n_tok = 0;
    do {
        words[n_tok] = strtok_r(packages_str,",",&brkb);
        if (0 == words[n_tok]) break;
        packages_str = NULL;
        n_tok ++;
        if (n_tok >= 20) {
            fprintf(stderr, "too many tokens\n");
            free(values);
            return(-1);
        }
    } while(true);
    
    char **p_names = (char **)malloc(sizeof(char *)*n_tok);
    if (!p_names) {
        fprintf(stderr,"cannot allocate\n");
        free(values);
        return -1;
    }
    
    for (int i=0; i< n_tok; i++) {
        p_names[i] = strdup(words[i]);
    }
    *filenames = p_names;
    *nfilenames = n_tok;

    free(values);
    
    return(0);
    
}

cl_rv citrusleaf_udf_get(cl_cluster *asc, const char * filename, char ** content, int * content_len) {
    return citrusleaf_udf_get_with_gen(asc, filename, content, content_len, NULL);
}


cl_rv citrusleaf_udf_get_with_gen(cl_cluster *asc, const char * filename, char ** content, int * content_len, char **gen) {

    if (content) {
        *content = NULL;
        *content_len = 0;
    }
    if (gen) {
        *gen = NULL;
    }
    
    char info_query[512];
    if (sizeof(info_query) <= 
       (size_t)snprintf(info_query, sizeof(info_query),
                        "package-get:package=%s\n", filename)) {
        return(-1);
    }

    char *values = 0;
    // shouldn't do this on a blocking thread --- todo, queue
    if (0 != citrusleaf_info_cluster(asc, info_query, &values, true/*asis*/, 100/*timeout*/)) {
        fprintf(stderr, "could not get file '%s' from cluster\n", filename);
        return(-1);
    }
    if (0 == values) {
        fprintf(stderr, "info cluster success, but no file %s on server\n",filename);
        return(-1);
    }
    
    // got response, add into cache
    // format: request\tresponse
    // response is gen=asdf;script=xxyefu
    // where gen is a simple string, and script
    // error is something else entirely
    
    char *value = strchr(values, '\t') + 1; // skip request, parse response 
    
    int n_tok=0;
    char *brkb = 0;
    char *words[20];
    do {
        words[n_tok] = strtok_r(value,"=",&brkb);
        if (0 == words[n_tok]) break;
        value = 0;
        words[n_tok+1] = strtok_r(value,";",&brkb);
        if (0 == words[n_tok+1]) break;
        char *newline = strchr(words[n_tok+1],'\n');
        if (newline) *newline = 0;
        n_tok += 2;
        if (n_tok >= 20) {
            fprintf(stderr, "too many tokens\n");
            free(values);
            return(-1);
        }
    } while(true);
    
    char *gen_str = 0;
    char *script64_str = 0;
    
    for (int i = 0; i < n_tok ; i += 2) {
        char *key = words[i];
        char *value = words[i+1];
        if (0 == strcmp(key,"gen")) {
            gen_str = value;
        }
        else if (0 == strcmp(key,"script")) {
            script64_str = value;
        } else {
            fprintf(stderr, "package get: unknown key %s value %s\n",key,value);
        }
    }
    if ( (!gen_str) || (!script64_str)) {
        fprintf(stderr, "get package did not return enough data\n");
        free(values);
        return(-1);
    }
    
    // unbase64
    int script_str_len = strlen(script64_str);
    char *script_str = malloc(script_str_len+1); // guarenteed to shrink it
    if (!script_str) {
        free(values);
        return(-1);
    }
    int rv = cf_base64_decode((uint8_t *) script64_str, (uint8_t *) script_str, &script_str_len, true/*validate*/);
    if (rv != 0) {
        fprintf(stderr,"could not decode base64 from server %s\n",script64_str);
        free(script_str);
        free(values);
        return(-1);
    }
    script_str[script_str_len] = 0;
    
    if (content) {
        *content = strdup(script_str);
        *content_len = script_str_len;
    }
    if (gen) {
        *gen = strdup(gen_str);
    }
    free(values);
    
    return(0);
    
}

cl_rv citrusleaf_udf_put(cl_cluster *asc, const char * filename, const char * content, char ** err) {

    if ( !filename || !content) {
        fprintf(stderr, "filename and content required\n");
        return CITRUSLEAF_FAIL_CLIENT;
    }

    int content_len = strlen(content);
    
    int  info_query_len = cf_base64_encode_maxlen(content_len)+strlen(filename)+100;
    char * info_query = malloc(info_query_len); 
    if ( !info_query ) {
        fprintf(stderr, "cannot malloc\n");
        return CITRUSLEAF_FAIL_CLIENT;
    }

    // put in the default stuff first
    snprintf(info_query, info_query_len, "set-package:package=%s",filename);

    cf_base64_tostring((uint8_t *)content, (char *)(info_query+strlen(info_query)), &content_len);
        
    //fprintf(stderr, "**[%s]\n",info_query);

    char *values = 0;

    // shouldn't do this on a blocking thread --- todo, queue
    if (0 != citrusleaf_info_cluster_all(asc, info_query, &values, true/*asis*/, 5000/*timeout*/)) {
        fprintf(stderr, "could not set package %s from cluster\n",filename);
        free(info_query);
        return CITRUSLEAF_FAIL_UNKNOWN;
    }
    if (0 == values) {
        fprintf(stderr, "info cluster success, but no response from server\n");
        free(info_query);
        return CITRUSLEAF_FAIL_UNKNOWN;
    }
    free(info_query);
    
    // got response, 
    // format: request\tresponse
    // response is a string "ok" or ???
    
    char *value = strchr(values, '\t') + 1; // skip request, parse response 
    
    int n_tok=0;
    char *brkb = 0;
    char *words[20];
    do {
        words[n_tok] = strtok_r(value,"=",&brkb);
        if (0 == words[n_tok]) break;
        value = 0;
        words[n_tok+1] = strtok_r(value,";",&brkb);
        if (0 == words[n_tok+1]) break;
        char *newline = strchr(words[n_tok+1],'\n');
        if (newline) *newline = 0;
        n_tok += 2;
        if (n_tok >= 20) {
            fprintf(stderr, "too many tokens\n");
            return CITRUSLEAF_FAIL_UNKNOWN;
        }
    } while(true);
    
    char *err_str = 0;
    
    for (int i = 0; i < n_tok ; i += 2) {
        char *key = words[i];
        char *value = words[i+1];
        if (0 == strcmp(key,"error")) {
            err_str = value;
        } else {
            //fprintf(stderr, "package set: unknown key %s value %s\n",key,value);
        }
    }
    if (err_str) {
        int err_str_len = strlen(err_str);
        // fprintf(stderr, "package set: server returned error %s\n",err_str);
        // fprintf(stderr, "package set: server returned error bytes %d\n",err_str_len);
        char *err_de64 = malloc(err_str_len+1); // guarenteed to shrink it
        if (!err_str) {
            free(values);
            return(-1);
        }
        cf_base64_decode((uint8_t *) err_str, (uint8_t *) err_de64, &err_str_len, true/*validate*/);
        *err = err_de64;
        err_de64[err_str_len] = 0;
        free(values);
        return CITRUSLEAF_FAIL_UNKNOWN;
    }   
    
    free(values);
    
    return(0);
    
}

cl_rv citrusleaf_udf_remove(cl_cluster *asc, const char * filename) {

    if ( !filename ) {
        fprintf(stderr, "filename name required\n");
        return CITRUSLEAF_FAIL_CLIENT;
    }

    char info_query[512];
    if (sizeof(info_query) <= (size_t) snprintf(info_query, sizeof(info_query), "package-delete:package=%s;",filename)) {
        return(-1);
    }
    char *values = 0;

    // shouldn't do this on a blocking thread --- todo, queue
    if (0 != citrusleaf_info_cluster_all(asc, info_query, &values, true/*asis*/, 5000/*timeout*/)) {
        fprintf(stderr, "could not delete file %s from cluster\n",filename);
        return CITRUSLEAF_FAIL_UNKNOWN;
    }
    if (0 == values) {
        fprintf(stderr, "info cluster success, but no response from server\n");
        return CITRUSLEAF_FAIL_UNKNOWN;
    }
    
    // got response, 
    // format: request\tresponse
    // response is a string "ok" or ???
    
    char *value = strchr(values, '\t') + 1; // skip request, parse response 
    
    int n_tok=0;
    char *brkb = 0;
    char *words[20];
    do {
        words[n_tok] = strtok_r(value,"=",&brkb);
        if (0 == words[n_tok]) break;
        value = 0;
        words[n_tok+1] = strtok_r(value,";",&brkb);
        if (0 == words[n_tok+1]) break;
        char *newline = strchr(words[n_tok+1],'\n');
        if (newline) *newline = 0;
        n_tok += 2;
        if (n_tok >= 20) {
            fprintf(stderr, "too many tokens\n");
            return CITRUSLEAF_FAIL_UNKNOWN;
        }
    } while(true);
    
    char *err_str = 0;
    
    for (int i = 0; i < n_tok ; i += 2) {
        char *key = words[i];
        char *value = words[i+1];
        if (0 == strcmp(key,"error")) {
            err_str = value;
        } else {
            //fprintf(stderr, "package set: unknown key %s value %s\n",key,value);
        }
    }
    if (err_str) {
        fprintf(stderr, "package set: server returned error %s\n",err_str);
        free(values);
        return CITRUSLEAF_FAIL_UNKNOWN;
    }   
    
    free(values);
    
    return(0);
    
}       
