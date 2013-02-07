/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#include "citrusleaf-internal.h"
#include "cl_udf.h"
#include "cl_write.h"

#include <as_msgpack.h>
#include <citrusleaf/cf_b64.h>

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct citrusleaf_udf_info_s citrusleaf_udf_info;
typedef struct citrusleaf_udf_filelist_s citrusleaf_udf_filelist;

struct citrusleaf_udf_info_s {
    char *      error;
    char *      filename;
    byte *      content;
    uint32_t content_len;
    char *      gen;
    char *      files;
    int         count;
};
struct citrusleaf_udf_filelist_s {
    int         capacity;
    int         size;
    char **     files;
};

typedef void * (* citrusleaf_parameters_fold_callback)(const char * key, const char * value, void * context);
typedef void * (* citrusleaf_split_fold_callback)(const char * value, void * context);

/******************************************************************************
 * STATIC FUNCTIONS
 ******************************************************************************/

static int citrusleaf_parameters_fold(char * parameters, void * context, citrusleaf_parameters_fold_callback callback);
static int citrusleaf_split_fold(char * str, const char delim, void * context, citrusleaf_split_fold_callback callback);
static void citrusleaf_udf_info_destroy(citrusleaf_udf_info * info);

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }


static void citrusleaf_udf_info_destroy(citrusleaf_udf_info * info) {
    if ( info->error ) free(info->error);
    if ( info->filename ) free(info->filename);
    if ( info->content ) free(info->content);
    if ( info->gen ) free(info->gen);
    if ( info->files ) free(info->files);

    info->error = NULL;
    info->filename = NULL;
    info->content = NULL;
    info->gen = NULL;
    info->files = NULL;
    info->count = 0;
}

static void * citrusleaf_udf_info_parameters(const char * key, const char * value, void * context) {
    citrusleaf_udf_info * info = (citrusleaf_udf_info *) context;
    if ( strcmp(key,"error") == 0 ) {
        info->error = strdup(value);
    }
    if ( strcmp(key,"filename") == 0 ) {
        info->filename = strdup(value);
    }
    else if ( strcmp(key,"gen") == 0 ) {
        info->gen = strdup(value);
    }
    else if ( strcmp(key,"content") == 0 ) {
	info->content = (byte*)calloc(1, sizeof(byte)*(strlen(value)));
        memcpy(info->content, value, strlen(value));
	info->content_len = strlen(value);
    }
    else if ( strcmp(key,"files") == 0 ) {
        info->files = strdup(value);
    }
    else if ( strcmp(key,"count") == 0 ) {
        info->count = atoi(value);
    }
    return info;
}

static void * citrusleaf_udf_list_files(const char * filename, void * context) {
    citrusleaf_udf_filelist * filelist = (citrusleaf_udf_filelist *) context;

    if ( filelist->size < filelist->capacity ) {
        filelist->files[filelist->size] = strdup(filename);
        filelist->size++;
    }

    return filelist;
}


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
    
    msgpack_object_print(stdout, deserialized);
    
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

    rv = do_the_full_monte( 
        cl, 0, CL_MSG_INFO2_WRITE, 0, 
        ns, set, key, 0, &bins, CL_OP_WRITE, 0, &nbins, 
        NULL, &wp, &trid, NULL, &call
    );

    as_buffer_destroy(&args);

	if (! (rv == CITRUSLEAF_OK || rv == CITRUSLEAF_FAIL_UDF_BAD_RESPONSE)) {
    	as_result_tofailure(res, (as_val *) as_string_new("None UDF failure"));
    } else if ( nbins == 1 && bins != NULL ) {
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
            }
        }
        else {
            as_result_tofailure(res, (as_val *) as_string_new("Invalid response. (2)"));
        }
    }
    else {
        as_result_tofailure(res, (as_val *) as_string_new("Invalid response. (3)"));
    }

    as_serializer_destroy(&ser);
    
    return rv;
}



cl_rv citrusleaf_udf_list(cl_cluster *asc, char *** files, int * count, char ** error) {
    
    *files = NULL;
    *count = 0;

    char *  query   = "udf-list";
    char *  result  = 0;

    if ( citrusleaf_info_cluster(asc, query, &result, true, 100) ) {
        if ( error ) {
            const char * emsg = "failed_request: ";
            int emsg_len = strlen(emsg);
            int query_len = strlen(query);
            *error = (char *) malloc(sizeof(char) * (emsg_len + query_len));
            strncpy(*error, emsg, emsg_len);
            strncpy(*error+emsg_len, query, query_len);
        }
        return -1;
    }

    if ( !result ) {
        if ( error ) *error = strdup("invalid_response");
        return -2;
    }
    
    /**
     * result   := {request}\t{response}
     * response := count=<int>;files={files};
     * files    := <string>[,<string>[,...]]
     */
    
    char * response = strchr(result, '\t') + 1;
    
    citrusleaf_udf_info info = { NULL };
    citrusleaf_parameters_fold(response, &info, citrusleaf_udf_info_parameters);

    free(result);
    result = NULL;

    if ( info.error ) {
        if ( error ) {
            *error = info.error;
            info.error = NULL;
        }
        citrusleaf_udf_info_destroy(&info);
        return 1;
    }

    if ( info.count == 0 ) {
        *files = NULL;
        *count = 0;
        citrusleaf_udf_info_destroy(&info);
        return 0;
    }

    citrusleaf_udf_filelist filelist = { 
        .capacity   = info.count,
        .size       = 0,
        .files      = (char **) malloc(info.count * sizeof(char *))
    };

    citrusleaf_split_fold(info.files, ':', &filelist, citrusleaf_udf_list_files);

    *files = filelist.files;
    *count = filelist.size;

    citrusleaf_udf_info_destroy(&info);

    return 0;
    
}

cl_rv citrusleaf_udf_get(cl_cluster *asc, const char * filename, as_udf_file * file, as_udf_type udf_type, char ** error) {
    return citrusleaf_udf_get_with_gen(asc, filename, file, 0, NULL, error);
}

cl_rv citrusleaf_udf_get_with_gen(cl_cluster *asc, const char * filename, as_udf_file * file, as_udf_type udf_type, char **gen, char ** error) {

    if ( !(file->content) ) return -1;

    char    query[512]  = {0};
    char *  result      = NULL;

    snprintf(query, sizeof(query), "udf-get:filename=%s;", filename);

    if ( citrusleaf_info_cluster(asc, query, &result, true, 100) ) {
        if ( error ) {
            const char * emsg = "failed_request: ";
            int emsg_len = strlen(emsg);
            int query_len = strlen(query);
            *error = (char *) malloc(sizeof(char) * (emsg_len + query_len));
            strncpy(*error, emsg, emsg_len);
            strncpy(*error+emsg_len, query, query_len);
        }
        return -1;
    }

    if ( !result ) {
        if ( error ) *error = strdup("invalid_response");
        return -2;
    }
    
    /**
     * result   := {request}\t{response}
     * response := gen=<string>;content=<string>
     */

    char * response = strchr(result, '\t') + 1;
    
    citrusleaf_udf_info info = { NULL };
    citrusleaf_parameters_fold(response, &info, citrusleaf_udf_info_parameters);

    free(result);
    result = NULL;

    if ( info.error ) {
        if ( error ) {
            *error = info.error;
            info.error = NULL;
        }
        citrusleaf_udf_info_destroy(&info);
        return 1;
    }

    if ( !info.content ) {
        *error = strdup("file_not_found");
        citrusleaf_udf_info_destroy(&info);
        return 2;
    }

    int clen = info.content ? info.content_len : 0;
    cf_base64_decode_inplace(info.content, &clen, true);
    strcpy(file->name, filename);
    file->content->data = calloc(1, sizeof(byte)*(info.content_len)); 
    memcpy((*(file->content)).data, info.content, info.content_len);
    (*(file->content)).size = clen;
    SHA1(info.content, info.content_len, file->hash);
    free(info.content);
    info.content = NULL;
    
    if ( gen ) {
        *gen = info.gen;
        info.gen = NULL;
    }

    citrusleaf_udf_info_destroy(&info);
    
    return 0;
}

cl_rv citrusleaf_udf_put(cl_cluster *asc, const char * filename, as_bytes *content, as_udf_type udf_type, char ** error) {

    if ( !filename || !(content->data && content->size > 0)) {
        fprintf(stderr, "filename and content required\n");
        return CITRUSLEAF_FAIL_CLIENT;
    }

    char * query = NULL;    
    char *  result      = NULL;
    char *  filepath    = strdup(filename);
    char *  filebase    = basename(filepath);

    char * content_base64 = malloc(cf_base64_encode_maxlen(content->size));
    cf_base64_tostring(content->data, content_base64, &(content->size));
    if (! asprintf(&query, "udf-put:filename=%s;content=%s;content-len=%d;udf-type=%d;", filebase, content_base64, content->size, udf_type)) {
        fprintf(stderr, "Query allocation failed");
        return CITRUSLEAF_FAIL_CLIENT;
    }
    
    free(filepath);

    if ( citrusleaf_info_cluster_all(asc, query, &result, true, 5000) ) {
        if ( error ) {
            const char * emsg = "failed_request: ";
            int emsg_len = strlen(emsg);
            int query_len = strlen(query);
            *error = (char *) malloc(sizeof(char) * (emsg_len + query_len));
            strncpy(*error, emsg, emsg_len);
            strncpy(*error+emsg_len, query, query_len);
        }
        free(query);
        free(content_base64);
        return -1;
    }

    if ( !result ) {
        if ( error ) *error = strdup("invalid_response");
        free(query);
        free(content_base64);
    	return -2;
    }

    free(query);
    free(content_base64);
    query = NULL;
    
    /**
     * result   := {request}\t{response}
     * response := gen=<string> | error=<string>
     */
    char * response = strchr(result, '\t') + 1; // skip request, parse response 
    
    citrusleaf_udf_info info = { NULL };
    citrusleaf_parameters_fold(response, &info, citrusleaf_udf_info_parameters);

    free(result);
    result = NULL;
    
    if ( info.error ) {
        if ( error ) {
            *error = info.error;
            info.error = NULL;
        }
        citrusleaf_udf_info_destroy(&info);
        return 1;
    }

    citrusleaf_udf_info_destroy(&info);
    return 0;
    
}

cl_rv citrusleaf_udf_remove(cl_cluster *asc, const char * filename, char ** error) {

    char    query[512]  = {0};
    char *  result      = NULL;

    snprintf(query, sizeof(query), "udf-remove:filename=%s;", filename);

    if ( citrusleaf_info_cluster(asc, query, &result, true, 100) ) {
        if ( error ) {
            const char * emsg = "failed_request: ";
            int emsg_len = strlen(emsg);
            int query_len = strlen(query);
            *error = (char *) malloc(sizeof(char) * (emsg_len + query_len));
            strncpy(*error, emsg, emsg_len);
            strncpy(*error+emsg_len, query, query_len);
        }
        return -1;
    }

    if ( !result ) {
        if ( error ) *error = strdup("invalid_response");
        return -2;
    }
    
    /**
     * result   := {request}\t{response}
     * response := gen=<string>;content=<string>
     */

    char * response = strchr(result, '\t') + 1;
    
    citrusleaf_udf_info info = { NULL };
    citrusleaf_parameters_fold(response, &info, citrusleaf_udf_info_parameters);

    free(result);
    result = NULL;

    if ( info.error ) {
        if ( error ) {
            *error = info.error;
            info.error = NULL;
        }
        citrusleaf_udf_info_destroy(&info);
        return 1;
    }
    
    citrusleaf_udf_info_destroy(&info);
    return 0;
    
}


static int citrusleaf_parameters_fold(char * parameters, void * context, citrusleaf_parameters_fold_callback callback) {
    if ( !parameters || !(*parameters) ) return 0;

    char *  ks = NULL;
    int     ke = 0;
    char *  vs = NULL;
    int     ve = 0;
    
    ks = parameters;
    for ( ke = 0; ks[ke] != '=' && ks[ke] != 0; ke++);

    if ( ks[ke] == 0 ) return 1;

    vs = ks + ke + 1;
    for ( ve = 0; vs[ve] != ';' && vs[ve] != 0; ve++);

    if ( vs[ve] == 0 ) return 2;
    
    char    k[128]  = {0};
    char *  v       = strndup(vs, ve);
    char *  p       = vs + ve + 1;

    memcpy(k, ks, ke);

    int rc = citrusleaf_parameters_fold(p, callback(k, v, context), callback);
    
    free(v);

    return rc;
}


static int citrusleaf_split_fold(char * str, const char delim, void * context, citrusleaf_split_fold_callback callback) {
    if ( !str || !(*str) ) return 0;

    char *  vs = NULL;
    int     ve = 0;
    
    vs = str;
    for ( ve = 0; vs[ve] != delim && vs[ve] != 0; ve++);

    if ( vs[ve] == 0 ) return 1;
    
    char *  v       = strndup(vs, ve);
    char *  p       = vs + ve + 1;

    int rc = citrusleaf_split_fold(p, delim, callback(v, context), callback);
    
    free(v);

    return rc;
}
