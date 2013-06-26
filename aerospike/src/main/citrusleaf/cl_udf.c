/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to 
 * deal in the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <citrusleaf/cf_b64.h>

#include <aerospike/as_bytes.h>
#include <aerospike/as_msgpack.h>

#include <citrusleaf/cl_udf.h>
#include <citrusleaf/cl_write.h>

#include "internal.h"

/******************************************************************************
 * TYPES
 ******************************************************************************/

typedef struct citrusleaf_udf_filelist_s citrusleaf_udf_filelist;

struct citrusleaf_udf_filelist_s {
    int         capacity;
    int         size;
    cl_udf_file **     files;
};


/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

#define LOG(msg, ...) \
    { printf("%s:%d - ", __FILE__, __LINE__); printf(msg, ##__VA_ARGS__ ); printf("\n"); }


void citrusleaf_udf_info_destroy(citrusleaf_udf_info * info) {
    if ( info->error ) free(info->error);
    as_val_destroy(&info->content);
    if ( info->gen ) free(info->gen);
    if ( info->files ) free(info->files);

    info->error = NULL;
    info->gen = NULL;
    info->files = NULL;
    info->count = 0;
}

void * citrusleaf_udf_info_parameters(const char * key, const char * value, void * context) {
    citrusleaf_udf_info * info = (citrusleaf_udf_info *) context;
    if ( strcmp(key,"error") == 0 ) {
        info->error = strdup(value);
    }
    if ( strcmp(key,"filename") == 0 ) {
        strncpy(info->filename, value, strlen(value));
    }
    else if ( strcmp(key,"gen") == 0 ) {
        info->gen = strdup(value);
    }
    else if ( strcmp(key,"content") == 0 ) {
    	as_bytes_destroy(&info->content);
    	int c_len = strlen(value);
    	uint8_t * c = (uint8_t *) malloc(c_len + 1);
        memcpy(c, value, c_len);
        c[c_len] = 0;
    	as_bytes_init(&info->content, c, c_len + 1, true /*memcpy*/);
    }
    else if ( strcmp(key,"files") == 0 ) {
        info->files = strdup(value);
    }
    else if ( strcmp(key,"count") == 0 ) {
        info->count = atoi(value);
    }
    else if (strcmp(key, "hash") == 0 ) {
        memcpy(info->hash, value, strlen(value));
    }
    return info;
}

void * citrusleaf_udf_list_files(char * filedata, void * context) {
    citrusleaf_udf_filelist * filelist = (citrusleaf_udf_filelist *) context;
    citrusleaf_udf_info file_info = {NULL};
    // Got a list of key-value pairs separated with commas
    citrusleaf_sub_parameters_fold(filedata, &file_info, citrusleaf_udf_info_parameters);
    if ( filelist->size < filelist->capacity ) {
   	filelist->files[filelist->size] = (cl_udf_file*)calloc(1,sizeof(cl_udf_file));
	strncpy(filelist->files[filelist->size]->name, file_info.filename, strlen(file_info.filename));
	memcpy(filelist->files[filelist->size]->hash, file_info.hash, CF_SHA_HEX_BUFF_LEN);
	filelist->size++;
    }

    return filelist;
}


int print_buffer(as_buffer * buff) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    sbuf.data = (char *) buff->data;
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




as_val *citrusleaf_udf_bin_to_val(as_serializer *ser, cl_bin *bin) {     

    as_val * val;

    switch( bin->object.type ) {
        case CL_INT : {
            val = (as_val *) as_integer_new(bin->object.u.i64);
            break;
        }
        case CL_STR : {
            // steal the pointer from the object into the val
            val = (as_val *) as_string_new(strdup(bin->object.u.str), true /*ismalloc*/);
            break;
        }
        case CL_BLOB:
        case CL_JAVA_BLOB:
        case CL_CSHARP_BLOB:
        case CL_PYTHON_BLOB:
        case CL_RUBY_BLOB:
        case CL_ERLANG_BLOB:
        {
            uint8_t *b = malloc(sizeof(bin->object.sz));
            memcpy(b, bin->object.u.blob, bin->object.sz);
            val = (as_val *)as_bytes_new(b, bin->object.sz, true /*ismalloc*/);
        }
        case CL_LIST :
        case CL_MAP : {
            // use a temporary buffer, which doesn't need to be destroyed
            as_buffer buf = {
                .capacity = (uint32_t) bin->object.sz,
                .size = (uint32_t) bin->object.sz,
                .data = (uint8_t *) bin->object.u.blob
            };
            // print_buffer(&buf);
            as_serializer_deserialize(ser, &buf, &val);
            break;
        }
        default : {
            val = NULL;
            break;
        }
    }
    return val;
}

cl_rv citrusleaf_udf_record_apply(cl_cluster * cl, const char * ns, const char * set, const cl_object * key, 
    const char * filename, const char * function, as_list * arglist, int timeout_ms, as_result * res) {

    cl_rv rv = CITRUSLEAF_OK;
    char err_str[256];

    as_serializer ser;
    as_msgpack_init(&ser);

    as_string file;
    as_string_init(&file, (char *) filename, true /*ismalloc*/);

    as_string func;
    as_string_init(&func, (char *) function, true /*ismalloc*/);
    
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

    cl_bin *bins = 0;
    int n_bins = 0;

    // print_buffer(&args);

    rv = do_the_full_monte( 
        cl, 0, CL_MSG_INFO2_WRITE, 0, 
        ns, set, key, 0, &bins, CL_OP_WRITE, 0, &n_bins, 
        NULL, &wp, &trid, NULL, &call
    );

    as_buffer_destroy(&args);

	if (! (rv == CITRUSLEAF_OK || rv == CITRUSLEAF_FAIL_UDF_BAD_RESPONSE)) {
		// Add the exact error-code to return value
		//snprintf(err_str, 256, "None UDF failure Error-code: %d", rv);
		snprintf(err_str, 256, "Error in parsing udf params Error-code: %d", rv);
    	as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
    } else if ( n_bins == 1  ) {

        cl_bin *bin = &bins[0];

        as_val *val = citrusleaf_udf_bin_to_val(&ser, bin);

        if ( val ) {
            if ( strcmp(bin->bin_name,"FAILURE") == 0 ) {
        		snprintf(err_str, 256, "Failure in converting udf-bin to value for type :%d", val->type);
        		as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
        		as_val_destroy(val);
        		//	as_result_setfailure(res, val);
            }
            else if ( strcmp(bin->bin_name,"SUCCESS") == 0 ) {
                as_result_setsuccess(res, val);
            }
            else {
            	snprintf(err_str, 256, "Invalid response in converting udf-bin to value for type :%d", val->type);
            	as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
            	as_val_destroy(val);
                //as_result_setfailure(res, (as_val *) as_string_new("Invalid response. (1)",false/*ismalloc*/));
            }
        }
        else {
        	snprintf(err_str, 256, "Null value returned in converting udf-bin to value for type :%d", val->type);
        	as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
           // as_result_setfailure(res, (as_val *) as_string_new("Invalid response. (2)",false/*ismalloc*/));
        }

    }
    else {
    	snprintf(err_str, 256, " Generic parser error for udf-apply, Error-code: %d", rv);
        as_result_setfailure(res, (as_val *) as_string_new(err_str,false));
       // as_result_setfailure(res, (as_val *) as_string_new("Invalid response. (3)",false/*ismalloc*/));
    }

    if (bins) {
        citrusleaf_bins_free(bins, n_bins);
        free(bins);
    }

    as_serializer_destroy(&ser);
    
    return rv;
}



cl_rv citrusleaf_udf_list(cl_cluster *asc, cl_udf_file *** files, int * count, char ** error) {
    
    *files = NULL;
    *count = 0;

    char *  query   = "udf-list";
    char *  result  = 0;

    if ( citrusleaf_info_cluster(asc, query, &result, true, /* check bounds */ true, 100) ) {
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
     * files    := filename=<name>,hash=<hash>,type=<type>[:filename=<name>...]
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
        .files      = (cl_udf_file **) malloc(info.count * sizeof(cl_udf_file *))
    };
    // Different files' data are separated by ':'. Parse each file dataset and feed them into the callback 
    citrusleaf_split_fold(info.files, ':', &filelist, citrusleaf_udf_list_files);

    *files = filelist.files;
    *count = filelist.size;

    citrusleaf_udf_info_destroy(&info);

    return 0;
    
}

cl_rv citrusleaf_udf_get(cl_cluster *asc, const char * filename, cl_udf_file * file, cl_udf_type udf_type, char ** error) {
    return citrusleaf_udf_get_with_gen(asc, filename, file, 0, NULL, error);
}

cl_rv citrusleaf_udf_get_with_gen(cl_cluster *asc, const char * filename, cl_udf_file * file, cl_udf_type udf_type, char **gen, char ** error) {

    if ( file->content ) return -1;

    char    query[512]  = {0};
    char *  result      = NULL;

    snprintf(query, sizeof(query), "udf-get:filename=%s;", filename);


    // fprintf(stderr, "QUERY: |%s|\n", query);


    if ( citrusleaf_info_cluster(asc, query, &result, true, /* check bounds */ true, 100) ) {
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

    if ( as_bytes_len(&info.content) == 0 ) {
        *error = strdup("file_not_found");
        citrusleaf_udf_info_destroy(&info);
        return 2;
    }

    uint8_t *   content = as_bytes_tobytes(&info.content);
    int         clen    = as_bytes_len(&info.content) - 1; // this is a byte array, not string. last char is NULL.

    cf_base64_decode_inplace(content, &clen, true);
    
    file->content = as_bytes_new(content, clen, true);

	info.content.value = NULL;
	info.content.free = false;
	info.content.len = 0;
	info.content.capacity = 0;

	as_bytes_destroy(&info.content);
   
    strcpy(file->name, filename);

    // Update file hash
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA1(as_bytes_tobytes(&info.content), as_bytes_len(&info.content), hash);

    cf_convert_sha1_to_hex(hash, file->hash);
    
    if ( gen ) {
        *gen = info.gen;
        info.gen = NULL;
    }

    citrusleaf_udf_info_destroy(&info);

    return 0;
}

static bool clusterinfo_cb(const cl_cluster_node *cn, const struct sockaddr_in * sa_in, const char *command, char *value, void *udata)
{
	char** error = (char**)udata;
	if(value != NULL){
		//fprintf(stdout, "Node %s: %s\n", cn->name,value);
		char * response = strchr(value, '\t') + 1; // skip request, parse response
        citrusleaf_udf_info info = { NULL };
		citrusleaf_parameters_fold(response, &info, citrusleaf_udf_info_parameters);
		free(value);
		value = NULL;

		if ( info.error ) {
			if (error ) {
				*error = info.error;
				info.error = NULL;
			}
			citrusleaf_udf_info_destroy(&info);
			return 1;
		}
		citrusleaf_udf_info_destroy(&info);
	}
	else{
			fprintf(stdout, "Node %s: No response from server.\n",cn->name);
	}
    return true;
}

cl_rv citrusleaf_udf_put(cl_cluster *asc, const char * filename, as_bytes *content, cl_udf_type udf_type, char ** error) {

    if ( !filename || !(content)) {
        fprintf(stderr, "filename and content required\n");
        return CITRUSLEAF_FAIL_CLIENT;
    }

    char * query = NULL;
    char *  filepath    = strdup(filename);
    char *  filebase    = basename(filepath);

    int  clen = as_bytes_len(content);
    char * content_base64 = malloc(cf_base64_encode_maxlen(clen));
    cf_base64_tostring(as_bytes_tobytes(content), content_base64, &clen);

    if (! asprintf(&query, "udf-put:filename=%s;content=%s;content-len=%d;udf-type=%d;", filebase, content_base64, clen, udf_type)) {
        fprintf(stderr, "Query allocation failed");
        return CITRUSLEAF_FAIL_CLIENT;
    }
    
    // fprintf(stderr, "QUERY: |%s|\n",query);

    free(filepath);

    int rc = 0;
    rc = citrusleaf_info_cluster(asc, query, (void *)(error), true, false, 1000);
    if (  rc ) {
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

    free(query);
    free(content_base64);
    content_base64 = 0;
    query = NULL;
    return 0;
}

cl_rv citrusleaf_udf_remove(cl_cluster *asc, const char * filename, char ** error) {

    char    query[512]  = {0};
    char *  result      = NULL;

    snprintf(query, sizeof(query), "udf-remove:filename=%s;", filename);

    if ( citrusleaf_info_cluster(asc, query, &result, true, /* check bounds */ true, 100) ) {
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
// Parameters are key-value pairs separated by ;
// Sub parameters are key-value pairs contained under a parameter set and are separated by commas
int citrusleaf_sub_parameters_fold(char * parameters, void * context, citrusleaf_parameters_fold_callback callback) {
    if ( !parameters || !(*parameters) ) return 0;
    char *  ks = NULL;
    int     ke = 0;
    char *  vs = NULL;
    int     ve = 0;
    ks = parameters;
    for ( ke = 0; ks[ke] != '=' && ks[ke] != 0; ke++);

    if ( ks[ke] == 0 ) return 1;

    vs = ks + ke + 1;
    for ( ve = 0; vs[ve] != ',' && vs[ve] != 0; ve++);

    char    k[128]  = {0};
    char *  v       = strndup(vs, ve);
    char * p = NULL;
    if (vs[ve] != 0) {
	    p = vs + ve + 1;
    }

    memcpy(k, ks, ke);
    int rc = citrusleaf_sub_parameters_fold(p, callback(k, v, context), callback);
    
    free(v);

    return rc;
}

int citrusleaf_parameters_fold(char * parameters, void * context, citrusleaf_parameters_fold_callback callback) {
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


int citrusleaf_split_fold(char * str, const char delim, void * context, citrusleaf_split_fold_callback callback) {
    if ( !str || !(*str) ) return 0;

    char *  vs = NULL;
    int     ve = 0;
    char * p = NULL;    
    vs = str;
    for ( ve = 0; vs[ve] != delim && vs[ve] != 0; ve++);

    char *  v       = strndup(vs, ve);
    // Move p only if this isn't the last string
    if ( vs[ve] != 0 ) {
	    p = vs + ve + 1;
    }

    int rc = citrusleaf_split_fold(p, delim, callback(v, context), callback);
    
    free(v);

    return rc;
}
