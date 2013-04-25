
#include "udf.h"
#include "../test.h"
#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/as_types.h>
#include <citrusleaf/cl_udf.h>
#include <stdlib.h>
#include <errno.h>


#define SCRIPT_LEN_MAX 1048576


int udf_put(const char * filename) {
    
    extern cl_cluster * cluster;

    FILE * file = fopen(filename,"r"); 

    if ( !file ) { 
        error("cannot open script file %s : %s", filename, strerror(errno));  
        return -1; 
    } 

    byte * content = malloc(SCRIPT_LEN_MAX); 
    if ( content == NULL ) { 
        error("malloc failed"); 
        return -1;
    }     

    int size = 0; 

    byte * buff = content; 
    int read = fread(buff, 1, 512, file); 
    while ( read ) { 
        size += read; 
        buff += read; 
        read = fread(buff, 1, 512, file); 
    }                        
    fclose(file); 
    as_bytes udf_content;
    as_bytes_init(&udf_content, content, size, true /*ismalloc*/);

    char * err = NULL;

    int rc = citrusleaf_udf_put(cluster, basename(filename), &udf_content, AS_UDF_LUA, &err); 

    if ( rc != 0 && err ) {
        error("error caused by citrusleaf_udf_put(): %s", err);
        free(err);
    }

    as_val_destroy(&udf_content);

    return rc;
}

int udf_remove(const char * filename) {
    
    extern cl_cluster * cluster;

    char * err = NULL;

    int rc = citrusleaf_udf_remove(cluster, basename(filename), &err); 

    if ( rc != 0 && err ) {
        error("error caused by citrusleaf_udf_remove(): %s", err);
        free(err);
    }

    return rc;
}

int udf_exists(const char * filename) {
    
    extern cl_cluster * cluster;

    char * err = NULL;
    as_udf_file file;
    memset(&file,0,sizeof(as_udf_file));

    int rc = citrusleaf_udf_get(cluster, basename(filename), &file, 0, &err);

    if ( rc != 0 ) {
        if ( err ) {
            error("error caused by citrusleaf_udf_get(): (%d) %s", rc, err);
            free(err);
        }
        else {
            error("error caused by citrusleaf_udf_get(): %d", rc);
        }
    }

    as_val_destroy(&file.content);

    return rc;
}

int udf_apply_record(const char * ns, const char * set, const char * key, const char * file, const char * func, as_list * arglist, as_result * result) {

    extern cl_cluster * cluster;

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    cl_object okey;
    citrusleaf_object_init_str(&okey, key);

    return citrusleaf_udf_record_apply(cluster, ns, set, &okey, file, func, arglist, 1000, result);
}

int udf_apply_stream(const char * ns, const char * set, const char * key, const char * file, const char * func, as_list * arglist, as_result * result) {

    extern cl_cluster * cluster;

    cl_write_parameters wp;
    cl_write_parameters_set_default(&wp);
    wp.timeout_ms = 1000;
    wp.record_ttl = 864000;

    cl_object okey;
    citrusleaf_object_init_str(&okey, key);

    return citrusleaf_udf_record_apply(cluster, ns, set, &okey, file, func, arglist, 1000, result);
}


void print_result(uint32_t rc, as_result * r) {
    if ( !r->is_success ) {
        char * s = as_val_tostring(r->value);
        info("failure: %s (%d)", s, rc);
        free(s);
    }
    else {
        char * s = as_val_tostring(r->value);
        info("success: %s", s);
        free(s);
    }
}
