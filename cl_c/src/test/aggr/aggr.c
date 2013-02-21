#include "../test.h"
#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_udf.h"
#include <errno.h>

/******************************************************************************
 * MACROS
 *****************************************************************************/

#define HOST "127.0.0.1"
#define PORT 3000
#define TIMEOUT 1000

#define SCRIPT_LEN_MAX 1048576

/******************************************************************************
 * VARIABLES
 *****************************************************************************/

cl_cluster * cluster = NULL;

/******************************************************************************
 * GLOBAL FUNCTIONS
 *****************************************************************************/

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
    as_bytes udf_content = {
       .data = content,
       .size = size
    }; 
    char * err = NULL;

    int rc = citrusleaf_udf_put(cluster, basename(filename), &udf_content, AS_UDF_LUA, &err); 

    if ( rc != 0 && err ) {
        error("error caused by citrusleaf_udf_put(): %s", err);
        free(err);
    }

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
    char * contents = NULL;
    as_udf_file file;
    memset(&file,0,sizeof(as_udf_file));
    file.content = calloc(1,sizeof(as_bytes));
    int rc = citrusleaf_udf_get(cluster, basename(filename), &file, 0, &err);

    if ( rc != 0 && err ) {
        error("error caused by citrusleaf_udf_remove(): %s", err);
        free(err);
    }
    else if ( contents ) {
        free(file.content->data);
        free(file.content);
        contents = NULL;
    }

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

/******************************************************************************
 * STATIC FUNCTIONS
 *****************************************************************************/

static bool before(atf_plan * plan) {

    if ( cluster ) {
        error("cluster already initialized");
        return false;
    }

    citrusleaf_init();

    cluster = citrusleaf_cluster_create();
    if ( !cluster ) { 
        info("could not create cluster"); 
        return false; 
    }

    if ( citrusleaf_cluster_add_host(cluster, HOST, PORT, TIMEOUT) != 0 ) {
        info("could not connect to host %s port %d", HOST, PORT);
        return false;
    }

    return true;
}

static bool after(atf_plan * plan) {

    if ( !cluster ) {
        error("cluster was not initialized");
        return false;
    }

    citrusleaf_cluster_destroy( cluster );
    citrusleaf_shutdown();

    return true;
}

/******************************************************************************
 * TEST PLAN
 *****************************************************************************/

PLAN( aggr ) {

    plan_before( before );
    plan_after( after );

    plan_add( aggr_simple );
}
