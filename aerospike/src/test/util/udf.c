// #include <citrusleaf/citrusleaf.h>
// #include <citrusleaf/cl_udf.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>
#include <aerospike/as_udf.h>

#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>

#include "udf.h"
#include "../test.h"


#define SCRIPT_LEN_MAX 1048576

extern aerospike * as;




bool udf_readfile(const char * filename, as_bytes * content) {
    
    FILE * file = fopen(filename,"r"); 

    if ( !file ) { 
        error("cannot open script file %s : %s", filename, strerror(errno));  
        return -1; 
    } 

    uint8_t * bytes = (uint8_t *) malloc(SCRIPT_LEN_MAX); 
    if ( bytes == NULL ) { 
        error("malloc failed"); 
        return -1;
    }     

    int size = 0; 

    uint8_t * buff = bytes; 
    int read = fread(buff, 1, 512, file); 
    while ( read ) { 
        size += read; 
        buff += read; 
        read = fread(buff, 1, 512, file); 
    }                        
    fclose(file); 

    as_bytes_init(content, bytes, size, true);

    return true;
}


bool udf_put(const char * filename) {
    
    FILE * file = fopen(filename,"r"); 

    if ( !file ) { 
        error("cannot open script file %s : %s", filename, strerror(errno));  
        return -1; 
    } 

    uint8_t * content = (uint8_t *) malloc(SCRIPT_LEN_MAX); 
    if ( content == NULL ) { 
        error("malloc failed"); 
        return -1;
    }     

    int size = 0; 

    uint8_t * buff = content; 
    int read = fread(buff, 1, 512, file); 
    while ( read ) { 
        size += read; 
        buff += read; 
        read = fread(buff, 1, 512, file); 
    }                        
    fclose(file); 

    as_bytes udf_content;
    as_bytes_init(&udf_content, content, size, true);

	as_error err;
	as_error_reset(&err);

    char * base = basename(filename);

    if ( aerospike_udf_put(as, &err, NULL, base, AS_UDF_TYPE_LUA, &udf_content) != AEROSPIKE_OK ) {
        error("error caused by aerospike_udf_put(): (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

    as_val_destroy(&udf_content);

    return err.code == AEROSPIKE_OK;
}

bool udf_remove(const char * filename) {
    
	as_error err;
	as_error_reset(&err);

    char * base = basename(filename);

    if ( aerospike_udf_remove(as, &err, NULL, base) != AEROSPIKE_OK ) {
        error("error caused by aerospike_udf_remove(): (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

    return err.code == AEROSPIKE_OK;
}

bool udf_exists(const char * filename) {
    
	as_error err;
	as_error_reset(&err);

    as_udf_file file;
    as_udf_file_init(&file);

    char * base = basename(filename);

    if ( aerospike_udf_get(as, &err, NULL, base, AS_UDF_TYPE_LUA, &file) != AEROSPIKE_OK ) {
        error("error caused by aerospike_udf_get: (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

    as_udf_file_destroy(&file);

    return err.code == AEROSPIKE_OK;
}
