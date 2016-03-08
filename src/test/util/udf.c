/*
 * Copyright 2008-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>
#include <aerospike/as_udf.h>

#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <time.h>

#include "udf.h"
#include "../test.h"


#define SCRIPT_LEN_MAX 1048576

extern aerospike * as;



bool udf_readfile(const char * filename, as_bytes * content) {
    
    FILE * file = fopen(filename,"r"); 

    if ( !file ) { 
        error("cannot open script file %s : %s", filename, strerror(errno));  
        return false;
    } 

    uint8_t * bytes = (uint8_t *) malloc(SCRIPT_LEN_MAX); 
    if ( bytes == NULL ) { 
        error("malloc failed");
		fclose(file);
        return false;
    }     

    int size = 0; 

    uint8_t * buff = bytes; 
    int read = (int)fread(buff, 1, 512, file);
    while ( read ) { 
        size += read; 
        buff += read; 
        read = (int)fread(buff, 1, 512, file);
    }                        
    fclose(file); 

    as_bytes_init_wrap(content, bytes, size, true);

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
		fclose(file);
        return -1;
    }     

    int size = 0; 

    uint8_t * buff = content; 
    int read = (int)fread(buff, 1, 512, file);
    while ( read ) { 
        size += read; 
        buff += read; 
        read = (int)fread(buff, 1, 512, file);
    }                        
    fclose(file); 

    as_bytes udf_content;
    as_bytes_init_wrap(&udf_content, content, size, true);

	as_error err;
	as_error_reset(&err);

	as_string filename_string;
	const char * base = as_basename(&filename_string, filename);

    if ( aerospike_udf_put(as, &err, NULL, base, AS_UDF_TYPE_LUA, &udf_content) == AEROSPIKE_OK ) {
		aerospike_udf_put_wait(as, &err, NULL, base, 100);
	}
	else {
        error("error caused by aerospike_udf_put(): (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

	as_string_destroy(&filename_string);
    as_val_destroy(&udf_content);

    return err.code == AEROSPIKE_OK;
}

bool udf_remove(const char * filename) {
    
	as_error err;
	as_error_reset(&err);

	as_string filename_string;
	const char * base = as_basename(&filename_string, filename);

    if ( aerospike_udf_remove(as, &err, NULL, base) != AEROSPIKE_OK ) {
        error("error caused by aerospike_udf_remove(): (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

	as_string_destroy(&filename_string);
 	WAIT_MS(100);
	
    return err.code == AEROSPIKE_OK;
}

bool udf_exists(const char * filename) {
    
	as_error err;
	as_error_reset(&err);

    as_udf_file file;
    as_udf_file_init(&file);

	as_string filename_string;
	const char * base = as_basename(&filename_string, filename);

    if ( aerospike_udf_get(as, &err, NULL, base, AS_UDF_TYPE_LUA, &file) != AEROSPIKE_OK ) {
        error("error caused by aerospike_udf_get: (%d) %s @ %s[%s:%d]", err.code, err.message, err.func, err.file, err.line);
    }

	as_string_destroy(&filename_string);

    as_udf_file_destroy(&file);

    return err.code == AEROSPIKE_OK;
}
