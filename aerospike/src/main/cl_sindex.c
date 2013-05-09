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

#include <sys/types.h>
#include <sys/socket.h> // socket calls
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <arpa/inet.h> // inet_ntop
#include <signal.h>
#include <netdb.h> //gethostbyname_r

#include <citrusleaf/cf_proto.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_sindex.h>
#include <citrusleaf/as_log.h>

#include "internal.h"


#define INFO_TIMEOUT_MS 300
extern int g_cl_turn_debug_on;

static char * citrusleaf_secondary_index_fold_args(as_list * arglist) {
    return "";
}

cl_rv citrusleaf_secondary_index_create(
    cl_cluster * asc, const char * ns, const char * set,
    const char * iname, const char * binname, const char * type,
    char ** response
){

    if (!ns || !iname || !binname || !type) return CITRUSLEAF_FAIL_CLIENT;

    char ddl[1024];
    
    sprintf(ddl, 
        "sindex-create:ns=%s%s%s;indexname=%s;" 
        "numbins=1;indexdata=%s,%s;priority=normal\n",
        ns, set ? ";set=" : "", set ? set : "",
        iname, binname, type
    );

    int rc = citrusleaf_info_cluster_all(asc, ddl, response, true, /* check bounds */ true, 5000);

    if ( rc != 0 ) return rc;

    char * fail = strstr(*response,"FAIL:");
    if ( fail != NULL ) {
        fail = fail + 5;
        char * end = strchr(fail,':');
        if ( end != NULL ) {
            *end     = '\0';
            int code = atoi(fail);
            return code; 
        }
        return CITRUSLEAF_FAIL_CLIENT;
    }

    return CITRUSLEAF_OK;
}        

cl_rv citrusleaf_secondary_index_create_functional(
    cl_cluster * asc, const char * ns, const char * set, const char * finame,
    const char * file, const char * func, as_list * args, const char * type,
    char **response
){

    if (!ns || !finame || !file || !func || !args || !type) {
        return CITRUSLEAF_FAIL_CLIENT;
    }

    char ddl[1024];

    sprintf(ddl,  
        "sindex-create:ns=%s%s%s;indexname=%s;"
        "funcdata=%s,%s;funcargs=%s;indextype=%s;priority=normal\n",
        ns, set ? ";set=" : "", set ? set : "", finame, file, func, 
        citrusleaf_secondary_index_fold_args(args), type
    );
    
    int rc = citrusleaf_info_cluster_all(asc, ddl, response, true, /* check bounds */ true, 5000);

    if ( rc != 0 ) return rc;

    char * fail = strstr(*response,"FAIL:");
    if ( fail != NULL ) {
        fail = fail + 5;
        char * end = strchr(fail,':');
        if ( end != NULL ) {
            *end     = '\0';
            int code = atoi(fail);
            return code;
        }
        return CITRUSLEAF_FAIL_CLIENT;
    }

    return CITRUSLEAF_OK;
}        

cl_rv citrusleaf_secondary_index_drop(cl_cluster *asc, const char *ns, const char *indexname, char **response) {

    char ddl[1024];
    sprintf(ddl, "sindex-drop:ns=%s;indexname=%s", ns, indexname);
    if ( citrusleaf_info_cluster_all(asc, ddl, response, true, /* check bounds */ true, 5000) ) {
        INFO("[ERROR] sindex-drop: response: %s\n", *response);
        return CITRUSLEAF_FAIL_CLIENT;
    }
    INFO("sindex-drop: response: %s\n", *response);
    return CITRUSLEAF_OK;
}        


