/*
 * A good, basic C client for the Aerospike protocol
 * Creates a library which is linkable into a variety of systems
 *
 * First attempt is a very simple non-threaded blocking interface
 * currently coded to C99 - in our tree, GCC 4.2 and 4.3 are used
 *
 * Citrusleaf Inc.
 * All rights reserved
 */

#include "citrusleaf.h"
#include "citrusleaf-internal.h"
#include "cl_sindex.h"
#include "as_log.h"

#include <citrusleaf/proto.h>

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


