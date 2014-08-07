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

cl_rv citrusleaf_secondary_index_create(
    as_cluster * asc, const char * ns, const char * set,
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

    return citrusleaf_info_cluster(asc, ddl, response, true, /* check bounds */ true, 5000);
}

cl_rv citrusleaf_secondary_index_drop(as_cluster *asc, const char *ns, const char *indexname, char **response) {

    char ddl[1024];
    sprintf(ddl, "sindex-delete:ns=%s;indexname=%s", ns, indexname);
	
	return citrusleaf_info_cluster(asc, ddl, response, true, /* check bounds */ true, 5000);
}
