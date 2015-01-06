/*
 * Copyright 2008-2014 Aerospike, Inc.
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

#include "internal.h"

cl_rv citrusleaf_secondary_index_create(
    as_cluster * asc, const char * ns, const char * set,
    const char * iname, const char * position, const char * itype, const char * dtype,
    char ** response
){

    if (!ns || !iname || !position || !dtype) return AEROSPIKE_ERR_CLIENT;

    char ddl[1024];
    
	if (strcmp(itype, "DEFAULT") == 0) {
		// Use old format, so command can work with older servers.
		sprintf(ddl,
			"sindex-create:ns=%s%s%s;indexname=%s;"
			"numbins=1;indexdata=%s,%s;priority=normal\n",
			ns, set ? ";set=" : "", set ? set : "",
			iname, position, dtype
			);
	}
	else {
		// Use new format.
		sprintf(ddl,
			"sindex-create:ns=%s%s%s;indexname=%s;" 
			"numbins=1;indextype=%s,indexdata=%s,%s;priority=normal\n",
			ns, set ? ";set=" : "", set ? set : "",
			iname, itype, position, dtype
		);
	}

    return citrusleaf_info_cluster(asc, ddl, response, true, /* check bounds */ true, 5000);
}

cl_rv citrusleaf_secondary_index_drop(as_cluster *asc, const char *ns, const char *indexname, char **response) {

    char ddl[1024];
    sprintf(ddl, "sindex-delete:ns=%s;indexname=%s", ns, indexname);
	
	return citrusleaf_info_cluster(asc, ddl, response, true, /* check bounds */ true, 5000);
}
