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

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/proto.h"

#define INFO_TIMEOUT_MS 300
extern int g_cl_turn_debug_on;

cl_rv citrusleaf_secondary_index_create(cl_cluster *asc, const char *ns, const char *set,
                                        sindex_metadata_t *imd) {
	// Iterate each node to register the function
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		
		// register the function using the first sockaddr
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, 0);
		
		char tmpBuf[1024];
		sprintf(tmpBuf,"sindex-create:indexname=%s;ns=%s;set=%s;binname=%s;type=%s;isuniq=%s;istime=%s;",
			imd->iname, ns, set, imd->binname, 
			imd->type, imd->isuniq ? "true":"false", imd->istime ? "true":"false");
		if (g_cl_turn_debug_on) {
			fprintf(stderr, "sindex-create: [%s]\n",tmpBuf);
		}
		
		char *values = 0;
		int resp =  citrusleaf_info_host(sa_in, tmpBuf, &values, INFO_TIMEOUT_MS, true);
		if (g_cl_turn_debug_on) {
			fprintf(stderr, "sindex-create: response: %d [%s]\n",resp,values);
		}

		// reminder: returned list is name1\tvalue1\nname2\tvalue2\n
		/*
		cf_vector_define(lines_v, sizeof(void *), 0);
		str_split('\n',values,&lines_v);
		for (uint j=0;j<cf_vector_size(&lines_v);j++) {
			char *line = cf_vector_pointer_get(&lines_v, j);
			cf_vector_define(pair_v, sizeof(void *), 0);
			str_split('\t',line, &pair_v);
			
			if (cf_vector_size(&pair_v) == 2) {
				char *name = cf_vector_pointer_get(&pair_v,0);
				char *value = cf_vector_pointer_get(&pair_v,1);
				
				if ( strcmp(name, "node") == 0) {
					
					if (strcmp(value, cn->name) != 0) {
						// node name has changed. Dun is easy, would be better to remove the address
						// from the list of addresses for this node, and only dun if there
						// are no addresses left
						fprintf(stderr, "node name has changed!!!\n");
						cl_cluster_node_dun(cn, NODE_DUN_INFO_ERR);
					}
				}
				else if (strcmp(name, "partition-generation") == 0) {
					if (cn->partition_generation != (uint32_t) atoi(value)) {
						update_partitions = true;				
						cn->partition_generation = atoi(value);
					}
				}
				else if (strcmp(name, "services")==0) {
					cluster_services_parse(asc, value, services_v);
				}
			}
			
			cf_vector_destroy(&pair_v);
			
		}
		
		cf_vector_destroy(&lines_v);
		*/
		
		free(values);
	}
	return 0;
}		

cl_rv citrusleaf_secondary_index_delete(cl_cluster *asc, const char *ns, const char *set, const char *indexname) {
	// Iterate each node to register the function
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);

		// register the function using the first sockaddr
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, 0);
		
		char tmpBuf[1024];
		sprintf(tmpBuf,"sindex-delete:indexname=%s;ns=%s;set=%s",indexname,ns,set);
		if (g_cl_turn_debug_on) {
			fprintf(stderr, "sindex-delete: [%s]\n",tmpBuf);
		}

		char *values = 0;
		int resp =  citrusleaf_info_host(sa_in, tmpBuf, &values, INFO_TIMEOUT_MS, true);
		fprintf(stderr, "response: %d [%s]\n",resp,values);
		
		if (g_cl_turn_debug_on) {
			fprintf(stderr, "sindex-delete: response: %d [%s]\n",resp,values);
		}
		free(values);
	}
	return 0;
}		


