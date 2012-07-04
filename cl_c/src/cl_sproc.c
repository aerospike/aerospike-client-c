/*
 * The stored procedure interface 
 *
 *
 * Citrusleaf, 2012
 * All rights reserved
 */

#include <sys/types.h>
#include <stdio.h>
#include <errno.h> //errno
#include <stdlib.h> //fprintf
#include <unistd.h> // close
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <assert.h>


#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_cluster.h"
#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cf_atomic.h"
#include "citrusleaf/proto.h"
#include "citrusleaf/cf_socket.h"
#include "citrusleaf/cf_vector.h"

extern int g_cl_turn_debug_on;

#define INFO_TIMEOUT_MS 300

cl_rv citrusleaf_load_sproc_context (cl_cluster *asc)
{
	// go to the 1st node and get all the stored procedures
	for (uint i=0;i<cf_vector_size(&asc->node_v);i++) {
		cl_cluster_node *cn = cf_vector_pointer_get(&asc->node_v, i);
		
		struct sockaddr_in *sa_in = cf_vector_getp(&cn->sockaddr_in_v, 0);
		
		// HACK hard coded to one package right now until server can return listing		
		int num_packages = 1;
		char *packname = "mrf101";

		for (int j=0; j<num_packages; j++) {
			char tmpBuf[1024];
			sprintf(tmpBuf,"get-package:package=%s;",packname);
			char *values = 0;
			
			int resp =  citrusleaf_info_host(sa_in, tmpBuf, &values, INFO_TIMEOUT_MS, true);
			if (g_cl_turn_debug_on) {
				fprintf(stderr, "packages: response: %d [%s]\n",resp,values);
			}
		}
	}
}

cl_mrjob *citrusleaf_mrjob_create(const char *package, const char *map_fname, const char *rdc_fname, const char *fnz_fname )
{
	cl_mrjob *mrjob = malloc(sizeof(cl_mrjob));
	if (mrjob==NULL) {
		return NULL;
	}
	memset(mrjob,0,sizeof(cl_mrjob));
	mrjob->package = strdup(package);
	if (mrjob->package==NULL) {
		goto Cleanup;
	}
		
	if (map_fname) {
		mrjob->map_fname = strdup(map_fname);
		if (mrjob->map_fname==NULL) {
			goto Cleanup;
		}
	}	
	if (rdc_fname) {
		mrjob->rdc_fname = strdup(rdc_fname);
		if (mrjob->rdc_fname==NULL) {
			goto Cleanup;
		}
	}
	if (fnz_fname) {
		mrjob->fnz_fname = strdup(fnz_fname);
		if (mrjob->fnz_fname==NULL) {
			goto Cleanup;
		}
	}
	return mrjob;
	
Cleanup:	
	citrusleaf_mrjob_destroy(mrjob);
	return NULL;	
}

cl_rv citrusleaf_mrjob_add_parameter_string(cl_mrjob *mrjob, cl_script_func_t ftype, const char *key, const char *value)
{
	cl_rv rsp = CITRUSLEAF_OK;
	char 		**argk;
	cl_object 	**argv; 
	int 		*argc;
	if ( CL_SCRIPT_FUNC_TYPE_MAP == ftype) {
		if (mrjob->map_argc>=CL_MAX_NUM_FUNC_ARGC) {
			fprintf(stderr,"too many map_arg %d\n",mrjob->map_argc);
			rsp = CITRUSLEAF_FAIL_CLIENT;
			goto Cleanup;
		}
		argk = &mrjob->map_argk[mrjob->map_argc];
		argv = &mrjob->map_argv[mrjob->map_argc];
		argc = &mrjob->map_argc;
	} else if ( CL_SCRIPT_FUNC_TYPE_REDUCE == ftype) {
		if (mrjob->rdc_argc>=CL_MAX_NUM_FUNC_ARGC) {
			fprintf(stderr,"too many rdc_arg %d\n",mrjob->rdc_argc);
			rsp = CITRUSLEAF_FAIL_CLIENT;
			goto Cleanup;
		}
		argk = &mrjob->rdc_argk[mrjob->rdc_argc];
		argv = &mrjob->rdc_argv[mrjob->rdc_argc];
		argc = &mrjob->rdc_argc;
	} else if ( CL_SCRIPT_FUNC_TYPE_FINALIZE == ftype) {
		if (mrjob->fnz_argc>=CL_MAX_NUM_FUNC_ARGC) {
			fprintf(stderr,"too many map_arg %d\n",mrjob->fnz_argc);
			rsp = CITRUSLEAF_FAIL_CLIENT;
			goto Cleanup;
		}
		argk = &mrjob->fnz_argk[mrjob->fnz_argc];
		argv = &mrjob->fnz_argv[mrjob->fnz_argc];
		argc = &mrjob->fnz_argc;
	} else {
		fprintf(stderr,"unrecognized job type %d\n",ftype);
		rsp = CITRUSLEAF_FAIL_CLIENT;
		goto Cleanup;
	}
	
	*argk = strdup(key);
	if ( *argk == NULL) {
		rsp = CITRUSLEAF_FAIL_CLIENT;
		goto Cleanup;
	}		
	*argv = (cl_object *)malloc(sizeof(cl_object));
	if ( *argv == NULL) {
		rsp = CITRUSLEAF_FAIL_CLIENT;
		goto Cleanup;
	}
	citrusleaf_object_init_str(*argv,value);
	*argc += 1;	
Cleanup:
	return rsp;
}

cl_rv citrusleaf_mrjob_add_parameter_numeric(cl_mrjob *mrjob, cl_script_func_t ftype, const char *key, uint64_t value)
{
	fprintf(stderr, "unimplemented\n");
	return CITRUSLEAF_FAIL_CLIENT;
}

cl_rv citrusleaf_mrjob_add_parameter_blob(cl_mrjob *mrjob, cl_script_func_t ftype, cl_type blobtype, const char *key, const uint8_t *value, int val_len)
{
	fprintf(stderr, "unimplemented\n");
	return CITRUSLEAF_FAIL_CLIENT;
}


void citrusleaf_mrjob_destroy(cl_mrjob *mrjob)
{
	if (!mrjob) return;
	if (mrjob->map_fname) free(mrjob->map_fname);
	if (mrjob->rdc_fname) free(mrjob->rdc_fname);
	if (mrjob->fnz_fname) free(mrjob->fnz_fname);
	
	for (int i=0; i<mrjob->map_argc; i++) {
		free (mrjob->map_argk[i]);
		citrusleaf_object_free (mrjob->map_argv[i]);
	}

	for (int i=0; i<mrjob->rdc_argc; i++) {
		free (mrjob->rdc_argk[i]);
		citrusleaf_object_free (mrjob->rdc_argv[i]);
	}

	for (int i=0; i<mrjob->fnz_argc; i++) {
		free (mrjob->fnz_argk[i]);
		citrusleaf_object_free (mrjob->fnz_argv[i]);
	}

	free (mrjob);
}

