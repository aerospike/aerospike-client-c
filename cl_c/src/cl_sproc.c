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

cl_mr_job *citrusleaf_mr_job_create(const char *package, const char *map_fname, const char *rdc_fname, const char *fnz_fname )
{
	cl_mr_job *mr_job = malloc(sizeof(cl_mr_job));
	if (mr_job==NULL) {
		return NULL;
	}
	memset(mr_job,0,sizeof(cl_mr_job));
	mr_job->package = strdup(package);
	if (mr_job->package==NULL) {
		goto Cleanup;
	}
		
	if (map_fname) {
		mr_job->map_fname = strdup(map_fname);
		if (mr_job->map_fname==NULL) {
			goto Cleanup;
		}
	}	
	if (rdc_fname) {
		mr_job->rdc_fname = strdup(rdc_fname);
		if (mr_job->rdc_fname==NULL) {
			goto Cleanup;
		}
	}
	if (fnz_fname) {
		mr_job->fnz_fname = strdup(fnz_fname);
		if (mr_job->fnz_fname==NULL) {
			goto Cleanup;
		}
	}
	return mr_job;
	
Cleanup:	
	citrusleaf_mr_job_destroy(mr_job);
	return NULL;	
}

cl_rv citrusleaf_mr_job_add_parameter_string(cl_mr_job *mr_job, cl_script_func_t ftype, const char *key, const char *value)
{
	cl_rv rsp = CITRUSLEAF_OK;
	char 		**argk;
	cl_object 	**argv; 
	int 		*argc;
	if ( CL_SCRIPT_FUNC_TYPE_MAP == ftype) {
		if (mr_job->map_argc>=CL_MAX_NUM_FUNC_ARGC) {
			fprintf(stderr,"too many map_arg %d\n",mr_job->map_argc);
			rsp = CITRUSLEAF_FAIL_CLIENT;
			goto Cleanup;
		}
		argk = &mr_job->map_argk[mr_job->map_argc];
		argv = &mr_job->map_argv[mr_job->map_argc];
		argc = &mr_job->map_argc;
	} else if ( CL_SCRIPT_FUNC_TYPE_REDUCE == ftype) {
		if (mr_job->rdc_argc>=CL_MAX_NUM_FUNC_ARGC) {
			fprintf(stderr,"too many rdc_arg %d\n",mr_job->rdc_argc);
			rsp = CITRUSLEAF_FAIL_CLIENT;
			goto Cleanup;
		}
		argk = &mr_job->rdc_argk[mr_job->rdc_argc];
		argv = &mr_job->rdc_argv[mr_job->rdc_argc];
		argc = &mr_job->rdc_argc;
	} else if ( CL_SCRIPT_FUNC_TYPE_FINALIZE == ftype) {
		if (mr_job->fnz_argc>=CL_MAX_NUM_FUNC_ARGC) {
			fprintf(stderr,"too many map_arg %d\n",mr_job->fnz_argc);
			rsp = CITRUSLEAF_FAIL_CLIENT;
			goto Cleanup;
		}
		argk = &mr_job->fnz_argk[mr_job->fnz_argc];
		argv = &mr_job->fnz_argv[mr_job->fnz_argc];
		argc = &mr_job->fnz_argc;
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

cl_rv citrusleaf_mr_job_add_parameter_numeric(cl_mr_job *mr_job, cl_script_func_t ftype, const char *key, uint64_t value)
{
	fprintf(stderr, "unimplemented\n");
	return CITRUSLEAF_FAIL_CLIENT;
}

cl_rv citrusleaf_mr_job_add_parameter_blob(cl_mr_job *mr_job, cl_script_func_t ftype, cl_type blobtype, const char *key, const uint8_t *value, int val_len)
{
	fprintf(stderr, "unimplemented\n");
	return CITRUSLEAF_FAIL_CLIENT;
}


void citrusleaf_mr_job_destroy(cl_mr_job *mr_job)
{
	if (!mr_job) return;
	if (mr_job->map_fname) free(mr_job->map_fname);
	if (mr_job->rdc_fname) free(mr_job->rdc_fname);
	if (mr_job->fnz_fname) free(mr_job->fnz_fname);
	
	for (int i=0; i<mr_job->map_argc; i++) {
		free (mr_job->map_argk[i]);
		citrusleaf_object_free (mr_job->map_argv[i]);
	}

	for (int i=0; i<mr_job->rdc_argc; i++) {
		free (mr_job->rdc_argk[i]);
		citrusleaf_object_free (mr_job->rdc_argv[i]);
	}

	for (int i=0; i<mr_job->fnz_argc; i++) {
		free (mr_job->fnz_argk[i]);
		citrusleaf_object_free (mr_job->fnz_argv[i]);
	}

	free (mr_job);
}

