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


// sproc argument field layout: contains - numargs, key1_len, key1, value1_len, val1, ....
// 
// generic field header
// 0   4 size = size of data only
// 4   1 field_type = CL_MSG_FIELD_TYPE_SPROC_XXX_ARG
//
// numarg
// 5   1 argc (max 255 ranges) 
//
// argk
// 6     1 argk_len 1
// 7     x argk
//
// argv
// +x    1 argv_particle_type
// +x+1  4 argv_particle_len
// +x+2 xx argv_particle_data
// 
// repeat argk
//
int sproc_compile_arg_field(char * const*argk, cl_object * const*argv, int argc, uint8_t *buf, int *sz_p)
{
	int sz = 0;
		
	// argc
	sz += 1;
	if (buf) {
		*buf++ = argc;
	}
	
	// iterate through each k,v pair	
	for (int i=0; i<argc; i++) {
		// argk size
		int argk_sz = strlen(argk[i]);
		sz += 1;
		if (buf) {
			*buf++ = argk_sz;
		}
		
		// argk
		sz += argk_sz;
		if (buf) {
			memcpy(buf,argk[i],argk_sz);
			buf += argk_sz;
		}
		
		// argv type
		sz += 1;
		if (buf) {
			*buf++ = argv[i]->type;
		}
		
		// argv particle len
		// particle len will be in network order 
		sz += 4;
		size_t psz = 0;
		cl_object_get_size(argv[i],&psz);
		if (buf) {
			uint32_t ss = psz; 
			*((uint32_t *)buf) = ntohl(ss);
			// fprintf(stderr, "*** ss %ld buf %ld\n",ss,*((uint32_t *)buf));
			buf += sizeof(uint32_t);
		} 
		
		// particle data
		sz += psz;
		if (buf) {
			//fprintf(stderr, "*** buf %ld\n",*((uint64_t *)buf));
			cl_object_to_buf(argv[i],buf);
			//fprintf(stderr, "*** buf %ld\n",*((uint64_t *)buf));
			buf += psz;
		}
		
	}		
	
	if (g_cl_turn_debug_on) {
		fprintf(stderr, "processing %d arguments to be %d long\n",argc,sz);
	}
	*sz_p = sz;
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


cl_sproc_def *citrusleaf_sproc_definition_create(const char *package, const char *fname)
{
	if (!package || !fname || strlen(package)==0 || strlen(fname)==0) {
		return NULL;
	}
	
	cl_sproc_def *sd = malloc(sizeof(cl_sproc_def));
	if (sd==NULL) {
		return NULL;
	}
	memset(sd,0,sizeof(cl_sproc_def));
	sd->package = strdup(package);
	if (sd->package==NULL) {
		goto Cleanup;
	}
		
	sd->fname = strdup(fname);
	if (sd->fname==NULL) {
		goto Cleanup;
	}
	return sd;
	
Cleanup:	
	citrusleaf_sproc_definition_destroy(sd);
	return NULL;		
}

void citrusleaf_sproc_definition_destroy(cl_sproc_def *sproc_def)
{
	if (!sproc_def) return;
	if (sproc_def->fname) free(sproc_def->fname);
	
	for (int i=0; i<sproc_def->num_param; i++) {
		free (sproc_def->param_key[i]);
		citrusleaf_object_free (sproc_def->param_val[i]);
	}

	free (sproc_def);
}

cl_rv citrusleaf_sproc_def_add_parameter_string(cl_sproc_def *sproc_def, const char *param_key, const char *param_value)
{
	cl_rv rsp = CITRUSLEAF_OK;
	
	int idx = sproc_def->num_param;
	
	if (idx >= CL_MAX_NUM_FUNC_ARGC) {
		fprintf(stderr,"too many parameters %d\n",sproc_def->num_param);
		rsp = CITRUSLEAF_FAIL_CLIENT;
		goto Cleanup;
	}
	
	sproc_def->param_key[idx] = strdup(param_key);
	if ( sproc_def->param_key[idx] == NULL) {
		rsp = CITRUSLEAF_FAIL_CLIENT;
		goto Cleanup;
	}		
	sproc_def->param_val[idx] = (cl_object *)malloc(sizeof(cl_object));
	if ( sproc_def->param_key[idx] == NULL) {
		rsp = CITRUSLEAF_FAIL_CLIENT;
		goto Cleanup;
	}
	citrusleaf_object_init_str(sproc_def->param_val[idx],param_value);
	sproc_def->num_param++;
		
Cleanup:
	if (rsp!=CITRUSLEAF_OK) {
		if (sproc_def->param_key[idx]) {
			free(sproc_def->param_key[idx]);
		}
		if (sproc_def->param_val[idx]) {
			citrusleaf_object_free (sproc_def->param_val[idx]);
		}
	}
	return rsp;
}



