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
#include "citrusleaf/cf_b64.h"
#include "citrusleaf/cf_socket.h"
#include "citrusleaf/cf_vector.h"

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
	
	if (cf_debug_enabled()) {
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


cl_sproc_params *citrusleaf_sproc_params_create()
{
	cl_sproc_params *sd = malloc(sizeof(cl_sproc_params));
	if (sd==NULL) {
		return NULL;
	}
	memset(sd,0,sizeof(cl_sproc_params));
	return sd;
}

void citrusleaf_sproc_params_destroy(cl_sproc_params *params)
{
	if (!params) return;
	for (int i=0; i<params->num_param; i++) {
		free (params->param_key[i]);
		citrusleaf_object_free (params->param_val[i]);
		free (params->param_val[i]);
	}                    

	free (params);
}

cl_rv citrusleaf_sproc_params_add_string(cl_sproc_params *sproc_def, const char *param_key, const char *param_value)
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

cl_rv
citrusleaf_sproc_package_set(cl_cluster *asc, const char *package_name, const char *script_str, char **err, cl_script_lang_t lang_t)
{	
	if (lang_t!=CL_SCRIPT_LANG_LUA) {
		fprintf(stderr, "unrecognized script language %d\n",lang_t);
		return(-1);
	}	
	const char lang[] = "lua";

	if (!package_name || !script_str) {
		fprintf(stderr, "package name and script required\n");
		return CITRUSLEAF_FAIL_CLIENT;
	}
	int script_str_len = strlen(script_str);
	
	int  info_query_len = cf_base64_encode_maxlen(script_str_len)+strlen(package_name)+strlen(lang)+100;
	char *info_query = malloc(info_query_len);	
	if (!info_query) {
		fprintf(stderr, "cannot malloc\n");
		return CITRUSLEAF_FAIL_CLIENT;
	}

	// put in the default stuff first
	snprintf(info_query, info_query_len, "set-package:package=%s;lang=%s;script=",package_name,lang);

	cf_base64_tostring((uint8_t *)script_str, (uint8_t *)(info_query+strlen(info_query)), &script_str_len);
		
	//fprintf(stderr, "**[%s]\n",info_query);

	char *values = 0;

	// shouldn't do this on a blocking thread --- todo, queue
	if (0 != citrusleaf_info_cluster_all(asc, info_query, &values, true/*asis*/, 5000/*timeout*/)) {
		fprintf(stderr, "could not set package %s from cluster\n",package_name);
		free(info_query);
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	if (0 == values) {
		fprintf(stderr, "info cluster success, but no response from server\n");
		free(info_query);
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	free(info_query);
	
	// got response, 
	// format: request\tresponse
	// response is a string "ok" or ???
	
	char *value = strchr(values, '\t') + 1; // skip request, parse response 
	
	int n_tok=0;
	char *brkb = 0;
	char *words[20];
	do {
		words[n_tok] = strtok_r(value,"=",&brkb);
		if (0 == words[n_tok]) break;
		value = 0;
		words[n_tok+1] = strtok_r(value,";",&brkb);
		if (0 == words[n_tok+1]) break;
		char *newline = strchr(words[n_tok+1],'\n');
		if (newline) *newline = 0;
		n_tok += 2;
		if (n_tok >= 20) {
			fprintf(stderr, "too many tokens\n");
			return CITRUSLEAF_FAIL_UNKNOWN;
		}
	} while(true);
	
	char *err_str = 0;
	
	for (int i = 0; i < n_tok ; i += 2) {
		char *key = words[i];
		char *value = words[i+1];
		if (0 == strcmp(key,"error")) {
			err_str = value;
		} else {
			//fprintf(stderr, "package set: unknown key %s value %s\n",key,value);
		}
	}
	if (err_str) {
		int err_str_len = strlen(err_str);
		// fprintf(stderr, "package set: server returned error %s\n",err_str);
		// fprintf(stderr, "package set: server returned error bytes %d\n",err_str_len);
		char *err_de64 = malloc(err_str_len+1); // guarenteed to shrink it
		if (!err_str) {
			free(values);
			return(-1);
		}
		cf_base64_decode(err_str, err_de64, &err_str_len, true/*validate*/);
		*err = err_de64;
		err_de64[err_str_len] = 0;
		free(values);
		return CITRUSLEAF_FAIL_UNKNOWN;
	}	
	
	free(values);
	
	return(0);
	
}


cl_rv
citrusleaf_sproc_package_list(cl_cluster *asc, char ***package_names, int *n_packages, cl_script_lang_t lang_t)
{
//	fprintf(stderr, "citrusleaf list get \n");
	
	*package_names = NULL;
	*n_packages = 0;

	if (lang_t!=CL_SCRIPT_LANG_LUA) {
		fprintf(stderr, "unrecognized script language %d\n",lang_t); 
		return(-1);
	}	
	const char *lang = "lua";
	
	char info_query[512];
	if (sizeof(info_query) <= (size_t) snprintf(info_query, sizeof(info_query), "packages:lang=%s;",lang)) {
		fprintf(stderr, "string too long \n"); 
		return(-1);
	}
	char *values = 0;
	// shouldn't do this on a blocking thread --- todo, queue
	if (0 != citrusleaf_info_cluster(asc, info_query, &values, true/*asis*/, 100/*timeout*/)) {
		fprintf(stderr, "could not get package_list from cluster\n");
		return(-1);
	}
	if (0 == values) {
		fprintf(stderr, "info cluster success, but no response on server\n");
		return(-1);
	}
	
	// got response, 
	// format: request\tresponse
	// response is packages=p1,p2,p3;
	
	char *value = strchr(values, '\t') + 1; // skip request, parse response 
	
	int n_tok=0;
	char *brkb = 0;
	char *words[20];
	do {
		words[n_tok] = strtok_r(value,"=",&brkb);
		if (0 == words[n_tok]) break;
		value = 0;
		words[n_tok+1] = strtok_r(value,";",&brkb);
		if (0 == words[n_tok+1]) break;
		char *newline = strchr(words[n_tok+1],'\n');
		if (newline) *newline = 0;
		n_tok += 2;
		if (n_tok >= 20) {
			fprintf(stderr, "too many tokens\n");
			free(values);
			return(-1);
		}
	} while(true);
	
	char *packages_str = 0;
	char *error_str = 0;
	
	
	for (int i = 0; i < n_tok ; i += 2) {
		char *key = words[i];
		char *value = words[i+1];
		if (0 == strcmp(key,"packages")) {
			packages_str = value;
		}
		if (0 == strcmp(key,"error")) {
			error_str = value;
		}
	}
	
	// now break down all the package names
	n_tok = 0;
	do {
		words[n_tok] = strtok_r(packages_str,",",&brkb);
		if (0 == words[n_tok]) break;
		packages_str = NULL;
		n_tok ++;
		if (n_tok >= 20) {
			fprintf(stderr, "too many tokens\n");
			free(values);
			return(-1);
		}
	} while(true);
	
	char **p_names = (char **)malloc(sizeof(char *)*n_tok);
	if (!p_names) {
		fprintf(stderr,"cannot allocate\n");
		free(values);
		return -1;
	}
	
	for (int i=0; i< n_tok; i++) {
		p_names[i] = strdup(words[i]);
	}
	*package_names = p_names;
	*n_packages = n_tok;

	free(values);
	
	return(0);
	
}

cl_rv citrusleaf_sproc_package_delete(cl_cluster *asc, const char *package_name, cl_script_lang_t lang_t)
{
	if (lang_t!=CL_SCRIPT_LANG_LUA) {
		fprintf(stderr, "unrecognized script language %d\n",lang_t);
		return(-1);
	}	
	const char *lang = "lua";

	if (!package_name) {
		fprintf(stderr, "package name required\n");
		return CITRUSLEAF_FAIL_CLIENT;
	}

	char info_query[512];
	if (sizeof(info_query) <= (size_t) snprintf(info_query, sizeof(info_query), "package-delete:package=%s;lang=%s;",package_name, lang)) {
		return(-1);
	}
	char *values = 0;

	// shouldn't do this on a blocking thread --- todo, queue
	if (0 != citrusleaf_info_cluster_all(asc, info_query, &values, true/*asis*/, 5000/*timeout*/)) {
		fprintf(stderr, "could not delete package %s from cluster\n",package_name);
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	if (0 == values) {
		fprintf(stderr, "info cluster success, but no response from server\n");
		return CITRUSLEAF_FAIL_UNKNOWN;
	}
	
	// got response, 
	// format: request\tresponse
	// response is a string "ok" or ???
	
	char *value = strchr(values, '\t') + 1; // skip request, parse response 
	
	int n_tok=0;
	char *brkb = 0;
	char *words[20];
	do {
		words[n_tok] = strtok_r(value,"=",&brkb);
		if (0 == words[n_tok]) break;
		value = 0;
		words[n_tok+1] = strtok_r(value,";",&brkb);
		if (0 == words[n_tok+1]) break;
		char *newline = strchr(words[n_tok+1],'\n');
		if (newline) *newline = 0;
		n_tok += 2;
		if (n_tok >= 20) {
			fprintf(stderr, "too many tokens\n");
			return CITRUSLEAF_FAIL_UNKNOWN;
		}
	} while(true);
	
	char *err_str = 0;
	
	for (int i = 0; i < n_tok ; i += 2) {
		char *key = words[i];
		char *value = words[i+1];
		if (0 == strcmp(key,"error")) {
			err_str = value;
		} else {
			//fprintf(stderr, "package set: unknown key %s value %s\n",key,value);
		}
	}
	if (err_str) {
		fprintf(stderr, "package set: server returned error %s\n",err_str);
		free(values);
		return CITRUSLEAF_FAIL_UNKNOWN;
	}	
	
	free(values);
	
	return(0);
	
}		

cl_rv
citrusleaf_sproc_package_get_content(cl_cluster *asc, const char *package_name, char **content, int *content_len, cl_script_lang_t lang_t)
{
	return citrusleaf_sproc_package_get_with_gen(asc, package_name, content, content_len, NULL, lang_t);
}

//
// grab the package from a server
// Not sure whether to do sync or async. Start with sync.
int
citrusleaf_sproc_package_get_with_gen(cl_cluster *asc, const char *package_name, char **content, int *content_len, char **gen, cl_script_lang_t lang_t)
{
//	fprintf(stderr, "citrusleaf mr package get %s\n",package_name);
	if (content) {
		*content = NULL;
		*content_len = 0;
	}
	if (gen) {
		*gen = NULL;
	}
	
	if (lang_t!=CL_SCRIPT_LANG_LUA) {
		fprintf(stderr, "unrecognized script language %d\n",lang_t);
		return(-1);
	}	
	const char *lang = "lua";
	
	char info_query[512];
	if (sizeof(info_query) <= (size_t) snprintf(info_query, sizeof(info_query), "get-package:package=%s;lang=%s;",package_name,lang)) {
		return(-1);
	}
	char *values = 0;
	// shouldn't do this on a blocking thread --- todo, queue
	if (0 != citrusleaf_info_cluster(asc, info_query, &values, true/*asis*/, 100/*timeout*/)) {
		fprintf(stderr, "could not get package %s from cluster\n",package_name);
		return(-1);
	}
	if (0 == values) {
		fprintf(stderr, "info cluster success, but no package %s on server\n",package_name);
		return(-1);
	}
	
	// got response, add into cache
	// format: request\tresponse
	// response is gen=asdf;script=xxyefu
	// where gen is a simple string, and script
	// error is something else entirely
	
	char *value = strchr(values, '\t') + 1; // skip request, parse response 
	
	int n_tok=0;
	char *brkb = 0;
	char *words[20];
	do {
		words[n_tok] = strtok_r(value,"=",&brkb);
		if (0 == words[n_tok]) break;
		value = 0;
		words[n_tok+1] = strtok_r(value,";",&brkb);
		if (0 == words[n_tok+1]) break;
		char *newline = strchr(words[n_tok+1],'\n');
		if (newline) *newline = 0;
		n_tok += 2;
		if (n_tok >= 20) {
			fprintf(stderr, "too many tokens\n");
			free(values);
			return(-1);
		}
	} while(true);
	
	char *gen_str = 0;
	char *script64_str = 0;
	
	for (int i = 0; i < n_tok ; i += 2) {
		char *key = words[i];
		char *value = words[i+1];
		if (0 == strcmp(key,"gen")) {
			gen_str = value;
		}
		else if (0 == strcmp(key,"script")) {
			script64_str = value;
		} else {
			fprintf(stderr, "package get: unknown key %s value %s\n",key,value);
		}
	}
	if ( (!gen_str) || (!script64_str)) {
		fprintf(stderr, "get package did not return enough data\n");
		free(values);
		return(-1);
	}
	
	// unbase64
	int script_str_len = strlen(script64_str);
	char *script_str = malloc(script_str_len+1); // guarenteed to shrink it
	if (!script_str) {
		free(values);
		return(-1);
	}
	int rv = cf_base64_decode(script64_str, script_str, &script_str_len, true/*validate*/);
	if (rv != 0) {
		fprintf(stderr,"could not decode base64 from server %s\n",script64_str);
		free(script_str);
		free(values);
		return(-1);
	}
	script_str[script_str_len] = 0;
	
	if (content) {
		*content = strdup(script_str);
		*content_len = script_str_len;
	}
	if (gen) {
		*gen = strdup(gen_str);
	}
	free(values);
	
	return(0);
	
}


