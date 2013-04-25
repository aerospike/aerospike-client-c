/*
 *  Citrusleaf Stored Procedure Test Program
 *  rec_udf.c - Validates stored procedure functionality
 *
 *  Copyright 2012 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <assert.h>
#include <ctype.h>
#include <time.h>

#include <fcntl.h>
#include <sys/stat.h>

#include "citrusleaf/citrusleaf.h"

#include "example.h"
#include "citrusleaf/cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>


#ifndef LUA_MODULE_PATH
#define LUA_MODULE_PATH "../lua_files"
#endif

static config *g_config = NULL;
void usage(int argc, char *argv[]) {
    fprintf(stderr, "Usage %s:\n", argv[0]);
    fprintf(stderr, "-h host [default 127.0.0.1] \n");
    fprintf(stderr, "-p port [default 3000]\n");
    fprintf(stderr, "-n namespace [default test]\n");
    fprintf(stderr, "-s set [default *all*]\n");
    fprintf(stderr, "-f udf_file [default ../lua_files/ad_udf.lua]\n");
}


int init_configuration (int argc, char *argv[])
{
	g_config = (config *)malloc(sizeof(config));
	memset(g_config, 0, sizeof(g_config));

	g_config->host         = "127.0.0.1";
	g_config->port         = 3000;
	g_config->ns           = "test";
	g_config->set          = "demo";
	g_config->timeout_ms   = 1000;
	g_config->record_ttl   = 864000;
	g_config->verbose      = false;
	g_config->package_file = LUA_MODULE_PATH"/ad_udf.lua";
	g_config->package_name = "ad_udf";
	g_config->n_behaviors = 1000;
	g_config->n_users = 100;
	fprintf(stderr, "Starting Record stored-procedure Unit Tests\n");
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:x:r:t:i:j:")) != -1) {
		switch (optcase) {
			case 'h': g_config->host         = strdup(optarg);          break;
			case 'p': g_config->port         = atoi(optarg);            break;
			case 'n': g_config->ns           = strdup(optarg);          break;
			case 's': g_config->set          = strdup(optarg);          break;
			case 'v': g_config->verbose      = true;                    break;
			case 'f': g_config->package_file = strdup(optarg);          break;
			case 'P': g_config->package_name = strdup(optarg);          break;
			default:  usage(argc, argv);                      return(-1);
		}
	}
	return 0;
}

// used to randomly generate test data
#define CLICK_RATE 100
#define N_CAMPAIGNS 10

int do_udf_user_write(int user_id) {
	// create a string for the key
	char keyStr[20];
	sprintf(keyStr, "%d",user_id);
	fprintf(stderr,"KEY IS %s\n",keyStr);
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	// (1) bundle what you're trying to write into a single string, pass to server
	char lua_arg[512];
	time_t now = time(0) - (rand() % (60 * 60 * 24)); // fake: last 1 day
	char *action = "imp";
	if (rand() % CLICK_RATE == 0) action = "click"; // 1% chance of click
	int campaign_id = rand() % N_CAMPAIGNS;          // the campaign that was served
	// campaign_id,action,timestamp,
	snprintf(lua_arg, sizeof(lua_arg), "%d,%s,%llu", campaign_id, action, (long long unsigned int)now);

	// (2) set up udf to call
	as_list * arglist = as_arraylist_new(2, 8);
	if (!arglist) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr, "can't create argument list\n");
		return(-1);
	}
	// adding parameter to be used by the udf
	as_list_add_string(arglist, lua_arg);

	// execute the udf, print the result
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	uint32_t gen;
	as_result res;
	as_result_init(&res);
	int rv = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "put_behavior", arglist, 
			g_config->timeout_ms, &res); 

	if (rv != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);	
		as_val_destroy(arglist);	
		fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rv);
		return -1;
	}
	char *result_str = as_val_tostring(res.value);
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", result_str);
	free(result_str);
	int rsp = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &rsp_bins, &rsp_n_bins, g_config->timeout_ms, &gen);  
	if (rsp != CITRUSLEAF_OK) {
		fprintf(stderr,"citrusleaf_get_all failed with %d\n",rsp);
		as_val_destroy(arglist);
		citrusleaf_object_free(&o_key);
		return -1;
	}
	if ((rsp_n_bins == 1) &&
			(rsp_bins[0].object.type == CL_STR) &&
			(0 == strcmp(rsp_bins[0].object.u.str,"OK"))) {
		; // OK
	}
	else {
		for (int b=0; b<rsp_n_bins; b++) {
			if (rsp_bins[b].object.type == CL_STR) {
				fprintf(stderr,"udf returned %s=[%s]\n",rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
			} else if (rsp_bins[b].object.type == CL_INT) {
				fprintf(stderr,"udf returned %s=[%ld]\n",rsp_bins[b].bin_name,rsp_bins[b].object.u.i64);
			} else {
				fprintf(stderr,"warning: udf returned object type %s=%d\n",rsp_bins[b].bin_name,rsp_bins[b].object.type);
			}
			citrusleaf_object_free(&rsp_bins[b].object);		
		}
	}
	if (rsp_bins) {
		citrusleaf_bins_free(rsp_bins, rsp_n_bins);
		free(rsp_bins);
	}
	as_val_destroy(arglist);
	as_result_destroy(&res);

	citrusleaf_object_free(&o_key);		
	return 0;
}


int do_udf_user_read(int user_id) {

	// create a string for the key
	char keyStr[20];
	sprintf(keyStr, "%d",user_id);
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	// a simple read to see what's there currently
	cl_bin *bins = 0;
	int 	n_bins = 0;
	uint32_t gen = 0;
	cl_rv rv = citrusleaf_get_all(g_config->asc, g_config->ns, g_config->set, &o_key, &bins, &n_bins, 
			g_config->timeout_ms, &gen);
	if (rv!=CITRUSLEAF_OK) {
		fprintf(stderr,"citrusleaf_get_all failed with %d\n",rv);
		citrusleaf_object_free(&o_key);
		return -1;
	}
	//    fprintf(stderr, "pre_read: user_id %d received %d bins\n",user_id,n_bins);
	for (int i=0 ; i < n_bins ; i++) {
		if (bins[i].object.type == CL_STR) {
			//			fprintf(stderr,"pre-read %d %s=[%s]\n",i,bins[i].bin_name,bins[i].object.u.str);
			;
		}
		else {
			fprintf(stderr,"pre-read: bin %d unexpected type %d\n",i, bins[i].object.type);
		}
		citrusleaf_object_free(&bins[i].object);
	}
	free(bins);

	// randomly choose which campaigns to read from
	as_list * arglist = as_arraylist_new(2, 8);
	if (!arglist) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr, "can't create udf_def\n");
		return(-1);
	}
	char lua_arg[512];
	int campaign_id1, campaign_id2;
	campaign_id1 = rand() % N_CAMPAIGNS;          // the campaign that was served
	do {
		campaign_id2 = rand() % N_CAMPAIGNS;
	} while (campaign_id1 == campaign_id2);
	sprintf(&lua_arg[0],"%d,%d",campaign_id1,campaign_id2);
	fprintf(stderr," sending udf campaigns %s\n",lua_arg);

	// adding parameter to be used by the udf
	as_list_add_string(arglist, lua_arg);

	// (2) execute the udf, print the result
	as_result res;
	as_result_init(&res);
	rv = citrusleaf_udf_record_apply(g_config->asc, g_config->ns, g_config->set, &o_key, 
			g_config->package_name, "get_campaign",arglist, 
			g_config->timeout_ms, &res);  
	char *result_str = as_val_tostring(res.value);
	fprintf(stderr,"%s: %s\n", res.is_success ? "SUCCESS" : "FAILURE", result_str);
	free(result_str);
	as_map * m = as_map_fromval(res.value);
	int map_size = as_map_size(m);
	if (rv != CITRUSLEAF_OK) {
		fprintf(stderr,"failed citrusleaf_run_udf rsp=%d\n",rv);
		goto Fail;
	}
	// Did not get the required number of campaigns
	if (map_size != 3) {
		fprintf(stderr,"FAILED read test -- expected 3, got %d\n",map_size);
		goto Fail;
	}
	
	as_val_destroy(arglist);
	citrusleaf_object_free(&o_key);		
	as_result_destroy(&res);
	return 0;
Fail: 
	if (arglist) as_val_destroy(arglist);
	citrusleaf_object_free(&o_key);		
	as_result_destroy(&res);
	return(-1);

}

int register_package() 
{ 
	fprintf(stderr, "Opening package file %s\n",g_config->package_file);  
	FILE *fptr = fopen(g_config->package_file,"r"); 
	if (!fptr) { 
		fprintf(stderr, "cannot open script file %s : %s\n",g_config->package_file,strerror(errno));  
		return(-1); 
	} 
	int max_script_len = 1048576; 
	byte *script_code = (byte *)malloc(max_script_len); 
	if (script_code == NULL) { 
		fprintf(stderr, "malloc failed"); return(-1); 
	}     

	byte *script_ptr = script_code; 
	int b_read = fread(script_ptr,1,512,fptr); 
	int b_tot = 0; 
	while (b_read) { 
		b_tot      += b_read; 
		script_ptr += b_read; 
		b_read      = fread(script_ptr,1,512,fptr); 
	}                     
	fclose(fptr);
	as_bytes udf_content;
	as_bytes_init(&udf_content, script_code, b_tot, true /*is_malloc*/ );

	char *err_str = NULL; 
	if (b_tot>0) { 
		int resp = citrusleaf_udf_put(g_config->asc, basename(g_config->package_file), &udf_content, AS_UDF_LUA, &err_str); 
		if (resp!=0) { 
			fprintf(stderr, "unable to register package file %s as %s resp = %d\n",g_config->package_file,g_config->package_name,resp); return(-1);
			fprintf(stderr, "%s\n",err_str); free(err_str);
			as_bytes_destroy(&udf_content);
			return(-1);
		}
		fprintf(stderr, "successfully registered package file %s as %s\n",g_config->package_file,g_config->package_name); 
	} else {   
		fprintf(stderr, "unable to read package file %s as %s b_tot = %d\n",g_config->package_file,g_config->package_name,b_tot); return(-1);    
	}
	as_bytes_destroy(&udf_content);
	
	return 0;
}
int main(int argc, char **argv) {
	// reading parameters
	if (init_configuration(argc,argv) !=0 ) {
		return -1;
	}
	fprintf(stderr, "Startup: host %s port %d ns %s set %s file %s\n",
			g_config->host, g_config->port, g_config->ns, g_config->set, g_config->package_file);
	citrusleaf_init();

	//citrusleaf_set_debug(true);

	// create the cluster object - attach
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { fprintf(stderr, "could not create cluster\n"); return(-1); }
	if (0 != citrusleaf_cluster_add_host(asc, g_config->host, g_config->port, g_config->timeout_ms)) {
		fprintf(stderr, "could not connect to host %s port %d\n",g_config->host,g_config->port);
		return(-1);
	}
	g_config->asc = asc;

	// register our package. 
	if (register_package() !=0 ) {
		return -1;
	}

	// Write behavior into the database
	fprintf(stderr, "\n*** WRITING %d behavioral points for %d users\n",g_config->n_behaviors,g_config->n_users);
	for (int i=0; i < g_config->n_behaviors ; i++) {
		do_udf_user_write(rand() % g_config->n_users);
	}

	//
	// for all possible users, do the first operation: read
	fprintf(stderr, "\n*** READING behavior do_user_read started\n");
	for (int i=0 ; i < g_config->n_users ; i++) {

		do_udf_user_read(i);

	}

	citrusleaf_cluster_destroy(asc);
	citrusleaf_shutdown();

	fprintf(stderr, "\n\nFinished Record UDF Unit Tests\n");
	return(0);
}
