/*
 *  Citrusleaf Stored Procedure Test Program
 *  rec_sproc.c - Validates stored procedure functionality
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

#include "ad_sproc.h"


// used to randomly generate test data
#define CLICK_RATE 100
#define N_CAMPAIGNS 10

int do_sproc_user_write(config *c, int user_id) {


    // create a string for the key
    char keyStr[20];
    sprintf(keyStr, "%d",user_id);
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	// (1) bundle what you're trying to write into a single string, pass to server
	char lua_arg[512];
	time_t now = time(0) - (rand() % (60 * 60 * 24)); // fake: last 1 day
	char *action = "imp";
	if (rand() % CLICK_RATE == 0) action = "click"; // 1% chance of click
	int campain_id = rand() % N_CAMPAIGNS;          // the campain that was served
	// campain_id,action,timestamp,
	snprintf(lua_arg,sizeof(lua_arg),"%d,%s,%llu",campain_id,action,(long long unsigned int)now);
	
	// (2) set up stored procedure to call
    cl_sproc_params *sproc_params = citrusleaf_sproc_params_create();
	if (!sproc_params) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr, "can't create sproc_def\n");
		return(-1);
	}
	// adding parameter to be used by the sproc
	citrusleaf_sproc_params_add_string(sproc_params, "w",lua_arg);

	// execute the stored proceedure, print the result
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
    uint32_t gen;
	int rv = citrusleaf_sproc_execute(c->asc, c->ns, c->set, &o_key, 
		c->package_name, "put_behavior", sproc_params, 
		&rsp_bins, &rsp_n_bins, c->timeout_ms, &gen);  
	if (rv != CITRUSLEAF_OK) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr,"failed citrusleaf_run_sproc rsp=%d\n",rv);
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
				fprintf(stderr,"sproc returned %s=[%s]\n",rsp_bins[b].bin_name,rsp_bins[b].object.u.str);
			} else if (rsp_bins[b].object.type == CL_INT) {
				fprintf(stderr,"sproc returned %s=[%ld]\n",rsp_bins[b].bin_name,rsp_bins[b].object.u.i64);
			} else {
				fprintf(stderr,"warning: sproc returned object type %s=%d\n",rsp_bins[b].bin_name,rsp_bins[b].object.type);
			}
			citrusleaf_object_free(&rsp_bins[b].object);		
		}
	}
	free(rsp_bins);	
    citrusleaf_sproc_params_destroy(sproc_params);

	citrusleaf_object_free(&o_key);		
    return 0;
}


int do_sproc_user_read(config *c, int user_id) {

    // create a string for the key
    char keyStr[20];
    sprintf(keyStr, "%d",user_id);
	cl_object o_key;
	citrusleaf_object_init_str(&o_key,keyStr);		

	// a simple read to see what's there currently
	cl_bin *bins = 0;
	int 	n_bins = 0;
	uint32_t gen = 0;
    cl_rv rv = citrusleaf_get_all(c->asc, c->ns, c->set, &o_key, &bins, &n_bins, 
    	c->timeout_ms, &gen);
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
    
    // randomly choose which campains to read from
    cl_sproc_params *sproc_params = citrusleaf_sproc_params_create();
	if (!sproc_params) {
		citrusleaf_object_free(&o_key);		
		fprintf(stderr, "can't create sproc_def\n");
		return(-1);
	}
	char lua_arg[512];
	int campain_id1, campain_id2;
	campain_id1 = rand() % N_CAMPAIGNS;          // the campain that was served
	do {
		campain_id2 = rand() % N_CAMPAIGNS;
	} while (campain_id1 == campain_id2);
	sprintf(&lua_arg[0],"%d,%d",campain_id1,campain_id2);
	fprintf(stderr," sending sproc campains %s\n",lua_arg);

	// adding parameter to be used by the sproc
	citrusleaf_sproc_params_add_string(sproc_params, "w",lua_arg);
    
	// (2) execute the stored proceedure, print the result
	cl_bin *rsp_bins = NULL;
	int     rsp_n_bins = 0;
	rv = citrusleaf_sproc_execute(c->asc, c->ns, c->set, &o_key, 
		c->package_name, "get_campaign",sproc_params, 
		&rsp_bins, &rsp_n_bins, c->timeout_ms, &gen);  
	if (rv != CITRUSLEAF_OK) {
		fprintf(stderr,"failed citrusleaf_run_sproc rsp=%d\n",rv);
		goto Fail;
	}
	// expect a RESULT bin, and the two campains I asked about
	if (rsp_n_bins != 3) {
		fprintf(stderr, "FAILURE: read bins test: expected 3 bins, got %d\n",rsp_n_bins);
		// return(-1);
	}
	for (int i=0;i<rsp_n_bins;i++) {
		if (rsp_bins[i].object.type != CL_STR) {
			fprintf(stderr, "FAILURE: read bins test: expected a string, found %d\n",rsp_bins[0].object.type);
			goto Fail;
		}
		if (0 == strcmp(rsp_bins[i].bin_name,"RESULT")) {
			if (0 != strcmp(rsp_bins[i].object.u.str,"OK")) {
				fprintf(stderr, "FAILURE: result is not OK, is %s\n",rsp_bins[i].object.u.str);
				goto Fail;
			}
		} else {
			fprintf(stderr, " read sproc test: campain %s result %s\n",
				rsp_bins[i].bin_name, rsp_bins[i].object.u.str);
		}
	}

	free(rsp_bins);	
    citrusleaf_sproc_params_destroy(sproc_params);
	citrusleaf_object_free(&o_key);		
    return 0;
    
Fail:
	if (rsp_bins) free(rsp_bins);
	if (sproc_params) citrusleaf_sproc_params_destroy(sproc_params);
	citrusleaf_object_free(&o_key);		
    return(-1);
}



void usage(int argc, char *argv[]) {
    fprintf(stderr, "Usage %s:\n", argv[0]);
    fprintf(stderr, "-h host [default 127.0.0.1] \n");
    fprintf(stderr, "-p port [default 3000]\n");
    fprintf(stderr, "-n namespace [test]\n");
    fprintf(stderr, "-s set [default *all*]\n");
    fprintf(stderr, "-u users [default 100]\n");
    fprintf(stderr, "-b behavioral points [default 1000]\n");
    fprintf(stderr, "-f package_file [lua_packages/ad_sproc.lua]\n");
    fprintf(stderr, "-P package_name [ad_sproc] \n");
    fprintf(stderr, "-v is verbose\n");
}

int main(int argc, char **argv) {
    config c; memset(&c, 0, sizeof(c));
    c.host         = "127.0.0.1";
    c.port         = 3000;
    c.ns           = "test";
    c.set          = 0;
    c.timeout_ms   = 1000;
    c.verbose      = true;
    c.package_file = "lua_packages/ad_sproc.lua";
    c.package_name = "ad_sproc";
    c.register_package = false;
    c.n_users = 100;
    c.n_behaviors = 1000;
        
    fprintf(stderr, "Starting Record stored-procedure Unit Tests\n");
    int optcase;
    while ((optcase = getopt(argc, argv, "ckmrh:p:n:s:P:f:v:u:b:")) != -1) {
        switch (optcase) {
        case 'h': c.host         = strdup(optarg);          break;
        case 'p': c.port         = atoi(optarg);            break;
        case 'n': c.ns           = strdup(optarg);          break;
        case 's': c.set          = strdup(optarg);          break;
        case 'v': c.verbose      = true;                    break;
        case 'r': c.register_package = true;				break;
        case 'f': c.package_file = strdup(optarg);          break;
        case 'P': c.package_name = strdup(optarg);          break;
        case 'u': c.n_users = atoi(optarg);  		        break;
        case 'b': c.n_behaviors = atoi(optarg);  		        break;
        default:  usage(argc, argv);                      return(-1);
        }
    }

    fprintf(stderr, "Startup: host %s port %d ns %s set %s file %s\n",
            c.host, c.port, c.ns, c.set, c.package_file);
    citrusleaf_init();
    
    //citrusleaf_set_debug(true);

    // create the cluster object - attach
    cl_cluster *asc = citrusleaf_cluster_create();
    if (!asc) { fprintf(stderr, "could not create cluster\n"); return(-1); }
    if (0 != citrusleaf_cluster_add_host(asc, c.host, c.port, c.timeout_ms)) {
        fprintf(stderr, "could not connect to host %s port %d\n",c.host,c.port);
        return(-1);
    }
    c.asc           = asc;
    
    // If asked to, register my function
    if (c.register_package) {
		//
		// register our package. 
		fprintf(stderr, "Opening package file %s\n",c.package_file); 
		FILE *fptr = fopen(c.package_file,"r");
		if (!fptr) {
			fprintf(stderr, "cannot open script file %s : %s\n",c.package_file,strerror(errno)); 
			return(-1);
		}
		int max_script_len = 1048576;
		char *script_code = malloc(max_script_len);
		if (script_code == NULL) {
			fprintf(stderr, "malloc failed"); return(-1);
		}    
		
		char *script_ptr = script_code;
		int b_read = fread(script_ptr,1,512,fptr);
		int b_tot = 0;
		while (b_read) {
			b_tot      += b_read;
			script_ptr += b_read;
			b_read      = fread(script_ptr,1,512,fptr);
		}			
		fclose(fptr);
		
		char *err_str = NULL;
		if (b_tot>0) {
			fprintf(stderr,"package name %s script_code %s\n",c.package_name, script_code);
			int resp = citrusleaf_sproc_package_set(asc, c.package_name, script_code, &err_str, CL_SCRIPT_LANG_LUA);
			if (resp!=0) {
				fprintf(stderr, "unable to register package file %s as %s resp = %d\n",c.package_file,c.package_name,resp); 
				if (*err_str) { fprintf(stderr, "%s\n",err_str); free(err_str); }
				return(-1);
			}
			fprintf(stderr, "successfully registered package file %s as %s\n",c.package_file,c.package_name); 
		} else {   
			fprintf(stderr, "unable to read package file %s as %s b_tot = %d\n",c.package_file,c.package_name,b_tot); return(-1);    
		}
		if (*err_str) free(*err_str);
	}
	
    // Write behavior into the database
    fprintf(stderr, "\n*** WRITING %d behavioral points for %d users\n",c.n_behaviors,c.n_users);
    for (int i=0; i < c.n_behaviors ; i++) {
    	do_sproc_user_write(&c, rand() % c.n_users);
    }
    
    //
    // for all possible users, do the first operation: read
    fprintf(stderr, "\n*** READING behavior do_user_read started\n");
    for (int i=0 ; i < c.n_users ; i++) {
    	
    	do_sproc_user_read(&c, i);
    	
    }
    
    citrusleaf_cluster_destroy(asc);

    fprintf(stderr, "\n\nFinished Record stored-procedure Unit Tests\n");
    return(0);
}
