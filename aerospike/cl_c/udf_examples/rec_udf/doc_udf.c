/* *  Citrusleaf Stored Procedure Test Program
 *  doc_udf.c - Validates stored procedure functionality
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
#include <stdbool.h>
#include <assert.h>
#include <ctype.h>

#include <fcntl.h>
#include <sys/stat.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_udf.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>
#include "doc_udf.h"


int do_doc_udf_test(config *c) {
    cl_write_parameters cl_wp;
    cl_write_parameters_set_default(&cl_wp);
    cl_wp.timeout_ms    = c->timeout_ms;
    cl_wp.record_ttl    = 864000;
    int num_records     = 1;
    int num_udf_calls = 2;

    // (0) delete old record to start afresh
    for (int i = 0; i < num_records; i++) {
        char *keyStr = "key1";
        cl_object o_key;
        citrusleaf_object_init_str(&o_key,keyStr);        
        int rsp = citrusleaf_delete(c->asc, c->ns, c->set, &o_key, &cl_wp);
        if (rsp != CITRUSLEAF_OK && rsp != CITRUSLEAF_FAIL_NOTFOUND) {
            printf("failed deleting test data %d rsp=%d\n", i, rsp); return -1;
        }
    }    
        
    // (1) inserting 2 records, one with short data and one with long data
    for (int i = 0; i < num_records; i++) {
        char *keyStr = "key1";
        cl_object o_key; citrusleaf_object_init_str(&o_key,keyStr);        
        // creating bins for the key
        int    num_bins = 3;
        cl_bin bins[num_bins];
        char *valStr = (i==0 ? "short line" : "longer than 10 character line");
        strcpy(bins[0].bin_name, "id");
        citrusleaf_object_init_str(&bins[0].object, keyStr);
        strcpy(bins[1].bin_name, "cats");
        citrusleaf_object_init_str(&bins[1].object, valStr);

        //TODO this is JSON ... WRONG, it should be cmspack.pack(LuaTable)
        char *json = "{\"i\":\"4\",\"j\":\"3\"}";
        strcpy(bins[2].bin_name, "nested");
        citrusleaf_object_init_blob2(&bins[2].object, json, strlen(json),
                                     CL_LUA_BLOB); // WAS: CL_JSON_BLOB);

        // inserting the data        
        int rsp = citrusleaf_put(c->asc, c->ns, c->set, &o_key, bins, num_bins,
                                 &cl_wp);
        // cleanup
        citrusleaf_object_free(&bins[0].object);
        citrusleaf_object_free(&bins[1].object);
        citrusleaf_object_free(&bins[2].object);
        citrusleaf_object_free(&o_key);        
        if (rsp != CITRUSLEAF_OK) {
            printf("failed inserting test data %d rsp=%d\n", i, rsp); return -1;
        }
    }
    
    for (int i = 0; i < num_udf_calls; i++) {
        as_list * arglist = as_arraylist_new(2, 8);
        if (!arglist) { 
		printf("can't create udf arglist\n"); 
		return -1; 
	}
	as_list_add_string(arglist, i ? "10": "20");
	
        char     *keyStr     = "key1";

        as_result res;
        as_result_init(&res);

        cl_object o_key; citrusleaf_object_init_str(&o_key,keyStr);        
        int rsp = citrusleaf_udf_record_apply(c->asc, c->ns, c->set, &o_key, 
                                           c->package_name, "sp_doc_test",
                                           arglist, c->timeout_ms,
                                           &res);  
        if (rsp != CITRUSLEAF_OK) {
            citrusleaf_object_free(&o_key);        
            printf("failed record_udf test data %d rsp=%d\n", i, rsp);
            as_val_destroy(arglist);
            as_result_destroy(&res);
            return -1;
        }
        citrusleaf_object_free(&o_key);        
	   as_val_destroy(arglist);
       as_result_destroy(&res);
    }
    
    // (4) verify record is updated 
    for (int i = 0; i < num_records; i++) {
        uint32_t  cl_gen;
        cl_bin   *rsp_bins   = NULL;
        int       rsp_n_bins = 0;
        char     *keyStr     = "key1";
        cl_object o_key; citrusleaf_object_init_str(&o_key,keyStr);        
        int rsp = citrusleaf_get_all(c->asc, c->ns, c->set, &o_key, &rsp_bins,
                                     &rsp_n_bins, c->timeout_ms, &cl_gen);  
        if (rsp != CITRUSLEAF_OK) {
            printf("failed record_udf test data %d rsp=%d\n", i, rsp);
            citrusleaf_object_free(&o_key); return -1;
        }
        citrusleaf_object_free(&o_key);        

        for (int b = 0; b < rsp_n_bins; b++) {
            char buf[rsp_bins[b].object.sz + 1];
            memcpy(buf, rsp_bins[b].object.u.str, rsp_bins[b].object.sz);
            buf[rsp_bins[b].object.sz] = '\0';
            if        (CL_LUA_BLOB  == rsp_bins[b].object.type) {
                printf("FOUND LUA: (%s)\n", buf);
            } else if (CL_STR       == rsp_bins[b].object.type) {
                printf("FOUND STRING: (%s)\n", buf);
            }
            citrusleaf_object_free(&rsp_bins[b].object);        
        }      
        free(rsp_bins);    
    }
    return 0;
}

void usage(int argc, char *argv[]) {
    printf("Usage %s:\n", argv[0]);
    printf("-h host [default 127.0.0.1] \n");
    printf("-p port [default 3000]\n");
    printf("-n namespace [test]\n");
    printf("-s set [default *all*]\n");
    printf("-f package_file [../lua_files/document_store_test.lua\n");
    printf("-P package_name [udf_unit_test] \n");
    printf("-v is verbose\n");
}

int register_package(config * c) 
{ 
	fprintf(stderr, "Opening package file %s\n",c->package_file);  
	FILE *fptr = fopen(c->package_file,"r"); 
	if (!fptr) { 
		fprintf(stderr, "cannot open script file %s : %s\n",c->package_file,strerror(errno));  
		return(-1); 
	} 
	int max_script_len = 1048576; 
	byte *script_code = (byte*)malloc(max_script_len); 
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
	as_bytes_init(&udf_content, script_code, b_tot, true);
	char *err_str = NULL; 
	if (b_tot>0) { 
		int resp = citrusleaf_udf_put(c->asc, basename(c->package_file), &udf_content, AS_UDF_LUA, &err_str); 
		if (resp!=0) { 
			fprintf(stderr, "unable to register package file %s as %s resp = %d\n",c->package_file,c->package_name,resp); return(-1);
			fprintf(stderr, "%s\n",err_str); free(err_str);
			return(-1);
		}
		fprintf(stderr, "successfully registered package file %s as %s\n", c->package_file,c->package_name); 
	} else {   
		fprintf(stderr, "unable to read package file %s as %s b_tot = %d\n",c->package_file, c->package_name,b_tot); return(-1);    
	}

	return 0;
}

int main(int argc, char **argv) {
    config c; memset(&c, 0, sizeof(c));
    c.host         = "127.0.0.1";
    c.port         = 3000;
    c.ns           = "test";
    c.set          = "demo";
    c.timeout_ms   = 1000;
    c.verbose      = true;
    c.package_file = "../../lua_files/document_store_test.lua";
    c.package_name = "doc_udf_unit_test";
        
    printf("Starting DocumentStore stored-procedure Unit Tests\n");
    int optcase;
    while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:")) != -1) {
        switch (optcase) {
        case 'h': c.host         = strdup(optarg);          break;
        case 'p': c.port         = atoi(optarg);            break;
        case 'n': c.ns           = strdup(optarg);          break;
        case 's': c.set          = strdup(optarg);          break;
        case 'v': c.verbose      = true;                    break;
        case 'f': c.package_file = strdup(optarg);          break;
        case 'P': c.package_name = strdup(optarg);          break;
        default:  usage(argc, argv);                        return -1;
        }
    }

    printf( "Startup: host %s port %d ns %s set %s file %s\n",
            c.host, c.port, c.ns, c.set, c.package_file);
    citrusleaf_init();
    
    // create the cluster object - attach
    cl_cluster *asc = citrusleaf_cluster_create();
    if (!asc) { printf( "could not create cluster\n"); return -1; }
    if (citrusleaf_cluster_add_host(asc, c.host, c.port, c.timeout_ms)) {
        printf( "could not connect to host %s port %d\n", c.host, c.port);
        return -1;
    }
    c.asc           = asc;

    // register our package. 

    if (register_package(&c) !=0 ) {
	    return -1;
    }
    if (do_doc_udf_test(&c)) {
        printf("FAILED: do_doc_udf_test\n"); return -1;
    } else {
        printf("SUCCESS: do_doc_udf_test\n");
    }
    citrusleaf_cluster_destroy(asc);
    printf("\n\nFinished DocumentStore stored-procedure Unit Tests\n");
    return 0;
}
