/* *  Citrusleaf Stored Procedure Test Program
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

typedef struct config_s {
	
	char *host;
	int   port;

	cl_cluster	*asc;
		
	char *package_path;
			
} config;


int read_file (char * filename, byte **content, uint32_t * content_len)
{
    //fprintf(stderr, "Opening package file %s\n", filename); 
    FILE *fptr = fopen(filename,"r");
    if (!fptr) {
        fprintf(stderr, "cannot open script file %s : %s\n",filename,strerror(errno)); 
        return(-1);
    }
    int max_script_len = 1048576;
    byte *script_code = malloc(max_script_len);
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
	script_code[b_tot] = 0;
    fclose(fptr);
    *content = script_code;
    *content_len = b_tot; 
	return 0;
}


void usage(int argc, char *argv[]) {
    fprintf(stderr, "Usage %s:\n", argv[0]);
    fprintf(stderr, "-h host [default 127.0.0.1] \n");
    fprintf(stderr, "-p port [default 3000]\n");
    fprintf(stderr, "-f package_path [/home/citrusleaf/code/client/test/sproc_tests/lua_packages]\n");
}

int main(int argc, char **argv) {
	config c; memset(&c, 0, sizeof(c));
	c.host         = "127.0.0.1";
	c.port         = 3000;
	c.package_path = "../lua_files/";

	fprintf(stderr, "Starting udf management Unit Tests\n");
	fprintf(stderr, "Before starting these tests, make sure you do not have already registered packages in the system path\n");
	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:")) != -1) {
		switch (optcase) {
			case 'h': c.host         = strdup(optarg);          break;
			case 'p': c.port         = atoi(optarg);            break;
			case 'f': c.package_path = strdup(optarg);          break;
			default:  usage(argc, argv);                      return(-1);
		}
	}

	fprintf(stderr, "Startup: host %s port %d path %s\n\n\n",
			c.host, c.port, c.package_path);
	citrusleaf_init();

	//citrusleaf_set_debug(true);
	
	// create the cluster object - attach
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { fprintf(stderr, "could not create cluster\n"); return(-1); }
	if (0 != citrusleaf_cluster_add_host(asc, c.host, c.port, 5000)) {
		fprintf(stderr, "could not connect to host %s port %d\n",c.host,c.port);
		free(asc);
		return(-1);
	}
	c.asc           = asc;

	// register our package. 
	byte *content = NULL;
	char filename[1024];
	char *package_name = "test_register";
	uint32_t content_len;
	sprintf(filename,"%s%s",c.package_path,"register1.lua");
	int rsp = read_file (filename, &content, &content_len); 	   
	char *err_str = NULL;
	as_bytes udf_content;
	as_bytes_init(&udf_content, content, content_len, true /*is_malloc*/);  // want to re-use content
	if (rsp==0) {
		int resp = citrusleaf_udf_put(asc, basename(filename), &udf_content, AS_UDF_LUA, &err_str);
		if (resp!=0) {
			fprintf(stderr, "unable to register package file %s as %s resp = %d\n", filename, package_name,resp); 
			fprintf(stderr, "[%s]\n",err_str); free(err_str);
			as_bytes_destroy(&udf_content);
			return(-1);
		}
		fprintf(stderr, "*** successfully registered package file %s as %s\n",filename, package_name); 
	} else {   
		fprintf(stderr, "unable to read package file %s\n",filename); return(-1);    
	}
	as_bytes_destroy(&udf_content);
	
	/* get the package */    
	as_udf_file file;
	memset(&file,0,sizeof(as_udf_file));
	int resp = citrusleaf_udf_get(asc, basename(filename), &file, 0, &err_str);
	if (resp!=0) {
		fprintf(stderr, "unable to retrieve package %s resp = %d\n", package_name,resp); return(-1);
	} else {
		char *s = as_val_tostring(&file.content);
		fprintf(stderr, "*** successfully retrieved package content for %s = [%s]\n",package_name, s );
		free(s);
	}
	as_val_destroy(&file.content);
	
	// list the packages
	as_udf_file ** packages = NULL;
	int num_packages = 0;
	resp = citrusleaf_udf_list(asc, &packages, &num_packages, &err_str);
	if (resp!=0) {
		fprintf(stderr, "unable to list package files %d\n", rsp); return(-1);
	} else {
		if (num_packages > 0 ) {
			fprintf(stderr,"*** successfully retrieved package list with %d items\n",num_packages); 
			for (int i=0; i<num_packages;i++) {
				fprintf(stderr,"[%d] = Name: %s, Hash: %s, Type: %d\n",i, packages[i]->name, packages[i]->hash, packages[i]->type);
				free(packages[i]);
			}
			free (packages);
			if (num_packages != 1) {
				fprintf(stderr,"FAILED: Expected 1 package, got %d\n",num_packages);
				return -1;
			}    
		}
	}
	// register another package.
	char filename2[1024];
	char *package_name2 = "test_register2";
	sprintf(filename2,"%s%s",c.package_path,"register2.lua");
	rsp = read_file (filename2, &content, &content_len); 	   
	as_bytes_init(&udf_content, content, content_len, true /*is_malloc*/ );
	if (rsp==0) {
		char *err_str = NULL;
		int resp = citrusleaf_udf_put(asc, basename(filename2), &udf_content, AS_UDF_LUA, &err_str);
		if (resp!=0) {
			fprintf(stderr, "unable to register package file %s as %s resp = %d\n", filename2, package_name2,resp); return(-1);
			fprintf(stderr, "[%s]\n",err_str); free(err_str);
		}
		fprintf(stderr, "*** successfully registered 2nd package file %s as %s\n",filename2, package_name2); 
	} else {   
		fprintf(stderr, "unable to read package file %s\n",filename2); return(-1);    
	}
	as_bytes_destroy(&udf_content);

	/* get the package */    
	memset(&file,0,sizeof(as_udf_file));
	file.content = calloc(1,sizeof(as_bytes));
	resp = citrusleaf_udf_get(asc, basename(filename2), &file, 0, &err_str);
	if (resp!=0) {
		fprintf(stderr, "unable to retrieve package %s resp = %d\n", package_name2,resp); return(-1);
	} else {
		char *s = as_val_tostring(&file.content);
		fprintf(stderr, "*** successfully retrieved package content for %s = [%s]\n",package_name2, s);
		free(s);
	}
	as_val_destroy(&file.content);

	// list the packages
	num_packages = 0;
	resp = citrusleaf_udf_list(asc, &packages, &num_packages, &err_str);
	if (resp!=0) {
		fprintf(stderr, "unable to list package files %d\n", resp); return(-1);
	} else {
		if (num_packages > 0) {
			fprintf(stderr,"*** successfully retrieved package list with %d items\n",num_packages); 
			for (int i=0; i<num_packages;i++) {
				fprintf(stderr,"[%d] = Name: %s, Hash: %s, Type: %d\n",i, packages[i]->name, packages[i]->hash, packages[i]->type);
				free (packages[i]);
			}
			free (packages);
			if (num_packages != 2) {
				fprintf(stderr,"FAILED: Expected 2 packages, got %d\n",num_packages);
				return -1;
			}
		}
	}

	/* delete the package */    
	resp = citrusleaf_udf_remove(asc, filename, &err_str);
	if (resp!=0) {
		fprintf(stderr, "unable to retrieve package %s resp = %d\n", package_name,resp); return(-1);
	} else {
		fprintf(stderr, "*** successfully deleted package %s\n",package_name); 
	}	
	/* list the packages again */
	num_packages = 0;	
	resp = citrusleaf_udf_list(asc, &packages, &num_packages, &err_str);
	if (resp!=0) {
		fprintf(stderr, "unable to list package files %d\n", resp); return(-1);
	} else {
		if (num_packages == 1) {
			fprintf(stderr,"*** successfully retrieved package list with %d items\n",num_packages); 
			for (int i=0; i<num_packages;i++) {
				fprintf(stderr,"[%d] = Name: %s, Hash: %s, Type: %d\n",i, packages[i]->name, packages[i]->hash, packages[i]->type);
				free (packages[i]);
			}
			free (packages);
		}
		else if (num_packages != 1) {
			free(packages);
			fprintf(stderr,"FAILED: Expected 1 package got %d\n",num_packages);
			return -1;
		}

	}
	
	// register a package with syntax error
	char *package_name3 = "test_register3";
	sprintf(filename,"%s%s",c.package_path,"register3.lua");
	rsp = read_file (filename, &content, &content_len); 
	as_bytes_init(&udf_content, content, content_len, true);
	if (rsp==0) {
		char *err_str = NULL;
		int resp = citrusleaf_udf_put(asc, filename, &udf_content, AS_UDF_LUA, &err_str);
		if (resp!=0) {
			fprintf(stderr, "*** successfully received registration error %s\n",package_name3); 
			fprintf(stderr, "[%s]\n",err_str); free(err_str);
		}
		else {
			fprintf(stderr,"FAILED: Registration returned 0, should not have happened\n");
			as_bytes_destroy(&udf_content);
			return -1;
		}
	}
	as_bytes_destroy(&udf_content);
	
	// List packages again
	num_packages = 0;
	resp = citrusleaf_udf_list(asc, &packages, &num_packages, &err_str);
	if (resp!=0) {
		fprintf(stderr, "unable to list package files %d\n", resp); return(-1);
	} else {
		if (num_packages > 0 ) {
			fprintf(stderr,"*** successfully retrieved package list with %d items\n",num_packages); 
			for (int i=0; i<num_packages;i++) {
				fprintf(stderr,"[%d] = Name: %s, Hash: %s, Type: %d\n",i, packages[i]->name, packages[i]->hash, packages[i]->type);
				free (packages[i]);
			}
			free (packages);
		} else {
			fprintf(stderr,"FAILED: Expected 1 package got %d\n",num_packages);
			return -1;
		}
	}
	citrusleaf_cluster_destroy(asc);
	fprintf(stderr, "\n\nFinished Record stored-procedure Unit Tests\n");
	return(0);
}
