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

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_udf.h>
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>

typedef struct config_s {
	char * 			host;
	int   			port;
	cl_cluster * 	asc;
	char * 			package_path;
} config;


int read_file (char * filename, byte **content, uint32_t * content_len) {
    //fprintf(stderr, "Opening package file %s\n", filename); 
    FILE *fptr = fopen(filename,"r");
    if (!fptr) {
        fprintf(stderr, "error: cannot open script file %s : %s\n",filename,strerror(errno)); 
        return(-1);
    }
    int max_script_len = 1048576;
    byte *script_code = malloc(max_script_len);
    if (script_code == NULL) {
        fprintf(stderr, "error: malloc failed"); 
        return(-1);
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

int udf_put(cl_cluster * asc, const char * module, const char * module_path) {

	int 		rc 				= 0;
	byte * 		content 		= NULL;
	char 		filename[1024] 	= {0};
	uint32_t 	content_len 	= 0;
	char * 		error 			= NULL;
	
	sprintf(filename,"%s%s.lua", module_path, module);

	rc = read_file(filename, &content, &content_len); 	   
	
	if ( rc == 0 ) {
		as_bytes udf_content;
		as_bytes_init(&udf_content, content, content_len, true /*is_malloc*/);  // want to re-use content

		rc = citrusleaf_udf_put(asc, basename(filename), &udf_content, AS_UDF_LUA, &error);
		if ( rc != 0 ) {
			fprintf(stderr, "error: unable to upload module: %s\n", filename); 
			fprintf(stderr, "error: (%d) %s\n", rc, error); 
			free(error);
			error = NULL;
		}
		else {
			fprintf(stderr, "info: module uploaded: %s\n",filename); 
		}
		
		as_bytes_destroy(&udf_content);
	}
	else {   
		fprintf(stderr, "error: unable to read module: %s\n", filename);
	}
	
	return rc;
}

int udf_get(cl_cluster * asc, const char * module, bool print) {

	int 		rc 				= 0;
	char * 		error 			= NULL;
	char 		filename[1024] 	= {0};
	
	sprintf(filename,"%s.lua", module);

	cl_udf_file file = {
		.name = {0},
		.hash = {0},
		.type = 0,
		.content = NULL
	};

	rc = citrusleaf_udf_get(asc, filename, &file, 0, &error);
	if ( rc != 0 ) {
		fprintf(stderr, "error: unable to get module '%s'\n", filename);
		fprintf(stderr, "error: (%d) %s\n", rc, error); 
	} 
	else {
		fprintf(stderr, "info: module downloaded: %s\n", filename);
		if ( print ) {
			fprintf(stderr, ">>\n");
			fprintf(stderr, "%s\n", file.content->value);
			fprintf(stderr, "<<\n");	
		}
	}

	as_val_destroy(&file.content);

	return rc;
}

int udf_remove(cl_cluster * asc, const char * module) {

	int 		rc 				= 0;
	char * 		error 			= NULL;
	char 		filename[1024] 	= {0};
	
	sprintf(filename,"%s.lua", module);

	rc = citrusleaf_udf_remove(asc, filename, &error);
	if ( rc != 0 ) {
		fprintf(stderr, "error: unable to remove module: %s\n", filename);
		fprintf(stderr, "error: (%d) %s\n", rc, error); 
	} 
	else {
		fprintf(stderr, "info: module removed: %s\n", filename);
	}

	return rc;
}

int udf_list(cl_cluster * asc, bool print) {

	int 			rc 			= 0;
	char * 			error 		= NULL;
	cl_udf_file ** 	modules 	= NULL;
	int 			nmodules 	= 0;

	rc = citrusleaf_udf_list(asc, &modules, &nmodules, &error);
	if ( rc != 0) {
		fprintf(stderr, "error: unable to list modules\n");
		fprintf(stderr, "error: (%d) %s\n", rc, error); 
	} 
	else {
		if ( nmodules > 0 ) {
			fprintf(stderr,"info: module list: %d\n", nmodules); 
			if ( print ) {
				fprintf(stderr,">>\n");
			}
			for (int i = 0; i < nmodules; i++) {
				if ( print ) {
					fprintf(stderr,"  [%d] Name: \"%s\", Hash: %s, Type: %d\n", i+1, modules[i]->name, modules[i]->hash, modules[i]->type);
				}
				free(modules[i]);
			}
			if ( print ) {
				fprintf(stderr,"<<\n");
			}
			free(modules);
		}
	}

	return nmodules;
}

void usage(int argc, char *argv[]) {
    fprintf(stderr, "Usage %s:\n", argv[0]);
    fprintf(stderr, "-h host [default 127.0.0.1] \n");
    fprintf(stderr, "-p port [default 3000]\n");
    fprintf(stderr, "-f package_path [./src/lua]\n");
}

int main(int argc, char **argv) {
	config c 		= {0};
	c.host         = "127.0.0.1";
	c.port         = 3000;
	c.package_path = "./src/lua/";

	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:v:")) != -1) {
		switch (optcase) {
			case 'h': c.host         = strdup(optarg);          break;
			case 'p': c.port         = atoi(optarg);            break;
			case 'f': c.package_path = strdup(optarg);          break;
			default:  usage(argc, argv);                      return(-1);
		}
	}

	fprintf(stderr, "configuration:\n");
	fprintf(stderr, "  host: %s\n", c.host);
	fprintf(stderr, "  port: %d\n", c.port);
	fprintf(stderr, "  path: %s\n", c.package_path);
	fprintf(stderr, "\n");

	citrusleaf_init();

	//citrusleaf_set_debug(true);
	
	// create the cluster object - attach
	cl_cluster * asc = citrusleaf_cluster_create();
	if ( !asc ) { 
		fprintf(stderr, "error: could not create cluster\n"); 
		return(-1); 
	}
	if ( 0 != citrusleaf_cluster_add_host(asc, c.host, c.port, 5000) ) {
		fprintf(stderr, "error: could not connect to host %s port %d\n",c.host,c.port);
		free(asc);
		return(-1);
	}

	c.asc = asc;


	// initial count
	int n = udf_list(asc, false);

	// remove these if they exist.
	udf_remove(asc, "register1");
	udf_remove(asc, "register2");
	udf_remove(asc, "register3");

	// did we remove anything?
	int m = udf_list(asc, false);
	if ( n != m ) {
		fprintf(stderr, "info: removed %d modules", n - m);
	}


	// put module
	udf_put(asc, "register1", c.package_path);

	// get module
	udf_get(asc, "register1", false);

	// list modules
	int a = udf_list(asc, false);

	if ( a == m+1 ) {

		// put module
		udf_put(asc, "register2", c.package_path);

		// get module
		udf_get(asc, "register2", false);

		// list modules
		int b = udf_list(asc, false);

		if ( b == a+1 ) {

			// put module
			udf_put(asc, "register3", c.package_path);
			
			// get module
			udf_get(asc, "register3", false);

			// list modules
			int c = udf_list(asc, false);

			if ( c != b+1 ) {
				fprintf(stderr, "error: expected %d modules to be on server.", b+1);
			}

			// remove module
			udf_remove(asc, "register3");
		}
		else {
			fprintf(stderr, "error: expected %d modules to be on server.", a+1);
		}

		// remove module
		udf_remove(asc, "register2");
	}
	else {
		fprintf(stderr, "error: expected %d modules to be on server.", m+1);
	}

	// remove module
	udf_remove(asc, "register1");

	// list modules
	n = udf_list(asc, false);

	if ( m != n ) {
		fprintf(stderr, "error: expected %d modules to be on server, but there were %d", m, n);
	}

	// cleanup
	citrusleaf_cluster_destroy(asc);


	fprintf(stderr, "~ fin ~\n", m);

	return 0;
}
