#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>
#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_udf.h"
#include "citrusleaf/cl_udf_scan.h"
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <citrusleaf/cf_hist.h>

#define INFO(fmt, args...) \
    __log_append(stderr,"", fmt, ## args);

#define ERROR(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

#define LOG(fmt, args...) \
    __log_append(stderr,"    ", fmt, ## args);

void __log_append(FILE * f, const char * prefix, const char * fmt, ...) {
    char msg[128] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, 128, fmt, ap);
    va_end(ap);
    fprintf(f, "%s%s\n",prefix,msg);
}

typedef struct config_s {

        char  *host;
        int    port;
        char  *ns;
        char  *set;
        uint32_t timeout_ms;
        char *package_file;
        char *function_name;
        cl_cluster      *asc;
} config;


static config *g_config = NULL;

void usage(int argc, char *argv[]) {
    INFO("Usage %s:", argv[0]);
    INFO("   -h host [default 127.0.0.1] ");
    INFO("   -p port [default 3000]");
    INFO("   -n namespace [default test]");
    INFO("   -s set [default *all*]");
    INFO("   -F udf_file [default lua_files/register1.lua]");
    INFO("   -f udf_function [default register_1]");
}

int init_configuration (int argc, char *argv[])
{
	g_config = (config *)malloc(sizeof(config));
	memset(g_config, 0, sizeof(g_config));

	g_config->host         = "127.0.0.1";
	g_config->port         = 3000;
	g_config->ns           = "test";
	g_config->set          = NULL;
	g_config->timeout_ms   = 1000;
	g_config->package_file = "../lua_files/register1.lua";
	g_config->function_name = "register_1";

	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:F:f:P:x:r:t:i:j:")) != -1) {
		switch (optcase) {
			case 'h': g_config->host         = strdup(optarg);          break;
			case 'p': g_config->port         = atoi(optarg);            break;
			case 'n': g_config->ns           = strdup(optarg);          break;
			case 's': g_config->set          = strdup(optarg);          break;
			case 'F': g_config->package_file = strdup(optarg);          break;
			case 'f': g_config->function_name = strdup(optarg);          break;
			default:  usage(argc, argv);                      return(-1);
		}
	}
	return 0;
}

int register_package() 
{ 
	INFO("Opening package file %s",g_config->package_file);  
	FILE *fptr = fopen(g_config->package_file,"r"); 
	if (!fptr) { 
		LOG("cannot open script file %s : %s",g_config->package_file,strerror(errno));  
		return(-1); 
	} 
	int max_script_len = 1048576; 
	byte *script_code = (byte *)malloc(max_script_len); 
	memset(script_code, 0, max_script_len);
	if (script_code == NULL) { 
		LOG("malloc failed"); return(-1); 
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

	char *err_str = NULL; 
	as_bytes udf_content;
	as_bytes_init(&udf_content, script_code, b_tot, true);
	if (b_tot>0) { 
		int resp = citrusleaf_udf_put(g_config->asc, basename(g_config->package_file), &udf_content, AS_UDF_LUA, &err_str); 
		if (resp!=0) { 
			INFO("unable to register package file %s resp = %d",g_config->package_file, resp); return(-1);
			INFO("%s",err_str); free(err_str);
			free(script_code);
			return(-1);
		}
		INFO("successfully registered package file %s",g_config->package_file); 
	} else {   
		INFO("unable to read package file %s b_tot = %d",g_config->package_file, b_tot); return(-1);    
	}
	free(script_code);
	return 0;
}

// Currently the callback only prints the return value, can add more logic here later
int cb(as_val * v, void * u) {
	char * s = as_val_tostring(v);
	INFO("%s", s);
	free(s);
	as_val_destroy(v);
	return 0;
}
int main(int argc, char **argv) {
	int sz;
	int rc;
	cf_vector * v;
	// reading parameters
	if (init_configuration(argc,argv) !=0 ) {
		return -1;
	}

	citrusleaf_init();

	// create the cluster object - attach
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { 
		INFO("could not create cluster");
		return(-1); 
	}
	if (0 != citrusleaf_cluster_add_host(asc, g_config->host, g_config->port, g_config->timeout_ms)) {
		INFO("Failed to add host");
		free(asc);
		return(-1);
	}
	g_config->asc           = asc;

	// register our package. 
	if (register_package() !=0 ) {
		return -1;
	}

	// Initialize scan	
	citrusleaf_scan_init();
	as_scan * scan = as_scan_new(g_config->ns, g_config->set);
	
	as_scan_params params = { 
		.fail_on_cluster_change  = false,
		.priority		= AS_SCAN_PRIORITY_AUTO,
		.nobindata		= false,
		.pct			= 100
	};

	as_scan_params_init(&scan->params, &params);

	// This function takes in the filename (not the absolute path and w/o .lua)
	as_scan_foreach(scan, "register1", g_config->function_name, NULL);

	// Execute scan udfs in background
	// Inputs : cluster object, scan object, callback function, arguments to the callback function 
	INFO("\nRunning background scan udf on the entire cluster");
	v = citrusleaf_udf_scan_background(asc, scan, cb, NULL);
	
	// This returns a vector of return values, the size of which is the size of the cluster
	sz = cf_vector_size(v);

	for(int i=0; i <= sz; i++) {
		cf_vector_get(v, i, &rc);
		INFO("Udf scan background for node %d returned %d", i, rc);
	}
	// Free the result vector
	cf_vector_destroy(v);


	// Execute normal udfs on a particular node
	// Inputs: cluster object, scan object, node name -- can be found by doing clinfo -h <host> -p <port> on a particular node
	// callback and the arguments to the callback function
	
	// For the test, we get node_name for every node in the cluster and run citrusleaf_udf_scan_node on each.
	INFO("\nRunning scan udf on each node of the cluster");
	int node_count = 0;
	char * node_names;
	cl_cluster_get_node_names(asc, &node_count, &node_names);
	char * node_name = node_names;
	for ( int i=0; i < node_count; i++ ) {
		rc = citrusleaf_udf_scan_node(asc, scan, node_name, cb, NULL);
		INFO("Udf scan for node %s returned %d", node_name, rc);
		node_name += NODE_NAME_SIZE;
	}
	free(node_names);
	node_names = NULL;

	// Execute normal udf for the entire cluster
	// Inputs: cluster object, scan object, callback, arguments to callback
	INFO("\nRunning scan udf on the entire cluster");
	v = citrusleaf_udf_scan_all_nodes(asc, scan, cb, NULL);
	
	// This returns a vector of return values, the size of which is the size of the cluster
	sz = cf_vector_size(v);
	for(int i=0; i <= sz; i++) {
		cf_vector_get(v, i, &rc);
		INFO("Udf scan node %d returned %d", i, rc);
	}
	// Free the result vector
	cf_vector_destroy(v);

	
	// Destroy the scan object
	as_scan_destroy(scan);
	citrusleaf_scan_shutdown();

	citrusleaf_cluster_destroy(asc);
	citrusleaf_shutdown();
}
