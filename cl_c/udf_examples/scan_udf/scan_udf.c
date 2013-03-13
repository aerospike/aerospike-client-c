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
	int	nkeys;
} config;


static config *g_config = NULL;

void usage(int argc, char *argv[]) {
    INFO("Usage %s:", argv[0]);
    INFO("   -h host [default 127.0.0.1] ");
    INFO("   -p port [default 3000]");
    INFO("   -K number of keys [default 25000]");
    INFO("   -n namespace [default test]");
    INFO("   -s set [default *all*]");
    INFO("   -F udf_file [default ../lua_files/scan_udf.lua]");
    INFO("   -f udf_function [default register_1]");
}

int init_configuration (int argc, char *argv[])
{
	g_config = (config *)malloc(sizeof(config));
	memset(g_config, 0, sizeof(g_config));

	g_config->host         	= "127.0.0.1";
	g_config->port         	= 3000;
	g_config->ns           	= "test";
	g_config->set          	= NULL;
	g_config->nkeys	 	= 25000;
	g_config->timeout_ms   = 1000;
	g_config->package_file = "../lua_files/scan_udf.lua";
	g_config->function_name = "do_scan_test";

	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:K:F:f:P:x:r:t:i:j:")) != -1) {
		switch (optcase) {
			case 'h': g_config->host         	= strdup(optarg);          break;
			case 'p': g_config->port         	= atoi(optarg);            break;
			case 'n': g_config->ns           	= strdup(optarg);          break;
			case 's': g_config->set          	= strdup(optarg);          break;
			case 'K': g_config->nkeys          	= atoi(optarg);            break;
			case 'F': g_config->package_file 	= strdup(optarg);          break;
			case 'f': g_config->function_name 	= strdup(optarg);          break;
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
	INFO("Advertiser id = %d", atoi(s));
	free(s);
	as_val_destroy(v);
	return 0;
}
int main(int argc, char **argv) {
	int sz;
	int rc = CITRUSLEAF_OK;
	
	// Response structure for every node
	as_node_response resp;
	memset(&resp, 0, sizeof(as_node_response));

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


	// insert records

	cl_write_parameters wp;
	cl_write_parameters_set_default(&wp);
	wp.timeout_ms = 1000;
	wp.record_ttl = 864000;

	cl_object okey;
	cl_bin bins[6];
	strcpy(bins[0].bin_name, "bid");
	strcpy(bins[1].bin_name, "timestamp");
	strcpy(bins[2].bin_name, "advertiser");
	strcpy(bins[3].bin_name, "campaign");
	strcpy(bins[4].bin_name, "line_item");
	strcpy(bins[5].bin_name, "spend");

	uint32_t ts = 275273225;
	uint32_t et = 0;

	srand(ts);
	// Inserting "nkeys" rows of 6 bins each
	for( int i = 0; i < g_config->nkeys; i++ ) {
		if ( i % 4 == 0 ) {
			et++;
		}
		int nbins = 6;

		uint32_t advertiserId = (rand() % 4) + 1;
		uint32_t campaignId = advertiserId * 10 + (rand() % 4) + 1;
		uint32_t lineItemId = campaignId * 10 + (rand() % 4) + 1;
		uint32_t bidId = lineItemId * 100000 + i;
		uint32_t timestamp = ts + et;
		uint32_t spend = advertiserId + campaignId + lineItemId;

		citrusleaf_object_init_int(&okey, bidId);
		citrusleaf_object_init_int(&bins[0].object, bidId);
		citrusleaf_object_init_int(&bins[1].object, timestamp);
		citrusleaf_object_init_int(&bins[2].object, advertiserId);
		citrusleaf_object_init_int(&bins[3].object, campaignId);
		citrusleaf_object_init_int(&bins[4].object, lineItemId);
		citrusleaf_object_init_int(&bins[5].object, spend);

		rc = citrusleaf_put(asc, g_config->ns, g_config->set, &okey, bins, nbins, &wp);
	}

	INFO("Inserted %d rows", g_config->nkeys);

	// Initialize scan	
	citrusleaf_scan_init();

	// Create job id for scan.
	// This will be useful to monitor your scan transactions
	uint64_t job_id;
	as_scan * scan = as_scan_new(g_config->ns, g_config->set, &job_id );
	INFO("Job Id for this transaction %"PRIu64"", job_id);
	
	as_scan_params params = { 
		.fail_on_cluster_change  = false,
		.priority		= AS_SCAN_PRIORITY_AUTO,
		.pct			= 100
	};


	as_scan_params_init(&scan->params, &params);

	//Initialize the udf to be run on all records
	// This function takes in the filename (not the absolute path and w/o .lua)
	as_scan_foreach(scan, "scan_udf", g_config->function_name, NULL);

	// Execute scan udfs in background
	// Inputs : cluster object, scan object, callback function, arguments to the callback function 
	INFO("\nRunning background scan udf on the entire cluster");
	v = citrusleaf_udf_scan_background(asc, scan);
	
	// This returns a vector of return values, the size of which is the size of the cluster
	sz = cf_vector_size(v);

	for(int i=0; i <= sz; i++) {
		cf_vector_get(v, i, &resp);
		INFO("Udf scan background for node %s returned %d", resp.node_name, resp.node_response);
		// Set the resp back to zero
		memset(&resp, 0, sizeof(as_node_response));
	}
	// Free the result vector
	cf_vector_destroy(v);
	
	// Destroy the scan object
	as_scan_destroy(scan);
	citrusleaf_scan_shutdown();

	citrusleaf_cluster_destroy(asc);
	citrusleaf_shutdown();
}
