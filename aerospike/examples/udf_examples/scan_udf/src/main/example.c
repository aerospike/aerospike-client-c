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
#include <pthread.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>
#include <libgen.h>
#include <citrusleaf/cf_random.h>
#include <citrusleaf/cf_atomic.h>
#include <aerospike/aerospike_scan.h>
#include "example_utils.h"

#ifndef LUA_MODULE_PATH
#define LUA_MODULE_PATH "src/lua"
#endif

typedef struct config_s {

    char  *host;
    int    port;
    char  *ns;
    char  *set;
    uint32_t timeout_ms;
    char *package_file;
    char *function_name;
    int    nkeys;
	aerospike as;
} config;


static config *g_config = NULL;
int g_threads    = 1;

void usage(int argc, char *argv[]) {
    LOG("Usage %s:", argv[0]);
    LOG("   -h host [default 127.0.0.1] ");
    LOG("   -p port [default 3000]");
    LOG("   -K number of keys [default 25000]");
    LOG("   -n namespace [default test]");
    LOG("   -s set [default *all*]");
    LOG("   -F udf_file [default ../lua_files/scan_udf.lua]");
    LOG("   -f udf_function [default register_1]");
    LOG("   -t number of parallel threads [default 10]");
}

int init_configuration (int argc, char *argv[])
{
    g_config = (config *)malloc(sizeof(config));
    memset(g_config, 0, sizeof(g_config));

    g_config->host          = "127.0.0.1";
    g_config->port          = 3000;
    g_config->ns            = "test";
    g_config->set           = NULL;
    g_config->nkeys         = 25000;
    g_config->timeout_ms    = 1000;
    g_config->package_file  = LUA_MODULE_PATH"/scan_udf.lua";
    g_config->function_name = "do_scan_test";

    int optcase;
    while ((optcase = getopt(argc, argv, "ckmh:p:n:s:K:F:f:P:x:r:t:i:j:")) != -1) {
        switch (optcase) {
            case 'h': g_config->host            = strdup(optarg);          break;
            case 'p': g_config->port            = atoi(optarg);            break;
            case 'n': g_config->ns              = strdup(optarg);          break;
            case 's': g_config->set             = strdup(optarg);          break;
            case 'K': g_config->nkeys           = atoi(optarg);            break;
            case 'F': g_config->package_file    = strdup(optarg);          break;
            case 'f': g_config->function_name   = strdup(optarg);          break;
            case 't': g_threads                 = atoi(optarg);            break;
            default:  usage(argc, argv);                      return(-1);
        }
    }
    return 0;
}

int register_package() 
{ 
    LOG("Opening package file %s",g_config->package_file);  
    FILE *fptr = fopen(g_config->package_file,"r"); 
    if (!fptr) { 
        LOG("cannot open script file %s : %s",g_config->package_file,strerror(errno));  
        return(-1); 
    } 
    int max_script_len = 1048576; 
    unsigned char *script_code = (unsigned char *)malloc(max_script_len); 
    memset(script_code, 0, max_script_len);
    if (script_code == NULL) { 
        LOG("malloc failed"); return(-1); 
    }     

    unsigned char *script_ptr = script_code; 
    int b_read = fread(script_ptr,1,512,fptr); 
    int b_tot = 0; 
    while (b_read) { 
        b_tot      += b_read; 
        script_ptr += b_read; 
        b_read      = fread(script_ptr,1,512,fptr); 
    }                        
    fclose(fptr); 

	as_error err;
    as_bytes udf_content;
    as_bytes_init(&udf_content, script_code, b_tot, true);
    if (b_tot>0) { 
        int resp = aerospike_udf_put(&g_config->as, &err, NULL, basename(g_config->package_file), AS_UDF_TYPE_LUA, &udf_content); 
        if (resp!=0) { 
            LOG("unable to register package file %s resp = %d",g_config->package_file, resp); return(-1);
            free(script_code);
            return(-1);
        }
        LOG("successfully registered package file %s",g_config->package_file); 
    } else {   
        LOG("unable to read package file %s b_tot = %d",g_config->package_file, b_tot); return(-1);    
    }
    free(script_code);
    return 0;
}

// Currently the callback only prints the return value, can add more logic here later
int cb(as_val * v, void * u) {
    char * s = as_val_tostring(v);
    LOG("Advertiser id = %d", atoi(s));
    free(s);
    as_val_destroy(v);
    return 0;
}

static int run_test2() {
    
	as_error err;
	as_error_reset(&err);

	as_scan * scan = as_scan_new(g_config->ns, g_config->set);
	as_scan_apply(scan, "scan_udf", g_config->function_name, NULL);

	uint64_t scan_id = cf_get_rand64();
	extern cf_atomic32 g_initialized;
	g_initialized = false;

	as_status udf_rc = aerospike_scan_background(&g_config->as, &err, NULL, scan, &scan_id);
	
    // Destroy the scan object
    as_scan_destroy(scan);
    return udf_rc;
}

static void *run_test(void *o) {
    // For each scan is unsupported right now 
    // run_test1();
    run_test2();
    return NULL;
}

int main(int argc, char **argv) {
	init_configuration(argc, argv);	
	
	// Start with default configuration.
	as_config cfg;
	as_config_init(&cfg);

	// Must provide host and port. Example must have called example_get_opts()!
	cfg.hosts[0].addr = g_config->host;
	cfg.hosts[0].port = g_config->port;

	as_error err;

	// Connect to the Aerospike database cluster. Assume this is the first thing
	// done after calling example_get_opts(), so it's ok to exit on failure.
	if (aerospike_connect(aerospike_init(&g_config->as, &cfg), &err) != AEROSPIKE_OK) {
		LOG("aerospike_connect() returned %d - %s", err.code, err.message);
		aerospike_destroy(&g_config->as);
		exit(-1);
	}
	int rc;

    // register our package. 
    if (register_package() !=0 ) {
        return -1;
    }


    // insert records
    as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.timeout = 1000;

	as_record rec;
    as_key okey;
    as_record_init(&rec, 6);

    uint32_t ts = 275273225;
    uint32_t et = 0;

    LOG("Inserting %d rows....", g_config->nkeys);

    srand(ts);
    // Inserting "nkeys" rows of 6 bins each
    for( int i = 0; i < g_config->nkeys; i++ ) {
        if ( i % 4 == 0 ) {
            et++;
        }

        uint32_t advertiserId = (rand() % 4) + 1;
        uint32_t campaignId = advertiserId * 10 + (rand() % 4) + 1;
        uint32_t lineItemId = campaignId * 10 + (rand() % 4) + 1;
        uint32_t bidId = lineItemId * 100000 + i;
        uint32_t timestamp = ts + et;
        uint32_t spend = advertiserId + campaignId + lineItemId;

		as_key_init_int64(&okey, g_config->ns, g_config->set, bidId);
        as_record_set_int64(&rec, "bid", bidId);
        as_record_set_int64(&rec, "timestamp", timestamp);
        as_record_set_int64(&rec, "advertiser", advertiserId);
        as_record_set_int64(&rec, "campaign", campaignId);
        as_record_set_int64(&rec, "line_item", lineItemId);
        as_record_set_int64(&rec, "spend", spend);
		rc = aerospike_key_put(&g_config->as, &err, NULL, &okey, &rec);
    }

    LOG("Complete! Inserted %d rows", g_config->nkeys);

    pthread_t slaps[g_threads];
    for (int j = 0; j < g_threads; j++) {
        if (pthread_create(&slaps[j], 0, run_test, NULL)) {
            LOG("[WARNING]: Thread Create Failed\n");
        }
    }
    for (int j = 0; j < g_threads; j++) {
        pthread_join(slaps[j], (void *)&rc);
    }
	example_cleanup(&g_config->as);
}
