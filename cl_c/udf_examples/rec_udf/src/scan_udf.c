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
        char *package_name;
        cl_cluster      *asc;
} config;


static config *g_config = NULL;

//INFO not working currently, use fprintf for now
void usage(int argc, char *argv[]) {
    fprintf(stderr, "Usage %s:\n", argv[0]);
    fprintf(stderr, "   -h host [default 127.0.0.1] \n");
    fprintf(stderr, "   -p port [default 3000]\n");
    fprintf(stderr, "   -n namespace [default test]\n");
    fprintf(stderr, "   -s set [default *all*]\n");
    fprintf(stderr, "   -F udf_file [default lua_files/register1.lua]\n");
    fprintf(stderr, "   -f udf_function [default register_1]\n");
    fprintf(stderr, "   -P package_name [default register]\n");
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
	g_config->package_file = "register1";
	g_config->function_name = "register_1";
	g_config->package_name = "register";

	int optcase;
	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:F:f:P:x:r:t:i:j:")) != -1) {
		switch (optcase) {
			case 'h': g_config->host         = strdup(optarg);          break;
			case 'p': g_config->port         = atoi(optarg);            break;
			case 'n': g_config->ns           = strdup(optarg);          break;
			case 's': g_config->set          = strdup(optarg);          break;
			case 'F': g_config->package_file = strdup(optarg);          break;
			case 'f': g_config->function_name = strdup(optarg);          break;
			case 'P': g_config->package_name = strdup(optarg);          break;
			default:  usage(argc, argv);                      return(-1);
		}
	}
	return 0;
}

// Currently the callback only prints the return value, can add more logic here later
int cb(as_val * v, void * u) {
	fprintf(stderr,"%s\n", as_val_tostring(v));
	return 0;
}
int main(int argc, char **argv) {
	// reading parameters
	if (init_configuration(argc,argv) !=0 ) {
		return -1;
	}

	citrusleaf_init();

	// create the cluster object - attach
	cl_cluster *asc = citrusleaf_cluster_create();
	if (!asc) { 
		fprintf(stderr, "could not create cluster\n");
		return(-1); 
	}
	if (0 != citrusleaf_cluster_add_host(asc, g_config->host, g_config->port, g_config->timeout_ms)) {
		fprintf(stderr, "Failed to add host\n");
		free(asc);
		return(-1);
	}
	g_config->asc           = asc;

	// Register package here
	
	// Initialize scan	
	citrusleaf_scan_init();
	as_scan * scan = as_scan_new(g_config->ns, g_config->set);
	as_scan_foreach(scan, g_config->package_file, g_config->function_name, NULL);

	// Execute scan -- still need to add a better API over this 
	as_scan_execute(asc, scan, NULL, cb, true);
	
	// Destroy the scan object
	as_scan_destroy(scan);
	citrusleaf_scan_shutdown();

	citrusleaf_cluster_destroy(asc);
	citrusleaf_shutdown();
}
