/*
 *  Citrusleaf Tools
 *  src/ascli.c - command-line interface
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
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
#include <getopt.h>

#include <event2/event.h>
#include <event2/dns.h>

#include "citrusleaf_event2/ev2citrusleaf.h"

typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *set;
	
	bool verbose;
	bool follow;
	
	int  timeout_ms;
	
	ev2citrusleaf_object  o_key;
		
	ev2citrusleaf_cluster	*asc;
	
	int	return_value;   // return value from the test
	
	uint32_t	blob_size;
	uint8_t	*blob;

	struct event_base *base;
	
} config;

config g_config;


void
test_terminate(int r)
{
	g_config.return_value = r;
	struct timeval x = { 0, 0 };
	if (g_config.base) event_base_loopexit(g_config.base, &x);
}


void
example_phase_three(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	config *c = (config *) udata;
	
	fprintf(stderr, "example2 phase 3 get - received\n");
	
	if (return_value != 0) {
		fprintf(stderr, "example has FAILED? stage 5 return value %d\n",return_value);
		test_terminate(return_value);
		return;
	}

	// validate the request from phase II
	if (n_bins != 4) {
		fprintf(stderr, "Get all returned wrong number of bins: is %d should be 4\n",n_bins);
		test_terminate(return_value);
		return;
	}
	
	fprintf(stderr, "get all returned %d bins:\n",n_bins);
	bool ok1, ok2, ok3, ok4;
	ok1 = ok2 = ok3 = ok4 = false;
	
	for (int i=0;i<n_bins;i++) {
		if (strcmp(bins[i].bin_name, "test_bin_one") == 0) {
			if (bins[i].object.type != CL_STR) {
				fprintf(stderr, "bin one has wrong type, should be string, FAIL\n");
				c->return_value = -1;
				break;
			}
			if (strcmp( bins[i].object.u.str, "example_value_one") != 0) {
				fprintf(stderr, "bin one has wrong value\n");
				c->return_value = -1;
				break;
			}
			ok1 = true;
		}
		else if (strcmp(bins[i].bin_name, "test_bin_two") == 0) {
			if (bins[i].object.type != CL_INT) {
				fprintf(stderr, "bin two has wrong type, should be int, FAIL\n");
				c->return_value = -1;
				break;
			}
			if (bins[i].object.u.i64 != 0xDEADBEEF) {
				fprintf(stderr, "bin two has wrong value, FAIL\n");
				c->return_value = -1;
				break;
			}
			ok2 = true;
		}
		else if (strcmp(bins[i].bin_name, "test_bin_three") == 0) {
			if (bins[i].object.type != CL_INT) {
				fprintf(stderr, "bin three has wrong type, should be int, FAIL\n");
				c->return_value = -1;
				break;
			}
			if (bins[i].object.u.i64 != 0xDEADBEEF12341234) {
				fprintf(stderr, "bin three has wrong value, FAIL\n");
				c->return_value = -1;
				break;
			}
			ok3 = true;
		}
		else if (strcmp(bins[i].bin_name, "test_bin_four") == 0) {
			if (bins[i].object.type != CL_BLOB) {
				fprintf(stderr, "bin four has wrong type, should be blob, FAIL\n");
				c->return_value = -1;
				break;
			}
			if (bins[i].object.size != 11000) {
				fprintf(stderr, "bin four has wrong size, should be 11000, FAIL\n");
				c->return_value = -1;
				break;
			}
			uint8_t *blob = bins[i].object.u.blob;
			for (uint i=0;i<11000;i++) {
				if (blob[i] != (i & 0xFF)) {
					fprintf(stderr, "bin four has wrong value at offset %d, FAIL\n",i);
				}
				c->return_value = -1;
				break;
			}
			ok4 = true;
		}
	}
	
	if (bins)		ev2citrusleaf_bins_free(bins, n_bins);
	
	if (ok1 && ok2 && ok3 && ok4) {
		fprintf(stderr,"citrusleaf getall succeeded\n");
		test_terminate(0);
	}
	else {
		fprintf(stderr, "citrusleaf getall FAILED: an expected bin was not received\n");
		test_terminate(-1);
	}
	
	

}

void
example_phase_two(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	config *c = (config *) udata;

	if (return_value != EV2CITRUSLEAF_OK) {
		fprintf(stderr, "put failed: return code %d\n",return_value);
		test_terminate(return_value);
		return;
	}
	
	if (bins)		ev2citrusleaf_bins_free(bins, n_bins);

	// Get all the values in this key (enjoy the fine c99 standard)
	if (0 != ev2citrusleaf_get_all(c->asc, c->ns, c->set, &c->o_key, c->timeout_ms,
				example_phase_three, c, c->base)) {
		fprintf(stderr, "get after put could not dispatch\n");
		test_terminate(-1);
		return;
	}
	fprintf(stderr, "get all dispatched\n");

}

void
example_phase_one(config *c)
{
	// Set up the key, used in all phases ---
	// be a little careful here, because the o_key will contain a pointer
	// to strings
	fprintf(stderr, "using key: example2_key\n");
	ev2citrusleaf_object_init_str(&c->o_key, "example2_key");
	
	ev2citrusleaf_bin values[4];
	strcpy(values[0].bin_name, "test_bin_one");
	ev2citrusleaf_object_init_str(&values[0].object, "example_value_one");
	strcpy(values[1].bin_name, "test_bin_two");
	ev2citrusleaf_object_init_int(&values[1].object, 0xDEADBEEF);
	// warning! the integer here is signed, so the C code will work properly
	// abusing the sign bit this way, but another language will see the value
	// as negative.
	strcpy(values[2].bin_name, "test_bin_three");
	ev2citrusleaf_object_init_int(&values[2].object, 0xDEADBEEF12341234);
	strcpy(values[3].bin_name, "test_bin_four");
	uint8_t blob[11000];
	for (int i=0;i<sizeof(blob);i++)
		blob[i] = i;
	ev2citrusleaf_object_init_blob(&values[3].object, blob, sizeof(blob) );
	
	ev2citrusleaf_write_parameters wparam;
	ev2citrusleaf_write_parameters_init(&wparam);
	
	int rv = ev2citrusleaf_put(c->asc, c->ns, c->set, &c->o_key, values, 4, &wparam,c->timeout_ms, example_phase_two, c, c->base);
	if (rv != 0) {
		fprintf(stderr, "citrusleaf put failed: error code %d\n",rv);
		test_terminate(rv);
		return;
	}
	fprintf(stderr, "citrusleaf put dispatched\n");
	
}



void usage(void) {
	fprintf(stderr, "Usage key_c:\n");
	fprintf(stderr, "-h host [default 127.0.0.1] \n");
	fprintf(stderr, "-p port [default 3000]\n");
	fprintf(stderr, "-n namespace [default test]\n");
	fprintf(stderr, "-s set [default ""]\n");
	fprintf(stderr, "-b bin [default value]\n");
	fprintf(stderr, "-m milliseconds timeout [default 200]\n");
	fprintf(stderr, "-v is verbose\n");
}





int
main(int argc, char **argv)
{
	memset(&g_config, 0, sizeof(g_config));
	
	g_config.host = "127.0.0.1";
	g_config.port = 3000;
	g_config.ns = "test";
	g_config.set = "";
	g_config.verbose = false;
	g_config.follow = true;
	g_config.return_value = -1;
	g_config.base = 0;
	g_config.timeout_ms = 200;
	
	int		c;
	
	printf("example of the C libevent citrusleaf library\n");
	
	while ((c = getopt(argc, argv, "h:p:n:s:m:v")) != -1) 
	{
		switch (c)
		{
		case 'h':
			g_config.host = strdup(optarg);
			break;
		
		case 'p':
			g_config.port = atoi(optarg);
			break;
		
		case 'n':
			g_config.ns = strdup(optarg);
			break;
			
		case 's':
			g_config.set = strdup(optarg);
			break;

		case 'm':
			g_config.timeout_ms = atoi(optarg);
			break;
			
		case 'v':
			g_config.verbose = true;
			break;

		case 'f':
			g_config.follow = false;
			break;
			
		default:
			usage();
			return(-1);
			
		}
	}

	fprintf(stderr, "example: host %s port %d ns %s set %s\n",
		g_config.host,g_config.port,g_config.ns,g_config.set);

    g_config.base = event_base_new();
	
	ev2citrusleaf_init(0);    // initialize citrusleaf
	
	// Create a citrusleaf cluster object for subsequent requests
	g_config.asc = ev2citrusleaf_cluster_create(g_config.base);
	if (!g_config.asc) {
		fprintf(stderr, "could not create cluster, internal error\n");
		return(-1);
	}
	
	ev2citrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port);
	
	// Start the train of example events
	example_phase_one(&g_config);
	
	// Burrow into the libevent event loop
	event_base_dispatch(g_config.base);

	ev2citrusleaf_shutdown(false);
	
	if (g_config.return_value != 0)
		fprintf(stderr, "test complete: FAILED return value %d\n",g_config.return_value);
	else
		fprintf(stderr, "test complete: SUCCESS\n");
		
	
	return(0);
}
