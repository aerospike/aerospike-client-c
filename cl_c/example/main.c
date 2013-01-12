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


#include "citrusleaf/citrusleaf.h"
#include "citrusleaf/cl_shm.h"

typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *set;
	
	bool verbose;
	bool follow;
	
	int  timeout_ms;
	
	cl_cluster	*asc;
	
} config;




/* SYNOPSIS */
/* This is an example of getting and setting values using the blocking
   citrusleaf API.
*/

int
do_example(config *c)
{
	int rv;
	
	// Put some test values
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, "example_key");
	cl_bin values[2];
	strcpy(values[0].bin_name, "test_bin_one");
	citrusleaf_object_init_str(&values[0].object, "example_value_one");
	strcpy(values[1].bin_name, "test_bin_two");
	citrusleaf_object_init_int(&values[1].object, 0xDEADBEEF);
	
	// set a non-default write parameter
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = 1000;
	
	if (0 != (rv = citrusleaf_put(c->asc, c->ns, c->set, &o_key, values, 2, &cl_wp))) {
		fprintf(stderr, "citrusleaf put failed: error %d\n",rv);
		return(-1);
	}
	fprintf(stderr, "citrusleaf put succeeded\n");
	
	// Get all the values in this key (enjoy the fine c99 standard)
	cl_bin *cl_v = 0;
	uint32_t generation;
	int 	cl_v_len;
	if (0 != (rv = citrusleaf_get_all(c->asc, c->ns, c->set, &o_key, &cl_v, &cl_v_len, c->timeout_ms, &generation))) {
		fprintf(stderr, "get after put failed, but there should be a key here - %d\n",rv);
		if (cl_v)	free(cl_v);
		return(-1);
	}
	fprintf(stderr, "get all returned %d bins\n",cl_v_len);
	for (int i=0;i<cl_v_len;i++) {
		fprintf(stderr, "%d:  bin %s ",i,cl_v[i].bin_name);
		switch (cl_v[i].object.type) {
			case CL_STR:
				fprintf(stderr, "type string: value %s\n", cl_v[i].object.u.str);
				break;
			case CL_INT:
				fprintf(stderr, "type int: value %"PRId64"\n",cl_v[i].object.u.i64);
				break;
			default:
				fprintf(stderr, "type unknown! (%d)\n",(int)cl_v[i].object.type);
				break;
		}
		// could have done this -- but let's free the objects in the bins later
		// citrusleaf_object_free(&cl_v[i].object);
	}
	if (cl_v)	{
		citrusleaf_bins_free(cl_v, cl_v_len);
		free(cl_v); // only one free for all bins
	}
	fprintf(stderr,"citrusleaf getall succeeded\n");
	
	// Delete the key you just set
	if (0 != (rv = citrusleaf_delete(c->asc, c->ns, c->set, &o_key, 0/*default write params*/))) {
		fprintf(stderr, "citrusleaf delete failed: error %d\n",rv);
		return(-1);
	}
	fprintf(stderr, "citrusleaf delete succeeded\n");
	
	return(0);
}



void usage(void) {
	fprintf(stderr, "Usage example:\n");
	fprintf(stderr, "-h host [default 127.0.0.1] \n");
	fprintf(stderr, "-p port [default 3000]\n");
	fprintf(stderr, "-n namespace [default test]\n");
	fprintf(stderr, "-b bin [default value]\n");
	fprintf(stderr, "-m milliseconds timeout [default 200]\n");
	fprintf(stderr, "-f do not follow cluster [default do follow]\n");
	fprintf(stderr, "-r use shared memory [default false]\n");
	fprintf(stderr, "-v is verbose\n");
}


config g_config;


int
main(int argc, char **argv)
{
	config g_config;
	memset(&g_config, 0, sizeof(g_config));
	
	g_config.host = "127.0.0.1";
	g_config.port = 3000;
	g_config.ns = "test";
	g_config.set = "example_set";
	g_config.verbose = false;
	g_config.follow = true;
	
	bool use_shm = false;
	int	c;
	
	printf("example of the C citrusleaf library\n");
	
	while ((c = getopt(argc, argv, "h:p:n:s:b:m:vr")) != -1)
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
			
		case 'r':
			use_shm = true;
			break;

		default:
			usage();
			return(-1);
			
		}
	}

	fprintf(stderr, "example: host %s port %d ns %s set %s\n",
		g_config.host,g_config.port,g_config.ns,g_config.set);

	if (use_shm) {
		citrusleaf_use_shm(10,788722985);
	}

	// init the unit before creating any clusters
	int rv = citrusleaf_init();
	if(rv!=0) {
		fprintf(stderr,"Citrusleaf init failed\n");
	}
	
	// Create a citrusleaf cluster object for subsequent requests
	g_config.asc = citrusleaf_cluster_create();
	if (!g_config.asc) {
		fprintf(stderr, "could not create cluster, internal error\n");
		return(-1);
	}
	if (g_config.follow == false)
		citrusleaf_cluster_follow(g_config.asc, false);
	
	citrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port, 200 /*timeout*/);

	// Make some example requests against the cluster
	if (0 != do_example(&g_config)) {
		fprintf(stderr, "example failed!\n");
		return(-1);
	}
	fprintf(stderr, "example succeeded!\n");
	citrusleaf_shutdown();
	return(0);
}
