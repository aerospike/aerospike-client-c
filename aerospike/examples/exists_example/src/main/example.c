/*
 *  Citrusleaf Tools
 *  src/exists_example.c - simple key existence test example program
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
#include <getopt.h>

#include "citrusleaf/citrusleaf.h"

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


/* SYNOPSIS
 * This is an example of checking for key existence via the Citrusleaf API.
 */

int
do_example(config *c)
{
	int rv = CITRUSLEAF_OK, rv2;

	// Put some test values
	cl_object o_key;
	citrusleaf_object_init_str(&o_key, "K9");
	cl_bin values[3];
	strcpy(values[0].bin_name, "B1");
	citrusleaf_object_init_str(&values[0].object, "V1");
	strcpy(values[1].bin_name, "B2");
	citrusleaf_object_init_str(&values[1].object, "V2");
	strcpy(values[2].bin_name, "B3");
	citrusleaf_object_init_str(&values[2].object, "V3");

	// Set a non-default write parameter:
	cl_write_parameters cl_wp;
	cl_write_parameters_set_default(&cl_wp);
	cl_wp.unique = true;
	cl_wp.unique_bin = true;
	cl_wp.timeout_ms = 1000;

	/* Does the key exist now? */
	fprintf(stderr, "\nChecking for key existence:  [Expected to fail.]\n\n");

	if (0 != (rv = citrusleaf_exists_key(c->asc, c->ns, c->set, &o_key, 0, 0, c->timeout_ms, 0))) {
		fprintf(stderr, "citrusleaf_exists_key() failed! rf = %d\n", rv);
	} else {
		fprintf(stderr, "citrusleaf_exists_key() succeeded!\n");
	}

	fprintf(stderr, "\nPutting data:\n\n");

	if (0 != (rv = citrusleaf_put(c->asc, c->ns, c->set, &o_key, values, 3, &cl_wp))) {
		fprintf(stderr, "citrusleaf_put() failed!  Error: %d\n", rv);
		return -1;
	}
	fprintf(stderr, "citrusleaf_put() succeeded!\n");

	fprintf(stderr, "\nGetting data:\n\n");

	cl_bin *cl_v = 0;
	uint32_t generation;
	int cl_v_len;
	if (0 != (rv = citrusleaf_get_all(c->asc, c->ns, c->set, &o_key, &cl_v, &cl_v_len, c->timeout_ms, &generation))) {
		fprintf(stderr, "citrusleaf_get_all() failed!  Error: %d\n", rv);
		if (cl_v)
		  free(cl_v);
		return -1;
	}
	fprintf(stderr, "citrusleaf_get_all() succeeded! Num. bins:  %d\n", cl_v_len);

	fprintf(stderr, "\nKey: \"%s\"\n", o_key.u.str);
	for (int i = 0; i < cl_v_len; i++) {
		fprintf(stderr, "  bin[%d]:  \"%s\",  ", i, cl_v[i].bin_name);
		switch (cl_v[i].object.type) {
		  case CL_STR:
			  fprintf(stderr, "Value: \"%s\" (Type: string)\n", cl_v[i].object.u.str);
			  break;
		  case CL_INT:
			  fprintf(stderr, "Value: %"PRId64" (Type: int)\n", cl_v[i].object.u.i64);
			  break;
		  default:
			  fprintf(stderr, "Unknown type! %d\n", (int) cl_v[i].object.type);
			  break;
		}
		citrusleaf_object_free(&cl_v[i].object);
	}

	if (cl_v) {
		citrusleaf_bins_free(cl_v, cl_v_len);
		free(cl_v);
	}

	/* Does the key exist now? */
	fprintf(stderr, "\nChecking for key existence: [Expected to succeed.]\n\n");

	if (0 != (rv = citrusleaf_exists_key(c->asc, c->ns, c->set, &o_key, 0, 0, c->timeout_ms, 0))) {
		fprintf(stderr, "citrusleaf_exists_key() failed! rf = %d\n", rv);
	} else {
		fprintf(stderr, "citrusleaf_exists_key() succeeded!\n");
	}

	fprintf(stderr, "\nDeleting data:\n\n");

	if (0 != (rv = citrusleaf_delete(c->asc, c->ns, c->set, &o_key, 0 /* default write params */))) {
		fprintf(stderr, "citrusleaf_delete() failed!  Error: %d\n", rv);
		return -1;
	}
	fprintf(stderr, "citrusleaf_delete() succeeded!\n");

	/* Does the key exist now? */
	fprintf(stderr, "\nChecking for key existence:  [Expected to fail.]\n\n");

	if (0 != (rv2 = citrusleaf_exists_key(c->asc, c->ns, c->set, &o_key, 0, 0, c->timeout_ms, 0))) {
		fprintf(stderr, "citrusleaf_exists_key() failed! rf = %d\n", rv2);
	} else {
		fprintf(stderr, "citrusleaf_exists_key() succeeded!\n");
	}

	return rv;
}

	
void usage(int argc, char *argv[]) {
	fprintf(stderr, "Usage %s:\n", argv[0]);
	fprintf(stderr, "-h host [default 127.0.0.1] \n");
	fprintf(stderr, "-p port [default 3000]\n");
	fprintf(stderr, "-n namespace [default test]\n");
	fprintf(stderr, "-b bin [default value]\n");
	fprintf(stderr, "-m milliseconds timeout [default 200]\n");
	fprintf(stderr, "-f do not follow cluster [default do follow]\n");
	fprintf(stderr, "-v is verbose\n");
}


config g_config;


int
main(int argc, char *argv[])
{
	config g_config;
	memset(&g_config, 0, sizeof(g_config));

	g_config.host = "127.0.0.1";
	g_config.port = 3000;
	g_config.ns = "test";
	g_config.set = "";
	g_config.verbose = false;
	g_config.follow = true;

	int		c;
	
	fprintf(stderr, "Key existence example using the Citrusleaf C API:\n\n");
	
	while ((c = getopt(argc, argv, "h:p:n:b:m:v")) != -1) 
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
			usage(argc, argv);
			return(-1);
		}
	}

	fprintf(stderr, "%s: host %s port %d ns %s set %s\n",
		argv[0], g_config.host,g_config.port,g_config.ns,g_config.set);

	// init the unit before creating any clusters
	citrusleaf_init();

	// Create a citrusleaf cluster object for subsequent requests
	g_config.asc = citrusleaf_cluster_create();
	if (!g_config.asc) {
		fprintf(stderr, "Could not create cluster, internal error\n");
		return(-1);
	}

	if (g_config.follow == false)
		citrusleaf_cluster_follow(g_config.asc, false);

	citrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port, 100 /*timeout*/);

	// Make calls to the cluster
	if (0 != do_example(&g_config)) {
		fprintf(stderr, "Example failed!\n");
		return(-1);
	}
	fprintf(stderr, "Example succeeded!\n");

	return(0);
}
