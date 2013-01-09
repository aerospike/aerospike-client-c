/*
 * Citrusleaf Tools
 * scan_sproc.c
 *
 * Copyright 2012 by Citrusleaf. All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE. THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fcntl.h>
#include <sys/stat.h>

#include "citrusleaf/citrusleaf.h"

#include "scan_sproc.h"


#define NUM_KEYS 110
static const char BIN_NAME[] = "bin1"; // must match name in sproc_scan_test.lua
static const char BIN_NAME_TO_DELETE[] = "bin2"; 


int do_sproc_scan_test(config *c)
{
	cl_write_parameters cl_wp;

	cl_write_parameters_set_default(&cl_wp);
	cl_wp.timeout_ms = c->timeout_ms;
	cl_wp.record_ttl = 864000;

	cl_object keys[NUM_KEYS];

	// First pre-populate with a bunch of one-bin records.
	for (int k = 0; k < NUM_KEYS; k++) {
		citrusleaf_object_init_int(&keys[k], k);

		cl_bin bins[2];

		strcpy(bins[0].bin_name, BIN_NAME);
		citrusleaf_object_init_int(&bins[0].object, k);
		strcpy(bins[1].bin_name, BIN_NAME_TO_DELETE);
		citrusleaf_object_init_str(&bins[1].object, "deleted if bin1 is divisible by 3");

		int rsp = citrusleaf_put(c->asc, c->ns, c->set, &keys[k], bins, 2,
				&cl_wp);

		if (rsp != CITRUSLEAF_OK) {
			fprintf(stderr, "failed inserting test data, rsp = %d\n", rsp);
			return -1;
		}
	}

	// Invoke the client's sproc-scan method to update all the records.
	uint64_t job_uid = 0;
	cf_vector* vec_p = citrusleaf_sproc_execute_all_nodes(c->asc, c->ns, c->set,
			c->package_name, "do_scan_test", NULL, NULL, NULL, NULL, &job_uid);

	if (! vec_p) {
		fprintf(stderr, "failed to start scan job\n");
		return -1;
	}

	fprintf(stderr, "started scan job %lu\n", job_uid);
	for (unsigned int n = 0; n < cf_vector_size(vec_p); n++) {
		cl_node_response response;

		if (cf_vector_get(vec_p, n, (void *)&response) != 0) {
			fprintf(stderr, "failed reading result vector index %d\n", n);
			continue;
		}

		fprintf(stderr, "node name %s: response code %d\n",
				response.node_name, response.node_response);
	}

	free(vec_p);

	// Wait and see what happened.
	fprintf(stderr, "\n... allowing scan job %lu to happen ...\n", job_uid);
	sleep(10);	// @TODO this is to ensure the job has finished 

	for (int k = 0; k < NUM_KEYS; k++) {
		uint32_t cl_gen;
		cl_bin *rsp_bins = NULL;
		int rsp_n_bins = 0;

		for (int repeat = 0; repeat < 4; repeat++) {
	 		int rsp = citrusleaf_get_all(c->asc, c->ns, c->set, &keys[k], &rsp_bins,
	 				&rsp_n_bins, c->timeout_ms, &cl_gen);

			if (rsp != CITRUSLEAF_OK) {
				fprintf(stderr, "%2ld: failed reading modified data, rsp = %d\n",
						keys[k].u.i64, rsp);
			}
			else if (rsp_n_bins == 0 || ! rsp_bins) {
				fprintf(stderr, "%2ld: no bins\n", keys[k].u.i64);
			}
			else {
				fprintf(stderr, "%2ld:", keys[k].u.i64);

				for (int b = 0; b < rsp_n_bins; b++) {
					cl_type type = rsp_bins[b].object.type;

					fprintf(stderr, " %s [%d]", rsp_bins[b].bin_name, type);

					switch (type) {
					case CL_INT:
						fprintf(stderr, " %ld;", rsp_bins[b].object.u.i64);
						break;
					case CL_STR: {
							size_t val_len = rsp_bins[b].object.sz;
							char val[val_len + 1];

							strncpy(val, rsp_bins[b].object.u.str, val_len);
							val[val_len] = 0;

							fprintf(stderr, " %s;", val);
							break;
						}
					default:
						fprintf(stderr, " (not printing this value type);");
						break;
					}
				}

				fprintf(stderr, "\n");
			}

			if (rsp_bins) {
				citrusleaf_bins_free(rsp_bins, rsp_n_bins);
				free(rsp_bins);
			}
		}
	}

	return 0;
}

int do_sproc_scan_test_no_data(config *c)
{
	// Invoke the client's sproc-scan method to update all the records.
	uint64_t job_uid = 0;
	cf_vector* vec_p = citrusleaf_sproc_execute_all_nodes(c->asc, c->ns, c->set,
			c->package_name, "do_scan_test", NULL, NULL, NULL, NULL, &job_uid);

	if (! vec_p) {
		fprintf(stderr, "failed to start scan job\n");
		return -1;
	}

	fprintf(stderr, "started scan job %lu\n", job_uid);
	for (unsigned int n = 0; n < cf_vector_size(vec_p); n++) {
		cl_node_response response;

		if (cf_vector_get(vec_p, n, (void *)&response) != 0) {
			fprintf(stderr, "failed reading result vector index %d\n", n);
			continue;
		}

		fprintf(stderr, "node name %s: response code %d\n",
				response.node_name, response.node_response);
	}

	free(vec_p);

	return 0;
}



void usage(int argc, char *argv[])
{
	fprintf(stderr, "Usage %s:\n", argv[0]);
	fprintf(stderr, "-h host [default 127.0.0.1] \n");
	fprintf(stderr, "-p port [default 3000]\n");
	fprintf(stderr, "-n namespace [test]\n");
	fprintf(stderr, "-s set [default *all*]\n");
	fprintf(stderr, "-i insert data [default not on]\n");
	fprintf(stderr, "-f package_file [lua_packages/sproc_scan_test.lua]\n");
	fprintf(stderr, "-P package_name [sproc_scan_test] \n");
	fprintf(stderr, "-v is verbose\n");
}

int main(int argc, char **argv)
{
	config c;

	memset(&c, 0, sizeof(c));
	c.host			= "127.0.0.1";
	c.port			= 3000;
	c.ns			= "test";
	c.set			= 0;
	c.timeout_ms	= 1000;
	c.verbose		= true;
	c.package_file	= "../lua_packages/sproc_scan_test.lua";
	c.package_name	= "sproc_scan_test";
	c.insert_data   = true;

	fprintf(stderr, "Starting stored-procedure Scan Test\n");

	int optcase;

	while ((optcase = getopt(argc, argv, "ckmh:p:n:s:P:f:vi")) != -1) {
		switch (optcase) {
		case 'h': c.host			= strdup(optarg);	break;
		case 'p': c.port			= atoi(optarg);		break;
		case 'n': c.ns				= strdup(optarg);	break;
		case 's': c.set				= strdup(optarg);	break;
		case 'v': c.verbose			= true;				break;
		case 'f': c.package_file	= strdup(optarg);	break;
		case 'P': c.package_name	= strdup(optarg);	break;
		case 'i': c.insert_data     = false;			break;
		default: usage(argc, argv);						return -1;
		}
	}

	fprintf(stderr, "Startup: host %s port %d ns %s set %s file %s\n",
			c.host, c.port, c.ns, c.set, c.package_file);

	citrusleaf_init();
//	citrusleaf_set_debug(true);

	// Create the cluster object - attach.
	cl_cluster *asc = citrusleaf_cluster_create();

	if (! asc) {
		fprintf(stderr, "can't create cluster\n");
		return -1;
	}

	if (0 != citrusleaf_cluster_add_host(asc, c.host, c.port, c.timeout_ms)) {
		fprintf(stderr, "can't connect to host %s port %d\n", c.host, c.port);
		return -1;
	}

	c.asc = asc;

	// Register our package.
	fprintf(stderr, "Opening package file %s\n", c.package_file);

	FILE *fptr = fopen(c.package_file, "r");

	if (!fptr) {
		fprintf(stderr, "can't open %s: %s\n", c.package_file, strerror(errno));
		return -1;
	}

	int max_script_len = 1048576;
	char *script_code = malloc(max_script_len);

	if (script_code == NULL) {
		fprintf(stderr, "malloc failed");
		return -1;
	}

	char *script_ptr = script_code;
	int b_read = fread(script_ptr,1,512,fptr);
	int b_tot = 0;

	while (b_read) {
		b_tot		+= b_read;
		script_ptr	+= b_read;
		b_read		= fread(script_ptr, 1, 512, fptr);
	}			

	fclose(fptr);

	if (b_tot > 0) {
		char *err_str = NULL;
		int resp = citrusleaf_sproc_package_set(asc, c.package_name,
				script_code, &err_str, CL_SCRIPT_LANG_LUA);

		if (resp != 0) {
			fprintf(stderr, "can't register package file %s as %s resp = %d\n",
					c.package_file, c.package_name,resp);
			fprintf(stderr, "[%s]\n", err_str);
			free(err_str);
			return -1;
		}

		fprintf(stderr, "successfully registered package file %s as %s\n",
				c.package_file, c.package_name);
	}
	else {
		fprintf(stderr, "can't read package file %s as %s b_tot = %d\n",
				c.package_file, c.package_name, b_tot);
		return -1;
	}

	// Run the test(s).
	if (c.insert_data) {
		fprintf(stderr, "\n*** do_sproc_scan_test started\n");

		if (do_sproc_scan_test(&c)) {
			fprintf(stderr, "*** do_sproc_scan_test failed\n");
			return -1;
		}
		else {
			fprintf(stderr, "*** do_sproc_scan_test succeeded\n");
		}
	} else {
		fprintf(stderr, "\n*** do_sproc_scan_test_no_data started\n");

		if (do_sproc_scan_test_no_data(&c)) {
			fprintf(stderr, "*** do_sproc_scan_test_no_data failed\n");
			return -1;
		}
		else {
			fprintf(stderr, "*** do_sproc_scan_test_no_data succeeded\n");
		}
	}

	citrusleaf_cluster_destroy(asc);

	fprintf(stderr, "\n\nFinished stored-procedure Scan Test\n");
	return 0;
}
