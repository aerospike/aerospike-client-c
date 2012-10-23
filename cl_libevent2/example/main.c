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
#include <pthread.h>

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
	
	struct event_base *base;
	struct evdns_base *dns_base;
	
	volatile int	return_value;   // return value from the test
	
	uint32_t	blob_size;
	uint8_t	*blob;
	
} config;


config g_config;

// one of the good ways to make this cast work
typedef void * pthread_fn(void *);

#define BLOB_SIZE ((1024 * 6) + 3)

void
blob_set( uint8_t *blob, uint32_t blob_size)
{
	for (int i = 0; i < blob_size ; i++) {
		blob[i] = i % 0xFF;
	}
}

int
blob_check( uint8_t *blob, uint32_t blob_size)
{
	for (int i = 0; i < blob_size ; i++) {
		if (blob[i] != i % 0xFF) {
			fprintf(stderr, " VALIDATION ERROR IN BLOB: byte %u should be %u is %u\n",
				i,blob[i],i % 0xFF);
			return(-1);
		}
	}
	return(0);
}

void
test_terminate(int r)
{
	g_config.return_value = r;
	struct timeval x = { 0, 0 };
	event_base_loopexit(g_config.base, &x);
	g_config.base = 0;
}

void
example_phase_eight(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	fprintf(stderr, "example phase 8 received\n");
	
	if (return_value != EV2CITRUSLEAF_FAIL_GENERATION) {
		fprintf(stderr, "example has FAILED? stage 8 return value %d should be %d\n",
			return_value, EV2CITRUSLEAF_FAIL_GENERATION);
		test_terminate(-1);
		return;
	}

	fprintf(stderr, " THAT IS ALL! SUCCESS!\n");	
	
	if (bins)		ev2citrusleaf_bins_free(bins, n_bins);
	
	// signals success
	test_terminate(1);
}



void
example_phase_seven(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	fprintf(stderr, "example phase 7 received\n");
	
	if (return_value != 0) {
		fprintf(stderr, "example has FAILED? stage 7 return value %d\n",return_value);
		test_terminate(-1);
		return;
	}

	// Validate that the get returned the right data
	if (n_bins != 1) {
		fprintf(stderr, "phase 7: number of bins is wrong, should be 1 is %d\n",n_bins);
		test_terminate(-1);
		return;
	}
	if (strcmp(bins[0].bin_name,"test_bin_blob") != 0) {
		fprintf(stderr, "phase 7: name of bin returned is wrong, should be test_bin_blob, is %s\n",bins[0].bin_name);
		test_terminate(-1);
		return;
	}
	if (bins[0].object.type != CL_BLOB) {
		fprintf(stderr, "phase 7: get returned wrong type, should be blob, is  %d\n",(int) bins[0].object.type);
		test_terminate(-1);
		return;
	}
	if (bins[0].object.size != BLOB_SIZE) {
		fprintf(stderr, "phase 6: get returned wrong size, should be %d, is %d\n", BLOB_SIZE, (int) bins[0].object.size);
		test_terminate(-1);
		return;
	}
	if (0 != blob_check( bins[0].object.u.blob, BLOB_SIZE) ) {
		test_terminate(-1);
		return;
	}

	if (bins)		ev2citrusleaf_bins_free(bins, n_bins);

	config *c = (config *) udata;
	
	// Do a write with the wrong generation count, make sure it fails
	ev2citrusleaf_bin values[1];
	strcpy(values[0].bin_name, "test_bin_bleb");
	ev2citrusleaf_object_init_blob(&values[0].object, c->blob, c->blob_size);
	
	ev2citrusleaf_write_parameters wparam;
	ev2citrusleaf_write_parameters_init(&wparam);
	wparam.use_generation = true;
	wparam.generation = generation - 1; // one too small!
	
	if (0 != ev2citrusleaf_put(c->asc, c->ns, c->set, &c->o_key, values, 1, 
		&wparam, c->timeout_ms, example_phase_eight, c, c->base)) {
	
		fprintf(stderr, "citrusleaf put could not dispatch - phase 4\n");
		test_terminate(-1);
		return;
	}
	
}

void
example_phase_six(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	config *c = (config *) udata;
	
	fprintf(stderr, "example phase 6 received\n");
	
	if (return_value != 0) { 
		fprintf(stderr, "example has FAILED? stage 6 return value %d\n",return_value);
		test_terminate(-1);
		return;
	}
	
	// Validate that the get returned the right data
	if (n_bins != 1) {
		fprintf(stderr, "phase 6: number of bins is wrong, should be 1 is %d\n",n_bins);
		test_terminate(-1);
		return;
	}
	if (strcmp(bins[0].bin_name,"test_bin_blob") != 0) {
		fprintf(stderr, "phase 6: name of bin returned is wrong, should be test-bin_blob is %s\n",bins[0].bin_name);
		test_terminate(-1);
		return;
	}
	if (bins[0].object.type != CL_BLOB) {
		fprintf(stderr, "phase 6: get returned wrong type, should be blob, is  %d\n",(int) bins[0].object.type);
		test_terminate(-1);
		return;
	}
	if (bins[0].object.size != BLOB_SIZE) {
		fprintf(stderr, "phase 6: get returned wrong type, should be blob, is  %d\n",(int) bins[0].object.type);
		test_terminate(-1);
		return;
	}
	if (0 != blob_check( bins[0].object.u.blob, BLOB_SIZE) ) {
		test_terminate(-1);
		return;
	}

	if (bins)		ev2citrusleaf_bins_free(bins, n_bins);
	
	ev2citrusleaf_operation ops[3];
	
	strcpy(ops[0].bin_name, "test_bin_zulu");
	ops[0].op = CL_OP_WRITE;
	ev2citrusleaf_object_init_str(&ops[0].object, "yodel!yodel!");
	
	strcpy(ops[1].bin_name, "test_bin_two");  // an overwrite!
	ops[1].op = CL_OP_WRITE; 
	ev2citrusleaf_object_init_int(&ops[1].object, 2);
	
	strcpy(ops[2].bin_name, "test_bin_blob");
	ops[2].op = CL_OP_READ;	

	ev2citrusleaf_write_parameters wparam;
	ev2citrusleaf_write_parameters_init(&wparam);
	wparam.use_generation = true;
	wparam.generation = generation;
	
	fprintf(stderr, "phase 6 - sending generation %d\n",generation);
	
	if (0 != ev2citrusleaf_operate(c->asc, c->ns, c->set, &c->o_key, ops, 3, &wparam, c->timeout_ms, example_phase_seven, c, c->base)) {
		fprintf(stderr, "citrusleaf operate could not dispatch - phase 6\n");
		g_config.return_value = -1;
		return;
	}
	fprintf(stderr, "citrusleaf operate dispatched - phase 6\n");
	
}




void
example_phase_five(int return_value, ev2citrusleaf_bin *bins_ignore, int n_bins_ignore, uint32_t generation,void *udata)
{
	config *c = (config *) udata;

	fprintf(stderr, "example phase 5 received\n");
	
	if (return_value != 0) {
		fprintf(stderr, "example has FAILED? stage 5 return value %d\n",return_value);
		test_terminate(-1);
		return;
	}
	
	if (bins_ignore)		ev2citrusleaf_bins_free(bins_ignore, n_bins_ignore);
	
	const char *bins[1] = { "test_bin_blob" }; 
	
	if (0 != ev2citrusleaf_get(c->asc, c->ns, c->set, &c->o_key, bins, 1, c->timeout_ms, example_phase_six, c, c->base)) {
		fprintf(stderr, "citrusleaf put could not dispatch - phase 5\n");
		test_terminate(-1);
		return;
	}
	fprintf(stderr, "citrusleaf get dispatched - phase 5\n");
	
}

/* SYNOPSIS */
/* This is an example of getting and setting values using the asynchronous
**	libevent-oriented asynchronous API.
**
** The example has several phases: phase I puts some values,
** phase II gets them back out
** phase III deletes the key.
** Phase IV checks the return code on the delete
**
** As the code is async, there is a different function for each one.
*/

void
example_phase_four(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	config *c = (config *) udata;
	
	fprintf(stderr, "example phase 4 received\n");
	
	if (return_value != 0) {
		fprintf(stderr, "example has FAILED! stage 4 return value %d\n",return_value);
		test_terminate(-1);
		return;
	}

	if (bins)		ev2citrusleaf_bins_free(bins, n_bins);

	// Try doing a put with a large blob
	c->blob = malloc( BLOB_SIZE );
	c->blob_size = BLOB_SIZE;
	blob_set(c->blob, c->blob_size);
	
	ev2citrusleaf_bin values[1];
	strcpy(values[0].bin_name, "test_bin_blob");
	ev2citrusleaf_object_init_blob(&values[0].object, c->blob, c->blob_size);
	
	ev2citrusleaf_write_parameters wparam;
	ev2citrusleaf_write_parameters_init(&wparam);
	
	if (0 != ev2citrusleaf_put(c->asc, c->ns, c->set, &c->o_key, values, 1, 
		&wparam, c->timeout_ms, example_phase_five, c, c->base)) {
	
		fprintf(stderr, "citrusleaf put could not dispatch - phase 4\n");
		test_terminate(-1);
		return;
	}
	fprintf(stderr, "citrusleaf put dispatched - phase 4\n");
	
}


void
example_phase_three(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	config *c = (config *) udata;
	
	fprintf(stderr, "example phase 3 received\n");
	
	if (return_value != 0) {
		fprintf(stderr, "example has FAILED? stage 3 return value %d\n",return_value);
		test_terminate(-1);
		return;
	}

	// validate the request from phase II
	fprintf(stderr, "get all returned %d bins:\n",n_bins);
	for (int i=0;i<n_bins;i++) {
		fprintf(stderr, "%d:  bin %s ",i,bins[i].bin_name);
		switch (bins[i].object.type) {
			case CL_STR:
				fprintf(stderr, "type string: value %s\n", bins[i].object.u.str);
				break;
			case CL_INT:
				fprintf(stderr, "type int: value %"PRId64"\n",bins[i].object.u.i64);
				break;
			default:
				fprintf(stderr, "type unknown! (%d)\n",(int)bins[i].object.type);
				break;
		}
	}

	if (bins)		ev2citrusleaf_bins_free(bins, n_bins);
	fprintf(stderr,"citrusleaf getall succeeded\n");

	// Delete the key you just set
	ev2citrusleaf_write_parameters wparam;
	ev2citrusleaf_write_parameters_init(&wparam);
	if (0 != ev2citrusleaf_delete(c->asc, c->ns, c->set, &c->o_key, &wparam,
			c->timeout_ms, example_phase_four, c, c->base)) {
	
		fprintf(stderr, "citrusleaf delete failed\n");
		c->return_value = -1;
		
	}
	else
		fprintf(stderr, "citrusleaf delete dispatched\n");

}

void
example_phase_two(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	config *c = (config *) udata;

	if (return_value != EV2CITRUSLEAF_OK) {
		fprintf(stderr, "put failed: return code %d\n",return_value);
		c->return_value = return_value;
		test_terminate(-1);
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
	ev2citrusleaf_object_init_str(&c->o_key, "example_key");
	
	ev2citrusleaf_bin values[2];
	strcpy(values[0].bin_name, "test_bin_one");
	ev2citrusleaf_object_init_str(&values[0].object, "example_value_one");
	strcpy(values[1].bin_name, "test_bin_two");
	ev2citrusleaf_object_init_int(&values[1].object, 0xDEADBEEF);
	
	ev2citrusleaf_write_parameters wparam;
	ev2citrusleaf_write_parameters_init(&wparam);
	
	if (0 != ev2citrusleaf_put(c->asc, c->ns, c->set, &c->o_key, values, 2, &wparam, c->timeout_ms, example_phase_two, c, c->base)) {
		fprintf(stderr, "citrusleaf put could not dispatch\n");
		test_terminate(-1);
		return;
	}
	fprintf(stderr, "citrusleaf put dispatched\n");
	
}


void
example_info_fn(int return_value, char *response, size_t response_len, void *udata)
{
//	fprintf(stderr, "example info return: rv %d response len %zd response %s\n",return_value,response_len, response);
	fprintf(stderr, "example info return: rv %d response len %zd\n",return_value,response_len);
	if (response) free(response);
	
}

void usage(void) {
	fprintf(stderr, "Usage key_c:\n");
	fprintf(stderr, "-h host [default 127.0.0.1] \n");
	fprintf(stderr, "-p port [default 3000]\n");
	fprintf(stderr, "-n namespace [default test]\n");
	fprintf(stderr, "-b bin [default value]\n");
	fprintf(stderr, "-m milliseconds timeout [default 200]\n");
	fprintf(stderr, "-f do not follow cluster [default do follow]\n");
	fprintf(stderr, "-v is verbose\n");
}


int
main(int argc, char **argv)
{
	memset(&g_config, 0, sizeof(g_config));
	
	g_config.host = "127.0.0.1";
	g_config.port = 3000;
	g_config.ns = "test";
	g_config.set = "example_set";
	g_config.verbose = false;
	g_config.follow = true;
	g_config.timeout_ms = 200;
	g_config.base = 0;
	g_config.dns_base = 0;
	
	int		c;
	
	printf("example of the C libevent2 citrusleaf library\n");
	
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
			usage();
			return(-1);
			
		}
	}

	fprintf(stderr, "example: host %s port %d ns %s set %s\n",
		g_config.host,g_config.port,g_config.ns,g_config.set);

	// Use default client logging, but set a filter.
	cf_set_log_level(CF_WARN);

	g_config.base = event_base_new();			// initialize the libevent system
	g_config.dns_base = evdns_base_new(g_config.base, 1);
	ev2citrusleaf_init(0);    // initialize citrusleaf
	
	
	// Create a citrusleaf cluster object for subsequent requests
	g_config.asc = ev2citrusleaf_cluster_create();
	if (!g_config.asc) {
		fprintf(stderr, "could not create cluster, internal error\n");
		return(-1);
	}
	if (g_config.follow == false)
		ev2citrusleaf_cluster_follow(g_config.asc, false);
	
	ev2citrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port);
	
	// complexity: we won't start doing all our node validation until a thread is sunk in event_dispatch.
	// so start a completecly different thread for event_dispatch. It'll probably like being its own thread --
	// and that leaves this thread as a good "monitor" to clean the process on error.
	//
	// THe more normal way is to simply call event dispatch from this thread
	pthread_t event_thread;
	pthread_create(&event_thread, 0, (pthread_fn *) event_base_dispatch, g_config.base);
	
	// Up to the application: wait to see if this cluster has good nodes, or just
	// start using?
	int node_count;
	int tries = 0;
	do {
		node_count = ev2citrusleaf_cluster_get_active_node_count(g_config.asc);
		if (node_count > 0)		break;
		usleep( 50 * 1000 );
		tries++;
	} while ( tries < 20);
	
	if (tries == 20) {
		fprintf(stderr, "example: could not connect to cluster, configuration bad?\n");
		ev2citrusleaf_cluster_destroy(g_config.asc, 0);
		return(-1);
	}


	// info test
	fprintf(stderr, "starting info test\n");
	ev2citrusleaf_info(g_config.base, g_config.dns_base,
		g_config.host, g_config.port, 0, g_config.timeout_ms, example_info_fn, 0);
	
	// Start the train of example stuff
	example_phase_one(&g_config);
	
	// join on the event thread
	void *value_ptr;
	pthread_join(event_thread, &value_ptr); 
	
	if (g_config.return_value != 1)
		fprintf(stderr, "TEST FAILED!\n");
	else
		fprintf(stderr, "TEST SUCCESS!\n");

	if (g_config.blob)	free(g_config.blob);
	
	ev2citrusleaf_shutdown(true);
	
	return(0);
}
