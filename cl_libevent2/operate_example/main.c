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
example_phase_three(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, uint32_t expiration, void *udata)
{
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
	test_terminate(1);
}

void
example_phase_two(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, uint32_t expiration, void *udata)
{
	config *c = (config *) udata;

	fprintf(stderr, "phase two started\n");
	if (return_value != EV2CITRUSLEAF_OK) {
		fprintf(stderr, "put failed: return code %d\n",return_value);
		c->return_value = return_value;
		test_terminate(-1);
		return;
	}

	if (0 != ev2citrusleaf_get_all(c->asc, c->ns, c->set, &c->o_key, c->timeout_ms, example_phase_three, c, c->base)) {
		fprintf(stderr, "citrusleaf put could not dispatch - phase three\n");
		test_terminate(-1);
		return;
	}
	fprintf(stderr, "citrusleaf get dispatched - phase two\n");
}

void
example_phase_one(int return_value, ev2citrusleaf_bin *bins, int n_bins, uint32_t generation, uint32_t expiration, void *udata)
{
	fprintf(stderr, "citrusleaf phase one\n");

	config *c = (config *) udata;
	if (return_value != EV2CITRUSLEAF_OK) {
		fprintf(stderr, "put failed: return code %d\n",return_value);
		c->return_value = return_value;
		test_terminate(-1);
		return;
	}

	ev2citrusleaf_operation ops[1];

	strcpy(ops[0].bin_name, "test_bin_two");  // an overwrite!
	ops[0].op = CL_OP_ADD;
	ev2citrusleaf_object_init_int(&ops[0].object, 2);
	
	if (0 != ev2citrusleaf_operate(c->asc, c->ns, c->set, &c->o_key, ops, 1, NULL, c->timeout_ms, example_phase_two, c, c->base)) {
		fprintf(stderr, "citrusleaf operate could not dispatch - phase 1\n");
		g_config.return_value = -1;
		return;
	}
	fprintf(stderr, "citrusleaf phase one finished\n");
	
}

void
example_phase_zero(config *c)
{
	fprintf(stderr, "citrusleaf phase zero\n");

	// Set up the key, used in all phases ---
	ev2citrusleaf_object_init_str(&c->o_key, "example_key");

	// Delete the key to start clean
	if (0 != ev2citrusleaf_delete(c->asc, c->ns, c->set, &c->o_key, NULL,
			c->timeout_ms, example_phase_one, c, c->base)) {

		fprintf(stderr, "citrusleaf delete failed\n");
		c->return_value = -1;
		test_terminate(-1);
	}

	fprintf(stderr, "citrusleaf phase zero finished\n");
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
	g_config.asc = ev2citrusleaf_cluster_create(0);
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
		ev2citrusleaf_cluster_destroy(g_config.asc);
		return(-1);
	}


	// info test
	fprintf(stderr, "starting info test\n");
	ev2citrusleaf_info(g_config.base, g_config.dns_base,
		g_config.host, g_config.port, 0, g_config.timeout_ms, example_info_fn, 0);
	
	// Start the train of example stuff
	example_phase_zero(&g_config);
	
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
