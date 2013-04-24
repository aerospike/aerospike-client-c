/*
 *  Citrusleaf Tools
 *  src/main.c - EXAMPLE5 for libevent
 *
 * The purpose of example5 is to show the shutdown/cancel sequence in libevent
 *
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

#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include <event.h>
#include <evdns.h>

#include "citrusleaf_event/evcitrusleaf.h"


#define STATUS_UNINIT 0
#define STATUS_INPROGRESS 1
#define STATUS_COMPLETE 2


typedef struct {
	
	int idx;
	
	evcitrusleaf_object o_key;
	
	int   status;
	
} request;



typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *set;
	
	bool verbose;
	bool follow;
	
	bool test_active; // set this to false to avoid rescheduloing 
	
	int  timeout_ms;
	int 	kill_secs;
	
	evcitrusleaf_object  o_key;
		
	evcitrusleaf_cluster	*asc;
	
	int	return_value;   // return value from the test
	
	uint32_t	blob_size;
	uint8_t	*blob;
	
	struct event ev; // occasionally want a timer event
	
	int		n_req;
	request *req_array;
	
} config;

config g_config;





void example5_request(uint64_t i);
void example5_response(int return_value, evcitrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata);



void
example5_response(int return_value, evcitrusleaf_bin *bins, int n_bins, uint32_t generation, void *udata)
{
	uint64_t i = (uint64_t) udata;

	request *req = &g_config.req_array[i];
	req->status = STATUS_COMPLETE;
	
	// don't really care what the return value is. The only point is to generate load so we have
	// outstanding transactions when we stop.
//	if (return_value != EVCITRUSLEAF_OK) {
//		fprintf(stderr, "get failed: return code %d\n",return_value);
//	}

	if (bins)		evcitrusleaf_bins_free(bins, n_bins);
	
	if (g_config.test_active)
		example5_request(i);

}




void
example5_request(uint64_t i)
{
	config *c = &g_config;
	// Set up a key and bin etc the key, used in all phases ---
	// be a little careful here, because the o_key will contain a pointer
	// to strings
	request *req = &c->req_array[i];
	
	evcitrusleaf_object_init_int(&req->o_key, i);
	
	int rv = evcitrusleaf_get_all(c->asc, c->ns, c->set, &req->o_key, c->timeout_ms, example5_response, (void *) i);
	if (rv != 0) {
		fprintf(stderr, "citrusleaf get_all failed to start: error code %d\n",rv);
	}
	// fprintf(stderr, "citrusleaf get_all dispatched\n");

	req->status = STATUS_INPROGRESS;	
}


// an example log-callback function

int g_logfile_fd = 0;

void
log_open(char *logfilename) 
{
	int fd = open(logfilename, O_APPEND | O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP);
	if (fd < 0) {
		fprintf(stderr, "can't open log file %s, terminating!",logfilename);
		return;
	}
	g_logfile_fd = fd;
}


void
log_callback(int level, const char *fmt, ...)
{
	va_list argp;
	if (g_logfile_fd == 0) {
		va_start(argp, fmt);
		vfprintf(stderr,  fmt, argp);
		va_end(argp);
	}
	else {
		// time buf
		char time_buf[60];
		struct tm tm;
		time_t raw_time;
		time ( &raw_time );
		int tlen = strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", gmtime_r( &raw_time, &tm) );
		
		// user buf
		char ubuf[2000];
		va_start(argp, fmt);
		int ulen = vsnprintf(ubuf, sizeof(ubuf)-1, fmt, argp);
		va_end(argp);
		
		// combined buf - could do with just memcpy instead, have all lengths
		char mbuf[2100];
		memcpy(mbuf, time_buf, tlen);
		mbuf[tlen] = ' ';
		memcpy(mbuf+tlen+1, ubuf, ulen);
		
		write(g_logfile_fd, mbuf,tlen+ulen+1);
		// fdatasync(g_logfile_fd); // turns out this is too harsh. Only turn it on
		// when you really need it.
	}
	
}



void usage(void) {
	fprintf(stderr, "Usage key_c:\n");
	fprintf(stderr, "-h host [default 127.0.0.1] \n");
	fprintf(stderr, "-p port [default 3000]\n");
	fprintf(stderr, "-n namespace [default test]\n");
	fprintf(stderr, "-s set [default ""]\n");
	fprintf(stderr, "-b bin [default value]\n");
	fprintf(stderr, "-t n_transactions [default value 10]\n");
	fprintf(stderr, "-k number of seconds before termination [default value 10]\n");
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
	g_config.return_value = 0;
	g_config.n_req = 10;
	g_config.kill_secs = 10;
	g_config.timeout_ms = 100; // 100 ms is a decent time
	
	int		c;
	
	printf("example of the C libevent citrusleaf library\n");
	
	while ((c = getopt(argc, argv, "h:p:n:s:m:t:k:v")) != -1) 
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

		case 't':
			g_config.n_req = atoi(optarg);
			break;

		case 'k':
			g_config.kill_secs = atoi(optarg);
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

	fprintf(stderr, "EXAMPLE5 -- tests shutdown while many transactions are in progress\n");
	
	event_init();			// initialize the libevent system
	evdns_init();			// used by the cluster system for async name resolution
	// evdns_resolv_conf_parse(DNS_OPTIONS_ALL, "/etc/resolv.conf"); ???
	
	evcitrusleaf_log_register( log_callback );
//	evcitrusleaf_log_level_set(EVCITRUSLEAF_NOTICE);
	evcitrusleaf_log_level_set(EVCITRUSLEAF_INFO);
	log_open("example5.log");
	
	evcitrusleaf_init();    // initialize citrusleaf
	
	// Create a citrusleaf cluster object for subsequent requests
	g_config.asc = evcitrusleaf_cluster_create();
	if (!g_config.asc) {
		fprintf(stderr, "could not create cluster, internal error\n");
		return(-1);
	}
	
	evcitrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port);
	
	// Start many example requests
	g_config.test_active = true;
	g_config.req_array = malloc(g_config.n_req * sizeof(request));
	memset(g_config.req_array, 0, g_config.n_req * sizeof(request) );
	
	for (int i=0;i<g_config.n_req;i++) {
		example5_request(i);
	}
	
	// This function will create a timer that waits N seconds before exiting the dispatch loop
	struct timeval le_tv = { g_config.kill_secs, 0 };
	event_loopexit (  	&le_tv );   	
	
	// Burrow into the libevent event loop
	// comes out -- how again?
	fprintf(stderr, "starting dispatch loop\n"); 
	event_dispatch();
	fprintf(stderr, "ending dispatch loop\n");

	g_config.test_active = false;
	evcitrusleaf_cluster_destroy(g_config.asc);
	evcitrusleaf_shutdown(true);
	
	// validate that all transactions have completed
	for (int i=0;i<g_config.n_req;i++) {
		request *req = &g_config.req_array[i];
		if (req->status != STATUS_COMPLETE) {
			fprintf(stderr, "ERROR! transaction %d is not complete!\n",i);
			g_config.return_value = -1;
			break;
		}
	}
	
	if (g_config.return_value != 0)
		fprintf(stderr, "test complete: FAILED return value %d\n",g_config.return_value);
	else
		fprintf(stderr, "test complete: SUCCESS\n");
		
	
	return(0);
}
