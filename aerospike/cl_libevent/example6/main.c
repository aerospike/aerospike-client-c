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

#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include <event.h>
#include <evdns.h>

#include "citrusleaf_event/evcitrusleaf.h"

typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *set;
	
	bool verbose;
	bool follow;
	
	int  timeout_ms;
	
	evcitrusleaf_object  o_key;
		
	evcitrusleaf_cluster	*asc;
	
	int	return_value;   // return value from the test
	
	uint32_t	blob_size;
	uint8_t	*blob;
	
    int counter;
	struct event ev; // occasionally want a timer event
	
} config;

config g_config;


void example_phase_waited(int fd, short event, void *udata);
void example_info_fn(int return_value, char *response, size_t response_len, void *udata);

void
test_terminate(int r)
{
	g_config.return_value = r;
	struct timeval x = { 0, 0 };
	event_loopexit(&x);
}

void
example_info_fn(int return_value, char *response, size_t response_len, void *udata)
{
//	fprintf(stderr, "example info return: rv %d response len %zd response %s\n",return_value,response_len, response);
	fprintf(stderr, "example info return: rv %d response len %zd\n",return_value,response_len);
	if (response) free(response);
    
	config *c = (config *) udata;
	if (c->counter<=0) {
        test_terminate(0);
    } else {
        c->counter--;
	    fprintf(stderr, "counter = %d. Will wait 90 sec now\n",c->counter);
	    evtimer_set(&c->ev, example_phase_waited, c);
	    struct timeval tv = { 90, 0 };
	    if (0 != evtimer_add(&c->ev, &tv)) {
		    fprintf(stderr, "evtimer fail: unknown reason, shouldn't in such a simple test\n");
		    test_terminate(-1);
		    return;
	    }
    }
}


void
example_phase_waited(int fd, short event, void *udata)
{
	// info test
	fprintf(stderr, "starting info test\n");
	evcitrusleaf_info(g_config.host, g_config.port, 0, g_config.timeout_ms, example_info_fn, udata);
	
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
    g_config.counter = 30;
	
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

	fprintf(stderr, "EXAMPLE6 -- tests long pauses between transactions\n");
	
	event_init();			// initialize the libevent system
	evdns_init();			// used by the cluster system for async name resolution
	// evdns_resolv_conf_parse(DNS_OPTIONS_ALL, "/etc/resolv.conf"); ???
	
	evcitrusleaf_log_register( log_callback );
	evcitrusleaf_log_level_set(EVCITRUSLEAF_DEBUG);
	log_open("example6.log");
	
	evcitrusleaf_init();    // initialize citrusleaf
	
	// Create a citrusleaf cluster object for subsequent requests
	g_config.asc = evcitrusleaf_cluster_create();
	if (!g_config.asc) {
		fprintf(stderr, "could not create cluster, internal error\n");
		return(-1);
	}
	
	evcitrusleaf_cluster_add_host(g_config.asc, g_config.host, g_config.port);
	
	// Start the train of example events
	example_phase_waited(0,0,&g_config);
	
	// Burrow into the libevent event loop
	event_dispatch();

	evcitrusleaf_shutdown(false);
	
	if (g_config.return_value != 0)
		fprintf(stderr, "test complete: FAILED return value %d\n",g_config.return_value);
	else
		fprintf(stderr, "test complete: SUCCESS\n");
		
	
	return(0);
}
