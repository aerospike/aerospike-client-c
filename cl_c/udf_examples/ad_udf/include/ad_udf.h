/*
 *  Citrusleaf Tools
 *  include/rec_query.h - Does some demo requests of the new secondary index and map reduce stuff
 *
 *  Copyright 2012 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once
 
#include <inttypes.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>

#include "utils.h"
#include "citrusleaf/citrusleaf.h"


extern void *start_counter_thread(atomic_int *records, atomic_int *bytes);
extern void stop_counter_thread(void *id);

typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *set;
	int   timeout_ms;
	
	bool verbose;

	bool register_package;
	
	cl_cluster	*asc;
		
	char *package_file;
	char *package_name;
	
	int n_users;
	int n_behaviors;
			
} config;


