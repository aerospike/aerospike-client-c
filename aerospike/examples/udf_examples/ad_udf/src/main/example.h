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
#include <citrusleaf/citrusleaf.h>


extern void *start_counter_thread(atomic_int *records, atomic_int *bytes);
extern void stop_counter_thread(void *id);
typedef struct config_s {
	
	char *host;
	int   port;
	char *ns;
	char *set;
	uint32_t timeout_ms;
	uint32_t record_ttl;
	char *package_file;
	char *package_name;
	cl_cluster      *asc;
	bool    verbose;
	cf_atomic_int success;
	cf_atomic_int fail;
	int n_behaviors;
	int n_users;
} config;


