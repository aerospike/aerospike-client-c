/*
 *  Citrusleaf Tools
 *  include/rec_query.h - Does some demo requests of redis' list as a sproc
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

typedef struct config_s {
	char *host;
	int   port;
	char *ns;
	char *set;
	int   timeout_ms;
	
	bool  verbose;

	cl_cluster	*asc;
		
	char *package_file;
	char *package_name;
} config;


