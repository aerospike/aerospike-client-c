/*
 *  Citrusleaf Tools
 *  include/scan_sproc.h
 *
 *  Copyright 2012 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once
 
#include <stdbool.h>
#include "citrusleaf/citrusleaf.h"

typedef struct config_s {
	char *host;
	int   port;
	char *ns;
	char *set;
	int   timeout_ms;

	bool verbose;

	cl_cluster	*asc;

	char *package_file;
	char *package_name;
	
	bool insert_data;
} config;


