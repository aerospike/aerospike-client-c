/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"

// Do a lookup with this name and port, and add the sockaddr to the
// vector using the unique lookup
int cl_lookup(cl_cluster *asc, char *hostname, short port, cf_vector *sockaddr_in_v);
