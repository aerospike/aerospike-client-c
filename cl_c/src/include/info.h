/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"

//
// Use the information system to request data from a given node
// Pass in a '\n' seperated list of names, or no names at all
// [Perhaps a better interface would be an array of pointers-to-names, returning an
//  array of pointers-to-values?]
// Returns a malloc'd string which is the response from the server.
//

int citrusleaf_info(char *host, short port, char *names, char **values, int timeout_ms);
int citrusleaf_info_host(struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms, bool send_asis);
int citrusleaf_info_cluster(cl_cluster *asc, char *names, char **values, bool send_asis, int timeout_ms);
int citrusleaf_info_cluster_all(cl_cluster *asc, char *names, char **values, bool send_asis, int timeout_ms);