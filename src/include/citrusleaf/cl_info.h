/*
 * Copyright 2008-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#pragma once

#include <citrusleaf/cl_types.h>
#include <aerospike/as_cluster.h>

#include <sys/socket.h> // socket calls
#include <arpa/inet.h> // inet_ntop
#include <netinet/in.h>
#include <stdbool.h>

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

//
// Use the information system to request data from a given node
// Pass in a '\n' seperated list of names, or no names at all
// [Perhaps a better interface would be an array of pointers-to-names, returning an
//  array of pointers-to-values?]
// Returns a malloc'd string which is the response from the server.
//

int citrusleaf_info(char *host, short port, char *names, char **values, int timeout_ms);
int citrusleaf_info_auth(as_cluster* cluster, char *host, short port, char *names, char **values, int timeout_ms);
int citrusleaf_info_host(struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms, bool send_asis, bool check_bounds);
int citrusleaf_info_host_auth(as_cluster* cluster, struct sockaddr_in *sa_in, char *names, char **values, int timeout_ms, bool send_asis, bool check_bounds);
int citrusleaf_info_host_limit(int fd, char *names, char **values, int timeout_ms, bool send_asis, uint64_t max_response_length, bool check_bounds);
int citrusleaf_info_cluster(as_cluster *asc, char *names, char **values, bool send_asis, bool check_bounds, int timeout_ms);

int citrusleaf_info_cluster_foreach(
	as_cluster *cluster, const char *command, bool send_asis, bool check_bounds, int timeout_ms, void *udata, char** error,
	bool (*callback)(const as_node * node, const char *command, char *value, void *udata));

int citrusleaf_info_validate(char* response, char** message);
int citrusleaf_info_parse_single(char *values, char **value);
