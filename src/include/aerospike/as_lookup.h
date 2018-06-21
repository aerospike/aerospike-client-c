/*
 * Copyright 2008-2018 Aerospike, Inc.
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

#include <aerospike/as_address.h>
#include <aerospike/as_error.h>
#include <aerospike/as_status.h>

#if !defined(_MSC_VER)
#include <netdb.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************************************
 * TYPES
 ******************************************************************************/
	
/**
 * @private
 * Iterator for IP addresses.
 */
typedef struct as_sockaddr_iterator_s {
	struct addrinfo* addresses;
	struct addrinfo* current;
	uint16_t port_be;
	bool hostname_is_alias;
} as_address_iterator;

struct as_cluster_s;
struct as_node_info_s;
struct as_host_s;

/******************************************************************************
 * FUNCTIONS
 ******************************************************************************/

/**
 * @private
 * Lookup hostname and initialize address iterator.
 */
as_status
as_lookup_host(as_address_iterator* iter, as_error* err, const char* hostname, uint16_t port);
	
/**
 * @private
 * Get next socket address with assigned port.  Return false when there are no more addresses.
 */
static inline bool
as_lookup_next(as_address_iterator* iter, struct sockaddr** addr)
{
	if (! iter->current) {
		return false;
	}
	
	struct sockaddr* sa = iter->current->ai_addr;
	iter->current = iter->current->ai_next;
	
	if (sa->sa_family == AF_INET) {
		((struct sockaddr_in*)sa)->sin_port = iter->port_be;
	}
	else {
		((struct sockaddr_in6*)sa)->sin6_port = iter->port_be;
	}
	*addr = sa;
	return true;
}

/**
 * @private
 * Release memory associated with address iterator.
 */
static inline void
as_lookup_end(as_address_iterator* iter)
{
	freeaddrinfo(iter->addresses);
}

/**
 * @private
 * Lookup and validate node.
 */
as_status
as_lookup_node(
	struct as_cluster_s* cluster, as_error* err, struct as_host_s* host, struct sockaddr* addr,
	bool detect_load_balancer, struct as_node_info_s* node_info
	);

#ifdef __cplusplus
} // end extern "C"
#endif
