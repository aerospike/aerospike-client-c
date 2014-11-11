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
#include <aerospike/as_lookup.h>
#include <aerospike/as_log_macros.h>
#include <citrusleaf/cl_info.h>
#include <citrusleaf/cf_byte_order.h>
#include <netdb.h>

int
as_lookup(as_cluster* cluster, char* hostname, uint16_t port, bool enable_warning, as_vector* /*<struct sockaddr_in>*/ addresses)
{
	// Check if there is an alternate address that should be used for this hostname.
	if (cluster && cluster->ip_map) {
		as_addr_map* entry = cluster->ip_map;
		
		for (uint32_t i = 0; i < cluster->ip_map_size; i++) {
			if (strcmp(entry->orig, hostname) == 0) {
				// Found mapping for this address.  Use alternate.
				as_log_debug("Using %s instead of %s", entry->alt, hostname);
				hostname = entry->alt;
				break;
			}
			entry++;
		}
	}
	
	// Lookup TCP addresses.
	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	
	struct addrinfo* results = 0;
	int ret = getaddrinfo(hostname, 0, &hints, &results);
	
	if (ret) {
		if (enable_warning) {
			as_log_warn("Invalid hostname %s: %s", hostname, gai_strerror(ret));
		}
		return AEROSPIKE_ERR_CLUSTER;
	}
	
	// Add addresses to vector if it exists.
	if (addresses) {
		uint16_t port_be = cf_swap_to_be16(port);
		
		for (struct addrinfo* r = results; r; r = r->ai_next) {
			struct sockaddr_in* addr = (struct sockaddr_in*)r->ai_addr;
			addr->sin_port = port_be;
			as_vector_append_unique(addresses, addr);
		}
	}
	
	freeaddrinfo(results);
	return 0;
}
