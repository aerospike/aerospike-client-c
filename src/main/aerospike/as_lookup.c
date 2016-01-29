/*
 * Copyright 2008-2016 Aerospike, Inc.
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
#include <citrusleaf/cf_byte_order.h>
#include <netdb.h>

as_status
as_lookup(as_error* err, char* hostname, uint16_t port, as_vector* /*<struct sockaddr_in>*/ addresses)
{
	// Lookup TCP addresses.
	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	
	struct addrinfo* results = 0;
	int ret = getaddrinfo(hostname, 0, &hints, &results);
	
	if (ret) {
		return as_error_update(err, AEROSPIKE_ERR_INVALID_HOST, "Invalid hostname %s: %s", hostname, gai_strerror(ret));
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
	return AEROSPIKE_OK;
}
