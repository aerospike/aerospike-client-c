/*
 * Copyright 2008-2025 Aerospike, Inc.
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
#include <aerospike/as_address.h>
#include <stdio.h>

/******************************************************************************
 * FUNCTIONS
 *****************************************************************************/

void
as_address_name(struct sockaddr* addr, char* name, socklen_t size)
{
	 // IPv4: xxx.xxx.xxx.xxx:<port>
	 // IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]:<port>
	if (addr->sa_family == AF_INET) {
		struct sockaddr_in* a = (struct sockaddr_in*)addr;
		const char* result = inet_ntop(AF_INET, &a->sin_addr, name, size);
		
		if (result) {
			size_t len = strlen(name);
			
			if (len + 5 < size) {
				sprintf(name + len, ":%d", cf_swap_from_be16(a->sin_port));
			}
		}
		else {
			*name = 0;
		}
	}
	else {
		struct sockaddr_in6* a = (struct sockaddr_in6*)addr;
		*name = '[';
		
		const char* result = inet_ntop(AF_INET6, &a->sin6_addr, name + 1, size - 1);
		
		if (result) {
			size_t len = strlen(name);
			
			if (len + 7 < size) {
				sprintf(name + len, "]:%d", cf_swap_from_be16(a->sin6_port));
			}
		}
		else {
			*name = 0;
		}
	}
}

void
as_address_short_name(struct sockaddr* addr, char* name, socklen_t size)
{
	// IPv4: xxx.xxx.xxx.xxx
	// IPv6: xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx
	const char* result;

	if (addr->sa_family == AF_INET) {
		struct sockaddr_in* a = (struct sockaddr_in*)addr;
		result = inet_ntop(AF_INET, &a->sin_addr, name, size);
	}
	else {
		struct sockaddr_in6* a = (struct sockaddr_in6*)addr;
		result = inet_ntop(AF_INET6, &a->sin6_addr, name, size);
	}

	if (! result) {
		*name = 0;
	}
}

bool
as_address_equals(struct sockaddr* addr1, struct sockaddr* addr2)
{
	if (addr1->sa_family != addr2->sa_family) {
		return false;
	}

	if (addr1->sa_family == AF_INET) {
		return memcmp(
			&((struct sockaddr_in*)addr1)->sin_addr,
			&((struct sockaddr_in*)addr2)->sin_addr,
			sizeof(struct in_addr)) == 0;
	}
	else {
		return memcmp(
			&((struct sockaddr_in6*)addr1)->sin6_addr,
			&((struct sockaddr_in6*)addr2)->sin6_addr,
			sizeof(struct in6_addr)) == 0;
	}
}
