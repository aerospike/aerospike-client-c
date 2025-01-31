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
#pragma once

#include <citrusleaf/cf_byte_order.h>
#include <string.h>

#if !defined(_MSC_VER)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#define in_addr_t ULONG
#endif

#define AS_IP_ADDRESS_SIZE 64

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @private
 * Convert socket address (including port) to a string.
 *
 * Formats:
 * ~~~~~~~~~~{.c}
 * IPv4: xxx.xxx.xxx.xxx:<port>
 * IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]:<port>
 * ~~~~~~~~~~
 */
AS_EXTERN void
as_address_name(struct sockaddr* addr, char* name, socklen_t size);

/**
 * @private
 * Convert socket address to a string without brackets or a port.
 *
 * Formats:
 * ~~~~~~~~~~{.c}
 * IPv4: xxx.xxx.xxx.xxx
 * IPv6: xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx
 * ~~~~~~~~~~
 */
AS_EXTERN void
as_address_short_name(struct sockaddr* addr, char* name, socklen_t size);

/**
 * @private
 * Are socket addresses equal. The port is not included in the comparison.
 */
AS_EXTERN bool
as_address_equals(struct sockaddr* addr1, struct sockaddr* addr2);

/**
 * @private
 * Return port of address.
 */
static inline uint16_t
as_address_port(struct sockaddr* addr)
{
	uint16_t port = (addr->sa_family == AF_INET)?
		((struct sockaddr_in*)addr)->sin_port :
		((struct sockaddr_in6*)addr)->sin6_port;
	return cf_swap_from_be16(port);
}

/**
 * @private
 * Return size of socket address.
 */
static inline socklen_t
as_address_size(struct sockaddr* addr)
{
	return (addr->sa_family == AF_INET)? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
}

/**
 * @private
 * Copy socket address to storage.
 */
static inline void
as_address_copy_storage(struct sockaddr* src, struct sockaddr_storage* trg)
{
	size_t size = as_address_size(src);
	memcpy(trg, src, size);
}

/**
 * @private
 * Return if socket address is localhost.
 */
static inline bool
as_address_is_local(struct sockaddr* addr)
{
	if (addr->sa_family == AF_INET) {
		struct sockaddr_in* a = (struct sockaddr_in*)addr;
		return (cf_swap_to_be32(a->sin_addr.s_addr) & 0xff000000) == 0x7f000000;
	}
	else {
		struct sockaddr_in6* a = (struct sockaddr_in6*)addr;
		return memcmp(&a->sin6_addr, &in6addr_loopback, sizeof(struct in6_addr)) == 0;
	}
}

#ifdef __cplusplus
} // end extern "C"
#endif
