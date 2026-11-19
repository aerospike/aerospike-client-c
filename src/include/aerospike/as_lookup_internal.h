/*
 * Copyright 2008-2026 Aerospike, Inc.
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

// Private to the client library and its tests. Not installed.

#include <aerospike/as_lookup.h>
#include <aerospike/as_node.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------
// Types
//---------------------------------

struct as_nodes_s;

/**
 * @private
 * Verdict of an as_lookup_accept_fn on a validated node.
 */
typedef enum as_lookup_verdict_e {
	/**
	 * Use this node.
	 */
	AS_LOOKUP_ACCEPT,

	/**
	 * Wrong node: drop it and keep trying the other addresses of the host.
	 */
	AS_LOOKUP_NEXT,

	/**
	 * Drop the node and stop: the answer is final for this host, for example a collision.
	 */
	AS_LOOKUP_STOP
} as_lookup_verdict;

/**
 * @private
 * Check of a node that validated at one address of a host.
 */
typedef as_lookup_verdict (*as_lookup_accept_fn)(struct as_node_info_s* info, void* udata);

/**
 * @private
 * Address last accepted for a host name and port that resolved to several addresses, and the
 * name of the node accepted there.
 */
typedef struct as_lookup_valid_address_s {
	char name[AS_HOSTNAME_SIZE];
	char node_name[AS_NODE_NAME_SIZE];
	uint16_t port;
	struct sockaddr_storage addr;
} as_lookup_valid_address;

/**
 * @private
 * Resolver with the getaddrinfo() signature.
 */
typedef int (*as_lookup_getaddrinfo_fn)(
	const char* node, const char* service, const struct addrinfo* hints, struct addrinfo** res
	);

/**
 * @private
 * Release a list returned by the matching as_lookup_getaddrinfo_fn.
 */
typedef void (*as_lookup_freeaddrinfo_fn)(struct addrinfo* res);

//---------------------------------
// Functions
//---------------------------------

/**
 * @private
 * Replace the resolver of as_lookup_host() and the function as_lookup_end() frees its list with.
 * NULL restores getaddrinfo() and freeaddrinfo(). Not thread-safe: set it while no lookup runs.
 */
void
as_lookup_set_resolver(
	as_lookup_getaddrinfo_fn getaddrinfo_fn, as_lookup_freeaddrinfo_fn freeaddrinfo_fn
	);

/**
 * @private
 * as_lookup_node() whose connect, and info requests without authentication, end at deadline in
 * milliseconds, zero for none. Login keeps login_timeout_ms.
 */
as_status
as_lookup_node_until(
	struct as_cluster_s* cluster, as_error* err, struct as_host_s* host, struct sockaddr* addr,
	bool detect_load_balancer, struct as_node_info_s* node_info, bool* alternate_missing,
	uint64_t deadline
	);

/**
 * @private
 * Validate the node at one of the remaining addresses of iter, which as_lookup_host() returned
 * for host, and run accept on each node that validates. NULL accept accepts every node. A node
 * that is not accepted is destroyed.
 *
 * A single address is validated as as_lookup_node() does. With several addresses, connects start
 * one at a time, the address last accepted for host first, then in resolver order, the next one
 * when 100 ms or bound / addresses pass or as soon as a connect fails, where bound is
 * min(conn_timeout_ms, tend_interval), or tend_interval when conn_timeout_ms is zero. The first
 * address whose TCP connect completes is validated on that socket. Its TLS handshake ends with
 * bound. Login keeps login_timeout_ms and info requests conn_timeout_ms, so an address that drops
 * connects costs at most bound, but a winner that goes silent after its TCP connect costs bound
 * plus login_timeout_ms with a user, or plus conn_timeout_ms without one; with conn_timeout_ms
 * zero its info requests have no deadline. When the winner fails or is not accepted, the other
 * addresses race again, their connects restarted, until bound has passed. AS_LOOKUP_STOP ends the
 * race at once.
 *
 * When no node is accepted, the first validation error is returned with its alternate_missing
 * flag. A missing alternate address is AEROSPIKE_ERR_INVALID_NODE too, so callers tell the cases
 * apart by the flag and their accept udata. Without a validation error, AEROSPIKE_ERR_INVALID_NODE
 * is returned after AS_LOOKUP_STOP, or when every validated node got AS_LOOKUP_NEXT, and otherwise
 * AEROSPIKE_ERR_CONNECTION, naming the first connect or TLS failure. An accepted address of a host
 * with several addresses is recorded as the last accepted one of host. Tend thread only.
 */
as_status
as_lookup_validate_any(
	struct as_cluster_s* cluster, as_error* err, struct as_host_s* host, as_address_iterator* iter,
	as_lookup_accept_fn accept, void* udata, struct as_node_info_s* node_info,
	bool* alternate_missing
	);

/**
 * @private
 * Forget the last accepted addresses of node names that no node of nodes has. Tend thread only.
 */
void
as_lookup_prune_valid_addresses(struct as_cluster_s* cluster, struct as_nodes_s* nodes);

/**
 * @private
 * Release the last accepted addresses of the cluster.
 */
void
as_lookup_destroy_valid_addresses(struct as_cluster_s* cluster);

#ifdef __cplusplus
} // end extern "C"
#endif
