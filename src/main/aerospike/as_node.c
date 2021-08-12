/*
 * Copyright 2008-2021 Aerospike, Inc.
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
#include <aerospike/as_node.h>
#include <aerospike/as_address.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_info.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_peers.h>
#include <aerospike/as_queue.h>
#include <aerospike/as_shm_cluster.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_string.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/cf_byte_order.h>

// Replicas take ~2K per namespace, so this will cover most deployments:
#define INFO_STACK_BUF_SIZE (16 * 1024)

/******************************************************************************
 * Function declarations.
 *****************************************************************************/

const char*
as_cluster_get_alternate_host(as_cluster* cluster, const char* hostname);

bool
as_partition_tables_update_all(as_cluster* cluster, as_node* node, char* buf, bool has_regime);

static void
as_node_create_connections(as_node* node, as_conn_pool* pool, uint32_t timeout_ms, int count);

void
as_event_node_balance_connections(as_cluster* cluster, as_node* node);

extern uint32_t as_event_loop_capacity;

/******************************************************************************
 * Functions.
 *****************************************************************************/

static inline void
as_racks_release(as_racks* racks)
{
	//as_fence_release();
	if (as_aaf_uint32(&racks->ref_count, -1) == 0) {
		cf_free(racks);
	}
}

static as_async_conn_pool*
as_node_create_async_pools(uint32_t min_conns_per_node, uint32_t max_conns_per_node)
{
	// Create one queue per event manager.
	as_async_conn_pool* pools = cf_malloc(sizeof(as_async_conn_pool) * as_event_loop_capacity);
	
	// Distribute connections over event loops taking remainder into account.
	uint32_t min = min_conns_per_node / as_event_loop_capacity;
	uint32_t rem_min = min_conns_per_node - (min * as_event_loop_capacity);
	uint32_t max = max_conns_per_node / as_event_loop_capacity;
	uint32_t rem_max = max_conns_per_node - (max * as_event_loop_capacity);

	for (uint32_t i = 0; i < as_event_loop_capacity; i++) {
		as_async_conn_pool* pool = &pools[i];
		uint32_t min_size = i < rem_min ? min + 1 : min;
		uint32_t max_size = i < rem_max ? max + 1 : max;
		as_async_conn_pool_init(pool, min_size, max_size);
	}
	return pools;
}

as_node*
as_node_create(as_cluster* cluster, as_node_info* node_info)
{
	as_node* node = cf_malloc(sizeof(as_node));

	if (!node) {
		return NULL;
	}
	
	node->ref_count = 1;
	node->partition_ref_count = 0;
	node->peers_generation = 0xFFFFFFFF;
	node->partition_generation = 0xFFFFFFFF;
	node->rebalance_generation = 0xFFFFFFFF;
	node->cluster = cluster;

	strcpy(node->name, node_info->name);
	node->session = node_info->session;
	node->features = node_info->features;
	node->address_index = (node_info->addr.ss_family == AF_INET) ? 0 : AS_ADDRESS4_MAX;
	node->address4_size = 0;
	node->address6_size = 0;
	node->addresses = cf_malloc(sizeof(as_address) * (AS_ADDRESS6_MAX));
	as_node_add_address(node, (struct sockaddr*)&node_info->addr);
	
	as_vector_init(&node->aliases, sizeof(as_alias), 2);

	memcpy(&node->info_socket, &node_info->socket, sizeof(as_socket));
	node->tls_name = node_info->host.tls_name ? cf_strdup(node_info->host.tls_name) : NULL;

	if (node->info_socket.ssl) {
		// Required to keep as_socket tls_name in scope.
		as_tls_set_name(&node->info_socket, node->tls_name);
	}

	node->racks = NULL;
	node->peers_count = 0;
	node->friends = 0;
	node->failures = 0;
	node->index = 0;
	node->perform_login = 0;
	node->active = true;
	node->partition_changed = true;
	node->rebalance_changed = cluster->rack_aware;

	// Create sync connection pools.
	node->sync_conn_pools = cf_malloc(sizeof(as_conn_pool) * cluster->conn_pools_per_node);
	node->sync_conns_opened = 1;
	node->sync_conns_closed = 0;
	node->error_count = 0;
	node->conn_iter = 0;

	uint32_t min = cluster->min_conns_per_node / cluster->conn_pools_per_node;
	uint32_t rem_min = cluster->min_conns_per_node - (min * cluster->conn_pools_per_node);
	uint32_t max = cluster->max_conns_per_node / cluster->conn_pools_per_node;
	uint32_t rem_max = cluster->max_conns_per_node - (max * cluster->conn_pools_per_node);

	for (uint32_t i = 0; i < cluster->conn_pools_per_node; i++) {
		as_conn_pool* pool = &node->sync_conn_pools[i];
		uint32_t min_size = i < rem_min ? min + 1 : min;
		uint32_t max_size = i < rem_max ? max + 1 : max;
		as_conn_pool_init(pool, sizeof(as_socket), min_size, max_size);
	}

	if (as_event_loop_capacity == 0) {
		node->async_conn_pools = NULL;
		node->pipe_conn_pools = NULL;
		return node;
	}

	// Create async connection pools.
	node->async_conn_pools = as_node_create_async_pools(cluster->async_min_conns_per_node,
		cluster->async_max_conns_per_node);

	node->pipe_conn_pools = as_node_create_async_pools(0, cluster->pipe_max_conns_per_node);
	return node;
}

void
as_node_destroy(as_node* node)
{
	// Close tend connection.
	if (node->info_socket.fd >= 0) {
		as_socket_close(&node->info_socket);
	}

	// Drain sync connection pools.
	uint32_t max = node->cluster->conn_pools_per_node;

	for (uint32_t i = 0; i < max; i++) {
		as_conn_pool_destroy(&node->sync_conn_pools[i]);
	}
	cf_free(node->sync_conn_pools);

	// Drain async connection pools.
	if (as_event_loop_capacity > 0) {
		// Close async and pipeline connections.
		as_event_node_destroy(node);
	}

	// Release memory.
	cf_free(node->addresses);
	as_vector_destroy(&node->aliases);

	if (node->tls_name) {
		cf_free(node->tls_name);
	}

	as_session* session = (as_session*)as_load_ptr(&node->session);

	if (session) {
		as_session_release(session);
	}

	as_racks* racks = (as_racks*)as_load_ptr(&node->racks);

	if (racks) {
		as_racks_release(racks);
	}
	cf_free(node);
}

void
as_node_create_min_connections(as_node* node)
{
	// Create sync connections.
	uint32_t max = node->cluster->conn_pools_per_node;

	for (uint32_t i = 0; i < max; i++) {
		as_conn_pool* pool = &node->sync_conn_pools[i];

		if (pool->min_size > 0) {
			as_node_create_connections(node, pool, node->cluster->conn_timeout_ms, pool->min_size);
		}
	}

	// Create async connections.
	if (as_event_loop_capacity > 0 && as_event_loop_size > 0 && !as_event_single_thread &&
		node->cluster->async_min_conns_per_node > 0) {
		as_event_create_connections(node, node->async_conn_pools);
	}
}

/**
 * Use non-inline function for garbarge collector function pointer reference.
 * Forward to inlined release.
 */
static void
release_node(as_node* node)
{
	as_node_release(node);
}

/**
 * Delay node release to avoid the following race condition:
 *
 * Transaction thread gets old node from partition map.
 * Tend thread frees old node that is no longer referenced in the cluster.
 * Transaction thread tries to reserve the node that has been freed.
 *
 * The gap between the transaction thread node retrieval and node reserve
 * is just a few instructions, but the race condition could happen.
 *
 * Solve by delaying the tend thread node release until the next tend
 * iteration. This minimum 1 second delay is enough time close the race 
 * condition loophole.
 */
void
as_node_release_delayed(as_node* node)
{
	as_gc_item item;
	item.data = node;
	item.release_fn = (as_release_fn)release_node;
	as_vector_append(node->cluster->gc, &item);
}

void
as_node_add_address(as_node* node, struct sockaddr* addr)
{
	// Add IP address
	as_address address;
	as_address_copy_storage(addr, &address.addr);
	as_address_name(addr, address.name, sizeof(address.name));

	// Address array is currently a fixed size.
	// Do not resize because multiple threads are accessing the array.
	if (addr->sa_family == AF_INET) {
		if (node->address4_size < AS_ADDRESS4_MAX) {
			node->addresses[node->address4_size] = address;
			node->address4_size++;
		}
		else {
			as_log_info("Failed to add node %s ipv4 address %s. Max size = %d", node->name, address.name, AS_ADDRESS4_MAX);
		}
	}
	else {
		uint32_t offset = AS_ADDRESS4_MAX + node->address6_size;
		
		if (offset < AS_ADDRESS6_MAX) {
			node->addresses[offset] = address;
			node->address6_size++;
		}
		else {
			as_log_info("Failed to add node %s ipv6 address %s. Max size = %d", node->name, address.name, AS_ADDRESS6_MAX - AS_ADDRESS4_MAX);
		}
	}
}

void
as_node_add_alias(as_node* node, const char* hostname, uint16_t port)
{
	as_vector* aliases = &node->aliases;
	as_alias* alias;
	
	for (uint32_t i = 0; i < aliases->size; i++) {
		alias = as_vector_get(aliases, i);
		
		if (strcmp(alias->name, hostname) == 0 && alias->port == port) {
			// Already exists.
			return;
		}
	}
	
	// Add new alias.
	as_alias a;
	
	if (as_strncpy(a.name, hostname, sizeof(a.name))) {
		as_log_warn("Hostname has been truncated: %s", hostname);
	}
	a.port = port;
	
	// Alias vector is currently a fixed size.
	if (aliases->size < aliases->capacity) {
		as_vector_append(aliases, &a);
	}
	else {
		as_log_info("Failed to add node %s alias %s. Max size = %u", node->name, hostname, aliases->capacity);
	}
}

static int
as_node_try_connections(as_socket* sock, as_address* addresses, int i, int max, uint64_t deadline_ms)
{
	while (i < max) {
		if (as_socket_start_connect(sock, (struct sockaddr*)&addresses[i].addr, deadline_ms)) {
			return i;
		}
		i++;
	}
	return -1;
}

static int
as_node_try_family_connections(as_node* node, int family, int begin, int end, int index, as_address* primary, as_socket* sock, uint64_t deadline_ms)
{
	// Create a non-blocking socket.
	as_tls_context* ctx = as_socket_get_tls_context(node->cluster->tls_ctx);
	int rv = as_socket_create(sock, family, ctx, node->tls_name);
	
	if (rv < 0) {
		return rv;
	}
	
	// Try addresses.
	as_address* addresses = node->addresses;
	
	if (index >= 0) {
		// Try primary address.
		if (as_socket_start_connect(sock, (struct sockaddr*)&primary->addr, deadline_ms)) {
			return index;
		}
		
		// Start from current index + 1 to end.
		rv = as_node_try_connections(sock, addresses, index + 1, end, deadline_ms);

		if (rv < 0) {
			// Start from begin to index.
			rv = as_node_try_connections(sock, addresses, begin, index, deadline_ms);
		}
	}
	else {
		rv = as_node_try_connections(sock, addresses, begin, end, deadline_ms);
	}
	
	if (rv < 0) {
		// Couldn't start a connection on any socket address - close the socket.
		as_socket_close(sock);
		return -5;
	}
	return rv;
}

static as_status
as_node_create_socket(
	as_error* err, as_node* node, as_conn_pool* pool, as_socket* sock, uint64_t deadline_ms
	)
{
	// Try addresses.
	uint32_t index = node->address_index;
	as_address* primary = &node->addresses[index];
	int rv;
	
	if (primary->addr.ss_family == AF_INET) {
		// Try IPv4 addresses first.
		rv = as_node_try_family_connections(node, AF_INET, 0, node->address4_size, index, primary, sock, deadline_ms);
		
		if (rv < 0) {
			// Try IPv6 addresses.
			rv = as_node_try_family_connections(node, AF_INET6, AS_ADDRESS4_MAX, AS_ADDRESS4_MAX + node->address6_size, -1, NULL, sock, deadline_ms);
		}
	}
	else {
		// Try IPv6 addresses first.
		rv = as_node_try_family_connections(node, AF_INET6, AS_ADDRESS4_MAX, AS_ADDRESS4_MAX + node->address6_size, index, primary, sock, deadline_ms);
		
		if (rv < 0) {
			// Try IPv4 addresses.
			rv = as_node_try_family_connections(node, AF_INET, 0, node->address4_size, -1, NULL, sock, deadline_ms);
		}
	}
	
	if (rv < 0) {
		return as_error_update(err, AEROSPIKE_ERR_CONNECTION, "Failed to connect: %s %s", node->name, primary->name);
	}
	sock->pool = pool;

	if (rv != index) {
		// Replace invalid primary address with valid alias.
		// Other threads may not see this change immediately.
		// It's just a hint, not a requirement to try this new address first.
		as_store_uint32(&node->address_index, rv);
		as_log_debug("Change node address %s %s", node->name, as_node_get_address_string(node));
	}
	as_incr_uint32(&node->sync_conns_opened);
	return AEROSPIKE_OK;
}

static as_status
as_node_create_connection(
	as_error* err, as_node* node, uint32_t socket_timeout, uint64_t deadline_ms, as_conn_pool* pool,
	as_socket* sock
	)
{
	as_status status = as_node_create_socket(err, node, pool, sock, deadline_ms);

	if (status) {
		return status;
	}

	// Authenticate connection.
	as_cluster* cluster = node->cluster;

	if (cluster->auth_enabled) {
		// Must reserve session because not called from cluster tend thread.
		as_session* session = (as_session*)as_load_ptr(&node->session);

		if (session) {
			as_incr_uint32(&session->ref_count);
			status = as_authenticate(cluster, err, sock, node, session, socket_timeout, deadline_ms);
			as_session_release(session);

			if (status) {
				as_node_signal_login(node);
				as_node_close_socket(node, sock);
				return status;
			}
		}
	}
	return AEROSPIKE_OK;
}

static void
as_node_create_connections(as_node* node, as_conn_pool* pool, uint32_t timeout_ms, int count)
{
	as_error err;
	as_status status;
	as_socket sock;

	// Create sync connections.
	while (count > 0) {
		uint64_t deadline_ms = as_socket_deadline(timeout_ms);
		status = as_node_create_connection(&err, node, 0, deadline_ms, pool, &sock);

		if (status != AEROSPIKE_OK) {
			as_log_debug("Failed to create min connections: %d %s", err.code, err.message);
			return;
		}

		// Update last used timestamp.
		sock.last_used = cf_getns();

		// Put into pool.
		if (as_conn_pool_push_head(pool, &sock)) {
			as_conn_pool_incr(pool);
		}
		else {
			as_node_close_socket(node, &sock);
			break;
		}
		count--;
	}
}

as_status
as_node_authenticate_connection(as_cluster* cluster, uint64_t deadline_ms)
{
	as_node* node = as_node_get_random(cluster);

	if (! node) {
		return AEROSPIKE_ERR_INVALID_NODE;
	}

	as_socket sock;
	as_error err;
	as_status status = as_node_create_connection(&err, node, 0, deadline_ms, NULL, &sock);

	if (status == AEROSPIKE_OK) {
		as_node_close_socket(node, &sock);
	}
	as_node_release(node);
	return status;
}

as_status
as_node_get_connection(as_error* err, as_node* node, uint32_t socket_timeout, uint64_t deadline_ms, as_socket* sock)
{
	as_conn_pool* pools = node->sync_conn_pools;
	as_cluster* cluster = node->cluster;
	uint32_t max = cluster->conn_pools_per_node;
	uint32_t initial_index;
	bool backward;

	if (max == 1) {
		initial_index = 0;
		backward = false;
	}
	else {
		uint32_t iter = node->conn_iter++; // not atomic by design
		initial_index = iter % max;
		backward = true;
	}

	as_socket s;
	as_conn_pool* pool = &pools[initial_index];
	uint32_t pool_index = initial_index;

	while (true) {
		if (as_conn_pool_pop_head(pool, &s)) {
			// Found socket. Verify that socket is active.
			if (! as_socket_current_tran(s.last_used, cluster->max_socket_idle_ns_tran)) {
				as_node_close_connection(node, &s, pool);
				continue;
			}

			// Verify that socket receive buffer is empty.
			int len = as_socket_validate_fd(s.fd);

			if (len != 0) {
				as_log_debug("Invalid socket %d from pool: %d", s.fd, len);
				as_node_close_conn_error(node, &s, pool);
				continue;
			}

			*sock = s;
			sock->pool = pool;
			return AEROSPIKE_OK;
		}
		else if (as_conn_pool_incr(pool)) {
			// Socket not found and queue has available slot.
			// Create new connection.
			as_status status = as_node_create_connection(err, node, socket_timeout, deadline_ms,
														 pool, sock);

			if (status != AEROSPIKE_OK) {
				as_conn_pool_decr(pool);
			}
			return status;
		}
		else {
			// Socket not found and queue is full.  Try another queue.
			as_conn_pool_decr(pool);

			if (backward) {
				if (pool_index > 0) {
					pool_index--;
				}
				else {
					pool_index = initial_index;

					if (++pool_index >= max) {
						break;
					}
					backward = false;
				}
			}
			else if (++pool_index >= max) {
				break;
			}
			pool = &pool[pool_index];
		}
	}
	// All queues full.
	return as_error_update(err, AEROSPIKE_ERR_NO_MORE_CONNECTIONS,
						   "Max node %s connections would be exceeded: %u",
						   node->name, cluster->max_conns_per_node);
}

static void
as_node_close_idle_connections(as_node* node, as_conn_pool* pool, int count)
{
	uint64_t max_socket_idle_ns = node->cluster->max_socket_idle_ns_trim;
	as_socket s;

	while (count > 0) {
		if (! as_conn_pool_pop_tail(pool, &s)) {
			break;
		}

		if (as_socket_current_trim(s.last_used, max_socket_idle_ns)) {
			if (! as_conn_pool_push_tail(pool, &s)) {
				as_node_close_connection(node, &s, pool);
			}
			break;
		}
		as_node_close_connection(node, &s, pool);
		count--;
	}
}

void
as_node_balance_connections(as_node* node)
{
	as_conn_pool* pools = node->sync_conn_pools;
	as_cluster* cluster = node->cluster;
	uint32_t max = cluster->conn_pools_per_node;
	uint32_t timeout_ms = cluster->conn_timeout_ms;

	for (uint32_t i = 0; i < max; i++) {
		as_conn_pool* pool = &pools[i];
		int excess = as_conn_pool_excess(pool);

		if (excess > 0) {
			as_node_close_idle_connections(node, pool, excess);
		}
		else if (excess < 0 && as_node_valid_error_count(node)) {
			as_node_create_connections(node, pool, timeout_ms, -excess);
		}
	}
}

void
as_node_signal_login(as_node* node)
{
	// Only login when login not already been requested.
	if (as_cas_uint8(&node->perform_login, 0, 1)) {
		// Signal tend thread to wake up from sleep, so node tend will occur faster.
		as_cluster* cluster = node->cluster;

		pthread_mutex_lock(&cluster->tend_lock);
		pthread_cond_signal(&cluster->tend_cond);
		pthread_mutex_unlock(&cluster->tend_lock);
	}
}

/**
 * Use non-inline function for garbarge collector function pointer reference.
 * Forward to inlined release.
 */
static void
release_session(as_session* session)
{
	as_session_release(session);
}

static as_status
as_node_login(as_error* err, as_node* node, as_socket* sock)
{
	as_node_info node_info;
	as_cluster* cluster = node->cluster;
	uint64_t deadline_ms = as_socket_deadline(cluster->login_timeout_ms);
	as_status status = as_cluster_login(cluster, err, sock, deadline_ms, &node_info);

	if (status) {
		as_fence_store();
		as_store_uint8(&node->perform_login, 1);
		as_error_append(err, as_node_get_address_string(node));
		return status;
	}

	as_session* old = node->session;

	as_fence_store();
	as_store_ptr(&node->session, node_info.session);
	as_store_uint8(&node->perform_login, 0);

	if (old) {
		// Put old session on garbage collector stack.
		as_gc_item item;
		item.data = old;
		item.release_fn = (as_release_fn)release_session;
		as_vector_append(cluster->gc, &item);
	}
	return AEROSPIKE_OK;
}

as_status
as_node_ensure_login_shm(as_error* err, as_node* node)
{
	if (as_load_uint8(&node->perform_login) ||
		(node->session && node->session->expiration > 0 && cf_getns() >= node->session->expiration)) {
		as_socket sock;
		uint64_t deadline_ms = as_socket_deadline(node->cluster->conn_timeout_ms);
		as_status status = as_node_create_socket(err, node, NULL, &sock, deadline_ms);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		status = as_node_login(err, node, &sock);

		if (status != AEROSPIKE_OK) {
			as_node_close_socket(node, &sock);
			return status;
		}

		// Shared memory prole tender only needs updated session token and not the socket.
		// Close socket immediately.
		as_node_close_socket(node, &sock);
	}
	return AEROSPIKE_OK;
}

static bool
as_node_should_login(as_node* node)
{
	// Return true if previous user authentication failed or session token expired.
	return as_load_uint8(&node->perform_login) ||
		(node->session && node->session->expiration > 0 && cf_getns() >= node->session->expiration);
}

bool
as_node_has_rack(as_cluster* cluster, as_node* node, const char* ns, int rack_id)
{
	as_racks* racks = (as_racks*)as_load_ptr(&node->racks);

	if (! racks) {
		return false;
	}

	// Reserve racks.
	as_incr_uint32(&racks->ref_count);

	// Try optimized check.
	if (racks->size == 0) {
		bool result = (racks->rack_id == rack_id);
		as_racks_release(racks);
		return result;
	}

	// Must search through namespaces.
	as_rack* r = racks->racks;

	for (uint32_t i = 0; i < racks->size; i++) {
		if (strcmp(r->ns, ns) == 0) {
			bool result = (r->rack_id == rack_id);
			as_racks_release(racks);
			return result;
		}
		r++;
	}
	as_racks_release(racks);
	return false;
}

static as_status
as_node_get_tend_connection(as_error* err, as_node* node)
{
	as_cluster* cluster = node->cluster;
	as_status status = AEROSPIKE_OK;

	if (node->info_socket.fd < 0) {
		// Try to open a new socket.
		as_socket sock;
		uint64_t deadline_ms = as_socket_deadline(cluster->conn_timeout_ms);
		status = as_node_create_socket(err, node, NULL, &sock, deadline_ms);

		if (status) {
			return status;
		}

		if (cluster->auth_enabled) {
			if (!node->session || as_node_should_login(node)) {
				status = as_node_login(err, node, &sock);

				if (status != AEROSPIKE_OK) {
					as_node_close_socket(node, &sock);
					return status;
				}
			}
			else {
				status = as_authenticate(cluster, err, &sock, node, node->session, 0, deadline_ms);

				if (status != AEROSPIKE_OK) {
					// Authentication failed.
					// Must login again to get new session token.
					status = as_node_login(err, node, &sock);

					if (status != AEROSPIKE_OK) {
						as_node_close_socket(node, &sock);
						return status;
					}
				}
			}
		}
		node->info_socket = sock;
	}
	else {
		if (cluster->auth_enabled && as_node_should_login(node)) {
			status = as_node_login(err, node, &node->info_socket);

			if (status != AEROSPIKE_OK) {
				as_node_close_socket(node, &node->info_socket);
				return status;
			}
		}
	}
	return status;
}

static uint8_t*
as_node_get_info(as_error* err, as_node* node, const char* names, size_t names_len, uint64_t deadline_ms, uint8_t* stack_buf)
{
	as_socket* sock = &node->info_socket;
	
	// Prepare the write request buffer.
	size_t write_size = sizeof(as_proto) + names_len;
	as_proto* proto = (as_proto*)stack_buf;
	
	proto->sz = names_len;
	proto->version = AS_PROTO_VERSION;
	proto->type = AS_INFO_MESSAGE_TYPE;
	as_proto_swap_to_be(proto);
	
	memcpy((void*)(stack_buf + sizeof(as_proto)), (const void*)names, names_len);

	// Write the request. Note that timeout_ms is never 0.
	if (as_socket_write_deadline(err, sock, node, stack_buf, write_size, 0, deadline_ms) != AEROSPIKE_OK) {
		return 0;
	}
	
	// Reuse the buffer, read the response - first 8 bytes contains body size.
	if (as_socket_read_deadline(err, sock, node, stack_buf, sizeof(as_proto), 0, deadline_ms) != AEROSPIKE_OK) {
		return 0;
	}
	
	proto = (as_proto*)stack_buf;

	as_status status = as_proto_parse_type(err, proto, AS_INFO_MESSAGE_TYPE);

	if (status) {
		return 0;
	}

	// Sanity check body size.
	if (proto->sz == 0 || proto->sz > 512 * 1024) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid info response size %lu", proto->sz);
		return 0;
	}
	
	// Allocate a buffer if the response is bigger than the stack buffer -
	// caller must free it if this call succeeds. Note that proto is overwritten
	// if stack_buf is used, so we save the sz field here.
	size_t proto_sz = proto->sz;
	uint8_t* rbuf = proto_sz >= INFO_STACK_BUF_SIZE ? (uint8_t*)cf_malloc(proto_sz + 1) : stack_buf;
	
	if (! rbuf) {
		as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Allocation failed for info response");
		return 0;
	}
	
	// Read the response body.
	if (as_socket_read_deadline(err, sock, node, rbuf, proto_sz, 0, deadline_ms) != AEROSPIKE_OK) {
		if (rbuf != stack_buf) {
			cf_free(rbuf);
		}
		return 0;
	}
	
	// Null-terminate the response body and return it.
	rbuf[proto_sz] = 0;
	return rbuf;
}

static as_status
as_node_verify_name(as_error* err, as_node* node, const char* name)
{
	if (name == 0 || *name == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Node name not returned from info request.");
	}
	
	if (strcmp(node->name, name) != 0) {
		// Set node to inactive immediately.
		// Make volatile write so changes are reflected in other threads.
		as_store_uint8(&node->active, false);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node name has changed. Old=%s New=%s", node->name, name);
	}
	return AEROSPIKE_OK;
}

static void
as_node_restart(as_cluster* cluster, as_node* node)
{
	if (cluster->max_error_rate > 0) {
		as_node_reset_error_count(node);
	}

	// Balance sync connections.
	as_node_balance_connections(node);

	if (as_event_loop_capacity > 0 && !as_event_single_thread) {
		as_event_node_balance_connections(cluster, node);
	}
}

static const char INFO_STR_CHECK_RACK[] = "node\npeers-generation\npartition-generation\nrebalance-generation\n";
static const char INFO_STR_CHECK_PEERS[] = "node\npeers-generation\npartition-generation\n";

static as_status
as_node_process_response(as_cluster* cluster, as_error* err, as_node* node, as_vector* values,
						 as_peers* peers)
{
	for (uint32_t i = 0; i < values->size; i++) {
		as_name_value* nv = as_vector_get(values, i);
		
		if (strcmp(nv->name, "node") == 0) {
			as_status status = as_node_verify_name(err, node, nv->value);
			
			if (status != AEROSPIKE_OK) {
				return status;
			}
		}
		else if (strcmp(nv->name, "peers-generation") == 0) {
			uint32_t gen = (uint32_t)strtoul(nv->value, NULL, 10);
			if (node->peers_generation != gen) {
				peers->gen_changed = true;

				if (node->peers_generation > gen && node->peers_generation != 0xFFFFFFFF) {
					as_log_info("Quick node restart detected: node=%s oldgen=%u newgen=%u",
						as_node_get_address_string(node), node->peers_generation, gen);

					as_node_restart(cluster, node);
				}
			}
		}
		else if (strcmp(nv->name, "partition-generation") == 0) {
			uint32_t gen = (uint32_t)strtoul(nv->value, NULL, 10);
			if (node->partition_generation != gen) {
				node->partition_changed = true;
			}
		}
		else if (strcmp(nv->name, "rebalance-generation") == 0) {
			uint32_t gen = (uint32_t)strtoul(nv->value, NULL, 10);
			if (node->rebalance_generation != gen) {
				node->rebalance_changed = true;
			}
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node %s did not request info '%s'", node->name, nv->name);
		}
	}
	return AEROSPIKE_OK;
}

/**
 * Request current status from server node.
 */
as_status
as_node_refresh(as_cluster* cluster, as_error* err, as_node* node, as_peers* peers)
{
	as_status status = as_node_get_tend_connection(err, node);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}

	// Set new deadline because login may have occurred which can take a long time.
	uint64_t deadline_ms = as_socket_deadline(cluster->conn_timeout_ms);

	const char* command;
	size_t command_len;
	
	if (cluster->rack_aware) {
		command = INFO_STR_CHECK_RACK;
		command_len = sizeof(INFO_STR_CHECK_RACK) - 1;
	}
	else {
		command = INFO_STR_CHECK_PEERS;
		command_len = sizeof(INFO_STR_CHECK_PEERS) - 1;
	}

	uint8_t stack_buf[INFO_STACK_BUF_SIZE];
	uint8_t* buf = as_node_get_info(err, node, command, command_len, deadline_ms, stack_buf);
	
	if (! buf) {
		as_node_close_socket(node, &node->info_socket);
		return err->code;
	}
	
	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), 4);
	
	as_info_parse_multi_response((char*)buf, &values);

	status = as_node_process_response(cluster, err, node, &values, peers);

	if (status == AEROSPIKE_ERR_CLIENT) {
		as_node_close_socket(node, &node->info_socket);
	}
		
	if (buf != stack_buf) {
		cf_free(buf);
	}

	as_vector_destroy(&values);

	if (status == AEROSPIKE_OK) {
		peers->refresh_count++;

		// Reload peers, partitions and racks if there were failures on previous tend.
		if (node->failures > 0) {
			peers->gen_changed = true;
			node->partition_changed = true;
			node->rebalance_changed = cluster->rack_aware;
		}
		node->failures = 0;
	}
	return status;
}

static const char INFO_STR_PEERS_TLS_ALT[] = "peers-tls-alt\n";
static const char INFO_STR_PEERS_TLS_STD[] = "peers-tls-std\n";
static const char INFO_STR_PEERS_CLEAR_ALT[] = "peers-clear-alt\n";
static const char INFO_STR_PEERS_CLEAR_STD[] = "peers-clear-std\n";

static as_status
as_node_process_peers(as_cluster* cluster, as_error* err, as_node* node, as_vector* values, as_peers* peers)
{
	for (uint32_t i = 0; i < values->size; i++) {
		as_name_value* nv = as_vector_get(values, i);
		
		if (strcmp(nv->name, "peers-tls-alt") == 0 ||
			strcmp(nv->name, "peers-tls-std") == 0 ||
			strcmp(nv->name, "peers-clear-alt") == 0 ||
			strcmp(nv->name, "peers-clear-std") == 0
			) {
			as_status status = as_peers_parse_peers(peers, err, cluster, node, nv->value);
			
			if (status != AEROSPIKE_OK) {
				return status;
			}
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node %s did not request info '%s'", node->name, nv->name);
		}
	}
	return AEROSPIKE_OK;
}

as_status
as_node_refresh_peers(as_cluster* cluster, as_error* err, as_node* node, as_peers* peers)
{
	as_log_debug("Update peers for node %s", as_node_get_address_string(node));

	uint64_t deadline_ms = as_socket_deadline(cluster->conn_timeout_ms);
	const char* command;
	size_t command_len;

	if (cluster->tls_ctx) {
		if (cluster->use_services_alternate) {
			command = INFO_STR_PEERS_TLS_ALT;
			command_len = sizeof(INFO_STR_PEERS_TLS_ALT) - 1;
		}
		else {
			command = INFO_STR_PEERS_TLS_STD;
			command_len = sizeof(INFO_STR_PEERS_TLS_STD) - 1;
		}
	}
	else {
		if (cluster->use_services_alternate) {
			command = INFO_STR_PEERS_CLEAR_ALT;
			command_len = sizeof(INFO_STR_PEERS_CLEAR_ALT) - 1;
		}
		else {
			command = INFO_STR_PEERS_CLEAR_STD;
			command_len = sizeof(INFO_STR_PEERS_CLEAR_STD) - 1;
		}
	}
	
	uint8_t stack_buf[INFO_STACK_BUF_SIZE];
	uint8_t* buf = as_node_get_info(err, node, command, command_len, deadline_ms, stack_buf);
	
	if (! buf) {
		as_node_close_socket(node, &node->info_socket);
		return err->code;
	}
	
	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), 4);
	
	as_info_parse_multi_response((char*)buf, &values);
	as_status status = as_node_process_peers(cluster, err, node, &values, peers);
	
	if (buf != stack_buf) {
		cf_free(buf);
	}
	
	as_vector_destroy(&values);

	if (status == AEROSPIKE_OK) {
		peers->refresh_count++;
	}
	return status;
}

static const char INFO_STR_GET_REPLICAS_REGIME[] = "partition-generation\nreplicas\n";

static as_status
as_node_process_partitions(as_cluster* cluster, as_error* err, as_node* node, as_vector* values)
{
	for (uint32_t i = 0; i < values->size; i++) {
		as_name_value* nv = as_vector_get(values, i);
		
		if (strcmp(nv->name, "partition-generation") == 0) {
			node->partition_generation = (uint32_t)strtoul(nv->value, NULL, 10);
		}
		else if (strcmp(nv->name, "replicas") == 0) {
			as_partition_tables_update_all(cluster, node, nv->value, true);
		}
		else if (strcmp(nv->name, "replicas-all") == 0) {
			as_partition_tables_update_all(cluster, node, nv->value, false);
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node %s did not request info '%s'", node->name, nv->name);
		}
	}
	return AEROSPIKE_OK;
}

as_status
as_node_refresh_partitions(as_cluster* cluster, as_error* err, as_node* node, as_peers* peers)
{
	as_log_debug("Update partition map for node %s", as_node_get_address_string(node));

	uint64_t deadline_ms = as_socket_deadline(cluster->conn_timeout_ms);
	const char* command = INFO_STR_GET_REPLICAS_REGIME;
	size_t command_len = sizeof(INFO_STR_GET_REPLICAS_REGIME) - 1;

	uint8_t stack_buf[INFO_STACK_BUF_SIZE];
	uint8_t* buf = as_node_get_info(err, node, command, command_len, deadline_ms, stack_buf);

	if (! buf) {
		as_node_close_socket(node, &node->info_socket);
		return err->code;
	}

	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), 4);

	as_info_parse_multi_response((char*)buf, &values);
	as_status status = as_node_process_partitions(cluster, err, node, &values);

	if (buf != stack_buf) {
		cf_free(buf);
	}

	as_vector_destroy(&values);
	return status;
}

/**
 * Use non-inline function for garbarge collector function pointer reference.
 * Forward to inlined release.
 */
static void
release_racks(as_racks* racks)
{
	as_racks_release(racks);
}

static void
as_node_replace_racks(as_cluster* cluster, as_node* node, as_racks* racks)
{
	racks->ref_count = 1;
	racks->pad = 0;

	if (cluster->shm_info) {
		as_shm_node_replace_racks(cluster->shm_info->cluster_shm, node, racks);
	}

	as_racks* old = node->racks;

	as_fence_store();
	as_store_ptr(&node->racks, racks);

	if (old) {
		// Put old racks on garbage collector stack.
		as_gc_item item;
		item.data = old;
		item.release_fn = (as_release_fn)release_racks;
		as_vector_append(cluster->gc, &item);
	}
}

static as_status
as_node_parse_racks(as_cluster* cluster, as_error* err, as_node* node, char* buf)
{
	// Use destructive parsing (ie modifying input buffer with null termination) for performance.
	// Receive format: <ns1>:<rack1>;<ns2>:<rack2>...\n

	// First, check if rack_ids are the same and get size of racks array.
	int rack_id = 0;
	bool first = true;
	bool same = true;
	char* p = buf;
	uint32_t size = 0;

	while (*p) {
		if (*p == ':') {
			p++;
			size++;

			if (same) {
				int r = (int)strtol(p, NULL, 10);

				if (first) {
					rack_id = r;
					first = false;
				}
				else if (rack_id != r) {
					same = false;
				}
			}
		}
		else {
			p++;
		}
	}

	if (same) {
		// Create optimized version of racks structure.
		as_racks* racks = cf_malloc(sizeof(as_racks));
		racks->rack_id = rack_id;
		racks->size = 0;
		as_node_replace_racks(cluster, node, racks);
		return AEROSPIKE_OK;
	}

	// Parse unoptimized version of racks structure.
	as_racks* racks = cf_malloc(sizeof(as_racks) + (sizeof(as_rack) * size));
	racks->rack_id = 0;
	racks->size = size;

	p = buf;
	char* begin = 0;
	char* ns = p;
	int64_t len;
	int current = 0;

	while (*p) {
		if (*p == ':') {
			// Parse namespace.
			*p = 0;
			len = p - ns;

			if (len <= 0 || len >= 32) {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									   "Racks update. Invalid rack namespace %s", ns);
			}
			begin = ++p;

			// Parse rack.
			while (*p) {
				if (*p == ';' || *p == '\n') {
					*p = 0;
					break;
				}
				p++;
			}

			int r = (int)strtol(begin, NULL, 10);
			as_rack* rack = &racks->racks[current++];

			strcpy(rack->ns, ns);
			rack->rack_id = r;
			ns = ++p;
		}
		else {
			p++;
		}
	}

	as_node_replace_racks(cluster, node, racks);
	return AEROSPIKE_OK;
}

static as_status
as_node_process_racks(as_cluster* cluster, as_error* err, as_node* node, as_vector* values)
{
	for (uint32_t i = 0; i < values->size; i++) {
		as_name_value* nv = as_vector_get(values, i);

		if (strcmp(nv->name, "rebalance-generation") == 0) {
			node->rebalance_generation = (uint32_t)strtoul(nv->value, NULL, 10);
		}
		else if (strcmp(nv->name, "rack-ids") == 0) {
			return as_node_parse_racks(cluster, err, node, nv->value);
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node %s did not request info '%s'", node->name, nv->name);
		}
	}
	return AEROSPIKE_OK;
}

static const char INFO_STR_GET_RACKS[] = "rebalance-generation\nrack-ids\n";

as_status
as_node_refresh_racks(as_cluster* cluster, as_error* err, as_node* node)
{
	as_log_debug("Update racks for node %s", as_node_get_address_string(node));

	uint64_t deadline_ms = as_socket_deadline(cluster->conn_timeout_ms);

	uint8_t stack_buf[INFO_STACK_BUF_SIZE];
	uint8_t* buf = as_node_get_info(err, node, INFO_STR_GET_RACKS, sizeof(INFO_STR_GET_RACKS) - 1, deadline_ms, stack_buf);

	if (! buf) {
		as_node_close_socket(node, &node->info_socket);
		return err->code;
	}

	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), 4);

	as_info_parse_multi_response((char*)buf, &values);
	as_status status = as_node_process_racks(cluster, err, node, &values);

	if (buf != stack_buf) {
		cf_free(buf);
	}

	as_vector_destroy(&values);
	return status;
}
