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
#include <aerospike/as_node.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_event_internal.h>
#include <aerospike/as_info.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_queue.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_string.h>
#include <citrusleaf/cf_byte_order.h>
#include <errno.h> //errno

// Replicas take ~2K per namespace, so this will cover most deployments:
#define INFO_STACK_BUF_SIZE (16 * 1024)

/******************************************************************************
 *	Function declarations.
 *****************************************************************************/

bool
as_partition_tables_update(struct as_cluster_s* cluster, as_node* node, char* buf, bool master);

bool
as_partition_tables_update_all(as_cluster* cluster, as_node* node, char* buf);

extern uint32_t as_event_loop_capacity;

/******************************************************************************
 *	Functions.
 *****************************************************************************/

static as_queue*
as_node_create_async_queues(uint32_t max_conns_per_node)
{
	// Create one queue per event manager.
	as_queue* queues = cf_malloc(sizeof(as_queue) * as_event_loop_capacity);
	
	// Distribute max_conns_per_node over event loops taking remainder into account.
	uint32_t max = max_conns_per_node / as_event_loop_capacity;
	uint32_t rem = max_conns_per_node - (max * as_event_loop_capacity);
	uint32_t capacity;
	
	for (uint32_t i = 0; i < as_event_loop_capacity; i++) {
		capacity = i < rem ? max + 1 : max;
		as_queue_init(&queues[i], sizeof(void*), capacity);
	}
	return queues;
}

as_node*
as_node_create(as_cluster* cluster, as_host* host, struct sockaddr_in* addr, as_node_info* node_info)
{
	as_node* node = cf_malloc(sizeof(as_node));

	if (!node) {
		return 0;
	}
	
	node->ref_count = 1;
	node->partition_generation = 0xFFFFFFFF;
	node->cluster = cluster;
			
	strcpy(node->name, node_info->name);
	node->has_batch_index = node_info->has_batch_index;
	node->has_replicas_all = node_info->has_replicas_all;
	node->has_double = node_info->has_double;
	node->has_geo = node_info->has_geo;
	node->address_index = 0;
	
	as_vector_init(&node->addresses, sizeof(as_address), 2);
	as_vector_init(&node->aliases, sizeof(as_host), 2);
	as_node_add_address(node, host, addr);
		
	node->conn_q = cf_queue_create(sizeof(int), true);
	
	// Initialize async queue.
	if (as_event_loop_capacity > 0) {
		node->async_conn_qs = as_node_create_async_queues(cluster->async_max_conns_per_node);
		node->pipe_conn_qs = as_node_create_async_queues(cluster->pipe_max_conns_per_node);
	}
	else {
		node->async_conn_qs = 0;
		node->pipe_conn_qs = 0;
	}

	node->info_fd = node_info->fd;
	node->conn_count = 0;
	node->friends = 0;
	node->failures = 0;
	node->index = 0;
	node->active = true;
	return node;
}

void
as_node_destroy(as_node* node)
{
	// Drain out connection queues and close the FDs
	int	fd;
	
	while (cf_queue_pop(node->conn_q, &fd, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
		as_node_close_connection(node, fd);
	}
		
	if (node->info_fd >= 0) {
		as_close(node->info_fd);
	}

	// Release memory
	as_vector_destroy(&node->addresses);
	as_vector_destroy(&node->aliases);
	cf_queue_destroy(node->conn_q);
	
	if (as_event_loop_capacity > 0) {
		// Close async and pipeline connections.
		as_event_node_destroy(node);
	}

	cf_free(node);
}

void
as_node_add_address(as_node* node, as_host* host, struct sockaddr_in* addr)
{
	// Add IP address
	as_address address;
	address.addr = *addr;
	as_socket_address_name(addr, address.name);
	as_vector_append(&node->addresses, &address);
	
	if (! host) {
		return;
	}
	
	// Add alias if not IP address and does not already exist.
	struct in_addr addr_tmp;
	if (inet_aton(host->name, &addr_tmp)) {
		// Do not add IP address to aliases.
		return;
	}
	
	as_vector* aliases = &node->aliases;
	as_host* alias;
	
	for (uint32_t i = 0; i < aliases->size; i++) {
		alias = as_vector_get(aliases, i);
		
		if (as_host_equals(alias, host)) {
			// Already exists.
			return;
		}
	}
	
	// Add new alias.
	as_vector_append(aliases, host);
}

static inline as_status
as_node_authenticate_connection(as_error* err, as_node* node, uint64_t deadline_ms, int fd)
{
	as_cluster* cluster = node->cluster;
	
	if (cluster->user) {
		as_status status = as_authenticate(err, fd, cluster->user, cluster->password, deadline_ms);
		
		if (status) {
			as_node_close_connection(node, fd);
			return status;
		}
	}
	return AEROSPIKE_OK;
}

static as_status
as_node_create_connection(as_error* err, as_node* node, uint64_t deadline_ms, int* fd_out)
{
	// Create a non-blocking socket.
	int fd = as_socket_create_nb();
	
	if (fd < 0) {
		ck_pr_dec_32(&node->conn_count);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Socket create failed");
	}
	*fd_out = fd;
	
	as_error error_local;
	
	// Try primary address.
	as_address* primary = as_vector_get(&node->addresses, node->address_index);
	
	if (as_socket_start_connect_nb(&error_local, fd, &primary->addr) == AEROSPIKE_OK) {
		// Connection started ok - we have our socket.
		return as_node_authenticate_connection(err, node, deadline_ms, fd);
	}
	
	// Try other addresses.
	as_vector* addresses = &node->addresses;
	for (uint32_t i = 0; i < addresses->size; i++) {
		as_address* address = as_vector_get(addresses, i);
		
		// Address points into alias array, so pointer comparison is sufficient.
		if (address != primary) {
			if (as_socket_start_connect_nb(&error_local, fd, &address->addr) == AEROSPIKE_OK) {
				// Replace invalid primary address with valid alias.
				// Other threads may not see this change immediately.
				// It's just a hint, not a requirement to try this new address first.
				as_log_debug("Change node address %s %s:%d", node->name, address->name, (int)cf_swap_from_be16(address->addr.sin_port));
				ck_pr_store_32(&node->address_index, i);
				return as_node_authenticate_connection(err, node, deadline_ms, fd);
			}
		}
	}
	
	// Couldn't start a connection on any socket address - close the socket.
	as_node_close_connection(node, fd);
	return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Failed to connect: %s %s:%d",
			node->name, primary->name, (int)cf_swap_from_be16(primary->addr.sin_port))
}

as_status
as_node_get_connection(as_error* err, as_node* node, uint64_t deadline_ms, int* fd_out)
{
	cf_queue* q = node->conn_q;
	int fd;
	
	while (cf_queue_pop(q, &fd, CF_QUEUE_NOWAIT) == CF_QUEUE_OK) {
		// Verify that socket is active and receive buffer is empty.
		int len = as_socket_validate(fd);
		
		if (len == 0) {
			*fd_out = fd;
			return AEROSPIKE_OK;
		}

		as_log_debug("Invalid socket %d from pool: %d", fd, len);
		as_node_close_connection(node, fd);
	}
	
	// We exhausted the queue. Try creating a fresh socket.
	if (ck_pr_faa_32(&node->conn_count, 1) < node->cluster->conn_queue_size) {
		return as_node_create_connection(err, node, deadline_ms, fd_out);
	}
	else {
		ck_pr_dec_32(&node->conn_count);
		return as_error_update(err, AEROSPIKE_ERR_NO_MORE_CONNECTIONS,
						"Max node %s connections would be exceeded: %u",
						node->name, node->cluster->conn_queue_size);
	}
}

static inline int
as_node_get_info_connection(as_error* err, as_node* node, uint64_t deadline_ms)
{
	if (node->info_fd < 0) {
		// Try to open a new socket.
		return as_node_create_connection(err, node, deadline_ms, &node->info_fd);
	}
	return AEROSPIKE_OK;
}

static void
as_node_close_info_connection(as_node* node)
{
	shutdown(node->info_fd, SHUT_RDWR);
	as_close(node->info_fd);
	node->info_fd = -1;
}

static uint8_t*
as_node_get_info(as_error* err, as_node* node, const char* names, size_t names_len, uint64_t deadline_ms, uint8_t* stack_buf)
{
	int fd = node->info_fd;
	
	// Prepare the write request buffer.
	size_t write_size = sizeof(as_proto) + names_len;
	as_proto* proto = (as_proto*)stack_buf;
	
	proto->sz = names_len;
	proto->version = AS_MESSAGE_VERSION;
	proto->type = AS_INFO_MESSAGE_TYPE;
	as_proto_swap_to_be(proto);
	
	memcpy((void*)(stack_buf + sizeof(as_proto)), (const void*)names, names_len);

	// Write the request. Note that timeout_ms is never 0.
	if (as_socket_write_deadline(err, fd, stack_buf, write_size, deadline_ms) != AEROSPIKE_OK) {
		return 0;
	}
	
	// Reuse the buffer, read the response - first 8 bytes contains body size.
	if (as_socket_read_deadline(err, fd, stack_buf, sizeof(as_proto), deadline_ms) != AEROSPIKE_OK) {
		return 0;
	}
	
	proto = (as_proto*)stack_buf;
	as_proto_swap_from_be(proto);
	
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
	if (as_socket_read_deadline(err, fd, rbuf, proto_sz, deadline_ms) != AEROSPIKE_OK) {
		if (rbuf != stack_buf) {
			cf_free(rbuf);
		}
		return 0;
	}
	
	// Null-terminate the response body and return it.
	rbuf[proto_sz] = 0;
	return rbuf;
}

static bool
as_node_verify_name(as_node* node, const char* name)
{
	if (name == 0 || *name == 0) {
		as_log_warn("Node name not returned from info request.");
		return false;
	}
	
	if (strcmp(node->name, name) != 0) {
		// Set node to inactive immediately.
		as_log_warn("Node name has changed. Old=%s New=%s", node->name, name);
		
		// Make volatile write so changes are reflected in other threads.
		ck_pr_store_8(&node->active, false);

		return false;
	}
	return true;
}

static as_node*
as_cluster_find_node_by_address(as_cluster* cluster, in_addr_t addr, in_port_t port)
{
	as_nodes* nodes = (as_nodes*)cluster->nodes;
	as_node* node;
	as_vector* addresses;
	as_address* address;
	struct sockaddr_in* sockaddr;
	in_port_t port_be = cf_swap_to_be16(port);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		node = nodes->array[i];
		addresses = &node->addresses;
		
		for (uint32_t j = 0; j < addresses->size; j++) {
			address = as_vector_get(addresses, j);
			sockaddr = &address->addr;
			
			if (sockaddr->sin_addr.s_addr == addr && sockaddr->sin_port == port_be) {
				return node;
			}
		}
	}
	return NULL;
}

static as_node*
as_cluster_find_node_by_host(as_cluster* cluster, as_host* host)
{
	as_nodes* nodes = (as_nodes*)cluster->nodes;
	as_node* node;
	as_vector* aliases;
	as_host* alias;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		node = nodes->array[i];
		aliases = &node->aliases;
		
		for (uint32_t j = 0; j < aliases->size; j++) {
			alias = as_vector_get(aliases, j);
			
			if (as_host_equals(alias, host)) {
				return node;
			}
		}
	}
	return NULL;
}

static bool
as_find_friend(as_vector* /* <as_host> */ friends, as_host* host)
{
	as_host* friend;
	
	for (uint32_t i = 0; i < friends->size; i++) {
		friend = as_vector_get(friends, i);
		
		if (as_host_equals(friend, host)) {
			return true;
		}
	}
	return false;
}

static bool
as_check_alternate_address(as_cluster* cluster, as_host* host)
{
	// Check if there is an alternate address that should be used for this hostname.
	as_addr_maps* ip_map = as_ip_map_reserve(cluster);
	bool status = true;

	if (ip_map) {
		as_addr_map* entry = ip_map->array;
		
		for (uint32_t i = 0; i < ip_map->size; i++) {
			if (strcmp(entry->orig, host->name) == 0) {
				// Found mapping for this address.  Use alternate.
				as_log_debug("Using %s instead of %s", entry->alt, host->name);
				
				if (as_strncpy(host->name, entry->alt, sizeof(host->name))) {
					as_log_warn("Hostname has been truncated: %s", host->name);
					status = false;
				}
				break;
			}
			entry++;
		}
		as_ip_map_release(ip_map);
	}
	return status;
}

static void
as_node_add_friends(as_cluster* cluster, as_node* node, char* buf, as_vector* /* <as_host> */ friends)
{
	// Friends format: <host1>:<port1>;<host2>:<port2>;...
	if (buf == 0 || *buf == 0) {
		// Must be a single node cluster.
		return;
	}
	
	// Use single pass parsing.
	char* p = buf;
	char* addr_str = p;
	char* port_str;
	struct in_addr addr_tmp;
	as_host friend;
	as_node* friend_node;
	
	while (*p) {
		if (*p == ':') {
			*p = 0;
			port_str = ++p;
			
			while (*p) {
				if (*p == ';') {
					*p = 0;
					break;
				}
				p++;
			}
			
			if (as_strncpy(friend.name, addr_str, sizeof(friend.name))) {
				as_log_warn("Hostname has been truncated: %s", friend.name);
				addr_str = ++p;
				continue;
			}
			
			friend.port = atoi(port_str);
			
			if (friend.port == 0) {
				as_log_warn("Invalid port: %s", port_str);
				addr_str = ++p;
				continue;
			}
			
			if (! as_check_alternate_address(cluster, &friend)) {
				addr_str = ++p;
				continue;
			}
			
			if (inet_aton(friend.name, &addr_tmp)) {
				// Address is an IP Address
				friend_node = as_cluster_find_node_by_address(cluster, addr_tmp.s_addr, friend.port);
			}
			else {
				// Address is a hostname.
				friend_node = as_cluster_find_node_by_host(cluster, &friend);
			}
						
			if (friend_node) {
				friend_node->friends++;
			}
			else {
				if (! as_find_friend(friends, &friend)) {
					as_vector_append(friends, &friend);
				}
			}
			addr_str = ++p;
		}
		else {
			p++;
		}
	}
}

static bool
as_node_process_response(as_cluster* cluster, as_node* node, as_vector* values,
	as_vector* /* <as_host> */ friends, bool* update_partitions)
{
	bool status = false;
	*update_partitions = false;
	
	for (uint32_t i = 0; i < values->size; i++) {
		as_name_value* nv = as_vector_get(values, i);
		
		if (strcmp(nv->name, "node") == 0) {
			if (as_node_verify_name(node, nv->value)) {
				status = true;
			}
			else {
				status = false;
				break;
			}
		}
		else if (strcmp(nv->name, "partition-generation") == 0) {
			uint32_t gen = (uint32_t)atoi(nv->value);
			if (node->partition_generation != gen) {
				as_log_debug("Node %s partition generation changed: %u", node->name, gen);
				*update_partitions = true;
			}
		}
		else if (strcmp(nv->name, "services") == 0 || strcmp(nv->name, "services-alternate") == 0) {
			as_node_add_friends(cluster, node, nv->value, friends);
		}
		else {
			as_log_warn("Node %s did not request info '%s'", node->name, nv->name);
		}
	}
	return status;
}

static void
as_node_process_partitions(as_cluster* cluster, as_node* node, as_vector* values)
{
	for (uint32_t i = 0; i < values->size; i++) {
		as_name_value* nv = as_vector_get(values, i);
		
		if (strcmp(nv->name, "partition-generation") == 0) {
			node->partition_generation = (uint32_t)atoi(nv->value);
		}
		else if (strcmp(nv->name, "replicas-all") == 0) {
			as_partition_tables_update_all(cluster, node, nv->value);
		}
		else if (strcmp(nv->name, "replicas-master") == 0) {
			as_partition_tables_update(cluster, node, nv->value, true);
		}
		else if (strcmp(nv->name, "replicas-prole") == 0) {
			as_partition_tables_update(cluster, node, nv->value, false);
		}
		else {
			as_log_warn("Node %s did not request info '%s'", node->name, nv->name);
		}
	}
}

const char INFO_STR_CHECK[] = "node\npartition-generation\nservices\n";
const char INFO_STR_CHECK_SVCALT[] = "node\npartition-generation\nservices-alternate\n";
const char INFO_STR_GET_REPLICAS[] = "partition-generation\nreplicas-master\nreplicas-prole\n";
const char INFO_STR_GET_REPLICAS_ALL[] = "partition-generation\nreplicas-all\n";

/**
 *	Request current status from server node.
 */
as_status
as_node_refresh(as_cluster* cluster, as_error* err, as_node* node, as_vector* /* <as_host> */ friends)
{
	uint64_t deadline_ms = as_socket_deadline(cluster->conn_timeout_ms);
	as_status status = as_node_get_info_connection(err, node, deadline_ms);
	
	if (status != AEROSPIKE_OK) {
		return status;
	}
	
	uint8_t stack_buf[INFO_STACK_BUF_SIZE];
	uint8_t* buf;
	if (cluster->use_services_alternate) {
		buf = as_node_get_info(err, node, INFO_STR_CHECK_SVCALT, sizeof(INFO_STR_CHECK_SVCALT) - 1, deadline_ms, stack_buf);
	}
	else {
		buf = as_node_get_info(err, node, INFO_STR_CHECK, sizeof(INFO_STR_CHECK) - 1, deadline_ms, stack_buf);
	}
	
	if (! buf) {
		as_node_close_info_connection(node);
		return err->code;
	}
	
	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), 4);
	
	as_info_parse_multi_response((char*)buf, &values);
	
	bool update_partitions;
	bool response_status = as_node_process_response(cluster, node, &values, friends, &update_partitions);
		
	if (buf != stack_buf) {
		cf_free(buf);
	}
	
	if (response_status && update_partitions) {
		if (node->has_replicas_all) {
			buf = as_node_get_info(err, node, INFO_STR_GET_REPLICAS_ALL, sizeof(INFO_STR_GET_REPLICAS_ALL) - 1, deadline_ms, stack_buf);
		}
		else {
			buf = as_node_get_info(err, node, INFO_STR_GET_REPLICAS, sizeof(INFO_STR_GET_REPLICAS) - 1, deadline_ms, stack_buf);
		}

		if (! buf) {
			as_node_close_info_connection(node);
			as_vector_destroy(&values);
			return err->code;
		}
		
		as_vector_clear(&values);
		
		as_info_parse_multi_response((char*)buf, &values);

		if (buf) {
			as_node_process_partitions(cluster, node, &values);
			
			if (buf != stack_buf) {
				cf_free(buf);
			}
		}
	}
	
	as_vector_destroy(&values);
	return status;
}
