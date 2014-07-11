/******************************************************************************
 * Copyright 2008-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *****************************************************************************/

#include <aerospike/as_node.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_info.h>
#include <aerospike/as_string.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cf_log_internal.h>
#include <citrusleaf/cf_proto.h>
#include <citrusleaf/cf_socket.h>
#include <errno.h> //errno

// Replicas take ~2K per namespace, so this will cover most deployments:
#define INFO_STACK_BUF_SIZE (16 * 1024)

/******************************************************************************
 *	Function declarations.
 *****************************************************************************/

bool
as_partition_tables_update(struct as_cluster_s* cluster, as_node* node, char* buf, bool master);

/******************************************************************************
 *	Functions.
 *****************************************************************************/

as_node*
as_node_create(as_cluster* cluster, const char* name, struct sockaddr_in* addr)
{
	as_node* node = cf_malloc(sizeof(as_node));

	if (!node) {
		return 0;
	}
	
	node->ref_count = 1;
	node->partition_generation = 0xFFFFFFFF;
	node->cluster = cluster;
			
	strcpy(node->name, name);
	node->address_index = 0;
	
	as_vector_init(&node->addresses, sizeof(as_address), 2);
	as_node_add_address(node, addr);
		
	node->conn_q = cf_queue_create(sizeof(int), true);
	// node->conn_q_asyncfd = cf_queue_create(sizeof(int), true);
	// node->asyncwork_q = cf_queue_create(sizeof(cl_async_work*), true);
	
	node->info_fd = -1;
	node->friends = 0;
	node->failures = 0;
	node->active = true;
	return node;
}

void
as_node_destroy(as_node* node)
{
	// Drain out the queue and close the FDs
	int rv;
	do {
		int	fd;
		rv = cf_queue_pop(node->conn_q, &fd, CF_QUEUE_NOWAIT);
		if (rv == CF_QUEUE_OK)
			cf_close(fd);
	} while (rv == CF_QUEUE_OK);
	
	/*
	 do {
	 int	fd;
	 rv = cf_queue_pop(node->conn_q_asyncfd, &fd, CF_QUEUE_NOWAIT);
	 if (rv == CF_QUEUE_OK)
	 cf_close(fd);
	 } while (rv == CF_QUEUE_OK);
	 */
	
	/*
	 do {
	 //When we reach this point, ideally there should not be any workitems.
	 cl_async_work *aw;
	 rv = cf_queue_pop(node->asyncwork_q, &aw, CF_QUEUE_NOWAIT);
	 if (rv == CF_QUEUE_OK) {
	 free(aw);
	 }
	 } while (rv == CF_QUEUE_OK);
	 
	 //We want to delete all the workitems of this node
	 if (g_cl_async_hashtab) {
	 shash_reduce_delete(g_cl_async_hashtab, cl_del_node_asyncworkitems, node);
	 }
	 */
	
	as_vector_destroy(&node->addresses);
	cf_queue_destroy(node->conn_q);
	//cf_queue_destroy(node->conn_q_asyncfd);
	//cf_queue_destroy(node->asyncwork_q);
	
	if (node->info_fd >= 0) {
		cf_close(node->info_fd);
	}

	cf_free(node);
}

void
as_node_add_address(as_node* node, struct sockaddr_in* addr)
{
	as_address address;
	address.addr = *addr;
	as_socket_address_name(addr, address.name);
	as_vector_append(&node->addresses, &address);
}

// A quick non-blocking check to see if a server is connected. It may have
// dropped a connection while it's queued, so don't use those connections. If
// the fd is connected, we actually expect an error - ewouldblock or similar.
#define CONNECTED		0
#define CONNECTED_NOT	1
#define CONNECTED_ERROR	2
#define CONNECTED_BADFD	3

static int
is_connected(int fd)
{
	uint8_t buf[8];
	ssize_t rv = recv(fd, (void*)buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT | MSG_NOSIGNAL);
	
	if (rv == 0) {
		cf_debug("Connected check: Found disconnected fd %d", fd);
		return CONNECTED_NOT;
	}
	
	if (rv < 0) {
		if (errno == EBADF) {
			cf_warn("Connected check: Bad fd %d", fd);
			return CONNECTED_BADFD;
		}
		else if (errno == EWOULDBLOCK || errno == EAGAIN) {
			// The normal case.
			return CONNECTED;
		}
		else {
			cf_info("Connected check: fd %d error %d", fd, errno);
			return CONNECTED_ERROR;
		}
	}
	
	cf_info("Connected check: Peek got unexpected data for fd %d", fd);
	return CONNECTED;
}

static int
as_node_authenticate_connection(as_node* node, int* fd)
{
	as_cluster* cluster = node->cluster;
	
	if (cluster->user) {
		int status = as_authenticate(*fd, cluster->user, cluster->password, cluster->conn_timeout_ms);
		
		if (status) {
			cf_debug("Authentication failed for %s", cluster->user);
			cf_close(*fd);
			*fd = -1;
			return status;
		}
	}
	return 0;
}

static int
as_node_create_connection(as_node* node, int* fd)
{
	// Create a non-blocking socket.
	*fd = cf_socket_create_nb();
	
	if (*fd == -1) {
		// Local problem - socket create failed.
		cf_debug("Socket create failed for %s", node->name);
		return CITRUSLEAF_FAIL_CLIENT;
	}
	
	// Try primary address.
	as_address* primary = as_vector_get(&node->addresses, node->address_index);
	
	if (cf_socket_start_connect_nb(*fd, &primary->addr) == 0) {
		// Connection started ok - we have our socket.
		return as_node_authenticate_connection(node, fd);
	}
	
	// Try other addresses.
	as_vector* addresses = &node->addresses;
	for (uint32_t i = 0; i < addresses->size; i++) {
		as_address* address = as_vector_get(addresses, i);
		
		// Address points into alias array, so pointer comparison is sufficient.
		if (address != primary) {
			if (cf_socket_start_connect_nb(*fd, &address->addr) == 0) {
				// Replace invalid primary address with valid alias.
				// Other threads may not see this change immediately.
				// It's just a hint, not a requirement to try this new address first.
				cf_debug("Change node address %s %s:%d", node->name, address->name, (int)cf_swap_from_be16(address->addr.sin_port));
				ck_pr_store_32(&node->address_index, i);
				return as_node_authenticate_connection(node, fd);
			}
		}
	}
	
	// Couldn't start a connection on any socket address - close the socket.
	cf_info("Failed to connect: %s %s:%d", node->name, primary->name, (int)cf_swap_from_be16(primary->addr.sin_port));
	cf_close(*fd);
	*fd = -1;
	return CITRUSLEAF_FAIL_UNAVAILABLE;
}

int
as_node_get_connection(as_node* node, int* fd)
{
	//cf_queue* q = asyncfd ? node->conn_q_asyncfd : node->conn_q;
	cf_queue* q = node->conn_q;
	
	while (1) {
		int rv = cf_queue_pop(q, fd, CF_QUEUE_NOWAIT);
		
		if (rv == CF_QUEUE_OK) {
			int rv2 = is_connected(*fd);
			
			switch (rv2) {
				case CONNECTED:
					// It's still good.
					return 0;
					
				case CONNECTED_BADFD:
					// Local problem, don't try closing.
					cf_warn("Found bad file descriptor in queue: fd %d", *fd);
					break;
				
				case CONNECTED_NOT:
					// Can't use it - the remote end closed it.
				case CONNECTED_ERROR:
					// Some other problem, could have to do with remote end.
				default:
					cf_close(*fd);
					break;
			}
		}
		else if (rv == CF_QUEUE_EMPTY) {
			// We exhausted the queue. Try creating a fresh socket.
			return as_node_create_connection(node, fd);
		}
		else {
			cf_error("Bad return value from cf_queue_pop");
			*fd = -1;
			return CITRUSLEAF_FAIL_CLIENT;
		}
	}
}

void
as_node_put_connection(as_node* node, int fd)
{
	cf_queue *q = node->conn_q;
	if (! cf_queue_push_limit(q, &fd, 300)) {
		cf_close(fd);
	}
	
	/*
	if (asyncfd == true) {
		q = cn->conn_q_asyncfd;
		// Async queue is used by XDS. It can open lot of connections
		// depending on batch-size. Dont worry about limiting the pool.
		cf_queue_push(q, &fd);
	} else {
		q = cn->conn_q;
		if (! cf_queue_push_limit(q, &fd, 300)) {
			cf_close(fd);
		}
	}*/
}

static int
as_node_get_info_connection(as_node* node)
{
	if (node->info_fd < 0) {
		// Try to open a new socket.
		return as_node_create_connection(node, &node->info_fd);
	}
	return 0;
}

static void
as_node_close_info_connection(as_node* node)
{
	shutdown(node->info_fd, SHUT_RDWR);
	cf_close(node->info_fd);
	node->info_fd = -1;
}

static uint8_t*
as_node_get_info(as_node* node, const char* names, size_t names_len, int timeout_ms, uint8_t* stack_buf)
{
	int fd = node->info_fd;
	
	// Prepare the write request buffer.
	size_t write_size = sizeof(cl_proto) + names_len;
	cl_proto* proto = (cl_proto*)stack_buf;
	
	proto->sz = names_len;
	proto->version = CL_PROTO_VERSION;
	proto->type = CL_PROTO_TYPE_INFO;
	cl_proto_swap_to_be(proto);
	
	memcpy((void*)(stack_buf + sizeof(cl_proto)), (const void*)names, names_len);
	
	// Write the request. Note that timeout_ms is never 0.
	if (cf_socket_write_timeout(fd, stack_buf, write_size, 0, timeout_ms) != 0) {
		cf_debug("Node %s failed info socket write", node->name);
		return 0;
	}
	
	// Reuse the buffer, read the response - first 8 bytes contains body size.
	if (cf_socket_read_timeout(fd, stack_buf, sizeof(cl_proto), 0, timeout_ms) != 0) {
		cf_debug("Node %s failed info socket read header", node->name);
		return 0;
	}
	
	proto = (cl_proto*)stack_buf;
	cl_proto_swap_from_be(proto);
	
	// Sanity check body size.
	if (proto->sz == 0 || proto->sz > 512 * 1024) {
		cf_info("Node %s bad info response size %lu", node->name, proto->sz);
		return 0;
	}
	
	// Allocate a buffer if the response is bigger than the stack buffer -
	// caller must free it if this call succeeds. Note that proto is overwritten
	// if stack_buf is used, so we save the sz field here.
	size_t proto_sz = proto->sz;
	uint8_t* rbuf = proto_sz >= INFO_STACK_BUF_SIZE ? (uint8_t*)cf_malloc(proto_sz + 1) : stack_buf;
	
	if (! rbuf) {
		cf_error("Node %s failed allocation for info response", node->name);
		return 0;
	}
	
	// Read the response body.
	if (cf_socket_read_timeout(fd, rbuf, proto_sz, 0, timeout_ms) != 0) {
		cf_debug("Node %s failed info socket read body", node->name);
		
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
		cf_warn("Node name not returned from info request.");
		return false;
	}
	
	if (strcmp(node->name, name) != 0) {
		// Set node to inactive immediately.
		cf_warn("Node name has changed. Old=%s New=%s", node->name, name);
		
		// Make volatile write so changes are reflected in other threads.
		ck_pr_store_8(&node->active, false);

		return false;
	}
	return true;
}

static as_node*
as_cluster_find_node(as_cluster* cluster, in_addr_t addr, in_port_t port)
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
	return 0;
}

static bool
as_cluster_find_friend(as_vector* /* <as_friend> */ friends, in_addr_t addr, in_port_t port)
{
	as_friend* friend;
	
	for (uint32_t i = 0; i < friends->size; i++) {
		friend = as_vector_get(friends, i);
		
		if (friend->addr == addr && friend->port == port) {
			return true;
		}
	}
	return false;
}

static void
as_node_add_friends(as_cluster* cluster, as_node* node, char* buf, as_vector* /* <as_friend> */ friends)
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
	as_node* friend;
	struct in_addr addr_tmp;
	in_addr_t addr;
	in_port_t port;
	
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
			port = atoi(port_str);
			
			if (port > 0 && inet_aton(addr_str, &addr_tmp)) {
				addr = addr_tmp.s_addr;
				friend = as_cluster_find_node(cluster, addr, port);
				
				if (friend) {
					friend->friends++;
				}
				else {
					if (! as_cluster_find_friend(friends, addr, port)) {
						as_friend f;
						as_strncpy(f.name, addr_str, INET_ADDRSTRLEN);
						f.addr = addr;
						f.port = port;
						as_vector_append(friends, &f);
					}
				}
			}
			else {
				cf_warn("Invalid services address: %s:%d", addr_str, (int)port);
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
	as_vector* /* <as_friend> */ friends, bool* update_partitions)
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
				cf_debug("Node %s partition generation changed: %u", node->name, gen);
				*update_partitions = true;
			}
		}
		else if (strcmp(nv->name, "services") == 0) {
			as_node_add_friends(cluster, node, nv->value, friends);
		}
		else {
			cf_warn("Node %s did not request info '%s'", node->name, nv->name);
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
		else if (strcmp(nv->name, "replicas-master") == 0) {
			as_partition_tables_update(cluster, node, nv->value, true);
		}
		else if (strcmp(nv->name, "replicas-prole") == 0) {
			as_partition_tables_update(cluster, node, nv->value, false);
		}
		else {
			cf_warn("Node %s did not request info '%s'", node->name, nv->name);
		}
	}
}

const char INFO_STR_CHECK[] = "node\npartition-generation\nservices\n";
const char INFO_STR_GET_REPLICAS[] = "partition-generation\nreplicas-master\nreplicas-prole\n";

/**
 *	Request current status from server node.
 */
bool
as_node_refresh(as_cluster* cluster, as_node* node, as_vector* /* <as_friend> */ friends)
{
	int ret = as_node_get_info_connection(node);
	
	if (ret) {
		return false;
	}
	
	uint32_t info_timeout = cluster->conn_timeout_ms;
	uint8_t stack_buf[INFO_STACK_BUF_SIZE];
	uint8_t* buf = as_node_get_info(node, INFO_STR_CHECK, sizeof(INFO_STR_CHECK) - 1, info_timeout, stack_buf);
	
	if (! buf) {
		as_node_close_info_connection(node);
		return false;
	}
	
	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), 4);
	
	as_info_parse_multi_response((char*)buf, &values);
	
	bool update_partitions;
	bool status = as_node_process_response(cluster, node, &values, friends, &update_partitions);
		
	if (buf != stack_buf) {
		cf_free(buf);
	}
	
	if (status && update_partitions) {
		buf = as_node_get_info(node, INFO_STR_GET_REPLICAS, sizeof(INFO_STR_GET_REPLICAS) - 1, info_timeout, stack_buf);
		
		if (! buf) {
			as_node_close_info_connection(node);
			as_vector_destroy(&values);
			return false;
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
