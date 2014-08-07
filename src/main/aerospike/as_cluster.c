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

#include <aerospike/as_cluster.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_password.h>
#include <aerospike/as_lookup.h>
#include <aerospike/as_vector.h>
#include <citrusleaf/as_scan.h>
#include <citrusleaf/cl_info.h>
#include <citrusleaf/cl_batch.h>
#include <citrusleaf/cf_byte_order.h>
#include <citrusleaf/cl_query.h>
#include <citrusleaf/cf_socket.h>
#include <citrusleaf/cf_log_internal.h>

/******************************************************************************
 *	Function declarations
 *****************************************************************************/

bool
as_node_refresh(as_cluster* cluster, as_node* node, as_vector* /* <as_friend> */ friends);

/******************************************************************************
 *	Functions
 *****************************************************************************/

static inline void
set_nodes(as_cluster* cluster, as_nodes* nodes)
{
	ck_pr_fence_store();
	ck_pr_store_ptr(&cluster->nodes, nodes);
}

static bool
as_find_seed(as_cluster* cluster, const char* hostname, in_port_t port) {
	as_seed* seed = cluster->seeds;
	
	for (uint32_t i = 0; i < cluster->seeds_size; i++) {
		if (seed->port == port && strcmp(seed->name, hostname) == 0) {
			return true;
		}
		seed++;
	}
	return false;
}

static void
as_add_seeds(as_cluster* cluster, as_seed* seeds, uint32_t seeds_size) {
	uint32_t old_length = cluster->seeds_size;
	uint32_t new_length = old_length + seeds_size;
	cluster->seeds = cf_realloc(cluster->seeds, sizeof(as_seed) * new_length);
	
	as_seed* src = seeds;
	as_seed* trg = cluster->seeds + old_length;

	for (uint32_t i = 0; i < seeds_size; i++) {
		cf_debug("Add seed %s:%d", src->name, src->port);
		trg->name = cf_strdup(src->name);
		trg->port = src->port;
		src++;
		trg++;
	}
	cluster->seeds_size = new_length;
}

static int
as_lookup_node_name(as_cluster* cluster, struct sockaddr_in* addr, char* node_name, int node_name_size)
{
	char* values = 0;
	int status = citrusleaf_info_host_auth(cluster, addr, "node", &values, cluster->conn_timeout_ms, false, true);
	
	if (status) {
		free(values);
		return status;
	}
	
	char* value;
	status = citrusleaf_info_parse_single(values, &value);
	
	if (status == 0) {
		as_strncpy(node_name, value, node_name_size);
	}
	free(values);
	return status;
}

static as_node*
as_cluster_find_node_in_vector(as_vector* nodes, const char* name)
{
	as_node* node;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		node = as_vector_get_ptr(nodes, i);
		
		if (strcmp(node->name, name) == 0) {
			return node;
		}
	}
	return 0;
}

static as_node*
as_cluster_find_node(as_nodes* nodes, const char* name)
{
	as_node* node;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		node = nodes->array[i];
		
		if (strcmp(node->name, name) == 0) {
			return node;
		}
	}
	return 0;
}

static as_nodes*
as_nodes_create(uint32_t capacity)
{
	size_t size = sizeof(as_nodes) + (sizeof(as_node*) * capacity);
	as_nodes* nodes = cf_malloc(size);
	memset(nodes, 0, size);
	nodes->ref_count = 1;
	nodes->size = capacity;
	return nodes;
}

/**
 *	Use non-inline function for garbarge collector function pointer reference.
 *	Forward to inlined release.
 */
static void
release_nodes(as_nodes* nodes)
{
	as_nodes_release(nodes);
}

/**
 *	Add nodes using copy on write semantics.
 */
static void
as_cluster_add_nodes_copy(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_add)
{
	// Create temporary nodes array.
	as_nodes* nodes_old = cluster->nodes;
	as_nodes* nodes_new = as_nodes_create(nodes_old->size + nodes_to_add->size);

	// Add existing nodes.
	memcpy(nodes_new->array, nodes_old->array, sizeof(as_node*) * nodes_old->size);
		
	// Add new nodes.
	memcpy(&nodes_new->array[nodes_old->size], nodes_to_add->list, sizeof(as_node*) * nodes_to_add->size);
		
	// Replace nodes with copy.
	set_nodes(cluster, nodes_new);
	
	// Put old nodes on garbage collector stack.
	as_gc_item item;
	item.data = nodes_old;
	item.release_fn = (as_release_fn)release_nodes;
	as_vector_append(cluster->gc, &item);
}

static bool
as_cluster_seed_nodes(as_cluster* cluster, bool enable_warnings)
{
	// Add all nodes at once to avoid copying entire array multiple times.
	as_vector nodes_to_add;
	as_vector_inita(&nodes_to_add, sizeof(as_node*), 64);
	
	as_vector addresses;
	as_vector_inita(&addresses, sizeof(struct sockaddr_in), 5);
	
	char name[AS_NODE_NAME_MAX_SIZE];
	
	for (uint32_t i = 0; i < cluster->seeds_size; i++) {
		as_seed* seed = &cluster->seeds[i];
		as_vector_clear(&addresses);
		
		if (! as_lookup(cluster, seed->name, seed->port, enable_warnings, &addresses)) {
			continue;
		}

		for (uint32_t i = 0; i < addresses.size; i++) {
			struct sockaddr_in* addr = as_vector_get(&addresses, i);
			int status = as_lookup_node_name(cluster, addr, name, AS_NODE_NAME_MAX_SIZE);
			
			if (status == 0) {
				as_node* node = as_cluster_find_node_in_vector(&nodes_to_add, name);
				
				if (node) {
					as_node_add_address(node, addr);
				}
				else {
					node = as_node_create(cluster, name, addr);
					as_address* a = as_node_get_address_full(node);
					cf_info("Add node %s %s:%d", node->name, a->name, (int)cf_swap_from_be16(a->addr.sin_port));
					as_vector_append(&nodes_to_add, &node);
				}
			}
			else {
				if (enable_warnings) {
					char name[INET_ADDRSTRLEN];
					as_socket_address_name(addr, name);
					cf_warn("Connection failed for %s (%s:%d)", seed->name, name, (int)seed->port);
				}
			}
		}
	}
	
	bool status = false;
	
	if (nodes_to_add.size > 0) {
		as_cluster_add_nodes_copy(cluster, &nodes_to_add);
		status = true;
	}
	
	as_vector_destroy(&nodes_to_add);
	as_vector_destroy(&addresses);
	return status;
}

static void
as_cluster_find_nodes_to_add(as_cluster* cluster, as_vector* /* <as_friend> */ friends, as_vector* /* <as_node*> */ nodes_to_add)
{
	as_vector addresses;
	as_vector_inita(&addresses, sizeof(struct sockaddr_in), 5);
	
	char name[AS_NODE_NAME_MAX_SIZE];

	for (uint32_t i = 0; i < friends->size; i++) {
		as_friend* friend = as_vector_get(friends, i);
		as_vector_clear(&addresses);
		
		if (! as_lookup(cluster, friend->name, friend->port, true, &addresses)) {
			continue;
		}
		
		for (uint32_t i = 0; i < addresses.size; i++) {
			struct sockaddr_in* addr = as_vector_get(&addresses, i);
			int status = as_lookup_node_name(cluster, addr, name, AS_NODE_NAME_MAX_SIZE);
			
			if (status == 0) {
				as_node* node = as_cluster_find_node(cluster->nodes, name);
				
				if (node) {
					// Duplicate node name found.  This usually occurs when the server
					// services list contains both internal and external IP addresses
					// for the same node.  Add new host to list of alias filters
					// and do not add new node.
					as_address* a = as_node_get_address_full(node);
					cf_info("Duplicate node found %s %s:%d", node->name, a->name, (int)cf_swap_from_be16(a->addr.sin_port));
					node->friends++;
					as_node_add_address(node, addr);
					continue;
				}
				
				node = as_node_create(cluster, name, addr);
				as_address* a = as_node_get_address_full(node);
				cf_info("Add node %s %s:%d", name, a->name, (int)cf_swap_from_be16(a->addr.sin_port));
				as_vector_append(nodes_to_add, &node);
			}
		}
	}
	as_vector_destroy(&addresses);
}

static void
as_cluster_find_nodes_to_remove(as_cluster* cluster, uint32_t refresh_count, as_vector* /* <as_node*> */ nodes_to_remove)
{
	as_nodes* nodes = cluster->nodes;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		
		if (! node->active) {
			// Inactive nodes must be removed.
			as_vector_append(nodes_to_remove, &node);
			continue;
		}
		
		switch (nodes->size) {
			case 1:
				// Single node clusters rely on whether it responded to info requests.
				if (node->failures >= 5) {
					// 5 consecutive info requests failed. Try seeds.
					if (as_cluster_seed_nodes(cluster, false)) {
						// Seed nodes found. Remove unresponsive node.
						as_vector_append(nodes_to_remove, &node);
					}
				}
				break;
				
			case 2:
				// Two node clusters require at least one successful refresh before removing.
				if (refresh_count == 1 && node->friends == 0 && node->failures > 0) {
					// Node is not referenced nor did it respond.
					as_vector_append(nodes_to_remove, &node);
				}
				break;
				
			default:
				// Multi-node clusters require two successful node refreshes before removing.
				if (refresh_count >= 2 && node->friends == 0) {
					// Node is not referenced by other nodes.
					// Check if node responded to info request.
					if (node->failures == 0) {
						// Node is alive, but not referenced by other nodes.  Check if mapped.
						if (! as_partition_tables_find_node(cluster->partition_tables, node)) {
							// Node doesn't have any partitions mapped to it.
							// There is not point in keeping it in the cluster.
							as_vector_append(nodes_to_remove, &node);
						}
					}
					else {
						// Node not responding. Remove it.
						as_vector_append(nodes_to_remove, &node);
					}
				}
				break;
		}
	}
}

static bool
as_cluster_find_node_by_reference(as_vector* /* <as_node*> */ nodes_to_remove, as_node* filter)
{
	as_node* node;
	
	for (uint32_t i = 0; i < nodes_to_remove->size; i++) {
		node = as_vector_get_ptr(nodes_to_remove, i);
		
		// Duplicate nodes can exist because single node clusters may be reseeded.  Then, a seeded
		// node with the same name can exist with the unresponsive node.  Therefore, check pointer
		// equality only and not name.
		if (node == filter) {
			return true;
		}
	}
	return false;
}

/**
 *	Use non-inline function for garbarge collector function pointer reference.
 *	Forward to inlined release.
 */
static void
release_node(as_node* node)
{
	as_node_release(node);
}

/**
 * Remove nodes using copy on write semantics.
 */
static void
as_cluster_remove_nodes_copy(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_remove)
{
	// Create temporary nodes array.
	// Since nodes are only marked for deletion using node references in the nodes array,
	// and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
	// in nodes_to_remove exist.  Therefore, we know the final array size.
	as_nodes* nodes_old = cluster->nodes;
	as_nodes* nodes_new = as_nodes_create(nodes_old->size - nodes_to_remove->size);
	as_node* node;
	int count = 0;
		
	// Add nodes that are not in remove list.
	for (uint32_t i = 0; i < nodes_old->size; i++) {
		node = nodes_old->array[i];
		
		if (as_cluster_find_node_by_reference(nodes_to_remove, node)) {
			as_address* a = as_node_get_address_full(node);
			cf_info("Remove node %s %s:%d", node->name, a->name, (int)cf_swap_from_be16(a->addr.sin_port));
			as_gc_item item;
			item.data = node;
			item.release_fn = (as_release_fn)release_node;
			as_vector_append(cluster->gc, &item);
		}
		else {
			if (count < nodes_new->size) {
				nodes_new->array[count++] = node;
			}
			else {
				as_address* a = as_node_get_address_full(node);
				cf_error("Remove node error. Node count exceeded %d, %s %s:%d", count, node->name, a->name, (int)cf_swap_from_be16(a->addr.sin_port));
			}
		}
	}
		
	// Do sanity check to make sure assumptions are correct.
	if (count < nodes_new->size) {
		cf_warn("Node remove mismatch. Expected %d Received %d", nodes_new->size, count);
	}
	
	// Replace nodes with copy.
	set_nodes(cluster, nodes_new);

	// Put old nodes on garbage collector stack.
	as_gc_item item;
	item.data = nodes_old;
	item.release_fn = (as_release_fn)release_nodes;
	as_vector_append(cluster->gc, &item);
}

static void
as_cluster_remove_nodes(as_cluster* cluster, as_vector* /* <as_node*> */ nodes_to_remove)
{
	// There is no need to delete nodes from partition tables because the nodes
	// have already been set to inactive. Further connection requests will result
	// in an exception and a different node will be tried.
	
	// Set node to inactive.
	for (uint32_t i = 0; i < nodes_to_remove->size; i++) {
		as_node* node = as_vector_get_ptr(nodes_to_remove, i);
		as_node_deactivate(node);
	}
			
	// Remove all nodes at once to avoid copying entire array multiple times.
	as_cluster_remove_nodes_copy(cluster, nodes_to_remove);
}

static bool
as_cluster_set_partition_size(as_cluster* cluster)
{
	as_nodes* nodes = cluster->nodes;
	
	for (uint32_t i = 0; i < nodes->size && cluster->n_partitions == 0; i++) {
		as_node* node = nodes->array[i];
		struct sockaddr_in* addr = as_node_get_address(node);
		char* values = 0;
		
		if (citrusleaf_info_host_auth(cluster, addr, "partitions", &values, cluster->conn_timeout_ms, false, true)) {
			free(values);
			continue;
		}
		
		char *value = 0;
		bool status = (citrusleaf_info_parse_single(values, &value) == 0);
		
		if (status) {
			cluster->n_partitions = atoi(value);
		}
		
		free(values);
	}
	return cluster->n_partitions > 0;
}

/**
 *	Release data structures schuleduled for removal in previous cluster tend.
 */
static void
as_cluster_gc(as_vector* /* <as_gc_item> */ vector)
{
	for (uint32_t i = 0; i < vector->size; i++) {
		as_gc_item* item = as_vector_get(vector, i);
		item->release_fn(item->data);
	}
	as_vector_clear(vector);
}

/**
 * Check health of all nodes in the cluster.
 */
static bool
as_cluster_tend(as_cluster* cluster, bool enable_seed_warnings)
{
	// All node additions/deletions are performed in tend thread.
	// Garbage collect data structures released in previous tend.
	// This tend interval delay substantially reduces the chance of
	// deleting a ref counted data structure when other threads
	// are stuck between assigment and incrementing the ref count.
	as_cluster_gc(cluster->gc);
	
	// If active nodes don't exist, seed cluster.
	as_nodes* nodes = cluster->nodes;
	if (nodes->size == 0) {
		if (! as_cluster_seed_nodes(cluster, enable_seed_warnings)) {
			return false;
		}
	}

	// Retrieve fixed number of partitions only once from any node.
	if (cluster->n_partitions == 0) {
		if (! as_cluster_set_partition_size(cluster)) {
			cf_warn("Failed to set retrieve n_partitions from cluster nodes.");
			return false;
		}
	}
	
	// Clear tend iteration node statistics.
	nodes = cluster->nodes;
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		node->friends = 0;
	}
	
	// Refresh all known nodes.
	as_vector friends;
	as_vector_inita(&friends, sizeof(as_friend), 8);
	uint32_t refresh_count = 0;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		
		if (node->active) {
			if (as_node_refresh(cluster, node, &friends)) {
				node->failures = 0;
				refresh_count++;
			}
			else {
				node->failures++;
			}
		}
	}
	
	// Handle nodes changes determined from refreshes.
	as_vector nodes_to_add;
	as_vector_inita(&nodes_to_add, sizeof(as_node*), friends.size);
	
	as_vector nodes_to_remove;
	as_vector_inita(&nodes_to_remove, sizeof(as_node*), nodes->size);

	as_cluster_find_nodes_to_add(cluster, &friends, &nodes_to_add);
	as_cluster_find_nodes_to_remove(cluster, refresh_count, &nodes_to_remove);
	
	// Remove nodes in a batch.
	if (nodes_to_remove.size > 0) {
		as_cluster_remove_nodes(cluster, &nodes_to_remove);
	}
	
	// Add nodes in a batch.
	if (nodes_to_add.size > 0) {
		as_cluster_add_nodes_copy(cluster, &nodes_to_add);
	}
	
	as_vector_destroy(&nodes_to_add);
	as_vector_destroy(&nodes_to_remove);
	as_vector_destroy(&friends);
	return true;
}

/**
 * Tend the cluster until it has stabilized and return control.
 * This helps avoid initial database request timeout issues when
 * a large number of threads are initiated at client startup.
 *
 * If the cluster has not stabilized by the timeout, return
 * control as well.  Do not return an error since future
 * database requests may still succeed.
 */
static bool
as_wait_till_stabilized(as_cluster* cluster)
{
	uint64_t limit = cf_getms() + cluster->conn_timeout_ms;
	uint32_t count = -1;
	
	do {
		if (! as_cluster_tend(cluster, true)) {
			return false;
		}
		
		// Check to see if cluster has changed since the last tend.
		// If not, assume cluster has stabilized and return.
		as_nodes* nodes = cluster->nodes;
		if (count == nodes->size) {
			return true;
		}
		count = nodes->size;
		usleep(1);  // Sleep 1 microsecond before next cluster tend.
	} while (cf_getms() < limit);
	
	return true;
}

static void*
as_cluster_tender(void* data)
{
	as_cluster* cluster = (as_cluster*)data;
	uint32_t tend_interval_micro = cluster->tend_interval * 1000;
	
	while (cluster->valid) {
		as_cluster_tend(cluster, false);
		usleep(tend_interval_micro);
	}
	return NULL;
}

static bool
as_init_tend_thread(as_cluster* cluster, bool fail_if_not_connected)
{
	// Tend cluster until all nodes identified.
	if (! as_wait_till_stabilized(cluster)) {
		if (fail_if_not_connected) {
			return false;
		}
	}
	
	if (cf_debug_enabled()) {
		as_seed* seed = cluster->seeds;
		for (uint32_t i = 0; i < cluster->seeds_size; i++) {
			cf_debug("Add seed %s:%d", seed->name, seed->port);
			seed++;
		}
	}
	
	// Add other nodes as seeds, if they don't already exist.
	as_nodes* nodes = cluster->nodes;
	as_vector seeds_to_add;
	as_vector_inita(&seeds_to_add, sizeof(as_seed), nodes->size);
	
	as_node* node;
	as_vector* addresses;
	as_address* address;
	as_seed seed;
	in_port_t port;
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		node = nodes->array[i];
		addresses = &node->addresses;
		
		for (uint32_t j = 0; j < addresses->size; j++) {
			address = as_vector_get(addresses, j);
			port = cf_swap_from_be16(address->addr.sin_port);
			
			if (! as_find_seed(cluster, address->name, port)) {
				seed.name = address->name;
				seed.port = port;
				as_vector_append(&seeds_to_add, &seed);
			}
		}
	}
	
	if (seeds_to_add.size > 0) {
		as_add_seeds(cluster, (as_seed*)seeds_to_add.list, seeds_to_add.size);
	}
	as_vector_destroy(&seeds_to_add);
	
	// Run cluster tend thread.
	cluster->valid = true;
	pthread_create(&cluster->tend_thread, 0, as_cluster_tender, cluster);
	return true;
}

static uint32_t
seeds_size(as_config* config)
{
	uint32_t nhosts = sizeof(config->hosts) / sizeof(as_config_host);
	uint32_t i = 0;
	
	while (i < nhosts && config->hosts[i].addr != NULL) {
		i++;
	}
	return i;
}

static as_seed*
seeds_create(as_config* config, uint32_t size)
{
	as_config_host* host = config->hosts;
	as_seed* seeds = cf_malloc(sizeof(as_seed) * size);
	as_seed* seed = seeds;
	
	for (uint32_t i = 0; i < size; i++) {
		seed->name = cf_strdup(host->addr);
		seed->port = host->port;
		host++;
		seed++;
	}
	return seeds;
}

static as_addr_map*
ip_map_create(as_addr_map* source_map, uint32_t size)
{
	as_addr_map* target_map = cf_malloc(sizeof(as_addr_map) * size);
	as_addr_map* target = target_map;
	as_addr_map* source = source_map;

	for (uint32_t i = 0; i < size; i++) {
		target->orig = cf_strdup(source->orig);
		target->alt = cf_strdup(source->alt);
		source++;
		target++;
	}
	return target_map;
}

as_node*
as_node_get_random(as_cluster* cluster)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t size = nodes->size;
	
	for (uint32_t i = 0; i < size; i++) {
		// Must handle concurrency with other threads.
		uint32_t index = ck_pr_faa_32(&cluster->node_index, 1);
		as_node* node = nodes->array[index % size];
		uint8_t active = ck_pr_load_8(&node->active);
		
		if (active) {
			as_node_reserve(node);
			as_nodes_release(nodes);
			return node;
		}
	}
	as_nodes_release(nodes);
	return 0;
}

as_node*
as_node_get_by_name(as_cluster* cluster, const char* name)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node* node = nodes->array[i];
		
		if (strcmp(node->name, name) == 0) {
			as_node_reserve(node);
			as_nodes_release(nodes);
			return node;
		}
	}
	as_nodes_release(nodes);
	return(0);
}

void
as_cluster_get_node_names(as_cluster* cluster, int* n_nodes, char** node_names)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	uint32_t size = nodes->size;
	*n_nodes = size;
	
	if (size == 0) {
		*node_names = 0;
		as_nodes_release(nodes);
		return;
	}
	
	*node_names = malloc(NODE_NAME_SIZE * size);
	if (*node_names == 0) {
		as_nodes_release(nodes);
		return;
	}
	
	char* nptr = *node_names;
	for (uint32_t i = 0; i < size; i++) {
		as_node* node = nodes->array[i];
		memcpy(nptr, node->name, NODE_NAME_SIZE);
		nptr += NODE_NAME_SIZE;
	}
	as_nodes_release(nodes);
}

bool
as_cluster_is_connected(as_cluster* cluster)
{
	as_nodes* nodes = as_nodes_reserve(cluster);
	bool connected = nodes->size > 0 && cluster->valid;
	as_nodes_release(nodes);
	return connected;
}

void
as_cluster_change_password(as_cluster* cluster, const char* user, const char* password)
{
	if (user && *user) {
		if (cluster->user == 0 || strcmp(cluster->user, user) == 0) {
			cf_free(cluster->user);
			cf_free(cluster->password);
			cluster->user = cf_strdup(user);
			cluster->password = cf_strdup(password);
		}
	}
}

as_cluster*
as_cluster_create(as_config* config)
{
	as_cluster* cluster = cf_malloc(sizeof(as_cluster));
	memset(cluster, 0, sizeof(as_cluster));
	
	// Initialize user/password.
	if (*(config->user)) {
		cluster->user = cf_strdup(config->user);
	}
	
	if (*(config->password)) {
		cluster->password = cf_strdup(config->password);
	}
	
	// Initialize cluster tend and node parameters
	cluster->tend_interval = (config->tender_interval < 1000)? 1000 : config->tender_interval;
	cluster->conn_queue_size = config->max_threads + 1;  // Add one connection for tend thread.
	cluster->conn_timeout_ms = (config->conn_timeout_ms == 0) ? 1000 : config->conn_timeout_ms;
	
	// Initialize seed hosts.
	cluster->seeds_size = seeds_size(config);
	cluster->seeds = seeds_create(config, cluster->seeds_size);

	// Initialize IP map translation if provided.
	if (config->ip_map && config->ip_map_size > 0) {
		cluster->ip_map_size = config->ip_map_size;
		cluster->ip_map = ip_map_create(config->ip_map, config->ip_map_size);
	}

	// Initialize empty nodes.
	cluster->nodes = as_nodes_create(0);
	
	// Initialize empty partition tables.
	cluster->partition_tables = as_partition_tables_create(0);
	
	// Initialize garbage collection array.
	cluster->gc = as_vector_create(sizeof(as_gc_item), 8);
	
	// Initialize batch.
	pthread_mutex_init(&cluster->batch_init_lock, 0);
		
	// Run cluster tend thread.
	if (! as_init_tend_thread(cluster, config->fail_if_not_connected)) {
		as_cluster_destroy(cluster);
		return 0;
	}
	return cluster;
}

void
as_cluster_destroy(as_cluster* cluster)
{
	// Shutdown work queues.
	cl_cluster_batch_shutdown(cluster);
	cl_cluster_scan_shutdown(cluster);
	cl_cluster_query_shutdown(cluster);

	// Stop tend thread and wait till finished.
	if (cluster->valid) {
		cluster->valid = false;
		pthread_join(cluster->tend_thread, NULL);
	}
	
	// Release everything in garbage collector.
	as_cluster_gc(cluster->gc);
	as_vector_destroy(cluster->gc);
		
	// Release paritition tables.
	as_partition_tables* tables = cluster->partition_tables;
	for (uint32_t i = 0; i < tables->size; i++) {
		as_partition_table_destroy(tables->array[i]);
	}
	as_partition_tables_release(tables);
	
	// Release nodes.
	as_nodes* nodes = cluster->nodes;
	for (uint32_t i = 0; i < nodes->size; i++) {
		as_node_release(nodes->array[i]);
	}
	as_nodes_release(nodes);
	
	// Destroy IP map.
	if (cluster->ip_map) {
		as_addr_map* entry = cluster->ip_map;
		for (uint32_t i = 0; i < cluster->ip_map_size; i++) {
			cf_free(entry->orig);
			cf_free(entry->alt);
			entry++;
		}
		cf_free(cluster->ip_map);
	}

	// Destroy seeds.
	as_seed* seed = cluster->seeds;
	for (uint32_t i = 0; i < cluster->seeds_size; i++) {
		cf_free(seed->name);
		seed++;
	}
	cf_free(cluster->seeds);
	
	// Destroy batch lock.
	pthread_mutex_destroy(&cluster->batch_init_lock);
	
	cf_free(cluster->user);
	cf_free(cluster->password);
	
	// Destroy cluster.
	cf_free(cluster);
}
