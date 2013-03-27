/*
 * Copyright 2012 Aerospike. All rights reserved.
 */
#include <stdio.h>
#include <string.h>

#include "citrusleaf/citrusleaf-internal.h"
#include "citrusleaf/cl_request.h"
#include "citrusleaf/cl_shm.h"
#include "citrusleaf/cf_log_internal.h"

#define INFO_TIMEOUT_MS 300

static char*
get_name_value(char* p, char** name, char** value)
{
	if (*p == 0) {
		return 0;
	}

	*name = p;
	*value = 0;

	while ((*p) && (*p != '\n')) {
		if (*p == '\t') {
			*p = 0;
			*value = p + 1;
		}
		p++;
	}

	if (*value == 0) {
		*value = p;
	}

	if (*p == '\n') {
		*p = 0;
		p++;
	}
	return p;
}

// Fast strncpy implementation.
// Return true if truncation occurred.
bool
cl_strncpy(char* trg, const char* src, int len)
{
	int max = len - 1;
	int i = 0;

	while (*src) {
		if (i >= max) {
			*trg = 0;
			return true;
		}
		*trg++ = *src++;
		i++;
	}
	*trg = 0;
	return false;
}

int
cl_get_node_info(const char* node_name, struct sockaddr_in* sa_in, cl_node_info* node_info)
{
	if (g_shared_memory) {
		shm_ninfo* shared = cl_shm_find_node_from_name(node_name);

		if (shared) {
			// cf_debug("Use shared memory for node info.");
			cl_shm_node_lock(shared);
			cl_strncpy(node_info->node_name, shared->node_name, NODE_NAME_SIZE);
			node_info->partition_generation = shared->partition_generation;
			node_info->values = strdup(shared->services);
			node_info->services = node_info->values;
			node_info->dun = shared->dun;
			cl_shm_node_unlock(shared);
			return 0;
		}
	}
	// cf_debug("Issue request for node info.");
	return cl_request_node_info(sa_in, node_info);
}

int
cl_request_node_info(struct sockaddr_in* sa_in, cl_node_info* node_info)
{
	node_info->node_name[0] = 0;
	node_info->partition_generation = 0;
	node_info->services = 0;
	node_info->dun = false;

	if (citrusleaf_info_host_limit(sa_in, "node\npartition-generation\nservices", &node_info->values, INFO_TIMEOUT_MS, false, 10000) != 0) {
		node_info->dun = true;
		return -1;
	}

	char* p = node_info->values;
	char* name;
	char* value;

	while ((p = get_name_value(p, &name, &value)) != 0) {
		if (strcmp(name, "node") == 0) {
			cl_strncpy(node_info->node_name, value, NODE_NAME_SIZE);
		}
		else if (strcmp(name, "partition-generation") == 0) {
			node_info->partition_generation = atoi(value);
		}
		else if (strcmp(name, "services") == 0) {
			node_info->services = value;
		}
		else {
			cf_warn("Invalid info name %s", name);
		}
	}
	return 0;
}

void
cl_node_info_free(cl_node_info* node_info)
{
	if (node_info->values) {
		free(node_info->values);
	}
}

int
cl_get_replicas(const char* node_name, struct sockaddr_in* sa_in, cl_replicas* replicas)
{
	if (g_shared_memory) {
		shm_ninfo* shared = cl_shm_find_node_from_name(node_name);

		if (shared) {
			// cf_debug("Use shared memory for replicas.");
			cl_shm_node_lock(shared);

			int wr_len = strlen(shared->write_replicas);
			int rr_len = strlen(shared->read_replicas);
			replicas->values = (char*)malloc(wr_len + rr_len + 2);

			replicas->write_replicas = replicas->values;
			memcpy(replicas->write_replicas, shared->write_replicas, wr_len);
			replicas->write_replicas[wr_len] = 0;

			replicas->read_replicas = replicas->values + wr_len + 1;
			memcpy(replicas->read_replicas, shared->read_replicas, rr_len);
			replicas->read_replicas[rr_len] = 0;

			cl_shm_node_unlock(shared);
			return 0;
		}
	}
	// cf_debug("Request replicas from server.");
	return cl_request_replicas(sa_in, replicas);
}

int
cl_request_replicas(struct sockaddr_in* sa_in, cl_replicas* replicas)
{
	replicas->write_replicas = 0;
	replicas->read_replicas = 0;

	if (citrusleaf_info_host_limit(sa_in, "replicas-read\nreplicas-write", &replicas->values, INFO_TIMEOUT_MS, false, 2000000) != 0) {
		return -1;
	}

	char* p = replicas->values;
	char* name;
	char* value;

	while ((p = get_name_value(p, &name, &value)) != 0) {
		if (strcmp(name, "replicas-write") == 0) {
			replicas->write_replicas = value;
		}
		else if (strcmp(name, "replicas-read") == 0) {
			replicas->read_replicas = value;
		}
		else {
			cf_warn("Invalid replicas name %s", name);
		}
	}
	return 0;
}

void
cl_replicas_free(cl_replicas* replicas)
{
	if (replicas->values) {
		free(replicas->values);
	}
}

int
cl_get_node_name(struct sockaddr_in* sa_in, char* node_name)
{
	if (g_shared_memory) {
		shm_ninfo* shared = cl_shm_find_node_from_address(sa_in);

		if (shared) {
			// cf_debug("Use shared memory for node name.");
			cl_shm_node_lock(shared);
			cl_strncpy(node_name, shared->node_name, NODE_NAME_SIZE);
			cl_shm_node_unlock(shared);
			return 0;
		}
	}
	return cl_request_node_name(sa_in, node_name);
}

int
cl_request_node_name(struct sockaddr_in* sa_in, char* node_name)
{
	*node_name = 0;
	char* values;

	if (citrusleaf_info_host(sa_in, "node", &values, INFO_TIMEOUT_MS, false) != 0){
		return -1;
	}

	char* p = values;
	char* name;
	char* value;

	while ((p = get_name_value(p, &name, &value)) != 0) {
		if (strcmp(name, "node") == 0) {
			cl_strncpy(node_name, value, NODE_NAME_SIZE);
		}
		else {
			cf_warn("Invalid node name %s", name);
		}
	}
	free(values);
	return 0;
}

int
cl_get_n_partitions(struct sockaddr_in* sa_in, int* n_partitions)
{
	if (g_shared_memory) {
		int count = cl_shm_get_partition_count();

		if (count > 0) {
			*n_partitions = count;
			return 0;
		}
	}
	return cl_request_n_partitions(sa_in, n_partitions);
}

int
cl_request_n_partitions(struct sockaddr_in* sa_in, int* n_partitions)
{
	*n_partitions = 0;
	char* values;

	if (citrusleaf_info_host(sa_in, "partitions", &values, INFO_TIMEOUT_MS, false) != 0) {
		return -1;
	}

	char* p = values;
	char* name;
	char* value;

	while ((p = get_name_value(p, &name, &value)) != 0) {
		if (strcmp(name, "partitions") == 0) {
			*n_partitions = atoi(value);
		}
		else {
			cf_warn("Invalid partitions %s", name);
		}
	}
	free(values);
	return 0;
}
