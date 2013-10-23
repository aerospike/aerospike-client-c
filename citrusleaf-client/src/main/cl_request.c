/******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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

#include <stdio.h>
#include <string.h>

#include <citrusleaf/cf_log_internal.h>

#include <citrusleaf/citrusleaf-internal.h>
#include <citrusleaf/cl_request.h>
#include <citrusleaf/cl_shm.h>


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
cl_request_node_info(struct sockaddr_in* sa_in, cl_node_info* node_info, int timeout_ms)
{
	node_info->node_name[0] = 0;
	node_info->partition_generation = 0;
	node_info->services = 0;

	if (citrusleaf_info_host_limit(sa_in, "node\npartition-generation\nservices\n",
			&node_info->values, timeout_ms, false, 10000) != 0) {
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
cl_request_replicas(struct sockaddr_in* sa_in, cl_replicas* replicas, int timeout_ms)
{
	return citrusleaf_info_host_limit(sa_in, "partition-generation\nreplicas-master\nreplicas-prole\n",
			&replicas->values, timeout_ms, false, 2000000);
}

void
cl_replicas_free(cl_replicas* replicas)
{
	if (replicas->values) {
		free(replicas->values);
	}
}

int
cl_get_node_name(struct sockaddr_in* sa_in, char* node_name, int timeout_ms)
{
	if (g_shared_memory) {
		cl_shm_ninfo* shared = cl_shm_find_node_from_address(sa_in);

		if (shared) {
			// cf_debug("Use shared memory for node name.");
			cl_shm_node_lock(shared);
			cl_strncpy(node_name, shared->node_name, NODE_NAME_SIZE);
			cl_shm_node_unlock(shared);
			return 0;
		}
	}
	return cl_request_node_name(sa_in, node_name, timeout_ms);
}

int
cl_request_node_name(struct sockaddr_in* sa_in, char* node_name, int timeout_ms)
{
	*node_name = 0;
	char* values;

	if (citrusleaf_info_host(sa_in, "node", &values, timeout_ms, false) != 0){
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
cl_get_n_partitions(struct sockaddr_in* sa_in, int* n_partitions, int timeout_ms)
{
	if (g_shared_memory) {
		int count = cl_shm_get_partition_count();

		if (count > 0) {
			*n_partitions = count;
			return 0;
		}
	}
	return cl_request_n_partitions(sa_in, n_partitions, timeout_ms);
}

int
cl_request_n_partitions(struct sockaddr_in* sa_in, int* n_partitions, int timeout_ms)
{
	*n_partitions = 0;
	char* values;

	if (citrusleaf_info_host(sa_in, "partitions", &values, timeout_ms, false) != 0) {
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
